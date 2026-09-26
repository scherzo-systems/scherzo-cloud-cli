use std::io::{self, Write};

use anyhow::Context;
use scherzo_cloud_api::{
    Publication, PublicationState, Run, RunArtifactDelivery, RunCancellationReceipt,
    RunPublicationHandoffState,
};
use serde::Serialize;

use crate::exit_code::ExitCode;

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct CloudSnapshot {
    pub run_id: Option<String>,
    pub run: Option<Box<Run>>,
    pub publication: Option<Box<Publication>>,
    pub cancellation_request: Option<Box<RunCancellationReceipt>>,
    pub replayed: Option<bool>,
}

impl CloudSnapshot {
    pub(super) fn for_run(run_id: impl Into<String>) -> Self {
        Self {
            run_id: Some(run_id.into()),
            ..Self::default()
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloudResult<'a> {
    schema_version: u8,
    operation: &'static str,
    deployment: &'a str,
    organization_ref: &'a str,
    run_id: Option<&'a str>,
    outcome: &'static str,
    run: Option<&'a Run>,
    publication: Option<&'a Publication>,
    cancellation_request: Option<&'a RunCancellationReceipt>,
    replayed: Option<bool>,
    error: Option<CloudError<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloudError<'a> {
    code: &'static str,
    idempotency_key: Option<&'a str>,
    requested_mode: Option<&'a str>,
    request_id: Option<&'a str>,
}

pub(super) struct CloudOutput<'a> {
    pub operation: &'static str,
    pub deployment: &'a str,
    pub organization: &'a str,
    pub snapshot: &'a CloudSnapshot,
    pub json: bool,
}

pub(super) fn write_cloud(
    output: CloudOutput<'_>,
    outcome: &'static str,
    error_code: Option<&'static str>,
    key: Option<&str>,
    mode: Option<&str>,
    exit: ExitCode,
) -> anyhow::Result<ExitCode> {
    let CloudOutput {
        operation,
        deployment,
        organization,
        snapshot,
        json,
    } = output;
    if json {
        let result = CloudResult {
            schema_version: 1,
            operation,
            deployment,
            organization_ref: organization,
            run_id: snapshot.run_id.as_deref(),
            outcome,
            run: snapshot.run.as_deref(),
            publication: snapshot.publication.as_deref(),
            cancellation_request: snapshot.cancellation_request.as_deref(),
            replayed: snapshot.replayed,
            error: error_code.map(|code| CloudError {
                code,
                idempotency_key: key,
                requested_mode: mode,
                request_id: snapshot
                    .cancellation_request
                    .as_deref()
                    .map(|request| request.id.as_str()),
            }),
        };
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        serde_json::to_writer(&mut stdout, &result).context("serialize Cloud run result")?;
        writeln!(stdout).context("write Cloud run result")?;
    } else if let Some(code) = error_code {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "error: Cloud run {operation}: {code}\n\nrun: {}\norganization: {organization}\nidempotency key: {}",
            snapshot.run_id.as_deref().unwrap_or("not established"),
            key.unwrap_or("none"),
        )?;
        if operation == "cancel" {
            write_cancel_context(&mut stderr, snapshot, mode)?;
        } else {
            if let Some(run) = snapshot.run.as_deref() {
                writeln!(
                    stderr,
                    "last observed run state: {}",
                    super::enum_text(&run.state)?
                )?;
                if let Some(handoff) = run.publication.as_deref() {
                    writeln!(
                        stderr,
                        "last observed handoff: {} · publication: {}",
                        super::enum_text(&handoff.state)?,
                        handoff.publication_id.as_deref().unwrap_or("none")
                    )?;
                }
            }
            if let Some(publication) = snapshot.publication.as_deref() {
                writeln!(
                    stderr,
                    "last observed publication: {} · state: {}",
                    publication.id,
                    super::enum_text(&publication.state)?
                )?;
            }
        }
        writeln!(
            stderr,
            "\n{}",
            if key.is_some() {
                "Inspect the run and reconcile this key with the same requested mode before retrying if acceptance is uncertain."
            } else {
                "Inspect the run and resolve the error before retrying."
            },
        )?;
    } else {
        if let Some(run) = snapshot.run.as_deref() {
            let publication_failed = run.publication.as_deref().is_some_and(|handoff| {
                handoff.state == scherzo_cloud_api::RunPublicationHandoffState::Failed
            }) || snapshot.publication.as_deref().is_some_and(
                |publication| publication.state == scherzo_cloud_api::PublicationState::Failed,
            );
            super::write_run_human(
                deployment,
                if publication_failed {
                    "✗ Automatic publication failed."
                } else {
                    "✓ Run observed."
                },
                run,
            )?;
        } else {
            writeln!(
                io::stdout().lock(),
                "✓ Run {outcome}.\n\nrun: {}\norganization: {organization}\ndeployment: {deployment}",
                snapshot.run_id.as_deref().unwrap_or("not established")
            )?;
        }
        if let Some(publication) = snapshot.publication.as_deref() {
            writeln!(io::stdout().lock(), "\nautomatic publication:")?;
            writeln!(io::stdout().lock(), "  publication: {}", publication.id)?;
            writeln!(
                io::stdout().lock(),
                "  export: {}",
                scherzo_cloud_execution::visible_text(&publication.export_name)
            )?;
            writeln!(
                io::stdout().lock(),
                "  state: {}",
                super::enum_text(&publication.state)?
            )?;
            if let Some(branch) = publication.branch.as_deref() {
                writeln!(
                    io::stdout().lock(),
                    "  branch: {}",
                    super::enum_text(&branch.disposition)?
                )?;
            }
            if let Some(pull_request) = publication.pull_request.as_deref() {
                writeln!(
                    io::stdout().lock(),
                    "  pull request: {} ({}, {})",
                    pull_request.number,
                    super::enum_text(&pull_request.disposition)?,
                    super::enum_text(&pull_request.state)?
                )?;
                let url = super::super::publication::redacted_human_url(&pull_request.url)?;
                writeln!(
                    io::stdout().lock(),
                    "  pull request url: {}",
                    url.escape_default()
                )?;
            }
            if let Some(outcome) = publication.outcome {
                writeln!(
                    io::stdout().lock(),
                    "  outcome: {}",
                    super::enum_text(&outcome)?
                )?;
            }
            if let Some(failure) = publication.failure.as_deref() {
                writeln!(
                    io::stdout().lock(),
                    "  failure: {}",
                    super::enum_text(&failure.code)?
                )?;
                writeln!(
                    io::stdout().lock(),
                    "  phase: {}",
                    super::enum_text(&failure.phase)?
                )?;
                writeln!(io::stdout().lock(), "  retryable: {}", failure.retryable)?;
            }
        }
        if snapshot
            .publication
            .as_deref()
            .is_some_and(|publication| publication.state == PublicationState::Failed)
        {
            writeln!(
                io::stdout().lock(),
                "\nInspect the failed attempt with scherzo-cloud publication show. After resolving the failure, review provider effects before creating another publication for this Run and export."
            )?;
        } else if snapshot
            .run
            .as_deref()
            .and_then(|run| run.publication.as_deref())
            .is_some_and(|handoff| handoff.state == RunPublicationHandoffState::Failed)
        {
            writeln!(
                io::stdout().lock(),
                "\nInspect the failed automatic publication handoff. After resolving the failure, check for an existing attempt before using scherzo-cloud publication create for this Run and export."
            )?;
        }
        if let Some(replayed) = snapshot.replayed {
            writeln!(
                io::stdout().lock(),
                "replayed: {}",
                if replayed { "yes" } else { "no" }
            )?;
        }
        if let Some(receipt) = snapshot.cancellation_request.as_deref() {
            let mode = if receipt.mode == scherzo_cloud_api::RunCancellationReceiptMode::Force {
                "force"
            } else {
                "graceful"
            };
            write_cancel_receipt(&mut io::stdout().lock(), snapshot, receipt, mode)?;
        }
    }
    Ok(exit)
}

fn write_cancel_receipt(
    out: &mut impl Write,
    snapshot: &CloudSnapshot,
    receipt: &RunCancellationReceipt,
    mode: &str,
) -> anyhow::Result<()> {
    writeln!(out, "request: {}", receipt.id)?;
    writeln!(out, "requested mode: {mode}")?;
    writeln!(out, "request state: {}", super::enum_text(&receipt.state)?)?;
    writeln!(
        out,
        "effective mode: {}",
        snapshot
            .run
            .as_deref()
            .and_then(|run| run.cancellation.as_deref())
            .map(|cancellation| super::enum_text(&cancellation.mode))
            .transpose()?
            .as_deref()
            .unwrap_or("not applied")
    )?;
    Ok(())
}

fn write_cancel_context(
    out: &mut impl Write,
    snapshot: &CloudSnapshot,
    mode: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(receipt) = snapshot.cancellation_request.as_deref() {
        let requested_mode = mode.unwrap_or_else(|| {
            if receipt.mode == scherzo_cloud_api::RunCancellationReceiptMode::Force {
                "force"
            } else {
                "graceful"
            }
        });
        write_cancel_receipt(out, snapshot, receipt, requested_mode)?;
    } else {
        if let Some(mode) = mode {
            writeln!(out, "requested mode: {mode}")?;
        }
        writeln!(out, "request: not observed")?;
    }
    if let Some(run) = snapshot.run.as_deref() {
        writeln!(out, "run state: {}", super::enum_text(&run.state)?)?;
        if let Some(interruption) = run.interruption.as_deref() {
            writeln!(
                out,
                "interruption phase: {}",
                super::enum_text(&interruption.phase)?
            )?;
            writeln!(
                out,
                "interruption cause: {}",
                super::enum_text(&interruption.cause)?
            )?;
            if let Some(fault) = interruption.executor_fault.as_ref() {
                writeln!(out, "executor fault: {}", super::enum_text(fault)?)?;
            }
            writeln!(
                out,
                "stop confirmed: {}",
                if interruption.stop_confirmed {
                    "yes"
                } else {
                    "no"
                }
            )?;
        }
        if let Some(delivery) = run.artifact_delivery.as_deref() {
            match delivery {
                RunArtifactDelivery::RunArtifactDeliverySucceeded(delivery) => {
                    writeln!(
                        out,
                        "artifact delivery: succeeded ({})",
                        delivery.artifact_set_id
                    )?;
                }
                RunArtifactDelivery::RunArtifactDeliveryRegistrationFailed(delivery) => {
                    write_delivery_failure(out, &delivery.phase, &delivery.code)?;
                }
                RunArtifactDelivery::RunArtifactDeliveryUploadFailed(delivery) => {
                    write_delivery_failure(out, &delivery.phase, &delivery.code)?;
                }
                RunArtifactDelivery::RunArtifactDeliveryPreparationFailed(delivery) => {
                    write_delivery_failure(out, &delivery.phase, &delivery.code)?;
                }
            }
        }
    } else {
        writeln!(out, "run state: not observed")?;
    }
    Ok(())
}

fn write_delivery_failure(
    out: &mut impl Write,
    phase: &impl Serialize,
    code: &impl Serialize,
) -> anyhow::Result<()> {
    writeln!(
        out,
        "artifact delivery: failed ({}: {})",
        super::enum_text(phase)?,
        super::enum_text(code)?
    )?;
    Ok(())
}
