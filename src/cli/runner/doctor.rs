use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;

use crate::exit_code::ExitCode;
use crate::runner::doctor::{CheckResult, Report, Status, built_in_registry};

pub(super) const ABOUT: &str = "Check local runner prerequisites";
const COMMAND_NAME: &str = "scherzo-cloud runner doctor";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long = "check", value_name = "ID", help = "Run only the named check")]
    checks: Vec<String>,

    #[arg(
        long,
        conflicts_with_all = ["checks", "json"],
        help = "List registered checks without running them"
    )]
    list_checks: bool,

    #[arg(long, help = "Print the report as JSON")]
    json: bool,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let registry = built_in_registry().map_err(|error| anyhow!(error))?;

        if self.list_checks {
            write_check_list(&registry.descriptors())?;
            return Ok(ExitCode::Success);
        }

        let report = registry.run(&self.checks).map_err(|error| {
            super::super::CommandFailure::with_exit_code(anyhow!(error), ExitCode::UsageError)
        })?;
        if self.json {
            write_json_report(&report)?;
        } else {
            write_human_report(&report).context("write runner doctor report")?;
        }
        Ok(if report.has_failures() {
            ExitCode::GeneralFailure
        } else {
            ExitCode::Success
        })
    }
}

fn write_check_list(descriptors: &[crate::runner::doctor::CheckDescriptor]) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    for descriptor in descriptors {
        writeln!(stdout, "{}", descriptor.id).context("write runner doctor check list")?;
    }
    Ok(())
}

fn write_human_report(report: &Report) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "Scherzo Cloud runner doctor")?;
    writeln!(stdout)?;

    for result in &report.results {
        let marker = match result.outcome.status {
            Status::Pass => '✓',
            Status::Fail => '✗',
        };
        writeln!(stdout, "{marker} {}", result.descriptor.title)?;
        writeln!(stdout, "  {}", result.outcome.message)?;
        if result.outcome.status == Status::Fail {
            writeln!(stdout, "  code: {}", result.outcome.code)?;
        }
        writeln!(stdout)?;
    }

    let summary = report.summary();
    writeln!(stdout, "── summary ──")?;
    writeln!(stdout, "passed: {}", summary.passed)?;
    writeln!(stdout, "failed: {}", summary.failed)?;
    Ok(())
}

fn write_json_report(report: &Report) -> anyhow::Result<()> {
    let output = JsonReport::from_report(report);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &output)
        .context("write JSON runner doctor report")?;
    writeln!(stdout).context("write runner doctor report")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonReport<'a> {
    schema_version: u8,
    command: &'static str,
    checks: Vec<JsonCheck<'a>>,
    summary: JsonSummary,
}

impl<'a> JsonReport<'a> {
    fn from_report(report: &'a Report) -> Self {
        let summary = report.summary();
        Self {
            schema_version: 1,
            command: COMMAND_NAME,
            checks: report.results.iter().map(JsonCheck::from_result).collect(),
            summary: JsonSummary {
                passed: summary.passed,
                failed: summary.failed,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonCheck<'a> {
    id: &'static str,
    title: &'static str,
    status: &'static str,
    code: &'static str,
    message: &'a str,
    details: &'a std::collections::BTreeMap<String, String>,
}

impl<'a> JsonCheck<'a> {
    fn from_result(result: &'a CheckResult) -> Self {
        Self {
            id: result.descriptor.id,
            title: result.descriptor.title,
            status: result.outcome.status.as_str(),
            code: result.outcome.code,
            message: &result.outcome.message,
            details: &result.outcome.details,
        }
    }
}

#[derive(Serialize)]
struct JsonSummary {
    passed: usize,
    failed: usize,
}
