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
    write_human_report_to(&mut stdout, report)
}

fn write_human_report_to(output: &mut impl Write, report: &Report) -> anyhow::Result<()> {
    writeln!(output, "Scherzo Cloud runner doctor")?;
    writeln!(output)?;

    for result in &report.results {
        let marker = match result.outcome.status {
            Status::Pass => '✓',
            Status::Fail => '✗',
        };
        writeln!(output, "{marker} {}", result.descriptor.title)?;
        writeln!(output, "  {}", result.outcome.message)?;
        if result.outcome.status == Status::Fail {
            writeln!(output, "  code: {}", result.outcome.code)?;
        }
        write_harness_compatibility_details(output, &result.outcome.details)?;
        writeln!(output)?;
    }

    let summary = report.summary();
    writeln!(output, "── summary ──")?;
    writeln!(output, "passed: {}", summary.passed)?;
    writeln!(output, "failed: {}", summary.failed)?;
    Ok(())
}

fn write_harness_compatibility_details(
    output: &mut impl Write,
    details: &std::collections::BTreeMap<String, String>,
) -> io::Result<()> {
    if !details.contains_key("profile") {
        return Ok(());
    }

    for (key, label) in [
        ("profile", "profile"),
        ("version", "observed version"),
        ("supportedRange", "supported range"),
        ("expectedVersion", "required version"),
        ("qualificationVersion", "qualification version"),
    ] {
        if let Some(value) = details.get(key) {
            writeln!(output, "  {label}: {value}")?;
        }
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::write_human_report_to;
    use crate::runner::doctor::{CheckDescriptor, CheckResult, Outcome, Report, Status};

    fn result(
        id: &'static str,
        title: &'static str,
        details: BTreeMap<String, String>,
    ) -> CheckResult {
        CheckResult {
            descriptor: CheckDescriptor {
                id,
                title,
                default: false,
            },
            outcome: Outcome {
                status: Status::Pass,
                code: "ok",
                message: "Fixture result".to_owned(),
                details,
            },
        }
    }

    #[test]
    fn human_harness_details_render_structured_compatibility_policy() {
        let report = Report {
            results: vec![
                result(
                    "execution.harness.range-fixture",
                    "Range fixture",
                    BTreeMap::from([
                        ("profile".to_owned(), "RangeProfile".to_owned()),
                        ("version".to_owned(), "2.3.5".to_owned()),
                        ("supportedRange".to_owned(), ">=2.3.4 <2.4.0".to_owned()),
                        ("qualificationVersion".to_owned(), "2.3.4".to_owned()),
                    ]),
                ),
                result(
                    "execution.harness.exact-fixture",
                    "Exact fixture",
                    BTreeMap::from([
                        ("profile".to_owned(), "ExactProfile".to_owned()),
                        ("expectedVersion".to_owned(), "7.8.9".to_owned()),
                    ]),
                ),
                result(
                    "environment.command.fixture",
                    "Environment fixture",
                    BTreeMap::from([("version".to_owned(), "1.2.3".to_owned())]),
                ),
            ],
        };
        let mut output = Vec::new();

        write_human_report_to(&mut output, &report).unwrap();

        let output = String::from_utf8(output).unwrap();
        for field in [
            "profile: RangeProfile",
            "observed version: 2.3.5",
            "supported range: >=2.3.4 <2.4.0",
            "qualification version: 2.3.4",
            "profile: ExactProfile",
            "required version: 7.8.9",
        ] {
            assert!(output.lines().any(|line| line.trim() == field), "{field}");
        }
        assert!(!output.contains("observed version: 1.2.3"));
    }
}
