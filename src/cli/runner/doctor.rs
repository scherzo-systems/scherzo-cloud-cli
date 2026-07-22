use std::fmt;
use std::io::{self, Write};
use std::process::ExitCode;

use clap::Args;
use serde::Serialize;

use crate::runner::doctor::{CheckResult, Report, Status, built_in_registry};

pub const ABOUT: &str = "Inspect local runner prerequisites";
const COMMAND_NAME: &str = "scherzo-cloud runner doctor";
const USAGE_ERROR: u8 = 2;

#[derive(Debug, Args)]
pub struct Command {
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
    pub fn execute(self) -> ExitCode {
        let registry = match built_in_registry() {
            Ok(registry) => registry,
            Err(error) => return report_error(error),
        };

        if self.list_checks {
            return match write_check_list(&registry.descriptors()) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => report_error(error),
            };
        }

        let report = match registry.run(&self.checks) {
            Ok(report) => report,
            Err(error) => {
                eprintln!("Error: {error}");
                return ExitCode::from(USAGE_ERROR);
            }
        };
        let output = if self.json {
            write_json_report(&report)
        } else {
            write_human_report(&report)
        };
        if let Err(error) = output {
            return report_error(error);
        }
        if report.has_failures() {
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        }
    }
}

fn report_error(error: impl fmt::Display) -> ExitCode {
    eprintln!("Error: {error}");
    ExitCode::FAILURE
}

fn write_check_list(
    descriptors: &[crate::runner::doctor::CheckDescriptor],
) -> Result<(), DoctorOutputError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    for descriptor in descriptors {
        writeln!(stdout, "{}", descriptor.id).map_err(DoctorOutputError::WriteOutput)?;
    }
    Ok(())
}

fn write_human_report(report: &Report) -> Result<(), DoctorOutputError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "Scherzo Cloud runner doctor").map_err(DoctorOutputError::WriteOutput)?;
    writeln!(stdout).map_err(DoctorOutputError::WriteOutput)?;

    for result in &report.results {
        let marker = match result.outcome.status {
            Status::Pass => '✓',
            Status::Fail => '✗',
        };
        writeln!(stdout, "{marker} {}", result.descriptor.title)
            .map_err(DoctorOutputError::WriteOutput)?;
        writeln!(stdout, "  {}", result.outcome.message).map_err(DoctorOutputError::WriteOutput)?;
        if result.outcome.status == Status::Fail {
            writeln!(stdout, "  Code: {}", result.outcome.code)
                .map_err(DoctorOutputError::WriteOutput)?;
        }
        writeln!(stdout).map_err(DoctorOutputError::WriteOutput)?;
    }

    let summary = report.summary();
    writeln!(
        stdout,
        "Summary: {} passed, {} failed",
        summary.passed, summary.failed
    )
    .map_err(DoctorOutputError::WriteOutput)?;
    let conclusion = if report.has_failures() {
        "Selected checks failed."
    } else {
        "Selected checks passed."
    };
    writeln!(stdout, "{conclusion}").map_err(DoctorOutputError::WriteOutput)
}

fn write_json_report(report: &Report) -> Result<(), DoctorOutputError> {
    let output = JsonReport::from_report(report);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &output).map_err(DoctorOutputError::WriteJson)?;
    writeln!(stdout).map_err(DoctorOutputError::WriteOutput)
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

#[derive(Debug)]
enum DoctorOutputError {
    WriteJson(serde_json::Error),
    WriteOutput(io::Error),
}

impl fmt::Display for DoctorOutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteJson(error) => write!(formatter, "write JSON runner doctor report: {error}"),
            Self::WriteOutput(error) => write!(formatter, "write runner doctor report: {error}"),
        }
    }
}
