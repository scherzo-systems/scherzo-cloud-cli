// Artifact validation and workflow status have separate structured output contracts despite
// sharing the standard I/O and cancellation primitives imported here.
// jscpd:ignore-start
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
// jscpd:ignore-end

use crate::execution::workflow::portable_artifact::{
    ArtifactDiagnostic, ArtifactValidationSummary, PortableArtifactValidation,
    PortableArtifactValidationFailure, validate_portable_artifact_set,
};
use crate::execution::workflow::presentation::visible_text;
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Validate a portable workflow artifact directory";
const COMMAND: &str = "scherzo-cloud artifact validate";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Print the artifact validation result as JSON")]
    json: bool,

    #[arg(
        value_name = "ARTIFACT_DIR",
        help = "Portable workflow artifact directory"
    )]
    artifact_directory: PathBuf,
}

impl Command {
    // This leaf owns artifact-specific arguments while the shared helper owns signal behavior.
    // jscpd:ignore-start
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_read_only_with_signals(
            "artifact validation",
            move |cancelled, completed| self.execute_blocking(cancelled, completed),
        )
    }
    // jscpd:ignore-end

    fn execute_blocking(
        &self,
        cancelled: &AtomicBool,
        completed: &AtomicBool,
    ) -> super::super::CommandResult {
        let validation = match validate_portable_artifact_set(&self.artifact_directory, cancelled) {
            Ok(validation) => validation,
            Err(PortableArtifactValidationFailure::Interrupted) => {
                return Ok(ExitCode::GeneralFailure);
            }
            Err(PortableArtifactValidationFailure::CurrentDirectoryUnavailable) => {
                return Err(anyhow!("the current directory is unavailable")
                    .context("resolve the initial artifact validation directory")
                    .into());
            }
            Err(PortableArtifactValidationFailure::ScratchUnavailable) => {
                return Err(anyhow!("scratch storage is unavailable")
                    .context("use artifact validation scratch storage")
                    .into());
            }
        };
        if cancelled.load(Ordering::Acquire) {
            return Ok(ExitCode::GeneralFailure);
        }
        if !self.json
            && validation
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code() == "artifact_directory_unavailable")
        {
            let source = std::fs::canonicalize(&self.artifact_directory)
                .map(|_| anyhow!("artifact directory became unavailable during validation"))
                .unwrap_or_else(anyhow::Error::new);
            return Err(source
                .context(format!(
                    "open artifact directory {}",
                    self.artifact_directory.display()
                ))
                .into());
        }
        let exit = if validation.is_valid() {
            ExitCode::Success
        } else {
            ExitCode::GeneralFailure
        };
        if self.json {
            write_json(&validation).context("write artifact validation output")?;
        } else {
            write_human(&validation).context("write artifact validation output")?;
        }
        completed.store(true, Ordering::Release);
        Ok(exit)
    }
}

fn write_human(validation: &PortableArtifactValidation) -> io::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    if let Some(summary) = validation.summary {
        let directory = validation
            .artifact_directory
            .as_deref()
            .map(visible_text)
            .unwrap_or_else(|| "<unrepresentable>".to_owned());
        writeln!(stdout, "✓ Artifact set is valid.")?;
        writeln!(stdout, "directory: {directory}")?;
        writeln!(stdout, "exports: {}", summary.declared_exports)?;
        writeln!(stdout, "carriers: {}", summary.referenced_carriers)?;
        writeln!(stdout, "bytes: {}", summary.carrier_bytes)?;
    } else {
        writeln!(stdout, "✗ Artifact set is invalid.")?;
        for (index, diagnostic) in validation.diagnostics.iter().enumerate() {
            if index != 0 {
                writeln!(stdout)?;
            }
            writeln!(stdout, "code: {}", diagnostic.code())?;
            writeln!(stdout, "location: {}", diagnostic.human_location())?;
            writeln!(stdout, "message: {}", diagnostic.message())?;
            if let (Some(directory), Some(carrier)) = (
                validation.artifact_directory.as_deref(),
                diagnostic.missing_carrier_path(),
            ) {
                let resolved = Path::new(directory).join(carrier);
                writeln!(
                    stdout,
                    "\nRestore the missing artifact carrier:\n  {}",
                    visible_text(&resolved.to_string_lossy())
                )?;
            }
        }
    }
    stdout.flush()?;
    Ok(())
}

fn write_json(validation: &PortableArtifactValidation) -> io::Result<()> {
    let outcome = match validation.summary {
        Some(summary) => JsonOutcome::Valid { summary },
        None => JsonOutcome::Invalid {
            diagnostics: &validation.diagnostics,
        },
    };
    let report = JsonReport {
        schema_version: 1,
        command: COMMAND,
        outcome,
        exit_status: if validation.is_valid() {
            ExitCode::Success.as_u8()
        } else {
            ExitCode::GeneralFailure.as_u8()
        },
        artifact_set_version: 1,
        artifact_directory: validation.artifact_directory.as_deref(),
    };
    super::super::write_pretty_json(&report)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonReport<'a> {
    schema_version: u8,
    command: &'static str,
    #[serde(flatten)]
    outcome: JsonOutcome<'a>,
    exit_status: u8,
    artifact_set_version: u8,
    artifact_directory: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum JsonOutcome<'a> {
    Valid {
        summary: ArtifactValidationSummary,
    },
    Invalid {
        diagnostics: &'a [ArtifactDiagnostic],
    },
}
