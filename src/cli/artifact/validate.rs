use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};

use clap::Args;
use serde::Serialize;

use crate::execution::workflow::portable_artifact::{
    ArtifactDiagnostic, ArtifactValidationSummary, PortableArtifactValidation,
    PortableArtifactValidationFailure, validate_portable_artifact_set,
};
use crate::execution::workflow::presentation::visible_text;

pub(super) const ABOUT: &str = "Validate one complete portable Artifact Set V1 directory";
const COMMAND: &str = "scherzo-cloud artifact validate";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Print one Artifact Validate Result Schema 1 document")]
    json: bool,

    #[arg(
        value_name = "ARTIFACT_DIR",
        help = "Portable Artifact Set V1 directory"
    )]
    artifact_directory: PathBuf,
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        super::super::execute_read_only_with_signals(
            "artifact validation",
            move |cancelled, completed| self.execute_blocking(cancelled, completed),
        )
    }

    fn execute_blocking(&self, cancelled: &AtomicBool, completed: &AtomicBool) -> ExitCode {
        let validation = match validate_portable_artifact_set(&self.artifact_directory, cancelled) {
            Ok(validation) => validation,
            Err(PortableArtifactValidationFailure::Interrupted) => return ExitCode::FAILURE,
            Err(PortableArtifactValidationFailure::AccessBookkeepingUnavailable) => {
                eprintln!("Error: open artifact set without updating access bookkeeping");
                return ExitCode::FAILURE;
            }
            Err(PortableArtifactValidationFailure::CurrentDirectoryUnavailable) => {
                eprintln!("Error: resolve the initial artifact validation directory");
                return ExitCode::FAILURE;
            }
        };
        if cancelled.load(Ordering::Acquire) {
            return ExitCode::FAILURE;
        }
        let exit = if validation.is_valid() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
        let output = if self.json {
            write_json(&validation)
        } else {
            write_human(&validation)
        };
        match output {
            Ok(()) => {
                completed.store(true, Ordering::Release);
                exit
            }
            Err(error) => {
                eprintln!("Error: write artifact validation output: {error}");
                ExitCode::FAILURE
            }
        }
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
        writeln!(
            stdout,
            "Artifact Set V1 is valid: {directory} ({} exports, {} carriers, {} bytes).",
            summary.declared_exports, summary.referenced_carriers, summary.carrier_bytes,
        )?;
    } else {
        for diagnostic in &validation.diagnostics {
            writeln!(
                stdout,
                "{}: {} ({})",
                diagnostic.code(),
                diagnostic.message(),
                diagnostic.human_location(),
            )?;
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
        exit_status: u8::from(!validation.is_valid()),
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
