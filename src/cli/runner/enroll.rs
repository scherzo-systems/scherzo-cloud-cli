use std::io::{self, Write};
use std::path::PathBuf;

use clap::Args;

use crate::exit_code::ExitCode;
use crate::runner::enrollment::{EnrollmentOutcome, enroll};

pub(super) const ABOUT: &str = "Enroll a protected runner credential";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        long,
        value_name = "PATH|-",
        required_unless_present = "resume",
        conflicts_with = "resume",
        help = "Read the activation artifact from a protected file or explicit stdin"
    )]
    activation_file: Option<PathBuf>,

    #[arg(
        long,
        conflicts_with_all = ["activation_file", "replace_credential"],
        help = "Resend the exact unresolved enrollment journal"
    )]
    resume: bool,

    #[arg(
        long,
        requires = "activation_file",
        conflicts_with = "resume",
        help = "Stage the enrolled credential as a replacement for this runner"
    )]
    replace_credential: bool,

    #[arg(
        long,
        value_name = "PATH",
        help = "Read the closed runner operator configuration"
    )]
    config: PathBuf,

    #[arg(long, help = "Print the non-secret enrollment result as JSON")]
    json: bool,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        self.execute_enrollment().map_err(Into::into)
    }

    fn execute_enrollment(self) -> anyhow::Result<ExitCode> {
        let outcome = enroll(
            self.activation_file.as_deref(),
            &self.config,
            self.replace_credential,
            self.resume,
        )
        .map_err(|error| anyhow::anyhow!(
            "runner enrollment did not complete: {error}\n\nKeep the protected enrollment journal and rerun with --resume when appropriate."
        ))?;
        match outcome {
            EnrollmentOutcome::Enrolled {
                response,
                replacement,
            } => {
                if self.json {
                    serde_json::to_writer_pretty(
                        &mut io::stdout().lock(),
                        &serde_json::json!({
                            "schemaVersion": 1,
                            "outcome": if replacement { "replacement_staged" } else { "enrolled" },
                            "runnerId": response.runner_id(),
                            "runnerName": response.runner_name(),
                            "runnerPoolName": response.pool_name(),
                            "credentialId": response.credential_id(),
                        }),
                    )?;
                    writeln!(io::stdout().lock())?;
                } else {
                    let heading = if replacement {
                        "✓ Runner replacement credential staged."
                    } else {
                        "✓ Runner enrolled."
                    };
                    writeln!(io::stdout().lock(), "{heading}\n")?;
                    writeln!(
                        io::stdout().lock(),
                        "  Runner:     {}",
                        response.runner_id()
                    )?;
                    writeln!(
                        io::stdout().lock(),
                        "  Name:       {}",
                        response.runner_name()
                    )?;
                    writeln!(
                        io::stdout().lock(),
                        "  Pool:       {}",
                        response.pool_name()
                    )?;
                    writeln!(
                        io::stdout().lock(),
                        "  Credential: {}",
                        response.credential_id()
                    )?;
                }
            }
            EnrollmentOutcome::Gone { activation_id } => {
                if self.json {
                    serde_json::to_writer_pretty(
                        &mut io::stdout().lock(),
                        &serde_json::json!({
                            "schemaVersion": 1,
                            "outcome": "gone",
                            "activationId": activation_id,
                        }),
                    )?;
                    writeln!(io::stdout().lock())?;
                } else {
                    writeln!(
                        io::stdout().lock(),
                        "Enrollment did not commit.\n\n  Activation: {activation_id}\n  The secret journal was replaced with a terminal receipt; a different activation may now be used."
                    )?;
                }
            }
        }
        Ok(ExitCode::Success)
    }
}
