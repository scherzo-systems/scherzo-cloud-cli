use std::io::{self, Write};
use std::path::PathBuf;

use clap::Args;

use crate::exit_code::ExitCode;
use crate::runner::control_protocol::{ControlError, Operation, Response};
use crate::runner::enrollment::{
    EnrollmentOutcome, EnrollmentResponse, ReplacementDisposition, enroll,
};

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
                replacement: false,
            } => {
                write_initial_enrollment(&mut io::stdout().lock(), &response, self.json)?;
                Ok(ExitCode::Success)
            }
            EnrollmentOutcome::Enrolled {
                response,
                replacement: true,
            } => self.finish_replacement(ReplacementEnrollment::from_response(response)),
            EnrollmentOutcome::ReplacementCredential {
                runner_id,
                credential_id,
            } => self.finish_replacement(ReplacementEnrollment {
                runner_id,
                credential_id,
                runner_name: None,
                runner_pool_name: None,
                cloud_outcome: "already_enrolled",
            }),
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
                Ok(ExitCode::Success)
            }
        }
    }

    fn finish_replacement(&self, enrollment: ReplacementEnrollment) -> anyhow::Result<ExitCode> {
        let promotion = promote_replacement(&self.config, &enrollment);
        if self.json {
            write_replacement_json(&mut io::stdout().lock(), &enrollment, promotion)?;
        } else {
            write_replacement_human(&mut io::stdout().lock(), &enrollment, promotion)?;
        }
        Ok(promotion.exit_code())
    }
}

struct ReplacementEnrollment {
    runner_id: String,
    credential_id: String,
    runner_name: Option<String>,
    runner_pool_name: Option<String>,
    cloud_outcome: &'static str,
}

impl ReplacementEnrollment {
    fn from_response(response: EnrollmentResponse) -> Self {
        Self {
            runner_id: response.runner_id().to_owned(),
            credential_id: response.credential_id().to_owned(),
            runner_name: Some(response.runner_name().to_owned()),
            runner_pool_name: Some(response.pool_name().to_owned()),
            cloud_outcome: "enrolled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromotionOutcome {
    Promoted,
    Incomplete(PromotionFailure),
}

impl PromotionOutcome {
    const fn exit_code(self) -> ExitCode {
        match self {
            Self::Promoted => ExitCode::Success,
            Self::Incomplete(failure) => failure.exit_code(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromotionFailure {
    RunnerServeUnreachable,
    Control(ControlError),
    InvalidControlResponse,
    StateUnavailable,
}

impl PromotionFailure {
    const fn category(self) -> &'static str {
        match self {
            Self::RunnerServeUnreachable => "runner_serve_unreachable",
            Self::Control(ControlError::InvalidRequest) => "invalid_request",
            Self::Control(ControlError::UnsupportedVersion) => "unsupported_version",
            Self::Control(ControlError::NoPendingCredential) => "no_pending_credential",
            Self::Control(ControlError::PendingRegistrationMismatch) => {
                "pending_registration_mismatch"
            }
            Self::Control(ControlError::PendingAuthenticationFailed) => {
                "pending_authentication_failed"
            }
            Self::Control(ControlError::PendingProtocolFailed) => "pending_protocol_failed",
            Self::Control(ControlError::PendingConnectionFailed) => "pending_connection_failed",
            Self::Control(ControlError::StateUpdateFailed) => "state_update_failed",
            Self::InvalidControlResponse => "invalid_control_response",
            Self::StateUnavailable => "state_unavailable",
        }
    }

    const fn exit_code(self) -> ExitCode {
        match self {
            Self::RunnerServeUnreachable | Self::Control(ControlError::PendingConnectionFailed) => {
                ExitCode::Unavailable
            }
            Self::Control(_) | Self::InvalidControlResponse | Self::StateUnavailable => {
                ExitCode::GeneralFailure
            }
        }
    }

    const fn remedy(self) -> &'static str {
        match self {
            Self::RunnerServeUnreachable => {
                "Confirm that Runner Serve is running and the configured local socket is reachable, then rerun the replacement enrollment with the same protected activation file."
            }
            Self::Control(ControlError::PendingAuthenticationFailed) => {
                "Keep the current credential active, verify the replacement credential in Cloud, then rerun the replacement enrollment with the same protected activation file."
            }
            Self::Control(ControlError::PendingProtocolFailed)
            | Self::Control(ControlError::UnsupportedVersion)
            | Self::Control(ControlError::InvalidRequest)
            | Self::InvalidControlResponse => {
                "Verify that the CLI and Runner Serve versions agree, then rerun the replacement enrollment with the same protected activation file."
            }
            Self::Control(ControlError::PendingConnectionFailed) => {
                "Keep the current credential active, restore Runner Serve network access to Cloud, then rerun the replacement enrollment with the same protected activation file."
            }
            Self::Control(ControlError::StateUpdateFailed) | Self::StateUnavailable => {
                "Keep the current credential active, correct the protected runner state access problem, then rerun the replacement enrollment with the same protected activation file."
            }
            Self::Control(ControlError::NoPendingCredential)
            | Self::Control(ControlError::PendingRegistrationMismatch) => {
                "Keep the current credential active and inspect the configured protected runner state before retrying rotation."
            }
        }
    }
}

fn promote_replacement(
    config: &std::path::Path,
    enrollment: &ReplacementEnrollment,
) -> PromotionOutcome {
    let Ok(socket) = crate::runner::enrollment::load_control_socket_path(config) else {
        return PromotionOutcome::Incomplete(PromotionFailure::StateUnavailable);
    };
    match crate::runner::control_client::request(&socket, Operation::ReloadCredential) {
        Ok(Response::Reloaded { credential_id }) if credential_id == enrollment.credential_id => {
            PromotionOutcome::Promoted
        }
        Ok(Response::Error(error)) => {
            PromotionOutcome::Incomplete(PromotionFailure::Control(error))
        }
        Ok(Response::Reloaded { .. } | Response::Status(_)) => {
            PromotionOutcome::Incomplete(PromotionFailure::InvalidControlResponse)
        }
        Err(_) => match crate::runner::enrollment::replacement_disposition(
            config,
            &enrollment.runner_id,
            &enrollment.credential_id,
        ) {
            Ok(ReplacementDisposition::Current) => PromotionOutcome::Promoted,
            Ok(ReplacementDisposition::Pending | ReplacementDisposition::Missing) | Err(_) => {
                PromotionOutcome::Incomplete(PromotionFailure::RunnerServeUnreachable)
            }
        },
    }
}

fn write_initial_enrollment(
    output: &mut impl Write,
    response: &EnrollmentResponse,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        serde_json::to_writer_pretty(
            &mut *output,
            &serde_json::json!({
                "schemaVersion": 1,
                "outcome": "enrolled",
                "runnerId": response.runner_id(),
                "runnerName": response.runner_name(),
                "runnerPoolName": response.pool_name(),
                "credentialId": response.credential_id(),
            }),
        )?;
        writeln!(output)?;
    } else {
        writeln!(output, "✓ Runner enrolled.\n")?;
        writeln!(output, "runner:     {}", response.runner_id())?;
        writeln!(output, "name:       {}", response.runner_name())?;
        writeln!(output, "pool:       {}", response.pool_name())?;
        writeln!(output, "credential: {}", response.credential_id())?;
    }
    Ok(())
}

fn write_replacement_json(
    output: &mut impl Write,
    enrollment: &ReplacementEnrollment,
    promotion: PromotionOutcome,
) -> anyhow::Result<()> {
    let live_promotion = match promotion {
        PromotionOutcome::Promoted => serde_json::json!({"outcome": "promoted"}),
        PromotionOutcome::Incomplete(failure) => serde_json::json!({
            "outcome": "pending",
            "error": failure.category(),
        }),
    };
    let mut report = serde_json::json!({
        "schemaVersion": 1,
        "outcome": if promotion == PromotionOutcome::Promoted {
            "rotation_completed"
        } else {
            "rotation_incomplete"
        },
        "runnerId": enrollment.runner_id,
        "credentialId": enrollment.credential_id,
        "cloudEnrollment": {"outcome": enrollment.cloud_outcome},
        "livePromotion": live_promotion,
    });
    if let Some(name) = &enrollment.runner_name {
        report["runnerName"] = serde_json::Value::String(name.clone());
    }
    if let Some(name) = &enrollment.runner_pool_name {
        report["runnerPoolName"] = serde_json::Value::String(name.clone());
    }
    serde_json::to_writer_pretty(&mut *output, &report)?;
    writeln!(output)?;
    Ok(())
}

fn write_replacement_human(
    output: &mut impl Write,
    enrollment: &ReplacementEnrollment,
    promotion: PromotionOutcome,
) -> io::Result<()> {
    writeln!(output, "✓ Replacement credential is enrolled in Cloud.\n")?;
    writeln!(output, "runner:           {}", enrollment.runner_id)?;
    if let Some(name) = &enrollment.runner_name {
        writeln!(output, "name:             {name}")?;
    }
    if let Some(name) = &enrollment.runner_pool_name {
        writeln!(output, "pool:             {name}")?;
    }
    writeln!(output, "credential:       {}", enrollment.credential_id)?;
    writeln!(output, "cloud enrollment: {}", enrollment.cloud_outcome)?;
    match promotion {
        PromotionOutcome::Promoted => {
            writeln!(
                output,
                "\n✓ Replacement credential promoted in Runner Serve.\n"
            )?;
            writeln!(output, "live promotion: promoted")
        }
        PromotionOutcome::Incomplete(failure) => {
            writeln!(output, "\n✗ Live credential promotion did not complete.\n")?;
            writeln!(output, "live promotion: pending")?;
            writeln!(output, "error:          {}\n", failure.category())?;
            writeln!(output, "{}", failure.remedy())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replacement() -> ReplacementEnrollment {
        ReplacementEnrollment {
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            credential_id: "rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
            runner_name: None,
            runner_pool_name: None,
            cloud_outcome: "already_enrolled",
        }
    }

    #[test]
    fn replacement_reports_cloud_and_live_outcomes_separately() {
        let mut output = Vec::new();
        write_replacement_json(
            &mut output,
            &replacement(),
            PromotionOutcome::Incomplete(PromotionFailure::Control(
                ControlError::PendingAuthenticationFailed,
            )),
        )
        .unwrap();
        let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(report["cloudEnrollment"]["outcome"], "already_enrolled");
        assert_eq!(report["livePromotion"]["outcome"], "pending");
        assert_eq!(
            report["livePromotion"]["error"],
            "pending_authentication_failed"
        );
        assert_eq!(
            PromotionOutcome::Incomplete(PromotionFailure::RunnerServeUnreachable).exit_code(),
            ExitCode::Unavailable
        );
    }
}
