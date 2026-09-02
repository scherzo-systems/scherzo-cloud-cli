use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Args;

use super::assembly::{ArtifactAssemblyError, assemble_artifact_set};
use crate::api::{ArtifactApi, ArtifactApiError, HttpClient};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};

pub(super) const ABOUT: &str = "Download and verify a run's Artifact Set";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: String,

    #[arg(
        value_name = "RUN",
        help = "Run identifier containing the Artifact Set"
    )]
    run_id: String,

    #[arg(
        long,
        value_name = "PATH",
        help = "Directory to create for the complete Artifact Set (must not already exist)"
    )]
    output: PathBuf,

    // Artifact download owns a filesystem commit in addition to Cloud access, so its
    // command shape and networking context remain separate from read-only auth status.
    // jscpd:ignore-start
    #[command(flatten)]
    http: super::super::HttpOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> super::super::CommandResult {
        let transport_policy = self.http.transport_policy();
        let session_client = HttpClient::new(transport_policy)
            .map_err(|error| anyhow!(error))
            .context("prepare Artifact Set networking")?;
        // jscpd:ignore-end
        let result = session::execute_required(
            &session_client,
            deployment,
            |access_token| {
                let mut api = ArtifactApi::new(
                    deployment.fingerprint().api_url(),
                    access_token,
                    transport_policy,
                )?;
                assemble_artifact_set(&mut api, &self.organization, &self.run_id, &self.output)
            },
            |result| {
                result.as_ref().is_err_and(|error| {
                    matches!(
                        error,
                        ArtifactAssemblyError::Api(error) if error.credential_rejected()
                    )
                })
            },
        );
        let result = match result {
            Ok(RequiredOperation::Unauthenticated) => Err(ArtifactAssemblyError::Api(
                ArtifactApiError::Unauthenticated,
            )),
            Ok(RequiredOperation::Completed(result)) => result,
            Err(error) => match error.unreachable_category() {
                Some(category) => Err(ArtifactAssemblyError::Api(ArtifactApiError::Unreachable(
                    category,
                ))),
                None => {
                    return Err(anyhow!(error)
                        .context("acquire human session for Artifact Set download")
                        .into());
                }
            },
        };
        write_result(deployment.fingerprint().api_url(), result).map_err(Into::into)
    }
}

fn write_result(
    deployment: &str,
    result: Result<super::assembly::AssembledArtifact, ArtifactAssemblyError>,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(assembled) => {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            writeln!(stdout, "✓ Artifact Set downloaded.\n")?;
            writeln!(stdout, "artifact set: {}", assembled.artifact_set_id)?;
            writeln!(stdout, "members: {}", assembled.member_count)?;
            writeln!(stdout, "bytes: {}", assembled.total_size_bytes)?;
            writeln!(stdout, "directory: {}", assembled.destination.display())?;
            writeln!(stdout, "deployment: {deployment}")?;
            Ok(ExitCode::Success)
        }
        Err(error) => {
            let outcome_class = match &error {
                ArtifactAssemblyError::Api(ArtifactApiError::Unauthenticated) => {
                    OutcomeClass::Unauthenticated
                }
                ArtifactAssemblyError::Api(ArtifactApiError::Forbidden) => OutcomeClass::Forbidden,
                ArtifactAssemblyError::Api(ArtifactApiError::Unreachable(category)) => {
                    super::super::unreachable_outcome_class(*category)
                }
                ArtifactAssemblyError::Api(ArtifactApiError::Protocol { .. }) => {
                    OutcomeClass::Protocol
                }
                _ => OutcomeClass::GeneralFailure,
            };
            let remedy = match &error {
                ArtifactAssemblyError::Api(ArtifactApiError::Unauthenticated) => {
                    "Sign in first:\n  scherzo-cloud auth login"
                }
                ArtifactAssemblyError::DestinationExists => {
                    "Choose an output path that does not exist, then try again."
                }
                ArtifactAssemblyError::Api(ArtifactApiError::Gone) => {
                    "The Artifact Set retention window has ended; no download is available."
                }
                ArtifactAssemblyError::Api(ArtifactApiError::NotFound) => {
                    "Check the organization and run identifier, then try again."
                }
                _ => "Check access and network availability, then try again.",
            };
            let stderr = io::stderr();
            let mut stderr = stderr.lock();
            writeln!(stderr, "error: download Artifact Set: {error}\n\n{remedy}")?;
            Ok(outcome_class.exit_code())
        }
    }
}
