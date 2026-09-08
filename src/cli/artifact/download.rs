use std::io::{self, Write};
use std::path::PathBuf;

use clap::Args;

use super::assembly::{ArtifactAssemblyError, AssembledArtifact, assemble_artifact_set};
use crate::api::{ArtifactApi, ArtifactApiError};
use crate::exit_code::{ExitCode, OutcomeClass};
pub(super) const ABOUT: &str = "Download and verify a run's Artifact Set";

pub(super) type Command = super::RemoteArtifactCommand<Operation>;

#[derive(Debug, Args)]
pub(super) struct Operation {
    #[arg(
        long,
        value_name = "PATH",
        help = "Directory to create for the complete Artifact Set (must not already exist)"
    )]
    output: PathBuf,
}

impl super::RemoteArtifactOperation for Operation {
    type Output = AssembledArtifact;
    type Error = ArtifactAssemblyError;

    const SESSION_CONTEXT: &'static str = "acquire human session for Artifact Set download";

    fn request(
        &self,
        api: &mut ArtifactApi,
        run: &super::RunArtifactReference,
    ) -> Result<Self::Output, Self::Error> {
        assemble_artifact_set(api, &run.organization, &run.run_id, &self.output)
    }

    fn credential_rejected(error: &Self::Error) -> bool {
        matches!(
            error,
            ArtifactAssemblyError::Api(error) if error.credential_rejected()
        )
    }

    fn write_result(
        &self,
        deployment: &str,
        _run: &super::RunArtifactReference,
        result: Result<Self::Output, Self::Error>,
    ) -> anyhow::Result<ExitCode> {
        write_download_result(deployment, result)
    }
}

fn write_download_result(
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
