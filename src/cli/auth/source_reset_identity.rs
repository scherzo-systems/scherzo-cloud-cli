use std::io;

use anyhow::{Context, anyhow};
use clap::Args;

use crate::api::{
    HttpClient, SourceResetIdentityError, SourceResetIdentityOutcome, derive_source_reset_identity,
};
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};

pub(super) const ABOUT: &str = "Derive authenticated source-reset identity evidence";

// The ceremony command intentionally keeps its no-argument machine-output
// contract separate from interactive auth status presentation.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    http: super::super::HttpOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> super::super::CommandResult {
        self.run(deployment).map_err(Into::into)
    }

    // jscpd:ignore-end
    fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let client = HttpClient::new(self.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare source-reset identity networking")?;
        let outcome = match session::execute_required(
            &client,
            deployment,
            |access_token| {
                derive_source_reset_identity(
                    &client,
                    deployment.fingerprint().api_url(),
                    access_token,
                )
            },
            |outcome| {
                matches!(outcome, Ok(SourceResetIdentityOutcome::Unauthenticated))
                    || outcome
                        .as_ref()
                        .is_err_and(SourceResetIdentityError::credential_rejected)
            },
        ) {
            Ok(RequiredOperation::Unauthenticated) => SourceResetIdentityOutcome::Unauthenticated,
            Ok(RequiredOperation::Completed(outcome)) => outcome
                .map_err(|error| anyhow!(error))
                .context("derive authenticated source-reset identity evidence")?,
            Err(error) => match error.unreachable_category() {
                Some(category) => SourceResetIdentityOutcome::Unavailable(category),
                None => return Err(anyhow!(error).context("acquire human session")),
            },
        };
        match outcome {
            SourceResetIdentityOutcome::Evidence(evidence) => {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                serde_json::to_writer_pretty(&mut stdout, &evidence)
                    .context("write source-reset identity evidence")?;
                use std::io::Write;
                writeln!(stdout).context("write source-reset identity evidence")?;
                Ok(ExitCode::Success)
            }
            SourceResetIdentityOutcome::Unauthenticated => Ok(ExitCode::AuthenticationRequired),
            SourceResetIdentityOutcome::Unavailable(_) => Ok(ExitCode::Unavailable),
        }
    }
}
