use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Args;

use crate::execution::pi::discover_and_validate_pi_installation;
use crate::exit_code::ExitCode;
use crate::runner::credential::Credential;
use crate::runner::service::{AssignmentConfig, Config};

pub(super) const ABOUT: &str = "Connect to Scherzo Cloud and serve run assignments";

#[derive(Debug, Args)]
pub(super) struct Command {
    /// WebSocket URL of the runner gateway.
    #[arg(long, value_name = "URL")]
    gateway_url: String,

    /// Path to the private development runner credential file.
    #[arg(long, value_name = "PATH")]
    credential_file: PathBuf,

    /// Permit ws:// only for an explicit loopback development gateway URL.
    #[arg(long)]
    allow_insecure_http: bool,

    /// Cloud workflow ID mapped by this development runner.
    #[arg(long, value_name = "WORKFLOW_ID")]
    workflow_id: String,

    /// Directory boundary for the registered workflow's local sources (must already exist).
    #[arg(long, value_name = "ROOT")]
    workflow_source_root: PathBuf,

    /// Workflow YAML path selected within the workflow source root.
    #[arg(long, value_name = "PATH")]
    workflow_path: PathBuf,

    /// Directory for runner-owned execution roots (must already exist).
    #[arg(long, value_name = "ROOT")]
    work_root: PathBuf,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let pi_installation = discover_and_validate_pi_installation().ok();
        let credential_file = std::path::absolute(&self.credential_file).with_context(|| {
            format!(
                "resolve runner credential file {}",
                self.credential_file.display()
            )
        })?;
        let credential = Credential::load(&credential_file).map_err(|error| {
            anyhow!(
                "load runner credential file {}: {error}\n\nReplace it with a valid runner credential and make the file readable only by its owner.",
                credential_file.display()
            )
        })?;
        let assignment = AssignmentConfig::new(
            self.workflow_id,
            &self.workflow_source_root,
            &self.workflow_path,
            &self.work_root,
        )
        .with_context(|| {
            format!(
                "configure runner workflow mapping from {} using work root {}",
                self.workflow_source_root.display(),
                self.work_root.display()
            )
        })?;
        let config = Config::new(
            &self.gateway_url,
            credential,
            self.allow_insecure_http,
            assignment,
        )
        .with_context(|| format!("configure runner gateway endpoint {}", self.gateway_url))?;
        let config = match pi_installation {
            Some(installation) => config.with_pi_installation(installation),
            None => config,
        };
        crate::runner::service::run(config)
            .with_context(|| format!("serve runner assignments from {}", self.gateway_url))?;
        Ok(ExitCode::Success)
    }
}
