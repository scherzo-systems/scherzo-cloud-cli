use std::path::PathBuf;
use std::process::ExitCode;

use clap::Args;

use crate::execution::pi::discover_and_validate_pi_installation;
use crate::runner::credential::Credential;
use crate::runner::service::{AssignmentConfig, Config};

pub(super) const ABOUT: &str = "Connect to Scherzo Cloud and serve run assignments";

#[derive(Debug, Args)]
pub(super) struct Command {
    /// WebSocket URL of the runner gateway.
    #[arg(long)]
    gateway_url: String,

    /// Path to the private development runner credential file.
    #[arg(long)]
    credential_file: PathBuf,

    /// Permit ws:// only for an explicit loopback development gateway URL.
    #[arg(long)]
    allow_insecure_http: bool,

    /// Cloud workflow ID mapped by this development runner.
    #[arg(long, value_name = "WORKFLOW_ID")]
    workflow_id: String,

    /// Existing directory boundary for the registered workflow's local sources.
    #[arg(long, value_name = "ROOT")]
    workflow_source_root: PathBuf,

    /// Workflow YAML path selected within the workflow source root.
    #[arg(long, value_name = "PATH")]
    workflow_path: PathBuf,

    /// Existing directory under which runner-owned execution roots are created.
    #[arg(long, value_name = "ROOT")]
    work_root: PathBuf,
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        let pi_installation = discover_and_validate_pi_installation().ok();
        let credential = match Credential::load(&self.credential_file) {
            Ok(credential) => credential,
            Err(error) => {
                eprintln!("Error: {error}");
                return ExitCode::FAILURE;
            }
        };
        let assignment = match AssignmentConfig::new(
            self.workflow_id,
            &self.workflow_source_root,
            &self.workflow_path,
            &self.work_root,
        ) {
            Ok(assignment) => assignment,
            Err(error) => {
                eprintln!("Error: {error}");
                return ExitCode::FAILURE;
            }
        };
        let config = match Config::new(
            &self.gateway_url,
            credential,
            self.allow_insecure_http,
            assignment,
        ) {
            Ok(config) => match pi_installation {
                Some(installation) => config.with_pi_installation(installation),
                None => config,
            },
            Err(error) => {
                eprintln!("Error: {error}");
                return ExitCode::FAILURE;
            }
        };
        match crate::runner::service::run(config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("Error: {error}");
                ExitCode::FAILURE
            }
        }
    }
}
