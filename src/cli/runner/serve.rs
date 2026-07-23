use std::path::PathBuf;
use std::process::ExitCode;

use clap::Args;

use crate::runner::credential::Credential;
use crate::runner::service::Config;

pub const ABOUT: &str = "Connect to Scherzo Cloud and serve run assignments";

#[derive(Debug, Args)]
pub struct Command {
    /// WebSocket URL of the runner gateway.
    #[arg(long)]
    gateway_url: String,

    /// Path to the private development runner credential file.
    #[arg(long)]
    credential_file: PathBuf,

    /// Permit ws:// only for an explicit loopback development gateway URL.
    #[arg(long)]
    allow_insecure_http: bool,
}

impl Command {
    pub fn execute(self) -> ExitCode {
        let credential = match Credential::load(&self.credential_file) {
            Ok(credential) => credential,
            Err(error) => {
                eprintln!("Error: {error}");
                return ExitCode::FAILURE;
            }
        };
        let config = match Config::new(&self.gateway_url, credential, self.allow_insecure_http) {
            Ok(config) => config,
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
