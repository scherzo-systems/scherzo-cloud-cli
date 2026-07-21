use std::process::ExitCode;

use clap::Args;

use super::deployment::Deployment;

pub const ABOUT: &str = "Inspect the server-confirmed authentication state";

#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Print authentication status as JSON")]
    json: bool,
}

impl Command {
    pub fn execute(self, deployment: &Deployment) -> ExitCode {
        let _options = (self.json, deployment.fingerprint());
        eprintln!("Error: scherzo-cloud auth status is not implemented yet");
        ExitCode::FAILURE
    }
}
