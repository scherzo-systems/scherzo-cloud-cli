use std::process::ExitCode;

use clap::Args;

use super::deployment::Deployment;

pub const ABOUT: &str = "Remove the local human credential";

#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Print the logout result as JSON")]
    json: bool,
}

impl Command {
    pub fn execute(self, deployment: &Deployment) -> ExitCode {
        let _options = (self.json, deployment.fingerprint());
        eprintln!("Error: scherzo-cloud auth logout is not implemented yet");
        ExitCode::FAILURE
    }
}
