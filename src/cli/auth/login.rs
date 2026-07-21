use std::process::ExitCode;

use clap::Args;

use super::deployment::Deployment;

pub const ABOUT: &str = "Authenticate through a browser on any machine";

#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Emit newline-delimited JSON events")]
    json: bool,

    #[arg(
        long,
        help = "Start a new login without checking an existing credential"
    )]
    force: bool,
}

impl Command {
    pub fn execute(self, deployment: &Deployment) -> ExitCode {
        let _options = (self.json, self.force, deployment.fingerprint());
        eprintln!("Error: scherzo-cloud auth login is not implemented yet");
        ExitCode::FAILURE
    }
}
