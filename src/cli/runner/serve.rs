use std::process::ExitCode;

use clap::Args;

pub const ABOUT: &str = "Connect to Scherzo Cloud and serve run assignments";

#[derive(Debug, Args)]
pub struct Command {}

impl Command {
    pub fn execute(self) -> ExitCode {
        eprintln!("Error: scherzo-cloud runner serve is not implemented yet");
        ExitCode::FAILURE
    }
}
