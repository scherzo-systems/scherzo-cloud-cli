use std::process::ExitCode;

use clap::Args;

pub const ABOUT: &str = "Print version information";

#[derive(Debug, Args)]
pub struct Command {}

impl Command {
    pub fn execute(self) -> ExitCode {
        println!("scherzo-cloud {}", crate::build_info::VERSION);
        ExitCode::SUCCESS
    }
}
