use std::env;
use std::fmt;
use std::io::{self, Write};
use std::process::ExitCode;

use clap::Args;
use serde::Serialize;

pub const ABOUT: &str = "Print version information";
const COMMAND_NAME: &str = "scherzo-cloud";

#[derive(Debug, Args)]
pub struct Command {
    #[arg(long, help = "Print version information as JSON")]
    json: bool,
}

impl Command {
    pub fn execute(self) -> ExitCode {
        let result = if self.json {
            write_json_output()
        } else {
            write_text_output()
        };

        match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("Error: {error}");
                ExitCode::FAILURE
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionOutput {
    schema_version: u8,
    command: &'static str,
    version: &'static str,
    executable_path: String,
    build_identity: &'static str,
}

fn write_text_output() -> Result<(), VersionError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{COMMAND_NAME} {}", crate::build_info::VERSION)
        .map_err(VersionError::WriteOutput)
}

fn write_json_output() -> Result<(), VersionError> {
    let executable_path = env::current_exe().map_err(VersionError::LocateExecutable)?;
    let output = VersionOutput {
        schema_version: 1,
        command: COMMAND_NAME,
        version: crate::build_info::VERSION,
        executable_path: executable_path.to_string_lossy().into_owned(),
        build_identity: crate::build_info::BUILD_IDENTITY,
    };

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &output).map_err(VersionError::WriteJson)?;
    writeln!(stdout).map_err(VersionError::WriteOutput)
}

#[derive(Debug)]
enum VersionError {
    LocateExecutable(io::Error),
    WriteJson(serde_json::Error),
    WriteOutput(io::Error),
}

impl fmt::Display for VersionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocateExecutable(error) => {
                write!(formatter, "locate the current executable: {error}")
            }
            Self::WriteJson(error) => write!(formatter, "write JSON version output: {error}"),
            Self::WriteOutput(error) => write!(formatter, "write version output: {error}"),
        }
    }
}
