use std::env;
use std::io::{self, Write};

use anyhow::Context;
use clap::Args;
use serde::Serialize;

use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Print version information";
const COMMAND_NAME: &str = "scherzo-cloud";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Print version information as JSON")]
    json: bool,
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        if self.json {
            write_json_output()?;
        } else {
            write_text_output()?;
        }
        Ok(ExitCode::Success)
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

fn write_text_output() -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{COMMAND_NAME} {}", crate::build_info::VERSION)
        .context("write version output")
}

fn write_json_output() -> anyhow::Result<()> {
    let executable_path = env::current_exe().context("locate the current executable")?;
    let output = VersionOutput {
        schema_version: 1,
        command: COMMAND_NAME,
        version: crate::build_info::VERSION,
        executable_path: executable_path.to_string_lossy().into_owned(),
        build_identity: crate::build_info::BUILD_IDENTITY,
    };

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &output).context("write JSON version output")?;
    writeln!(stdout).context("write version output")
}
