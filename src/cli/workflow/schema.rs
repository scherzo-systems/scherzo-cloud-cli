use std::io::{self, Write};

use anyhow::Context;
use clap::Args;

use crate::execution::workflow::STRUCTURAL_SCHEMA;
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Show the workflow structural schema";
pub(super) const AFTER_HELP: &str = "Scope:
  This schema checks workflow document structure only. Use `scherzo-cloud workflow
  validate` to validate the complete workflow definition and its referenced files.";

#[derive(Debug, Args)]
pub(super) struct Command {}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        stdout
            .write_all(STRUCTURAL_SCHEMA.as_bytes())
            .and_then(|()| stdout.flush())
            .context("write workflow schema")?;
        Ok(ExitCode::Success)
    }
}
