use clap::Args;

use crate::execution::workflow::STRUCTURAL_SCHEMA;

pub(super) const ABOUT: &str = "Show the workflow structural schema";
pub(super) const AFTER_HELP: &str = "Scope:
  This schema checks workflow document structure only. Use `scherzo-cloud workflow
  validate` to validate the complete workflow definition and its referenced files.";

#[derive(Debug, Args)]
pub(super) struct Command {}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::write_embedded_asset(STRUCTURAL_SCHEMA, "write workflow schema")
    }
}
