use clap::Args;

pub(super) const ABOUT: &str = "Show the workflow authoring reference";

const AUTHORING_REFERENCE: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/docs/workflow-v1.md"));

#[derive(Debug, Args)]
pub(super) struct Command {}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::write_embedded_asset(AUTHORING_REFERENCE, "write workflow reference")
    }
}
