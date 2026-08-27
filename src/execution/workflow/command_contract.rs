use std::fmt;

use super::resolution::ResolvedWorkflow;
const MAXIMUM_WORKFLOW_NODES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServeWorkflowContractFailureKind {
    InvalidNodeCount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServeWorkflowContractFailure {
    kind: ServeWorkflowContractFailureKind,
}

impl ServeWorkflowContractFailure {
    pub(crate) fn kind(&self) -> ServeWorkflowContractFailureKind {
        self.kind
    }
}

impl fmt::Display for ServeWorkflowContractFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "serve workflow contract failure: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ServeWorkflowContractFailure {}

pub(crate) fn require_serve_workflow(
    workflow: ResolvedWorkflow,
) -> Result<ResolvedWorkflow, ServeWorkflowContractFailure> {
    let node_count = workflow
        .definition
        .steps
        .len()
        .checked_add(workflow.definition.finalizers.len())
        .ok_or_else(|| failure(ServeWorkflowContractFailureKind::InvalidNodeCount))?;
    if !(1..=MAXIMUM_WORKFLOW_NODES).contains(&node_count) {
        return Err(failure(ServeWorkflowContractFailureKind::InvalidNodeCount));
    }
    Ok(workflow)
}

fn failure(kind: ServeWorkflowContractFailureKind) -> ServeWorkflowContractFailure {
    ServeWorkflowContractFailure { kind }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;
    use crate::execution::workflow::resolution;

    fn contract_result(source: &str) -> Result<ResolvedWorkflow, ServeWorkflowContractFailure> {
        let temporary = tempfile::tempdir().unwrap();
        fs::write(temporary.path().join("workflow.yaml"), source).unwrap();
        let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
        require_serve_workflow(workflow)
    }

    #[test]
    fn accepts_supported_cloud_outputs() {
        let accepted = contract_result(
            "schemaVersion: 1\nsteps:\n  capture:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      branch:\n        kind: git_branch\n        from: workspace\nexports:\n  branch:\n    ref: outputs.capture.branch\n",
        )
        .unwrap();
        assert_eq!(accepted.definition.exports.len(), 1);
    }

    #[test]
    fn accepts_outputless_finalizers_and_engine_context() {
        let accepted = contract_result(
            "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    inputs:\n      context:\n        ref: finalization.context\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();

        assert_eq!(accepted.definition.steps.len(), 1);
        assert_eq!(accepted.definition.finalizers.len(), 1);
    }

    #[test]
    fn accepts_inputless_command_and_agent_nodes() {
        let accepted = contract_result(
            "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        assert_eq!(accepted.definition.steps.len(), 1);

        let agent = "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: openai/gpt-5\n        thinking: high\nsteps:\n  agent:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n        text: [{ file: system.md }]\n";
        let temporary = tempfile::tempdir().unwrap();
        fs::write(temporary.path().join("system.md"), "System.\n").unwrap();
        fs::write(temporary.path().join("workflow.yaml"), agent).unwrap();
        let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
        require_serve_workflow(workflow).unwrap();
    }
}
