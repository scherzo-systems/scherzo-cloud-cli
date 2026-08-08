use std::fmt;

use super::resolution::ResolvedWorkflow;

const MAXIMUM_WORKFLOW_STEPS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServeWorkflowContractFailureKind {
    InvalidStepCount,
    DeclaredOutput,
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

pub(crate) fn require_inputless_workflow_no_outputs(
    workflow: ResolvedWorkflow,
) -> Result<ResolvedWorkflow, ServeWorkflowContractFailure> {
    if !(1..=MAXIMUM_WORKFLOW_STEPS).contains(&workflow.definition.steps.len()) {
        return Err(failure(ServeWorkflowContractFailureKind::InvalidStepCount));
    }
    if workflow.definition.steps.values().any(|step| {
        let outputs = match step {
            super::validated::ValidatedStep::Command(step) => &step.common.outputs,
            super::validated::ValidatedStep::Agent(step) => &step.common.outputs,
        };
        !outputs.is_empty()
    }) {
        return Err(failure(ServeWorkflowContractFailureKind::DeclaredOutput));
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
        require_inputless_workflow_no_outputs(workflow)
    }

    #[test]
    fn accepts_outputless_command_and_agent_workflows_but_rejects_outputs_and_exports() {
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
        require_inputless_workflow_no_outputs(workflow).unwrap();

        for output in [
            "        kind: file\n        path: result.txt\n        mediaType: text/plain\n",
            "        kind: git_branch\n",
        ] {
            let source = format!(
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      result:\n{output}"
            );
            assert_eq!(
                contract_result(&source).unwrap_err().kind(),
                ServeWorkflowContractFailureKind::DeclaredOutput,
            );
        }

        assert_eq!(
            contract_result(
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      result:\n        kind: file\n        path: result.txt\n        mediaType: text/plain\nexports:\n  result:\n    ref: outputs.check.result\n",
            )
            .unwrap_err()
            .kind(),
            ServeWorkflowContractFailureKind::DeclaredOutput,
        );
    }
}
