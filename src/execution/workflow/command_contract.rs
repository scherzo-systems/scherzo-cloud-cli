use std::fmt;

use super::resolution::ResolvedWorkflow;
use super::validated::ValidatedStep;

const MAXIMUM_COMMAND_WORKFLOW_STEPS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandWorkflowContractFailureKind {
    InvalidStepCount,
    AgentStep,
    DeclaredOutput,
    DeclaredExport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandWorkflowContractFailure {
    kind: CommandWorkflowContractFailureKind,
}

impl CommandWorkflowContractFailure {
    pub(crate) fn kind(&self) -> CommandWorkflowContractFailureKind {
        self.kind
    }
}

impl fmt::Display for CommandWorkflowContractFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "command workflow contract failure: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for CommandWorkflowContractFailure {}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedCommandWorkflow {
    workflow: ResolvedWorkflow,
}

impl ResolvedCommandWorkflow {
    pub(crate) fn workflow(&self) -> &ResolvedWorkflow {
        &self.workflow
    }

    pub(super) fn into_workflow(self) -> ResolvedWorkflow {
        self.workflow
    }
}

pub(crate) fn require_command_workflow_no_outputs(
    workflow: ResolvedWorkflow,
) -> Result<ResolvedCommandWorkflow, CommandWorkflowContractFailure> {
    if !(1..=MAXIMUM_COMMAND_WORKFLOW_STEPS).contains(&workflow.definition.steps.len()) {
        return Err(failure(
            CommandWorkflowContractFailureKind::InvalidStepCount,
        ));
    }
    if !workflow.definition.exports.is_empty() {
        return Err(failure(CommandWorkflowContractFailureKind::DeclaredExport));
    }
    for step in workflow.definition.steps.values() {
        let ValidatedStep::Command(step) = step else {
            return Err(failure(CommandWorkflowContractFailureKind::AgentStep));
        };
        if !step.common.outputs.is_empty() {
            return Err(failure(CommandWorkflowContractFailureKind::DeclaredOutput));
        }
    }
    Ok(ResolvedCommandWorkflow { workflow })
}

fn failure(kind: CommandWorkflowContractFailureKind) -> CommandWorkflowContractFailure {
    CommandWorkflowContractFailure { kind }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;
    use crate::execution::workflow::resolution;

    fn contract_result(
        source: &str,
    ) -> Result<ResolvedCommandWorkflow, CommandWorkflowContractFailure> {
        let temporary = tempfile::tempdir().unwrap();
        fs::write(temporary.path().join("workflow.yaml"), source).unwrap();
        let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
        require_command_workflow_no_outputs(workflow)
    }

    #[test]
    fn accepts_only_command_workflows_without_outputs_or_exports() {
        let accepted = contract_result(
            "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        assert_eq!(accepted.workflow().definition.steps.len(), 1);

        let mut oversized = String::from("schemaVersion: 1\nsteps:\n");
        for index in 0..=MAXIMUM_COMMAND_WORKFLOW_STEPS {
            oversized.push_str(&format!(
                "  step{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
            ));
        }
        assert_eq!(
            contract_result(&oversized).unwrap_err().kind(),
            CommandWorkflowContractFailureKind::InvalidStepCount
        );

        let cases = [
            (
                "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: openai/gpt-5\n        thinking: high\nsteps:\n  agent:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n        text: [{ file: system.md }]\n",
                CommandWorkflowContractFailureKind::AgentStep,
            ),
            (
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      result:\n        kind: file\n        path: result.txt\n        mediaType: text/plain\n",
                CommandWorkflowContractFailureKind::DeclaredOutput,
            ),
            (
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      result:\n        kind: file\n        path: result.txt\n        mediaType: text/plain\nexports:\n  result:\n    ref: outputs.check.result\n",
                CommandWorkflowContractFailureKind::DeclaredExport,
            ),
        ];
        for (source, expected) in cases {
            let temporary = tempfile::tempdir().unwrap();
            fs::write(temporary.path().join("system.md"), "System.\n").unwrap();
            fs::write(temporary.path().join("workflow.yaml"), source).unwrap();
            let workflow =
                resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
            assert_eq!(
                require_command_workflow_no_outputs(workflow)
                    .unwrap_err()
                    .kind(),
                expected
            );
        }
    }
}
