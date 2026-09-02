use std::collections::{BTreeMap, BTreeSet};

use super::claude_code::ClaudeCodeConfig;
use super::codex::CodexConfig;
use super::condition::ResolvedPredicate;
use super::document::{FailurePolicy, FinalizationTrigger, Output};
use super::evidence::Prerequisite;
use super::pi::PiConfig;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum WorkflowValueType {
    Text,
    AttachmentCollection,
    Json,
    File,
    GitBranch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowImport {
    Prompt,
    Attachments,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RequiredImports {
    pub(crate) prompt: bool,
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowNodeRole {
    Step,
    Finalizer,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowNode {
    pub(crate) id: String,
    pub(crate) role: WorkflowNodeRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedWorkflow {
    pub(crate) schema_version: u8,
    pub(crate) description: Option<String>,
    pub(crate) steps: BTreeMap<String, ValidatedStep>,
    pub(crate) recoveries: BTreeMap<String, Option<ValidatedStepRecovery>>,
    pub(crate) source_order: Vec<String>,
    pub(crate) presentation_order: Vec<String>,
    pub(crate) finalizers: BTreeMap<String, ValidatedFinalizer>,
    pub(crate) finalizer_source_order: Vec<String>,
    pub(crate) finalizer_presentation_order: Vec<String>,
    pub(crate) exports: BTreeMap<String, ResolvedOutputSource>,
    pub(crate) required_imports: RequiredImports,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedFinalizer {
    pub(crate) body: ValidatedStep,
    pub(crate) when: BTreeSet<FinalizationTrigger>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedStepRecovery {
    pub(crate) retries: u8,
    pub(crate) handler: Option<ValidatedRecoveryHandler>,
}

// Source and validated handlers deliberately remain separate: validation pins an agent
// harness, while source syntax must not carry one. Sharing the type would blur that boundary.
// jscpd:ignore-start
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedRecoveryHandler {
    Command {
        argv: Vec<String>,
        cwd: Option<String>,
    },
    Agent {
        profile: String,
        prompt: String,
        cwd: Option<String>,
        harness: ValidatedHarness,
    },
}
// jscpd:ignore-end

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedStep {
    Command(ValidatedCommandStep),
    Agent(ValidatedAgentStep),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedCommandStep {
    pub(crate) common: ValidatedCommonStep,
    pub(crate) inputs: BTreeMap<String, ResolvedValueReference>,
    pub(crate) argv: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedAgentStep {
    pub(crate) common: ValidatedCommonStep,
    pub(crate) agent: ValidatedAgent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedDirectPrerequisite {
    pub(crate) producer: String,
    pub(crate) control: bool,
    pub(crate) disposition_control: bool,
    pub(crate) data: bool,
    pub(crate) condition_data: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedCommonStep {
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) condition: Option<ResolvedPredicate>,
    pub(crate) condition_values: BTreeMap<String, ResolvedValueSource>,
    pub(crate) prerequisites: Vec<ResolvedDirectPrerequisite>,
    pub(crate) evidence_prerequisites: Vec<Prerequisite>,
    pub(crate) cwd: Option<String>,
    pub(crate) outputs: BTreeMap<String, ValidatedOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedValueReference {
    pub(crate) source: ResolvedValueSource,
    pub(crate) value_type: WorkflowValueType,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResolvedValueSource {
    Import(WorkflowImport),
    Output(ResolvedOutputSource),
    FinalizationContext,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ResolvedOutputSource {
    pub(crate) node: WorkflowNode,
    pub(crate) output: String,
    pub(crate) value_type: WorkflowValueType,
}

impl ResolvedOutputSource {
    pub(crate) fn reference(&self) -> String {
        format!("outputs.{}.{}", self.node.id, self.output)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedOutput {
    pub(crate) definition: Output,
    pub(crate) value_type: WorkflowValueType,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedAgent {
    pub(crate) profile: String,
    pub(crate) system_prompt: String,
    pub(crate) message: ValidatedAgentMessage,
    pub(crate) harness: ValidatedHarness,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedHarness {
    Pi(PiConfig),
    ClaudeCode(ClaudeCodeConfig),
    Codex(CodexConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedAgentMessage {
    pub(crate) text: Vec<ValidatedMessageSource>,
    pub(crate) attachments: Vec<ValidatedMessageSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedMessageSource {
    File {
        path: String,
    },
    Reference {
        source: ResolvedValueSource,
        value_type: WorkflowValueType,
    },
}
