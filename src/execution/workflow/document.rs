use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowDocument {
    pub(crate) schema_version: u8,
    pub(crate) description: Option<String>,
    pub(crate) agent_profiles: BTreeMap<String, AgentProfile>,
    pub(crate) steps: BTreeMap<String, StepDefinition>,
    pub(crate) step_order: Vec<String>,
    pub(crate) finalizers: BTreeMap<String, FinalizerDefinition>,
    pub(crate) finalizer_order: Vec<String>,
    pub(crate) exports: BTreeMap<String, OutputReference>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepDefinition {
    pub(crate) body: NodeBody,
    pub(crate) control_dependencies: Vec<String>,
    pub(crate) recovery: Option<StepRecovery>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepRecovery {
    pub(crate) retries: u8,
    pub(crate) handler: Option<RecoveryHandler>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryHandler {
    Command {
        argv: Vec<String>,
        cwd: Option<String>,
    },
    Agent {
        profile: String,
        prompt: String,
        cwd: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FinalizerDefinition {
    pub(crate) body: NodeBody,
    pub(crate) after: Vec<String>,
    pub(crate) when: BTreeSet<FinalizationTrigger>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinalizationTrigger {
    Succeeded,
    Failed,
    Cancelled,
}

impl FinalizationTrigger {
    pub(crate) fn all() -> BTreeSet<Self> {
        [Self::Succeeded, Self::Failed, Self::Cancelled]
            .into_iter()
            .collect()
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentProfile {
    pub(crate) harness: HarnessDefinition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NodeBody {
    Command(CommandNode),
    Agent(AgentNode),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandNode {
    pub(crate) common: CommonNode,
    pub(crate) inputs: BTreeMap<String, ValueReference>,
    pub(crate) argv: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentNode {
    pub(crate) common: CommonNode,
    pub(crate) agent: Agent,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailurePolicy {
    #[default]
    Required,
    Advisory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommonNode {
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) cwd: Option<String>,
    pub(crate) outputs: BTreeMap<String, Output>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValueReference {
    Import { name: String },
    Output(OutputReference),
    FinalizationContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutputReference {
    pub(crate) node: String,
    pub(crate) output: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Output {
    TextPath { path: String },
    TextAgentResponse,
    JsonPath { path: String, schema: String },
    JsonAgentResult { schema: String },
    FilePath { path: String, media_type: String },
    GitBranchWorkspace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Agent {
    pub(crate) profile: String,
    pub(crate) system_prompt: String,
    pub(crate) message: AgentMessage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentMessage {
    pub(crate) text: Vec<MessageSource>,
    pub(crate) attachments: Vec<MessageSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MessageSource {
    File { path: String },
    Reference(ValueReference),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HarnessDefinition {
    Pi { config: Value },
    ClaudeCode { config: Value },
    Codex { config: Value },
}
