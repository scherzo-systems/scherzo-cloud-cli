use std::collections::BTreeMap;

use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowDocument {
    pub(crate) schema_version: u8,
    pub(crate) description: Option<String>,
    pub(crate) agent_profiles: BTreeMap<String, AgentProfile>,
    pub(crate) steps: BTreeMap<String, Step>,
    pub(crate) exports: BTreeMap<String, OutputReference>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentProfile {
    pub(crate) harness: HarnessDefinition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Step {
    Command(CommandStep),
    Agent(AgentStep),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandStep {
    pub(crate) common: CommonStep,
    pub(crate) inputs: BTreeMap<String, ValueReference>,
    pub(crate) argv: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentStep {
    pub(crate) common: CommonStep,
    pub(crate) agent: Agent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommonStep {
    pub(crate) control_dependencies: Vec<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) outputs: BTreeMap<String, Output>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValueReference {
    Import { name: String },
    Output(OutputReference),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutputReference {
    pub(crate) step: String,
    pub(crate) output: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Output {
    AgentResponse,
    AgentResult { schema: String },
    File { path: String, media_type: String },
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
}
