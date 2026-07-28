use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowDocument {
    pub(crate) schema_version: u8,
    pub(crate) description: Option<String>,
    pub(crate) steps: BTreeMap<String, Step>,
    pub(crate) exports: BTreeMap<String, OutputReference>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Step {
    Command(CommandStep),
    Agent(AgentStep),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandStep {
    pub(crate) common: CommonStep,
    pub(crate) argv: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentStep {
    pub(crate) common: CommonStep,
    pub(crate) agent: Agent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommonStep {
    pub(crate) dependencies: Vec<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) inputs: BTreeMap<String, InputReference>,
    pub(crate) outputs: BTreeMap<String, Output>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InputReference {
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
    pub(crate) system_prompt: String,
    pub(crate) message: AgentMessage,
    pub(crate) harness: Harness,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentMessage {
    pub(crate) text: Vec<MessageSource>,
    pub(crate) attachments: Vec<MessageSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MessageSource {
    File { path: String },
    Input { name: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Harness {
    Pi(PiConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PiConfig {
    pub(crate) model: String,
    pub(crate) thinking: Thinking,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Thinking {
    Off,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
}
