use std::collections::BTreeMap;

use serde::Deserialize;

use super::document::{
    Agent, AgentMessage, AgentStep, CommandStep, CommonStep, Harness, InputReference,
    MessageSource, Output, OutputReference, PiConfig, Step, Thinking, WorkflowDocument,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkflowDto {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    description: Option<String>,
    steps: BTreeMap<String, StepDto>,
    #[serde(default)]
    exports: BTreeMap<String, ReferenceDto>,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum StepDto {
    #[serde(rename = "cmd")]
    Command {
        #[serde(flatten)]
        common: CommonStepDto,
        command: CommandDto,
    },
    #[serde(rename = "agent")]
    Agent {
        #[serde(flatten)]
        common: CommonStepDto,
        agent: AgentDto,
    },
}

#[derive(Deserialize)]
struct CommonStepDto {
    #[serde(rename = "dependsOn", default)]
    dependencies: Vec<String>,
    cwd: Option<String>,
    #[serde(default)]
    inputs: BTreeMap<String, ReferenceDto>,
    #[serde(default)]
    outputs: BTreeMap<String, OutputDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandDto {
    argv: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDto {
    #[serde(rename = "systemPrompt")]
    system_prompt: String,
    message: AgentMessageDto,
    harness: HarnessDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentMessageDto {
    text: Vec<MessageSourceDto>,
    #[serde(default)]
    attachments: Vec<MessageSourceDto>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum MessageSourceDto {
    File {
        file: String,
    },
    Reference {
        #[serde(rename = "ref")]
        reference: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HarnessDto {
    id: String,
    config: PiConfigDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PiConfigDto {
    model: String,
    thinking: ThinkingDto,
}

#[derive(Deserialize)]
enum ThinkingDto {
    #[serde(rename = "off")]
    Off,
    #[serde(rename = "minimal")]
    Minimal,
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "medium")]
    Medium,
    #[serde(rename = "high")]
    High,
    #[serde(rename = "xhigh")]
    XHigh,
    #[serde(rename = "max")]
    Max,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum OutputDto {
    #[serde(rename = "agent_response")]
    AgentResponse,
    #[serde(rename = "agent_result")]
    AgentResult { schema: String },
    #[serde(rename = "file")]
    File {
        path: String,
        #[serde(rename = "mediaType")]
        media_type: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceDto {
    #[serde(rename = "ref")]
    reference: String,
}

impl WorkflowDto {
    pub(super) fn into_document(self) -> Option<WorkflowDocument> {
        let steps = self
            .steps
            .into_iter()
            .map(|(name, step)| step.into_step().map(|step| (name, step)))
            .collect::<Option<_>>()?;
        let exports = self
            .exports
            .into_iter()
            .map(|(name, reference)| {
                parse_output_reference(&reference.reference).map(|reference| (name, reference))
            })
            .collect::<Option<_>>()?;

        Some(WorkflowDocument {
            schema_version: self.schema_version,
            description: self.description,
            steps,
            exports,
        })
    }
}

impl StepDto {
    fn into_step(self) -> Option<Step> {
        match self {
            Self::Command { common, command } => Some(Step::Command(CommandStep {
                common: common.into_common_step()?,
                argv: command.argv,
            })),
            Self::Agent { common, agent } => Some(Step::Agent(AgentStep {
                common: common.into_common_step()?,
                agent: agent.into_agent()?,
            })),
        }
    }
}

impl CommonStepDto {
    fn into_common_step(self) -> Option<CommonStep> {
        let inputs = self
            .inputs
            .into_iter()
            .map(|(name, reference)| {
                parse_input_reference(&reference.reference).map(|reference| (name, reference))
            })
            .collect::<Option<_>>()?;
        let outputs = self
            .outputs
            .into_iter()
            .map(|(name, output)| (name, output.into_output()))
            .collect();

        Some(CommonStep {
            dependencies: self.dependencies,
            cwd: self.cwd,
            inputs,
            outputs,
        })
    }
}

impl AgentDto {
    fn into_agent(self) -> Option<Agent> {
        if self.harness.id != "pi" {
            return None;
        }

        Some(Agent {
            system_prompt: self.system_prompt,
            message: AgentMessage {
                text: message_sources(self.message.text)?,
                attachments: message_sources(self.message.attachments)?,
            },
            harness: Harness::Pi(PiConfig {
                model: self.harness.config.model,
                thinking: self.harness.config.thinking.into(),
            }),
        })
    }
}

fn message_sources(sources: Vec<MessageSourceDto>) -> Option<Vec<MessageSource>> {
    sources
        .into_iter()
        .map(|source| match source {
            MessageSourceDto::File { file } => Some(MessageSource::File { path: file }),
            MessageSourceDto::Reference { reference } => {
                reference
                    .strip_prefix("inputs.")
                    .map(|name| MessageSource::Input {
                        name: name.to_owned(),
                    })
            }
        })
        .collect()
}

impl OutputDto {
    fn into_output(self) -> Output {
        match self {
            Self::AgentResponse => Output::AgentResponse,
            Self::AgentResult { schema } => Output::AgentResult { schema },
            Self::File { path, media_type } => Output::File { path, media_type },
        }
    }
}

impl From<ThinkingDto> for Thinking {
    fn from(value: ThinkingDto) -> Self {
        match value {
            ThinkingDto::Off => Self::Off,
            ThinkingDto::Minimal => Self::Minimal,
            ThinkingDto::Low => Self::Low,
            ThinkingDto::Medium => Self::Medium,
            ThinkingDto::High => Self::High,
            ThinkingDto::XHigh => Self::XHigh,
            ThinkingDto::Max => Self::Max,
        }
    }
}

fn parse_input_reference(reference: &str) -> Option<InputReference> {
    if let Some(name) = reference.strip_prefix("imports.") {
        return Some(InputReference::Import {
            name: name.to_owned(),
        });
    }

    parse_output_reference(reference).map(InputReference::Output)
}

fn parse_output_reference(reference: &str) -> Option<OutputReference> {
    let mut segments = reference.split('.');
    if segments.next() != Some("outputs") {
        return None;
    }
    let step = segments.next()?.to_owned();
    let output = segments.next()?.to_owned();
    if segments.next().is_some() {
        return None;
    }

    Some(OutputReference { step, output })
}
