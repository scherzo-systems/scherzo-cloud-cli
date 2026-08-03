use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::document::{
    Agent, AgentMessage, AgentProfile, AgentStep, CommandStep, CommonStep, HarnessDefinition,
    MessageSource, Output, OutputReference, Step, ValueReference, WorkflowDocument,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkflowDto {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    description: Option<String>,
    #[serde(rename = "agentProfiles", default)]
    agent_profiles: BTreeMap<String, AgentProfileDto>,
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
        #[serde(default)]
        inputs: BTreeMap<String, ReferenceDto>,
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
    control_dependencies: Vec<String>,
    cwd: Option<String>,
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
struct AgentProfileDto {
    harness: HarnessDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDto {
    profile: String,
    #[serde(rename = "systemPrompt")]
    system_prompt: String,
    message: AgentMessageDto,
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
#[serde(tag = "kind")]
enum HarnessDto {
    #[serde(rename = "pi")]
    Pi { config: Value },
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
    pub(super) fn into_document(self, step_order: Vec<String>) -> Option<WorkflowDocument> {
        let agent_profiles = self
            .agent_profiles
            .into_iter()
            .map(|(name, profile)| (name, profile.into_agent_profile()))
            .collect();
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
            agent_profiles,
            steps,
            step_order,
            exports,
        })
    }
}

impl StepDto {
    fn into_step(self) -> Option<Step> {
        match self {
            Self::Command {
                common,
                inputs,
                command,
            } => Some(Step::Command(CommandStep {
                common: common.into_common_step(),
                inputs: parse_references(inputs)?,
                argv: command.argv,
            })),
            Self::Agent { common, agent } => Some(Step::Agent(AgentStep {
                common: common.into_common_step(),
                agent: agent.into_agent()?,
            })),
        }
    }
}

impl CommonStepDto {
    fn into_common_step(self) -> CommonStep {
        let outputs = self
            .outputs
            .into_iter()
            .map(|(name, output)| (name, output.into_output()))
            .collect();

        CommonStep {
            control_dependencies: self.control_dependencies,
            cwd: self.cwd,
            outputs,
        }
    }
}

fn parse_references(
    references: BTreeMap<String, ReferenceDto>,
) -> Option<BTreeMap<String, ValueReference>> {
    references
        .into_iter()
        .map(|(name, reference)| {
            parse_value_reference(&reference.reference).map(|reference| (name, reference))
        })
        .collect()
}

impl AgentProfileDto {
    fn into_agent_profile(self) -> AgentProfile {
        AgentProfile {
            harness: self.harness.into_harness(),
        }
    }
}

impl HarnessDto {
    fn into_harness(self) -> HarnessDefinition {
        match self {
            Self::Pi { config } => HarnessDefinition::Pi { config },
        }
    }
}

impl AgentDto {
    fn into_agent(self) -> Option<Agent> {
        Some(Agent {
            profile: self.profile,
            system_prompt: self.system_prompt,
            message: AgentMessage {
                text: message_sources(self.message.text)?,
                attachments: message_sources(self.message.attachments)?,
            },
        })
    }
}

fn message_sources(sources: Vec<MessageSourceDto>) -> Option<Vec<MessageSource>> {
    sources
        .into_iter()
        .map(|source| match source {
            MessageSourceDto::File { file } => Some(MessageSource::File { path: file }),
            MessageSourceDto::Reference { reference } => {
                parse_value_reference(&reference).map(MessageSource::Reference)
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

fn parse_value_reference(reference: &str) -> Option<ValueReference> {
    if let Some(name) = reference.strip_prefix("imports.") {
        return Some(ValueReference::Import {
            name: name.to_owned(),
        });
    }

    parse_output_reference(reference).map(ValueReference::Output)
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
