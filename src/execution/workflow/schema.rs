use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use super::document::{
    Agent, AgentMessage, AgentNode, AgentProfile, CommandNode, CommonNode, ConditionOperand,
    ConditionPredicate, ConditionSelector, FailurePolicy, FinalizationTrigger, FinalizerDefinition,
    HarnessDefinition, MessageSource, NodeBody, Output, OutputReference, RecoveryHandler,
    StepDefinition, StepRecovery, ValueReference, WorkflowDocument,
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
    finalizers: BTreeMap<String, FinalizerDto>,
    #[serde(default)]
    exports: BTreeMap<String, ReferenceDto>,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum StepDto {
    #[serde(rename = "cmd")]
    Command {
        #[serde(flatten)]
        body: CommandNodeDto,
        #[serde(rename = "dependsOn", default)]
        control_dependencies: Vec<String>,
        recovery: Option<RecoveryDto>,
    },
    #[serde(rename = "agent")]
    Agent {
        #[serde(flatten)]
        body: AgentNodeDto,
        #[serde(rename = "dependsOn", default)]
        control_dependencies: Vec<String>,
        recovery: Option<RecoveryDto>,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum FinalizerDto {
    #[serde(rename = "cmd")]
    Command {
        #[serde(flatten)]
        body: CommandNodeDto,
        #[serde(default)]
        after: Vec<String>,
        #[serde(default = "all_finalization_triggers")]
        when: BTreeSet<FinalizationTrigger>,
    },
    #[serde(rename = "agent")]
    Agent {
        #[serde(flatten)]
        body: AgentNodeDto,
        #[serde(default)]
        after: Vec<String>,
        #[serde(default = "all_finalization_triggers")]
        when: BTreeSet<FinalizationTrigger>,
    },
}

fn all_finalization_triggers() -> BTreeSet<FinalizationTrigger> {
    FinalizationTrigger::all()
}

#[derive(Deserialize)]
struct CommandNodeDto {
    #[serde(flatten)]
    common: CommonNodeDto,
    #[serde(default)]
    inputs: BTreeMap<String, ReferenceDto>,
    command: CommandDto,
}

#[derive(Deserialize)]
struct AgentNodeDto {
    #[serde(flatten)]
    common: CommonNodeDto,
    agent: AgentDto,
}

#[derive(Deserialize)]
struct CommonNodeDto {
    #[serde(rename = "failurePolicy", default)]
    failure_policy: FailurePolicy,
    condition: Option<ConditionPredicateDto>,
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
struct RecoveryDto {
    retries: u8,
    handler: Option<RecoveryHandlerDto>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum RecoveryHandlerDto {
    #[serde(rename = "cmd")]
    Command {
        command: CommandDto,
        cwd: Option<String>,
    },
    #[serde(rename = "agent")]
    Agent {
        profile: String,
        prompt: String,
        cwd: Option<String>,
    },
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
    #[serde(rename = "claude_code")]
    ClaudeCode { config: Value },
    #[serde(rename = "codex")]
    Codex { config: Value },
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum OutputDto {
    #[serde(rename = "text")]
    Text {
        #[serde(rename = "from")]
        source: TextOutputSourceDto,
        path: Option<String>,
    },
    #[serde(rename = "json")]
    Json {
        #[serde(rename = "from")]
        source: JsonOutputSourceDto,
        path: Option<String>,
        schema: String,
    },
    #[serde(rename = "file")]
    File {
        #[serde(rename = "from")]
        source: PathOutputSourceDto,
        path: String,
        #[serde(rename = "mediaType")]
        media_type: String,
    },
    #[serde(rename = "git_branch")]
    GitBranch {
        #[serde(rename = "from")]
        source: WorkspaceOutputSourceDto,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum TextOutputSourceDto {
    Path,
    AgentResponse,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum JsonOutputSourceDto {
    Path,
    AgentResult,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PathOutputSourceDto {
    Path,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkspaceOutputSourceDto {
    Workspace,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ConditionPredicateDto {
    All(ConditionAllDto),
    Any(ConditionAnyDto),
    Not(ConditionNotDto),
    Equals(ConditionEqualsDto),
    Exists(ConditionExistsDto),
    Disposition(ConditionDispositionDto),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionAllDto {
    all: Vec<ConditionPredicateDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionAnyDto {
    any: Vec<ConditionPredicateDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionNotDto {
    not: Box<ConditionPredicateDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionEqualsDto {
    equals: [ConditionOperandDto; 2],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionExistsDto {
    exists: ConditionSelectorDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionDispositionDto {
    disposition: ConditionDispositionValueDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionDispositionValueDto {
    node: String,
    is: super::condition::TerminalDisposition,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ConditionOperandDto {
    Reference(ConditionReferenceDto),
    Literal(ConditionLiteralDto),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionReferenceDto {
    #[serde(rename = "ref")]
    reference: String,
    pointer: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionLiteralDto {
    value: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionSelectorDto {
    #[serde(rename = "ref")]
    reference: String,
    pointer: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceDto {
    #[serde(rename = "ref")]
    reference: String,
}

impl WorkflowDto {
    pub(super) fn into_document(
        self,
        step_order: Vec<String>,
        finalizer_order: Vec<String>,
    ) -> Option<WorkflowDocument> {
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
        let finalizers = self
            .finalizers
            .into_iter()
            .map(|(name, finalizer)| {
                finalizer
                    .into_finalizer()
                    .map(|finalizer| (name, finalizer))
            })
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
            finalizers,
            finalizer_order,
            exports,
        })
    }
}

impl StepDto {
    fn into_step(self) -> Option<StepDefinition> {
        let (body, control_dependencies, recovery) = match self {
            Self::Command {
                body,
                control_dependencies,
                recovery,
            } => (body.into_body()?, control_dependencies, recovery),
            Self::Agent {
                body,
                control_dependencies,
                recovery,
            } => (body.into_body()?, control_dependencies, recovery),
        };
        Some(StepDefinition {
            body,
            control_dependencies,
            recovery: recovery.map(RecoveryDto::into_recovery),
        })
    }
}

impl FinalizerDto {
    fn into_finalizer(self) -> Option<FinalizerDefinition> {
        let (body, after, when) = match self {
            Self::Command { body, after, when } => (body.into_body()?, after, when),
            Self::Agent { body, after, when } => (body.into_body()?, after, when),
        };
        Some(FinalizerDefinition { body, after, when })
    }
}

impl CommandNodeDto {
    fn into_body(self) -> Option<NodeBody> {
        Some(NodeBody::Command(CommandNode {
            common: self.common.into_common_node()?,
            inputs: parse_references(self.inputs)?,
            argv: self.command.argv,
        }))
    }
}

impl AgentNodeDto {
    fn into_body(self) -> Option<NodeBody> {
        Some(NodeBody::Agent(AgentNode {
            common: self.common.into_common_node()?,
            agent: self.agent.into_agent()?,
        }))
    }
}

impl CommonNodeDto {
    fn into_common_node(self) -> Option<CommonNode> {
        let outputs = self
            .outputs
            .into_iter()
            .map(|(name, output)| output.into_output().map(|output| (name, output)))
            .collect::<Option<_>>()?;

        Some(CommonNode {
            failure_policy: self.failure_policy,
            condition: match self.condition {
                Some(condition) => Some(condition.into_predicate()?),
                None => None,
            },
            cwd: self.cwd,
            outputs,
        })
    }
}

impl ConditionPredicateDto {
    fn into_predicate(self) -> Option<ConditionPredicate> {
        match self {
            Self::All(value) => Some(ConditionPredicate::All(
                value
                    .all
                    .into_iter()
                    .map(Self::into_predicate)
                    .collect::<Option<_>>()?,
            )),
            Self::Any(value) => Some(ConditionPredicate::Any(
                value
                    .any
                    .into_iter()
                    .map(Self::into_predicate)
                    .collect::<Option<_>>()?,
            )),
            Self::Not(value) => Some(ConditionPredicate::Not(Box::new(
                value.not.into_predicate()?,
            ))),
            Self::Equals(value) => Some(ConditionPredicate::Equals(
                value
                    .equals
                    .map(ConditionOperandDto::into_operand)
                    .into_iter()
                    .collect::<Option<Vec<_>>>()?
                    .try_into()
                    .ok()?,
            )),
            Self::Exists(value) => Some(ConditionPredicate::Exists(ConditionSelector {
                reference: parse_value_reference(&value.exists.reference)?,
                pointer: value.exists.pointer,
            })),
            Self::Disposition(value) => Some(ConditionPredicate::Disposition {
                node: value.disposition.node,
                is: value.disposition.is,
            }),
        }
    }
}

impl ConditionOperandDto {
    fn into_operand(self) -> Option<ConditionOperand> {
        match self {
            Self::Reference(value) => Some(ConditionOperand::Reference {
                reference: parse_value_reference(&value.reference)?,
                pointer: value.pointer,
            }),
            Self::Literal(value) => Some(ConditionOperand::Literal(value.value)),
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

impl RecoveryDto {
    fn into_recovery(self) -> StepRecovery {
        StepRecovery {
            retries: self.retries,
            handler: self.handler.map(RecoveryHandlerDto::into_handler),
        }
    }
}

impl RecoveryHandlerDto {
    fn into_handler(self) -> RecoveryHandler {
        match self {
            Self::Command { command, cwd } => RecoveryHandler::Command {
                argv: command.argv,
                cwd,
            },
            Self::Agent {
                profile,
                prompt,
                cwd,
            } => RecoveryHandler::Agent {
                profile,
                prompt,
                cwd,
            },
        }
    }
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
            Self::ClaudeCode { config } => HarnessDefinition::ClaudeCode { config },
            Self::Codex { config } => HarnessDefinition::Codex { config },
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
    fn into_output(self) -> Option<Output> {
        match self {
            Self::Text {
                source: TextOutputSourceDto::Path,
                path: Some(path),
            } => Some(Output::TextPath { path }),
            Self::Text {
                source: TextOutputSourceDto::AgentResponse,
                path: None,
            } => Some(Output::TextAgentResponse),
            Self::Json {
                source: JsonOutputSourceDto::Path,
                path: Some(path),
                schema,
            } => Some(Output::JsonPath { path, schema }),
            Self::Json {
                source: JsonOutputSourceDto::AgentResult,
                path: None,
                schema,
            } => Some(Output::JsonAgentResult { schema }),
            Self::File {
                source: PathOutputSourceDto::Path,
                path,
                media_type,
            } => Some(Output::FilePath { path, media_type }),
            Self::GitBranch {
                source: WorkspaceOutputSourceDto::Workspace,
            } => Some(Output::GitBranchWorkspace),
            Self::Text { .. } | Self::Json { .. } => None,
        }
    }
}

fn parse_value_reference(reference: &str) -> Option<ValueReference> {
    if reference == "finalization.context" {
        return Some(ValueReference::FinalizationContext);
    }
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
    let node = segments.next()?.to_owned();
    let output = segments.next()?.to_owned();
    if segments.next().is_some() {
        return None;
    }

    Some(OutputReference { node, output })
}
