use std::collections::BTreeMap;

use time::OffsetDateTime;

use super::agent::{
    AgentDiagnosticLevel, AgentLifecycleMilestone, AgentObservation, AgentObservationEnvelope,
    AgentToolCallPhase, AgentValueKind,
};
use super::document::Output;
use super::observation::{
    CommandOutputClosedObservation, CommandOutputObservation, CommandOutputSource,
    ExecutionObservation, SourceSequence, TransitionObservation,
};
use super::pi::Thinking;
use super::resolution::ResolvedWorkflow;
use super::runtime::{ActionId, TransitionEvent};
use super::validated::{ValidatedHarness, ValidatedStep};

pub(crate) const MAX_NORMALIZED_CHILD_RECORD_BYTES: usize = 16 * 1024;
const CONTROL_SEQUENCE_BYTES: usize = 4096;

pub(crate) trait DisplayDeadline: Clone + Send + 'static {
    fn deadline_utc(&self) -> OffsetDateTime;
}

impl DisplayDeadline for OffsetDateTime {
    fn deadline_utc(&self) -> OffsetDateTime {
        *self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowPresentationDefinition {
    pub(crate) workflow_path: String,
    pub(crate) presentation_order: Vec<String>,
    pub(crate) steps: BTreeMap<String, WorkflowPresentationStep>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowPresentationStep {
    Command {
        argv: Vec<String>,
        cwd: Option<String>,
        direct_dependencies: Vec<String>,
        outputs: BTreeMap<String, Output>,
    },
    Agent {
        profile: String,
        harness: AgentPresentationHarness,
        direct_dependencies: Vec<String>,
        outputs: BTreeMap<String, Output>,
    },
}

impl WorkflowPresentationStep {
    pub(crate) fn direct_dependencies(&self) -> &[String] {
        match self {
            Self::Command {
                direct_dependencies,
                ..
            }
            | Self::Agent {
                direct_dependencies,
                ..
            } => direct_dependencies,
        }
    }

    pub(crate) fn outputs(&self) -> &BTreeMap<String, Output> {
        match self {
            Self::Command { outputs, .. } | Self::Agent { outputs, .. } => outputs,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentPresentationHarness {
    Pi { model: String, thinking: Thinking },
}

impl WorkflowPresentationDefinition {
    fn from_workflow(workflow: &ResolvedWorkflow) -> Self {
        let steps = workflow
            .definition
            .steps
            .iter()
            .map(|(id, step)| {
                let presentation = match step {
                    ValidatedStep::Command(command) => WorkflowPresentationStep::Command {
                        argv: command.argv.clone(),
                        cwd: command.common.cwd.clone(),
                        direct_dependencies: command.common.prerequisites.clone(),
                        outputs: presentation_outputs(&command.common.outputs),
                    },
                    ValidatedStep::Agent(agent) => {
                        let ValidatedHarness::Pi(config) = &agent.agent.harness;
                        WorkflowPresentationStep::Agent {
                            profile: agent.agent.profile.clone(),
                            harness: AgentPresentationHarness::Pi {
                                model: config.model.clone(),
                                thinking: config.thinking,
                            },
                            direct_dependencies: agent.common.prerequisites.clone(),
                            outputs: presentation_outputs(&agent.common.outputs),
                        }
                    }
                };
                (id.clone(), presentation)
            })
            .collect();
        Self {
            workflow_path: workflow.source.workflow_path.clone(),
            presentation_order: workflow.definition.presentation_order.clone(),
            steps,
        }
    }
}

fn presentation_outputs(
    outputs: &BTreeMap<String, super::validated::ValidatedOutput>,
) -> BTreeMap<String, Output> {
    outputs
        .iter()
        .map(|(name, output)| (name.clone(), output.definition.clone()))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AcceptedRecordOrder(u64);

impl AcceptedRecordOrder {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PresentationRecord {
    pub(crate) accepted_order: AcceptedRecordOrder,
    pub(crate) observed_at: OffsetDateTime,
    pub(crate) kind: PresentationRecordKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PresentationRecordKind {
    Transition(PresentationTransition),
    ChildOutput(NormalizedChildOutput),
    AgentObservation(NormalizedAgentObservation),
}

pub(crate) type PresentationTransition = TransitionObservation<OffsetDateTime>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedChildOutput {
    pub(crate) step: String,
    pub(crate) invocation: ActionId,
    pub(crate) source: CommandOutputSource,
    pub(crate) source_sequence: SourceSequence,
    pub(crate) payload: String,
    pub(crate) continuation: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentPresentationObservationKind {
    Assistant,
    Reasoning,
    ToolCall,
    ToolResult,
    Diagnostic,
    Usage,
    Model,
    Lifecycle,
    ValueRejected,
    HarnessEvent,
}

impl AgentPresentationObservationKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Assistant => "assistant",
            Self::Reasoning => "reasoning",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::Diagnostic => "diagnostic",
            Self::Usage => "usage",
            Self::Model => "model",
            Self::Lifecycle => "lifecycle",
            Self::ValueRejected => "value_rejected",
            Self::HarnessEvent => "harness_event",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAgentObservation {
    pub(crate) step: String,
    pub(crate) invocation: ActionId,
    pub(crate) observation_sequence: u64,
    pub(crate) kind: AgentPresentationObservationKind,
    pub(crate) payload: String,
    pub(crate) continuation: bool,
}

pub(crate) struct WorkflowPresentationFeed {
    definition: WorkflowPresentationDefinition,
    child_streams: BTreeMap<ChildStreamKey, ActiveChildStream>,
    next_accepted_order: u64,
}

impl WorkflowPresentationFeed {
    pub(crate) fn new(workflow: &ResolvedWorkflow) -> Self {
        Self {
            definition: WorkflowPresentationDefinition::from_workflow(workflow),
            child_streams: BTreeMap::new(),
            next_accepted_order: 1,
        }
    }

    pub(crate) fn definition(&self) -> &WorkflowPresentationDefinition {
        &self.definition
    }

    pub(crate) fn accept<Deadline: DisplayDeadline>(
        &mut self,
        observed_at: OffsetDateTime,
        observation: ExecutionObservation<Deadline>,
    ) -> Vec<PresentationRecord> {
        match observation {
            ExecutionObservation::Transition(transition) => vec![self.record(
                observed_at,
                PresentationRecordKind::Transition(normalize_transition(transition)),
            )],
            ExecutionObservation::CommandOutput(output) => {
                self.accept_child_output(observed_at, output)
            }
            ExecutionObservation::CommandOutputClosed(closed) => {
                self.close_child_output(observed_at, closed)
            }
            ExecutionObservation::Agent(observation) => {
                self.accept_agent_observation(observed_at, observation)
            }
        }
    }

    pub(crate) fn finish_child_streams(
        &mut self,
        observed_at: OffsetDateTime,
    ) -> Vec<PresentationRecord> {
        let streams = std::mem::take(&mut self.child_streams);
        let mut presentation = Vec::new();
        for (key, stream) in streams {
            let source = key.source();
            let identity = ChildRecordIdentity {
                step: key.step,
                invocation: key.invocation,
                source,
                source_sequence: stream.next_source_sequence,
            };
            for record in stream.framing.close() {
                presentation.push(self.child_record(observed_at, &identity, record));
            }
        }
        presentation
    }

    fn accept_child_output(
        &mut self,
        observed_at: OffsetDateTime,
        output: CommandOutputObservation,
    ) -> Vec<PresentationRecord> {
        let identity = ChildRecordIdentity {
            step: output.step,
            invocation: output.invocation,
            source: output.source,
            source_sequence: output.sequence,
        };
        let key = ChildStreamKey::from_identity(&identity);
        let stream = self
            .child_streams
            .entry(key)
            .or_insert_with(|| ActiveChildStream {
                framing: ChildStream::default(),
                next_source_sequence: identity.source_sequence,
            });
        stream.next_source_sequence = identity.source_sequence.next();
        let normalized = stream.framing.push(&output.bytes);
        normalized
            .into_iter()
            .map(|record| self.child_record(observed_at, &identity, record))
            .collect()
    }

    fn close_child_output(
        &mut self,
        observed_at: OffsetDateTime,
        closed: CommandOutputClosedObservation,
    ) -> Vec<PresentationRecord> {
        let identity = ChildRecordIdentity {
            step: closed.step,
            invocation: closed.invocation,
            source: closed.source,
            source_sequence: closed.sequence,
        };
        let key = ChildStreamKey::from_identity(&identity);
        self.child_streams
            .remove(&key)
            .map_or_else(Vec::new, |stream| stream.framing.close())
            .into_iter()
            .map(|record| self.child_record(observed_at, &identity, record))
            .collect()
    }

    fn accept_agent_observation(
        &mut self,
        observed_at: OffsetDateTime,
        envelope: AgentObservationEnvelope,
    ) -> Vec<PresentationRecord> {
        let step = envelope.step().to_owned();
        let invocation = envelope.invocation();
        let observation_sequence = envelope.sequence().get();
        let (kind, payload) = normalized_agent_payload(envelope.observation());
        let mut stream = ChildStream::default();
        let mut normalized = stream.push(payload.as_bytes());
        normalized.extend(stream.close());
        normalized
            .into_iter()
            .map(|record| {
                self.record(
                    observed_at,
                    PresentationRecordKind::AgentObservation(NormalizedAgentObservation {
                        step: step.clone(),
                        invocation,
                        observation_sequence,
                        kind,
                        payload: record.payload,
                        continuation: record.continuation,
                    }),
                )
            })
            .collect()
    }

    fn child_record(
        &mut self,
        observed_at: OffsetDateTime,
        identity: &ChildRecordIdentity,
        record: ChildRecord,
    ) -> PresentationRecord {
        self.record(
            observed_at,
            PresentationRecordKind::ChildOutput(NormalizedChildOutput {
                step: identity.step.clone(),
                invocation: identity.invocation,
                source: identity.source,
                source_sequence: identity.source_sequence,
                payload: record.payload,
                continuation: record.continuation,
            }),
        )
    }

    fn record(
        &mut self,
        observed_at: OffsetDateTime,
        kind: PresentationRecordKind,
    ) -> PresentationRecord {
        let accepted_order = AcceptedRecordOrder(self.next_accepted_order);
        self.next_accepted_order = self.next_accepted_order.saturating_add(1);
        PresentationRecord {
            accepted_order,
            observed_at,
            kind,
        }
    }
}

fn normalize_transition<Deadline: DisplayDeadline>(
    transition: TransitionObservation<Deadline>,
) -> PresentationTransition {
    let event = match transition.event {
        TransitionEvent::Step {
            sequence,
            step,
            from,
            to,
        } => TransitionEvent::Step {
            sequence,
            step,
            from,
            to,
        },
        TransitionEvent::Workflow { sequence, from, to } => {
            TransitionEvent::Workflow { sequence, from, to }
        }
        TransitionEvent::CancellationAccepted {
            sequence,
            reason,
            deadline,
        } => TransitionEvent::CancellationAccepted {
            sequence,
            reason,
            deadline: deadline.deadline_utc(),
        },
    };
    PresentationTransition {
        event,
        step: transition.step,
    }
}

fn normalized_agent_payload(
    observation: &AgentObservation,
) -> (AgentPresentationObservationKind, String) {
    match observation {
        AgentObservation::AssistantText { text } => (
            AgentPresentationObservationKind::Assistant,
            text.to_string(),
        ),
        AgentObservation::Reasoning { text } => (
            AgentPresentationObservationKind::Reasoning,
            text.to_string(),
        ),
        AgentObservation::ToolCall {
            call_id,
            name,
            phase,
        } => (
            AgentPresentationObservationKind::ToolCall,
            format!("{} · {} · {}", agent_tool_call_phase(*phase), name, call_id),
        ),
        AgentObservation::ToolResult {
            call_id,
            is_error,
            content,
        } => (
            AgentPresentationObservationKind::ToolResult,
            format!(
                "{} · {} · {}",
                call_id,
                if *is_error { "error" } else { "ok" },
                content
            ),
        ),
        AgentObservation::Diagnostic { level, message } => (
            AgentPresentationObservationKind::Diagnostic,
            format!("{} · {}", agent_diagnostic_level(*level), message),
        ),
        AgentObservation::Usage {
            input_tokens,
            output_tokens,
        } => (
            AgentPresentationObservationKind::Usage,
            format!("input {input_tokens} · output {output_tokens}"),
        ),
        AgentObservation::Model { name } => {
            (AgentPresentationObservationKind::Model, name.to_string())
        }
        AgentObservation::Lifecycle { milestone } => (
            AgentPresentationObservationKind::Lifecycle,
            agent_lifecycle_milestone(*milestone).to_owned(),
        ),
        AgentObservation::ValueRejected { kind, feedback } => (
            AgentPresentationObservationKind::ValueRejected,
            format!("{} · {}", agent_value_kind(*kind), feedback),
        ),
        AgentObservation::UnrecognizedHarnessEvent { .. } => (
            AgentPresentationObservationKind::HarnessEvent,
            "unrecognized harness event".to_owned(),
        ),
    }
}

const fn agent_tool_call_phase(phase: AgentToolCallPhase) -> &'static str {
    match phase {
        AgentToolCallPhase::Started => "started",
        AgentToolCallPhase::Updated => "updated",
        AgentToolCallPhase::Completed => "completed",
    }
}

const fn agent_diagnostic_level(level: AgentDiagnosticLevel) -> &'static str {
    match level {
        AgentDiagnosticLevel::Information => "information",
        AgentDiagnosticLevel::Warning => "warning",
        AgentDiagnosticLevel::Error => "error",
    }
}

const fn agent_value_kind(kind: AgentValueKind) -> &'static str {
    match kind {
        AgentValueKind::None => "none",
        AgentValueKind::Response => "response",
        AgentValueKind::Result => "result",
    }
}

const fn agent_lifecycle_milestone(milestone: AgentLifecycleMilestone) -> &'static str {
    match milestone {
        AgentLifecycleMilestone::SessionEstablished => "session_established",
        AgentLifecycleMilestone::HarnessStarted => "harness_started",
        AgentLifecycleMilestone::MessageStarted => "message_started",
        AgentLifecycleMilestone::MessageUpdated => "message_updated",
        AgentLifecycleMilestone::MessageCompleted => "message_completed",
        AgentLifecycleMilestone::TurnStarted => "turn_started",
        AgentLifecycleMilestone::TurnCompleted => "turn_completed",
        AgentLifecycleMilestone::RetryStarted => "retry_started",
        AgentLifecycleMilestone::RetryCompleted => "retry_completed",
        AgentLifecycleMilestone::CompactionStarted => "compaction_started",
        AgentLifecycleMilestone::CompactionCompleted => "compaction_completed",
        AgentLifecycleMilestone::QueueUpdated => "queue_updated",
        AgentLifecycleMilestone::HarnessCompleted => "harness_completed",
        AgentLifecycleMilestone::HarnessQuiescent => "harness_quiescent",
    }
}

struct ChildRecordIdentity {
    step: String,
    invocation: ActionId,
    source: CommandOutputSource,
    source_sequence: SourceSequence,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ChildStreamKey {
    step: String,
    invocation: ActionId,
    source: u8,
}

impl ChildStreamKey {
    fn from_identity(identity: &ChildRecordIdentity) -> Self {
        Self {
            step: identity.step.clone(),
            invocation: identity.invocation,
            source: source_number(identity.source),
        }
    }

    fn source(&self) -> CommandOutputSource {
        if self.source == 0 {
            CommandOutputSource::StandardOutput
        } else {
            CommandOutputSource::StandardError
        }
    }
}

fn source_number(source: CommandOutputSource) -> u8 {
    match source {
        CommandOutputSource::StandardOutput => 0,
        CommandOutputSource::StandardError => 1,
    }
}

struct ActiveChildStream {
    framing: ChildStream,
    next_source_sequence: SourceSequence,
}

#[derive(Default)]
struct ChildStream {
    normalizer: ChildNormalizer,
    pending_carriage_return: bool,
    line_has_data: bool,
    line_has_record: bool,
}

struct ChildRecord {
    payload: String,
    continuation: bool,
}

impl ChildStream {
    fn push(&mut self, bytes: &[u8]) -> Vec<ChildRecord> {
        let mut records = Vec::new();
        for &byte in bytes {
            if self.pending_carriage_return {
                self.pending_carriage_return = false;
                self.finish_line(&mut records, true);
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\r' => self.pending_carriage_return = true,
                b'\n' => self.finish_line(&mut records, true),
                _ => {
                    self.line_has_data = true;
                    self.normalizer
                        .push(byte, &mut records, &mut self.line_has_record);
                }
            }
        }
        records
    }

    fn close(mut self) -> Vec<ChildRecord> {
        let mut records = Vec::new();
        if self.pending_carriage_return {
            self.pending_carriage_return = false;
            self.finish_line(&mut records, true);
        } else if self.line_has_data {
            self.finish_line(&mut records, false);
        }
        records
    }

    fn finish_line(&mut self, records: &mut Vec<ChildRecord>, framed: bool) {
        self.normalizer.finish(records, &mut self.line_has_record);
        if !self.normalizer.payload.is_empty()
            || (!self.line_has_record && (framed || self.line_has_data))
        {
            records.push(ChildRecord {
                payload: std::mem::take(&mut self.normalizer.payload),
                continuation: self.line_has_record,
            });
        }
        self.normalizer.reset_line();
        self.line_has_data = false;
        self.line_has_record = false;
    }
}

#[derive(Default)]
struct ChildNormalizer {
    payload: String,
    column: usize,
    utf8: Vec<u8>,
    control: Option<ControlCandidate>,
}

struct ControlCandidate {
    bytes: Vec<u8>,
    kind: ControlKind,
}

#[derive(Clone, Copy)]
enum ControlKind {
    Dispatch,
    Csi { intermediates: bool },
    Osc,
    String,
}

impl ChildNormalizer {
    fn push(&mut self, byte: u8, records: &mut Vec<ChildRecord>, line_has_record: &mut bool) {
        if self.control.is_some() {
            self.push_control(byte, records, line_has_record);
        } else if byte == 0x1b {
            self.finish_utf8(records, line_has_record);
            self.control = Some(ControlCandidate {
                bytes: vec![byte],
                kind: ControlKind::Dispatch,
            });
        } else {
            self.push_ordinary(byte, records, line_has_record);
        }
    }

    fn push_control(
        &mut self,
        byte: u8,
        records: &mut Vec<ChildRecord>,
        line_has_record: &mut bool,
    ) {
        let Some(mut candidate) = self.control.take() else {
            return;
        };
        candidate.bytes.push(byte);
        let complete = match candidate.kind {
            ControlKind::Dispatch => match byte {
                b'[' => {
                    candidate.kind = ControlKind::Csi {
                        intermediates: false,
                    };
                    false
                }
                b']' => {
                    candidate.kind = ControlKind::Osc;
                    false
                }
                b'P' | b'X' | b'^' | b'_' => {
                    candidate.kind = ControlKind::String;
                    false
                }
                0x30..=0x7e => true,
                _ => {
                    self.abandon(candidate, records, line_has_record);
                    return;
                }
            },
            ControlKind::Csi { intermediates } => {
                if (0x40..=0x7e).contains(&byte) {
                    true
                } else if !intermediates && (0x30..=0x3f).contains(&byte) {
                    false
                } else if (0x20..=0x2f).contains(&byte) {
                    candidate.kind = ControlKind::Csi {
                        intermediates: true,
                    };
                    false
                } else {
                    self.abandon(candidate, records, line_has_record);
                    return;
                }
            }
            ControlKind::Osc => byte == 0x07 || candidate.bytes.ends_with(b"\x1b\\"),
            ControlKind::String => candidate.bytes.ends_with(b"\x1b\\"),
        };
        if complete {
            return;
        }
        if candidate.bytes.len() == CONTROL_SEQUENCE_BYTES {
            self.abandon(candidate, records, line_has_record);
        } else {
            self.control = Some(candidate);
        }
    }

    fn abandon(
        &mut self,
        candidate: ControlCandidate,
        records: &mut Vec<ChildRecord>,
        line_has_record: &mut bool,
    ) {
        for byte in candidate.bytes {
            self.push_ordinary(byte, records, line_has_record);
        }
        self.finish_utf8(records, line_has_record);
    }

    fn push_ordinary(
        &mut self,
        byte: u8,
        records: &mut Vec<ChildRecord>,
        line_has_record: &mut bool,
    ) {
        if self.utf8.is_empty() {
            if byte.is_ascii() {
                self.emit_ascii(byte, records, line_has_record);
            } else if utf8_length(byte).is_some() {
                self.utf8.push(byte);
            } else {
                self.emit_invalid_byte(byte, records, line_has_record);
            }
            return;
        }

        self.utf8.push(byte);
        let expected = utf8_length(self.utf8[0]).unwrap_or(1);
        if self.utf8.len() < expected && (byte & 0xc0) == 0x80 {
            return;
        }
        let bytes = std::mem::take(&mut self.utf8);
        if bytes.len() == expected
            && let Ok(value) = std::str::from_utf8(&bytes)
            && let Some(character) = value.chars().next()
        {
            self.emit_character(character, records, line_has_record);
            return;
        }
        self.emit_invalid_byte(bytes[0], records, line_has_record);
        for byte in bytes.into_iter().skip(1) {
            self.push_ordinary(byte, records, line_has_record);
        }
    }

    fn emit_ascii(&mut self, byte: u8, records: &mut Vec<ChildRecord>, line_has_record: &mut bool) {
        match byte {
            b'\t' => {
                let spaces = 8 - self.column % 8;
                self.emit_text(&" ".repeat(spaces), records, line_has_record);
                self.column += spaces;
            }
            0x20..=0x7e => {
                self.emit_text(&char::from(byte).to_string(), records, line_has_record);
                self.column += 1;
            }
            _ => self.emit_invalid_byte(byte, records, line_has_record),
        }
    }

    fn emit_character(
        &mut self,
        character: char,
        records: &mut Vec<ChildRecord>,
        line_has_record: &mut bool,
    ) {
        if is_unicode_control(character) {
            let escaped = format!("\\u{{{:x}}}", u32::from(character));
            self.column += escaped.len();
            self.emit_text(&escaped, records, line_has_record);
        } else {
            self.column += 1;
            self.emit_text(&character.to_string(), records, line_has_record);
        }
    }

    fn emit_invalid_byte(
        &mut self,
        byte: u8,
        records: &mut Vec<ChildRecord>,
        line_has_record: &mut bool,
    ) {
        let escaped = format!("\\x{byte:02x}");
        self.column += escaped.len();
        self.emit_text(&escaped, records, line_has_record);
    }

    fn emit_text(
        &mut self,
        text: &str,
        records: &mut Vec<ChildRecord>,
        line_has_record: &mut bool,
    ) {
        if !self.payload.is_empty()
            && self.payload.len() + text.len() > MAX_NORMALIZED_CHILD_RECORD_BYTES
        {
            self.emit_fragment(records, line_has_record);
        }
        self.payload.push_str(text);
        if self.payload.len() >= MAX_NORMALIZED_CHILD_RECORD_BYTES {
            self.emit_fragment(records, line_has_record);
        }
    }

    fn emit_fragment(&mut self, records: &mut Vec<ChildRecord>, line_has_record: &mut bool) {
        records.push(ChildRecord {
            payload: std::mem::take(&mut self.payload),
            continuation: *line_has_record,
        });
        *line_has_record = true;
    }

    fn finish(&mut self, records: &mut Vec<ChildRecord>, line_has_record: &mut bool) {
        if let Some(candidate) = self.control.take() {
            self.abandon(candidate, records, line_has_record);
        }
        self.finish_utf8(records, line_has_record);
    }

    fn finish_utf8(&mut self, records: &mut Vec<ChildRecord>, line_has_record: &mut bool) {
        let pending = std::mem::take(&mut self.utf8);
        for byte in pending {
            self.emit_invalid_byte(byte, records, line_has_record);
        }
    }

    fn reset_line(&mut self) {
        self.payload.clear();
        self.column = 0;
        self.utf8.clear();
        self.control = None;
    }
}

fn utf8_length(byte: u8) -> Option<usize> {
    match byte {
        0xc2..=0xdf => Some(2),
        0xe0..=0xef => Some(3),
        0xf0..=0xf4 => Some(4),
        _ => None,
    }
}

pub(crate) fn is_unicode_control(character: char) -> bool {
    let value = u32::from(character);
    matches!(value, 0x80..=0x9f | 0xad | 0x61c | 0x6dd | 0x70f | 0x890..=0x891 | 0x8e2 | 0x180e | 0x200b..=0x200f | 0x202a..=0x202e | 0x2060..=0x2064 | 0x2066..=0x206f | 0xfeff | 0xfff9..=0xfffb | 0x110bd | 0x110cd | 0x13430..=0x1345f | 0x1bca0..=0x1bca3 | 0x1d173..=0x1d17a | 0xe0001 | 0xe0020..=0xe007f)
        || matches!(value, 0x600..=0x605)
}

#[cfg(test)]
mod tests;
