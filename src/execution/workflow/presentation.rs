use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::future::{Future, ready};
use std::io::{self, IsTerminal, Write};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::Serialize;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};
use tokio::sync::watch;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::admission::{AdmissionFailure, CancellationReason};
use super::artifact::CaptureFailureKind;
use super::document::Output;
use super::input::InputPreparationFailureKind;
use super::observation::{
    CommandOutputClosedObservation, CommandOutputObservation, CommandOutputSource,
    ExecutionObservation, ExecutionObserver, ObservedStepTransition, TransitionObservation,
};
use super::publication::{
    LocalPublicationError, WorkflowRunResult, WorkflowRunStep, WorkflowRunTerminalResultV1,
};
use super::rejection::RejectionDiagnostic;
use super::resolution::{ResolutionFailure, ResolvedWorkflow};
use super::runtime::{
    FailurePhase, NotRunReason, RunOutcome, StepState, StepStateKind, TransitionEvent,
};
use super::step_runtime::{
    CommandExecutionFailure, CommandLaunchFailure, CommandPreparationFailure, OutputCaptureFailure,
    StepBodyKind, StepExecutionFailure, StepFailureCause, StepStartFailure,
    WorkingDirectoryFailure,
};
use super::validated::{ValidatedHarness, ValidatedStep};

const COMMAND: &str = "scherzo-cloud workflow run";
const CHILD_FRAGMENT_BYTES: usize = 16 * 1024;
const CONTROL_SEQUENCE_BYTES: usize = 4096;
const EVENT_TOKEN_WIDTH: usize = 10;
const MIN_INLINE_DETAIL_WIDTH: usize = 24;
const STACKED_DETAIL_INDENT: usize = 2;
const STACKED_CONTINUATION_INDENT: usize = 4;
const SAFETY_CONTINUATION_MARKER: &str = "↪";
const VISUAL_CONTINUATION_MARKER: &str = "↳";
const STYLE_PRIMARY: &str = "38;2;205;214;244";
const STYLE_SECONDARY: &str = "38;2;166;173;200";
const STYLE_MUTED: &str = "38;2;127;132;156";
const STYLE_ACTIVE: &str = "38;2;137;180;250";
const STYLE_OUTPUT: &str = "38;2;148;226;213";
const STYLE_SUCCESS: &str = "38;2;166;227;161";
const STYLE_FAILURE: &str = "38;2;243;139;168";
const STYLE_BLOCKED: &str = "38;2;250;179;135";
const STYLE_CONTINUATION: &str = "2;38;2;127;132;156";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestedPresentationMode {
    Automatic,
    Plain,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PresentationMode {
    Plain,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColorChoice {
    Auto,
    Always,
    Never,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TerminalCapabilities {
    pub(crate) stdout_is_terminal: bool,
    pub(crate) stderr_is_terminal: bool,
    pub(crate) stdout_width: Option<usize>,
    pub(crate) stderr_width: Option<usize>,
    pub(crate) term: Option<OsString>,
    pub(crate) no_color: Option<OsString>,
}

impl TerminalCapabilities {
    pub(crate) fn detect() -> Self {
        let stdout = io::stdout();
        let stderr = io::stderr();
        let stdout_is_terminal = stdout.is_terminal();
        let stderr_is_terminal = stderr.is_terminal();
        Self {
            stdout_is_terminal,
            stderr_is_terminal,
            stdout_width: if stdout_is_terminal {
                detected_terminal_width(&stdout)
            } else {
                None
            },
            stderr_width: if stderr_is_terminal {
                detected_terminal_width(&stderr)
            } else {
                None
            },
            term: std::env::var_os("TERM"),
            no_color: std::env::var_os("NO_COLOR"),
        }
    }
}

#[cfg(unix)]
fn detected_terminal_width<Fd: rustix::fd::AsFd>(descriptor: Fd) -> Option<usize> {
    rustix::termios::tcgetwinsize(descriptor)
        .ok()
        .map(|size| usize::from(size.ws_col))
        .filter(|width| *width != 0)
}

#[cfg(not(unix))]
fn detected_terminal_width<Descriptor>(_descriptor: &Descriptor) -> Option<usize> {
    None
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PresentationConfig {
    pub(crate) requested_mode: RequestedPresentationMode,
    pub(crate) color: ColorChoice,
    pub(crate) capabilities: TerminalCapabilities,
}

impl PresentationConfig {
    pub(crate) fn mode(&self) -> PresentationMode {
        match self.requested_mode {
            // The TUI is not yet an available projection, so automatic human output is plain.
            RequestedPresentationMode::Automatic | RequestedPresentationMode::Plain => {
                PresentationMode::Plain
            }
            RequestedPresentationMode::Json => PresentationMode::Json,
        }
    }

    pub(crate) fn color_enabled(&self) -> bool {
        match self.color {
            ColorChoice::Always => true,
            ColorChoice::Never => false,
            ColorChoice::Auto => {
                let destination_is_terminal = match self.mode() {
                    PresentationMode::Plain => self.capabilities.stdout_is_terminal,
                    PresentationMode::Json => self.capabilities.stderr_is_terminal,
                };
                destination_is_terminal
                    && usable_term(self.capabilities.term.as_deref())
                    && self
                        .capabilities
                        .no_color
                        .as_deref()
                        .is_none_or(OsStr::is_empty)
            }
        }
    }

    fn wrapping_width(&self) -> Option<usize> {
        match self.mode() {
            PresentationMode::Plain if self.capabilities.stdout_is_terminal => {
                self.capabilities.stdout_width
            }
            PresentationMode::Json if self.capabilities.stderr_is_terminal => {
                self.capabilities.stderr_width
            }
            PresentationMode::Plain | PresentationMode::Json => None,
        }
    }
}

fn usable_term(term: Option<&OsStr>) -> bool {
    term.is_some_and(|term| !term.is_empty() && term.as_encoded_bytes() != b"dumb")
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservationTime {
    utc: OffsetDateTime,
    monotonic: Instant,
}

pub(crate) trait ObservationClock: Clone + Send + Sync + 'static {
    fn sample(&self) -> ObservationTime;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SystemObservationClock;

impl ObservationClock for SystemObservationClock {
    fn sample(&self) -> ObservationTime {
        ObservationTime {
            utc: crate::timing::utc_now(),
            monotonic: crate::timing::monotonic_now(),
        }
    }
}

pub(crate) trait DisplayDeadline: Clone + Send + 'static {
    fn deadline_utc(&self) -> OffsetDateTime;
}

impl DisplayDeadline for OffsetDateTime {
    fn deadline_utc(&self) -> OffsetDateTime {
        *self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PresentationFailureOperation {
    HeaderWriter,
    LineWriter,
    DiagnosticWriter,
    TerminalJsonWriter,
    TimestampFormatting,
    UnsupportedRejection,
    InvalidTerminalResult,
    AlreadyFinished,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PresentationFailure {
    pub(crate) operation: PresentationFailureOperation,
    pub(crate) error_kind: Option<io::ErrorKind>,
    pub(crate) result_directory: Option<String>,
}

impl PresentationFailure {
    fn writer(operation: PresentationFailureOperation, error: &io::Error) -> Self {
        Self {
            operation,
            error_kind: Some(error.kind()),
            result_directory: None,
        }
    }

    fn operation(operation: PresentationFailureOperation) -> Self {
        Self {
            operation,
            error_kind: None,
            result_directory: None,
        }
    }

    fn with_result_directory(mut self, result_directory: Option<&str>) -> Self {
        self.result_directory = result_directory.map(str::to_owned);
        self
    }
}

impl fmt::Display for PresentationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "workflow run output failure: {:?}",
            self.operation
        )?;
        if let Some(kind) = self.error_kind {
            write!(formatter, " ({kind:?})")?;
        }
        if let Some(path) = &self.result_directory {
            write!(formatter, "; result published at {}", visible_text(path))?;
        }
        Ok(())
    }
}

impl std::error::Error for PresentationFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunPresentationResult {
    Rejected,
    Published {
        exit_status: u16,
        result_directory: String,
    },
    PublicationFailed,
    Failed(PresentationFailure),
}

pub(crate) enum PublicationPresentation<'a> {
    Published(&'a WorkflowRunTerminalResultV1),
    Failed(&'a LocalPublicationError),
}

pub(crate) struct WorkflowRunOutput<StandardOutput, StandardError> {
    config: PresentationConfig,
    standard_output: StandardOutput,
    standard_error: StandardError,
}

impl<StandardOutput, StandardError> WorkflowRunOutput<StandardOutput, StandardError>
where
    StandardOutput: Write + Send + 'static,
    StandardError: Write + Send + 'static,
{
    pub(crate) fn new(
        config: PresentationConfig,
        standard_output: StandardOutput,
        standard_error: StandardError,
    ) -> Self {
        Self {
            config,
            standard_output,
            standard_error,
        }
    }

    pub(crate) fn render_resolution_rejection(
        mut self,
        failure: &ResolutionFailure,
    ) -> WorkflowRunPresentationResult {
        let diagnostic = RejectionDiagnostic::from_resolution(failure);
        let rejection = TerminalRejectionV1 {
            schema_version: 1,
            command: COMMAND,
            outcome: "rejected",
            exit_status: 1,
            phase: "resolution",
            workflow: failure
                .workflow_path()
                .map(|path| RejectedWorkflowV1 { path }),
            diagnostics: [diagnostic.clone()],
        };
        self.write_rejection(&rejection, &diagnostic)
    }

    pub(crate) fn render_admission_rejection(
        mut self,
        workflow: &ResolvedWorkflow,
        failure: &AdmissionFailure,
    ) -> WorkflowRunPresentationResult {
        let Some(diagnostic) = RejectionDiagnostic::from_admission(failure) else {
            return WorkflowRunPresentationResult::Failed(PresentationFailure::operation(
                PresentationFailureOperation::UnsupportedRejection,
            ));
        };
        let rejection = TerminalRejectionV1 {
            schema_version: 1,
            command: COMMAND,
            outcome: "rejected",
            exit_status: 1,
            phase: "admission",
            workflow: Some(RejectedWorkflowV1 {
                path: &workflow.source.workflow_path,
            }),
            diagnostics: [diagnostic.clone()],
        };
        self.write_rejection(&rejection, &diagnostic)
    }

    fn write_rejection(
        &mut self,
        rejection: &TerminalRejectionV1<'_>,
        diagnostic: &RejectionDiagnostic<'_>,
    ) -> WorkflowRunPresentationResult {
        let result = match self.config.mode() {
            PresentationMode::Plain => writeln!(
                self.standard_error,
                "Error: workflow rejected: {} at {}: {}",
                diagnostic.code, diagnostic.location, diagnostic.message
            )
            .and_then(|()| self.standard_error.flush())
            .map_err(|error| {
                PresentationFailure::writer(PresentationFailureOperation::DiagnosticWriter, &error)
            }),
            PresentationMode::Json => write_pretty_json(&mut self.standard_output, rejection)
                .map_err(|error| {
                    PresentationFailure::writer(
                        PresentationFailureOperation::TerminalJsonWriter,
                        &error,
                    )
                }),
        };
        match result {
            Ok(()) => WorkflowRunPresentationResult::Rejected,
            Err(failure) => {
                if self.config.mode() == PresentationMode::Json {
                    let _ = writeln!(self.standard_error, "Error: {failure}")
                        .and_then(|()| self.standard_error.flush());
                }
                WorkflowRunPresentationResult::Failed(failure)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn start<Clock>(
        self,
        workflow: &ResolvedWorkflow,
        maximum_parallel_steps: usize,
        clock: Clock,
    ) -> Result<WorkflowRunPresentation<StandardOutput, StandardError, Clock>, PresentationFailure>
    where
        Clock: ObservationClock,
    {
        self.start_for_result(workflow, "result", maximum_parallel_steps, clock)
    }

    pub(crate) fn start_for_result<Clock>(
        self,
        workflow: &ResolvedWorkflow,
        result_directory: &str,
        maximum_parallel_steps: usize,
        clock: Clock,
    ) -> Result<WorkflowRunPresentation<StandardOutput, StandardError, Clock>, PresentationFailure>
    where
        Clock: ObservationClock,
    {
        WorkflowRunPresentation::start(
            self.config,
            self.standard_output,
            self.standard_error,
            workflow,
            result_directory,
            maximum_parallel_steps,
            clock,
        )
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TerminalRejectionV1<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    phase: &'static str,
    workflow: Option<RejectedWorkflowV1<'a>>,
    diagnostics: [RejectionDiagnostic<'a>; 1],
}

#[derive(Serialize)]
struct RejectedWorkflowV1<'a> {
    path: &'a str,
}

fn write_pretty_json(writer: &mut impl Write, value: &impl Serialize) -> io::Result<()> {
    serde_json::to_writer_pretty(&mut *writer, value).map_err(|error| {
        let kind = error.io_error_kind().unwrap_or(io::ErrorKind::Other);
        io::Error::new(kind, error)
    })?;
    writer.write_all(b"\n")?;
    writer.flush()
}

pub(crate) struct WorkflowRunPresentation<StandardOutput, StandardError, Clock> {
    state: Arc<Mutex<PresentationState<StandardOutput, StandardError>>>,
    clock: Clock,
}

impl<StandardOutput, StandardError, Clock> Clone
    for WorkflowRunPresentation<StandardOutput, StandardError, Clock>
where
    Clock: Clone,
{
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            clock: self.clock.clone(),
        }
    }
}

impl<StandardOutput, StandardError, Clock>
    WorkflowRunPresentation<StandardOutput, StandardError, Clock>
where
    StandardOutput: Write + Send + 'static,
    StandardError: Write + Send + 'static,
    Clock: ObservationClock,
{
    fn start(
        config: PresentationConfig,
        standard_output: StandardOutput,
        standard_error: StandardError,
        workflow: &ResolvedWorkflow,
        result_directory: &str,
        maximum_parallel_steps: usize,
        clock: Clock,
    ) -> Result<Self, PresentationFailure> {
        let (failure_sender, _) = watch::channel(None);
        let mut state = PresentationState {
            mode: config.mode(),
            color: config.color_enabled(),
            terminal_width: config.wrapping_width(),
            standard_output,
            standard_error,
            definition: PresentationDefinition::from_workflow(workflow, result_directory),
            child_streams: BTreeMap::new(),
            step_starts: BTreeMap::new(),
            failure: None,
            failure_sender,
            finished: false,
        };
        let opened_at = clock.sample().utc;
        if let Err(failure) = state.write_header(opened_at, maximum_parallel_steps) {
            state.report_output_failure(&failure);
            return Err(failure);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            clock,
        })
    }

    pub(crate) fn subscribe_failures(&self) -> watch::Receiver<Option<PresentationFailure>> {
        lock_state(&self.state).failure_sender.subscribe()
    }

    pub(crate) fn failure(&self) -> Option<PresentationFailure> {
        lock_state(&self.state).failure.clone()
    }

    pub(crate) fn finish(
        &self,
        run: &WorkflowRunResult,
        publication: PublicationPresentation<'_>,
    ) -> WorkflowRunPresentationResult {
        self.finish_internal(run, publication, true)
    }

    pub(crate) fn finish_without_terminal_json(
        &self,
        run: &WorkflowRunResult,
        publication: PublicationPresentation<'_>,
    ) -> WorkflowRunPresentationResult {
        self.finish_internal(run, publication, false)
    }

    fn finish_internal(
        &self,
        run: &WorkflowRunResult,
        publication: PublicationPresentation<'_>,
        emit_terminal_json: bool,
    ) -> WorkflowRunPresentationResult {
        let mut state = lock_state(&self.state);
        if state.finished {
            return WorkflowRunPresentationResult::Failed(PresentationFailure::operation(
                PresentationFailureOperation::AlreadyFinished,
            ));
        }
        state.finished = true;
        let result_directory = match publication {
            PublicationPresentation::Published(terminal) => Some(terminal.result_directory()),
            PublicationPresentation::Failed(_) => None,
        };
        if let Some(failure) = state.failure.clone() {
            let failure = failure.with_result_directory(result_directory);
            state.report_output_failure(&failure);
            return WorkflowRunPresentationResult::Failed(failure);
        }

        let observed_at = self.clock.sample().utc;
        if let Err(failure) = state.finish_child_streams(observed_at) {
            let failure = failure.with_result_directory(result_directory);
            state.report_output_failure(&failure);
            return WorkflowRunPresentationResult::Failed(failure);
        }
        if let Err(failure) = state.write_summary(run, &publication) {
            let failure = failure.with_result_directory(result_directory);
            state.report_output_failure(&failure);
            return WorkflowRunPresentationResult::Failed(failure);
        }

        match publication {
            PublicationPresentation::Failed(error) => {
                state.write_publication_diagnostic(error);
                WorkflowRunPresentationResult::PublicationFailed
            }
            PublicationPresentation::Published(terminal) => {
                if emit_terminal_json
                    && state.mode == PresentationMode::Json
                    && let Err(error) = write_pretty_json(&mut state.standard_output, terminal)
                {
                    let failure = PresentationFailure::writer(
                        PresentationFailureOperation::TerminalJsonWriter,
                        &error,
                    )
                    .with_result_directory(Some(terminal.result_directory()));
                    state.report_output_failure(&failure);
                    return WorkflowRunPresentationResult::Failed(failure);
                }
                WorkflowRunPresentationResult::Published {
                    exit_status: terminal.exit_status(),
                    result_directory: terminal.result_directory().to_owned(),
                }
            }
        }
    }
}

impl<StandardOutput, StandardError, Clock, Deadline> ExecutionObserver<Deadline>
    for WorkflowRunPresentation<StandardOutput, StandardError, Clock>
where
    StandardOutput: Write + Send + 'static,
    StandardError: Write + Send + 'static,
    Clock: ObservationClock,
    Deadline: DisplayDeadline,
{
    fn observe(
        &self,
        observation: ExecutionObservation<Deadline>,
    ) -> impl Future<Output = ()> + Send {
        let mut state = lock_state(&self.state);
        if state.failure.is_none() && !state.finished {
            let observed_at = self.clock.sample();
            if let Err(failure) = state.render_observation(observed_at, observation) {
                state.record_failure(failure);
            }
        }
        ready(())
    }
}

fn lock_state<T, U>(
    state: &Mutex<PresentationState<T, U>>,
) -> MutexGuard<'_, PresentationState<T, U>> {
    match state.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct PresentationState<StandardOutput, StandardError> {
    mode: PresentationMode,
    color: bool,
    terminal_width: Option<usize>,
    standard_output: StandardOutput,
    standard_error: StandardError,
    definition: PresentationDefinition,
    child_streams: BTreeMap<ChildStreamKey, ChildStream>,
    step_starts: BTreeMap<String, Instant>,
    failure: Option<PresentationFailure>,
    failure_sender: watch::Sender<Option<PresentationFailure>>,
    finished: bool,
}

#[derive(Clone)]
struct PresentationDefinition {
    workflow_path: String,
    result_directory: String,
    presentation_order: Vec<String>,
    steps: BTreeMap<String, PresentationStep>,
    scope_width: usize,
}

#[derive(Clone)]
struct PresentationStep {
    kind: &'static str,
    start_detail: String,
    success: StepSuccessPresentation,
    outputs: BTreeMap<String, String>,
}

#[derive(Clone, Copy)]
enum StepSuccessPresentation {
    Command,
    AgentResponse,
    AgentResult,
    AgentWithoutValue,
}

impl PresentationDefinition {
    fn from_workflow(workflow: &ResolvedWorkflow, result_directory: &str) -> Self {
        let steps = workflow
            .definition
            .steps
            .iter()
            .map(|(id, step)| {
                let presentation = match step {
                    ValidatedStep::Command(command) => {
                        let argv = command
                            .argv
                            .iter()
                            .map(|argument| shell_quote(argument))
                            .collect::<Vec<_>>()
                            .join(" ");
                        let detail = command.common.cwd.as_ref().map_or_else(
                            || format!("cmd · {argv}"),
                            |cwd| format!("cmd · {cwd} $ {argv}"),
                        );
                        PresentationStep {
                            kind: "cmd",
                            start_detail: visible_text(&detail),
                            success: StepSuccessPresentation::Command,
                            outputs: presentation_outputs(&command.common.outputs),
                        }
                    }
                    ValidatedStep::Agent(agent) => {
                        let ValidatedHarness::Pi(config) = &agent.agent.harness;
                        let thinking = format!("{:?}", config.thinking).to_ascii_lowercase();
                        let success = agent
                            .common
                            .outputs
                            .values()
                            .find_map(|output| match output.definition {
                                Output::AgentResponse => {
                                    Some(StepSuccessPresentation::AgentResponse)
                                }
                                Output::AgentResult { .. } => {
                                    Some(StepSuccessPresentation::AgentResult)
                                }
                                Output::File { .. } => None,
                            })
                            .unwrap_or(StepSuccessPresentation::AgentWithoutValue);
                        PresentationStep {
                            kind: "agent",
                            start_detail: visible_text(&format!(
                                "agent · {} · pi · {} · thinking={thinking}",
                                agent.agent.profile, config.model
                            )),
                            success,
                            outputs: presentation_outputs(&agent.common.outputs),
                        }
                    }
                };
                (id.clone(), presentation)
            })
            .collect::<BTreeMap<_, _>>();
        let scope_width = steps
            .keys()
            .map(String::len)
            .chain(std::iter::once("@workflow".len()))
            .max()
            .unwrap_or("@workflow".len());
        Self {
            workflow_path: workflow.source.workflow_path.clone(),
            result_directory: result_directory.to_owned(),
            presentation_order: workflow.definition.presentation_order.clone(),
            steps,
            scope_width,
        }
    }
}

fn presentation_outputs(
    outputs: &BTreeMap<String, super::validated::ValidatedOutput>,
) -> BTreeMap<String, String> {
    outputs
        .iter()
        .map(|(name, output)| {
            let detail = match &output.definition {
                Output::AgentResponse => "agent_response".to_owned(),
                Output::AgentResult { schema } => format!("agent_result → {schema}"),
                Output::File { path, .. } => format!("file → {path}"),
            };
            (name.clone(), visible_text(&detail))
        })
        .collect()
}

impl<StandardOutput, StandardError> PresentationState<StandardOutput, StandardError>
where
    StandardOutput: Write,
    StandardError: Write,
{
    fn write_header(
        &mut self,
        opened_at: OffsetDateTime,
        maximum_parallel_steps: usize,
    ) -> Result<(), PresentationFailure> {
        let step_count = self.definition.steps.len();
        let step_label = if step_count == 1 { "step" } else { "steps" };
        let identity = visible_text(&self.definition.result_directory);
        let metadata = format!(
            " · {} · {step_count} {step_label} · concurrency {maximum_parallel_steps}",
            visible_text(&self.definition.workflow_path)
        );
        let header = format!(
            "{} {}{}\n{} {}\n\n",
            self.styled_text("run", TextTone::Muted),
            self.styled_text(&identity, TextTone::Primary),
            self.styled_text(&metadata, TextTone::Secondary),
            self.styled_text("started", TextTone::Muted),
            self.styled_text(&header_timestamp(opened_at), TextTone::Secondary),
        );
        let writer = self.line_writer();
        writer
            .write_all(header.as_bytes())
            .and_then(|()| writer.flush())
            .map_err(|error| {
                PresentationFailure::writer(PresentationFailureOperation::HeaderWriter, &error)
            })
    }

    fn render_observation<Deadline: DisplayDeadline>(
        &mut self,
        observed_at: ObservationTime,
        observation: ExecutionObservation<Deadline>,
    ) -> Result<(), PresentationFailure> {
        match observation {
            ExecutionObservation::Transition(transition) => {
                self.render_transition(observed_at, transition)
            }
            ExecutionObservation::CommandOutput(output) => {
                self.render_child_output(observed_at.utc, output)
            }
            ExecutionObservation::CommandOutputClosed(closed) => {
                self.close_child_output(observed_at.utc, closed)
            }
        }?;
        self.flush_line_writer()
    }

    fn render_transition<Deadline: DisplayDeadline>(
        &mut self,
        observed_at: ObservationTime,
        transition: TransitionObservation<Deadline>,
    ) -> Result<(), PresentationFailure> {
        match transition.event {
            TransitionEvent::CancellationAccepted {
                reason, deadline, ..
            } => {
                let deadline = rfc3339(deadline.deadline_utc())?;
                self.write_event(
                    observed_at.utc,
                    "@workflow",
                    "cancelling",
                    &format!("{} · force stop at {deadline}", cancellation_reason(reason)),
                    TokenRole::Blocked,
                )
            }
            TransitionEvent::Step { step, to, .. } => match to {
                StepStateKind::Starting => {
                    self.step_starts
                        .entry(step.clone())
                        .or_insert(observed_at.monotonic);
                    let detail = self
                        .definition
                        .steps
                        .get(&step)
                        .map(|step| step.start_detail.clone())
                        .unwrap_or_else(|| "cmd".to_owned());
                    self.write_event(observed_at.utc, &step, "start", &detail, TokenRole::Active)
                }
                StepStateKind::Succeeded => {
                    let outputs = match transition.step {
                        Some(ObservedStepTransition::OutputsCommitted { outputs }) => outputs,
                        _ => Vec::new(),
                    };
                    for output in &outputs {
                        let detail = self
                            .definition
                            .steps
                            .get(&step)
                            .and_then(|definition| definition.outputs.get(output))
                            .map_or_else(
                                || format!("{} · committed", visible_text(output)),
                                |declaration| format!("{} · {declaration}", visible_text(output)),
                            );
                        self.write_event(
                            observed_at.utc,
                            &step,
                            "output",
                            &detail,
                            TokenRole::Output,
                        )?;
                    }
                    let detail = self
                        .definition
                        .steps
                        .get(&step)
                        .map_or_else(String::new, |definition| {
                            success_detail(definition.success, outputs.len())
                        });
                    let detail = completion_detail(
                        detail,
                        self.finish_step_duration(&step, observed_at.monotonic),
                    );
                    self.write_event(observed_at.utc, &step, "done", &detail, TokenRole::Success)
                }
                StepStateKind::Failed => {
                    let detail = match transition.step {
                        Some(ObservedStepTransition::Failed { phase, cause }) => {
                            failure_detail(phase, &cause)
                        }
                        _ => "authoritative step failure".to_owned(),
                    };
                    let detail = completion_detail(
                        detail,
                        self.finish_step_duration(&step, observed_at.monotonic),
                    );
                    self.write_event(
                        observed_at.utc,
                        &step,
                        "failed",
                        &detail,
                        TokenRole::Failure,
                    )
                }
                StepStateKind::Blocked => {
                    let detail = match transition.step {
                        Some(ObservedStepTransition::Blocked { dependency }) => {
                            format!("by {}", visible_text(&dependency))
                        }
                        _ => "dependency did not succeed".to_owned(),
                    };
                    self.step_starts.remove(&step);
                    self.write_event(
                        observed_at.utc,
                        &step,
                        "blocked",
                        &detail,
                        TokenRole::Blocked,
                    )
                }
                StepStateKind::NotRun => {
                    self.step_starts.remove(&step);
                    self.write_event(
                        observed_at.utc,
                        &step,
                        "not-run",
                        "failure stop",
                        TokenRole::Neutral,
                    )
                }
                StepStateKind::Cancelling => {
                    let detail = match transition.step {
                        Some(ObservedStepTransition::Cancelling { reason }) => {
                            cancellation_reason(reason)
                        }
                        _ => "cancellation accepted",
                    };
                    self.write_event(
                        observed_at.utc,
                        &step,
                        "cancelling",
                        detail,
                        TokenRole::Blocked,
                    )
                }
                StepStateKind::Cancelled => {
                    let detail = match transition.step {
                        Some(ObservedStepTransition::Cancelled { reason }) => {
                            cancellation_reason(reason)
                        }
                        _ => "cancelled",
                    };
                    let detail = completion_detail(
                        detail.to_owned(),
                        self.finish_step_duration(&step, observed_at.monotonic),
                    );
                    self.write_event(
                        observed_at.utc,
                        &step,
                        "cancelled",
                        &detail,
                        TokenRole::Blocked,
                    )
                }
                StepStateKind::Pending
                | StepStateKind::Running
                | StepStateKind::CapturingOutputs => Ok(()),
            },
            TransitionEvent::Workflow { .. } => Ok(()),
        }
    }

    fn finish_step_duration(&mut self, step: &str, finished_at: Instant) -> Option<Duration> {
        self.step_starts
            .remove(step)
            .map(|started_at| finished_at.saturating_duration_since(started_at))
    }

    fn render_child_output(
        &mut self,
        observed_at: OffsetDateTime,
        output: CommandOutputObservation,
    ) -> Result<(), PresentationFailure> {
        let key = ChildStreamKey::from_output(&output);
        let records = self
            .child_streams
            .entry(key)
            .or_default()
            .push(&output.bytes);
        for record in records {
            self.write_child_record(observed_at, &output.step, output.source, record)?;
        }
        Ok(())
    }

    fn close_child_output(
        &mut self,
        observed_at: OffsetDateTime,
        closed: CommandOutputClosedObservation,
    ) -> Result<(), PresentationFailure> {
        let key = ChildStreamKey::from_closed(&closed);
        let records = self
            .child_streams
            .remove(&key)
            .map_or_else(Vec::new, ChildStream::close);
        for record in records {
            self.write_child_record(observed_at, &closed.step, closed.source, record)?;
        }
        Ok(())
    }

    fn finish_child_streams(
        &mut self,
        observed_at: OffsetDateTime,
    ) -> Result<(), PresentationFailure> {
        let streams = std::mem::take(&mut self.child_streams);
        for (key, stream) in streams {
            for record in stream.close() {
                self.write_child_record(observed_at, &key.step, key.source(), record)?;
            }
        }
        Ok(())
    }

    fn write_child_record(
        &mut self,
        observed_at: OffsetDateTime,
        step: &str,
        source: CommandOutputSource,
        record: ChildRecord,
    ) -> Result<(), PresentationFailure> {
        let token = match source {
            CommandOutputSource::StandardOutput => "stdout",
            CommandOutputSource::StandardError => "stderr",
        };
        let detail = if record.continuation {
            format!("{SAFETY_CONTINUATION_MARKER} {}", record.payload)
        } else {
            record.payload
        };
        self.write_event(observed_at, step, token, &detail, TokenRole::Neutral)
    }

    fn write_event(
        &mut self,
        observed_at: OffsetDateTime,
        scope: &str,
        token: &str,
        detail: &str,
        role: TokenRole,
    ) -> Result<(), PresentationFailure> {
        let raw_timestamp = format!("[{}]", observation_timestamp(observed_at));
        let visible_scope = visible_text(scope);
        let scope_padding = " ".repeat(
            self.definition
                .scope_width
                .saturating_sub(visible_scope.len()),
        );
        let timestamp = self.styled_text(&raw_timestamp, TextTone::Muted);
        let scope = self.styled_text(&visible_scope, TextTone::Secondary);
        let styled_token = self.styled_token(token, role);

        if detail.is_empty() {
            let line = format!("{timestamp} {scope}{scope_padding}  {styled_token}\n");
            return self.write_line_bytes(line.as_bytes());
        }

        let token_padding = " ".repeat(EVENT_TOKEN_WIDTH.saturating_sub(token.len()));
        let aligned_prefix =
            format!("{timestamp} {scope}{scope_padding}  {styled_token}{token_padding}  ");
        let raw_aligned_prefix =
            format!("{raw_timestamp} {visible_scope}{scope_padding}  {token}{token_padding}  ");
        let detail_column = display_width(&raw_aligned_prefix);
        let Some(terminal_width) = self.terminal_width else {
            let line = format!(
                "{aligned_prefix}{}\n",
                self.styled_text(detail, TextTone::Secondary)
            );
            return self.write_line_bytes(line.as_bytes());
        };

        let mut rendered = String::new();
        if terminal_width.saturating_sub(detail_column) >= MIN_INLINE_DETAIL_WIDTH {
            let continuation_prefix_width =
                detail_column + display_width(VISUAL_CONTINUATION_MARKER) + " ".len();
            let (first, continuations) = wrap_detail(
                detail,
                terminal_width.saturating_sub(detail_column),
                terminal_width.saturating_sub(continuation_prefix_width),
            );
            rendered.push_str(&aligned_prefix);
            rendered.push_str(&self.styled_text(&first, TextTone::Secondary));
            rendered.push('\n');
            for continuation in &continuations {
                rendered.push_str(&" ".repeat(detail_column));
                rendered.push_str(&self.styled(VISUAL_CONTINUATION_MARKER, STYLE_CONTINUATION));
                rendered.push(' ');
                rendered.push_str(&self.styled_text(continuation, TextTone::Secondary));
                rendered.push('\n');
            }
        } else {
            rendered.push_str(&format!("{timestamp} {scope} {styled_token}\n"));
            let continuation_prefix_width =
                STACKED_CONTINUATION_INDENT + display_width(VISUAL_CONTINUATION_MARKER) + " ".len();
            let (first, continuations) = wrap_detail(
                detail,
                terminal_width.saturating_sub(STACKED_DETAIL_INDENT),
                terminal_width.saturating_sub(continuation_prefix_width),
            );
            rendered.push_str(&" ".repeat(STACKED_DETAIL_INDENT));
            rendered.push_str(&self.styled_text(&first, TextTone::Secondary));
            rendered.push('\n');
            for continuation in &continuations {
                rendered.push_str(&" ".repeat(STACKED_CONTINUATION_INDENT));
                rendered.push_str(&self.styled(VISUAL_CONTINUATION_MARKER, STYLE_CONTINUATION));
                rendered.push(' ');
                rendered.push_str(&self.styled_text(continuation, TextTone::Secondary));
                rendered.push('\n');
            }
        }
        self.write_line_bytes(rendered.as_bytes())
    }

    fn write_summary(
        &mut self,
        run: &WorkflowRunResult,
        publication: &PublicationPresentation<'_>,
    ) -> Result<(), PresentationFailure> {
        let steps = run
            .steps
            .iter()
            .map(|step| (step.id.as_str(), step))
            .collect::<BTreeMap<_, _>>();
        if steps.len() != self.definition.steps.len() || steps.len() != run.steps.len() {
            return Err(PresentationFailure::operation(
                PresentationFailureOperation::InvalidTerminalResult,
            ));
        }
        let divider = self.styled_text(
            "── summary ────────────────────────────────────────────",
            TextTone::Muted,
        );
        self.write_line_bytes(format!("\n{divider}\n\n").as_bytes())?;
        let mut rows = Vec::with_capacity(steps.len());
        let order = self.definition.presentation_order.clone();
        for id in order {
            let Some(step) = steps.get(id.as_str()) else {
                return Err(PresentationFailure::operation(
                    PresentationFailureOperation::InvalidTerminalResult,
                ));
            };
            let Some(definition) = self.definition.steps.get(&id) else {
                return Err(PresentationFailure::operation(
                    PresentationFailureOperation::InvalidTerminalResult,
                ));
            };
            let Some((state, detail, role)) = summary_step(step, definition.success) else {
                return Err(PresentationFailure::operation(
                    PresentationFailureOperation::InvalidTerminalResult,
                ));
            };
            rows.push(SummaryRow {
                step: visible_text(&id),
                kind: definition.kind,
                state,
                duration: step
                    .timing
                    .as_ref()
                    .map_or_else(|| "–".to_owned(), |timing| human_duration(timing.duration)),
                detail: visible_text(&detail),
                role,
            });
        }
        let step_width = rows
            .iter()
            .map(|row| row.step.len())
            .chain(std::iter::once("step".len()))
            .max()
            .unwrap_or("step".len());
        let kind_width = rows
            .iter()
            .map(|row| row.kind.len())
            .chain(std::iter::once("kind".len()))
            .max()
            .unwrap_or("kind".len());
        let state_width = rows
            .iter()
            .map(|row| row.state.len())
            .chain(std::iter::once("state".len()))
            .max()
            .unwrap_or("state".len());
        let duration_width = rows
            .iter()
            .map(|row| row.duration.chars().count())
            .chain(std::iter::once("duration".len()))
            .max()
            .unwrap_or("duration".len());
        let header = format!(
            "{:<step_width$}  {:<kind_width$}  {:<state_width$}  {:<duration_width$}  detail",
            "step", "kind", "state", "duration"
        );
        let header = self.styled_text(&header, TextTone::Muted);
        self.write_line_bytes(format!("{header}\n").as_bytes())?;
        for row in rows {
            let step_padding = " ".repeat(step_width.saturating_sub(row.step.len()));
            let kind_padding = " ".repeat(kind_width.saturating_sub(row.kind.len()));
            let state_padding = " ".repeat(state_width.saturating_sub(row.state.len()));
            let duration_padding =
                " ".repeat(duration_width.saturating_sub(row.duration.chars().count()));
            let step = self.styled_text(&row.step, TextTone::Primary);
            let kind = self.styled_text(row.kind, TextTone::Secondary);
            let state = self.styled_token(row.state, row.role);
            let duration = self.styled_text(&row.duration, TextTone::Secondary);
            let detail = self.styled_text(&row.detail, TextTone::Secondary);
            let line = format!(
                "{step}{step_padding}  {kind}{kind_padding}  {state}{state_padding}  {duration}{duration_padding}  {detail}\n"
            );
            self.write_line_bytes(line.as_bytes())?;
        }
        self.write_line_bytes(b"\n")?;

        let counts = terminal_counts(run);
        let outcome = match &run.outcome {
            RunOutcome::Succeeded => "succeeded",
            RunOutcome::Failed { .. } => "failed",
            RunOutcome::Cancelled { .. } => "cancelled",
        };
        let role = match &run.outcome {
            RunOutcome::Succeeded => TokenRole::Success,
            RunOutcome::Failed { .. } => TokenRole::Failure,
            RunOutcome::Cancelled { .. } => TokenRole::Blocked,
        };
        let mut outcome_line = match publication {
            PublicationPresentation::Published(terminal) => format!(
                "{} {} {}{}",
                self.styled_text("run", TextTone::Muted),
                self.styled_text(
                    &visible_text(terminal.result_directory()),
                    TextTone::Primary,
                ),
                self.styled_token(outcome, role),
                self.styled_text(
                    &format!(" · exit {}", terminal.exit_status()),
                    TextTone::Secondary,
                ),
            ),
            PublicationPresentation::Failed(_) => format!(
                "{} {}",
                self.styled_text("workflow", TextTone::Secondary),
                self.styled_token(outcome, role),
            ),
        };
        for (name, count) in counts {
            if count != 0 {
                outcome_line.push_str(
                    &self.styled_text(&format!(" · {count} {name}"), TextTone::Secondary),
                );
            }
        }
        outcome_line.push_str(&self.styled_text(
            &format!(" · {} total", human_duration(run.timing.duration)),
            TextTone::Secondary,
        ));
        outcome_line.push('\n');
        self.write_line_bytes(outcome_line.as_bytes())?;

        match &run.outcome {
            RunOutcome::Failed {
                primary_failure, ..
            } => {
                let detail = failure_detail(primary_failure.phase, &primary_failure.cause);
                let line = format!(
                    "{} {}\n",
                    self.styled_token("failure:", TokenRole::Failure),
                    self.styled_text(
                        &format!(
                            "{} · {}",
                            visible_text(&primary_failure.step),
                            visible_text(&detail)
                        ),
                        TextTone::Secondary,
                    ),
                );
                self.write_line_bytes(line.as_bytes())?;
            }
            RunOutcome::Cancelled { reason } => {
                let line = format!(
                    "{} {}\n",
                    self.styled_token("cancellation:", TokenRole::Blocked),
                    self.styled_text(cancellation_reason(*reason), TextTone::Secondary),
                );
                self.write_line_bytes(line.as_bytes())?;
            }
            RunOutcome::Succeeded => {}
        }

        if let PublicationPresentation::Failed(error) = publication {
            let line = format!(
                "{} {}\n",
                self.styled_token("result publication failed:", TokenRole::Failure),
                self.styled_text(
                    &format!("{:?} · {:?}", error.phase(), error.kind()),
                    TextTone::Secondary,
                ),
            );
            self.write_line_bytes(line.as_bytes())?;
        }
        self.flush_line_writer()
    }

    fn flush_line_writer(&mut self) -> Result<(), PresentationFailure> {
        self.line_writer().flush().map_err(|error| {
            PresentationFailure::writer(PresentationFailureOperation::LineWriter, &error)
        })
    }

    fn write_publication_diagnostic(&mut self, error: &LocalPublicationError) {
        let _ = writeln!(self.standard_error, "Error: {error}")
            .and_then(|()| self.standard_error.flush());
    }

    fn report_output_failure(&mut self, failure: &PresentationFailure) {
        if self.mode == PresentationMode::Json
            && matches!(
                failure.operation,
                PresentationFailureOperation::HeaderWriter
                    | PresentationFailureOperation::LineWriter
            )
        {
            return;
        }
        let _ = writeln!(self.standard_error, "Error: {failure}")
            .and_then(|()| self.standard_error.flush());
    }

    fn write_line_bytes(&mut self, bytes: &[u8]) -> Result<(), PresentationFailure> {
        self.line_writer().write_all(bytes).map_err(|error| {
            PresentationFailure::writer(PresentationFailureOperation::LineWriter, &error)
        })
    }

    fn line_writer(&mut self) -> &mut dyn Write {
        match self.mode {
            PresentationMode::Plain => &mut self.standard_output,
            PresentationMode::Json => &mut self.standard_error,
        }
    }

    fn styled_token(&self, token: &str, role: TokenRole) -> String {
        let code = match role {
            TokenRole::Active => STYLE_ACTIVE,
            TokenRole::Output => STYLE_OUTPUT,
            TokenRole::Success => STYLE_SUCCESS,
            TokenRole::Failure => STYLE_FAILURE,
            TokenRole::Blocked => STYLE_BLOCKED,
            TokenRole::Neutral => STYLE_MUTED,
        };
        self.styled(token, code)
    }

    fn styled_text(&self, text: &str, tone: TextTone) -> String {
        let code = match tone {
            TextTone::Primary => STYLE_PRIMARY,
            TextTone::Secondary => STYLE_SECONDARY,
            TextTone::Muted => STYLE_MUTED,
        };
        self.styled(text, code)
    }

    fn styled(&self, text: &str, code: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_owned()
        }
    }

    fn record_failure(&mut self, failure: PresentationFailure) {
        if self.failure.is_none() {
            self.failure = Some(failure.clone());
            let _ = self.failure_sender.send(Some(failure));
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenRole {
    Active,
    Output,
    Success,
    Failure,
    Blocked,
    Neutral,
}

#[derive(Clone, Copy)]
enum TextTone {
    Primary,
    Secondary,
    Muted,
}

struct SummaryRow {
    step: String,
    kind: &'static str,
    state: &'static str,
    duration: String,
    detail: String,
    role: TokenRole,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ChildStreamKey {
    step: String,
    invocation_sequence: u64,
    source: u8,
}

impl ChildStreamKey {
    fn from_output(output: &CommandOutputObservation) -> Self {
        Self::new(&output.step, output.invocation, output.source)
    }

    fn from_closed(output: &CommandOutputClosedObservation) -> Self {
        Self::new(&output.step, output.invocation, output.source)
    }

    fn new(step: &str, invocation: super::runtime::ActionId, source: CommandOutputSource) -> Self {
        Self {
            step: step.to_owned(),
            invocation_sequence: invocation.transition_sequence.get(),
            source: source_number(source),
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
        if !self.payload.is_empty() && self.payload.len() + text.len() > CHILD_FRAGMENT_BYTES {
            self.emit_fragment(records, line_has_record);
        }
        self.payload.push_str(text);
        if self.payload.len() >= CHILD_FRAGMENT_BYTES {
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

fn is_unicode_control(character: char) -> bool {
    let value = u32::from(character);
    matches!(value, 0x80..=0x9f | 0xad | 0x61c | 0x6dd | 0x70f | 0x890..=0x891 | 0x8e2 | 0x180e | 0x200b..=0x200f | 0x202a..=0x202e | 0x2060..=0x2064 | 0x2066..=0x206f | 0xfeff | 0xfff9..=0xfffb | 0x110bd | 0x110cd | 0x13430..=0x1345f | 0x1bca0..=0x1bca3 | 0x1d173..=0x1d17a | 0xe0001 | 0xe0020..=0xe007f)
        || matches!(value, 0x600..=0x605)
}

fn observation_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:02}:{:02}:{:02}",
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

fn wrap_detail(
    detail: &str,
    first_width: usize,
    continuation_width: usize,
) -> (String, Vec<String>) {
    let (first, mut remainder) = next_detail_segment(detail, first_width);
    let mut continuations = Vec::new();
    while !remainder.is_empty() {
        let (continuation, next) = next_detail_segment(remainder, continuation_width);
        continuations.push(continuation.to_owned());
        remainder = next;
    }
    (first.to_owned(), continuations)
}

fn next_detail_segment(value: &str, maximum_width: usize) -> (&str, &str) {
    let maximum_width = maximum_width.max(1);
    if display_width(value) <= maximum_width {
        return (value, "");
    }

    let mut used_width = 0_usize;
    let mut fitting_end = 0;
    for (index, character) in value.char_indices() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used_width.saturating_add(character_width) > maximum_width {
            if used_width == 0 {
                fitting_end = index + character.len_utf8();
            }
            break;
        }
        used_width += character_width;
        fitting_end = index + character.len_utf8();
    }

    let candidate = &value[..fitting_end];
    if value[fitting_end..]
        .chars()
        .next()
        .is_some_and(char::is_whitespace)
    {
        return (
            candidate.trim_end_matches(char::is_whitespace),
            value[fitting_end..].trim_start_matches(char::is_whitespace),
        );
    }
    if let Some((boundary, whitespace)) =
        candidate.char_indices().rev().find(|(index, character)| {
            character.is_whitespace()
                && *index != 0
                && candidate[..*index]
                    .chars()
                    .any(|candidate| !candidate.is_whitespace())
        })
    {
        return (
            candidate[..boundary].trim_end_matches(char::is_whitespace),
            value[boundary + whitespace.len_utf8()..].trim_start_matches(char::is_whitespace),
        );
    }
    (candidate, &value[fitting_end..])
}

fn header_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}Z",
        value.year(),
        u8::from(value.month()),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn rfc3339(value: OffsetDateTime) -> Result<String, PresentationFailure> {
    value
        .to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .map_err(|_| {
            PresentationFailure::operation(PresentationFailureOperation::TimestampFormatting)
        })
}

fn shell_quote(argument: &str) -> String {
    let argument = visible_argument_text(argument);
    if !argument.is_empty()
        && argument.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'_' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-'
                )
        })
    {
        return argument;
    }
    format!("'{}'", argument.replace('\'', "'\\''"))
}

fn visible_argument_text(value: &str) -> String {
    visible_text_with_backslash(value, true)
}

fn visible_text(value: &str) -> String {
    visible_text_with_backslash(value, false)
}

fn visible_text_with_backslash(value: &str, escape_backslash: bool) -> String {
    let mut visible = String::new();
    for character in value.chars() {
        let scalar = u32::from(character);
        if escape_backslash && character == '\\' {
            visible.push_str("\\\\");
        } else if scalar <= 0x1f || scalar == 0x7f {
            visible.push_str(&format!("\\x{scalar:02x}"));
        } else if is_unicode_control(character) {
            visible.push_str(&format!("\\u{{{scalar:x}}}"));
        } else {
            visible.push(character);
        }
    }
    visible
}

fn cancellation_reason(reason: CancellationReason) -> &'static str {
    match reason {
        CancellationReason::UserRequest => "user_request",
        CancellationReason::TerminationRequest => "termination_request",
        CancellationReason::CallerOutputFailure => "caller_output_failure",
        CancellationReason::RunnerShutdown => "runner_shutdown",
    }
}

fn failure_phase(phase: FailurePhase) -> &'static str {
    match phase {
        FailurePhase::Start => "start",
        FailurePhase::Execution => "execution",
        FailurePhase::OutputCapture => "output_capture",
    }
}

fn failure_detail(phase: FailurePhase, cause: &StepFailureCause) -> String {
    format!("{} · {}", failure_phase(phase), failure_cause(cause))
}

fn failure_cause(cause: &StepFailureCause) -> String {
    match cause {
        StepFailureCause::Start(cause) => match cause {
            StepStartFailure::StepUnavailable => "step unavailable".to_owned(),
            StepStartFailure::PreparationTaskUnavailable => {
                "preparation task unavailable".to_owned()
            }
            StepStartFailure::InputsUnavailable => "inputs unavailable".to_owned(),
            StepStartFailure::InputPreparation(failure) => {
                let code = match failure.kind() {
                    InputPreparationFailureKind::InvalidInputName => "input invalid name",
                    InputPreparationFailureKind::ValueCountLimitExceeded => {
                        "input value count limit"
                    }
                    InputPreparationFailureKind::ValueSizeLimitExceeded => "input value size limit",
                    InputPreparationFailureKind::TotalSizeLimitExceeded => "input total size limit",
                    InputPreparationFailureKind::CollectionOrdinalLimitExceeded => {
                        "input collection ordinal limit"
                    }
                    InputPreparationFailureKind::ValueTypeMismatch => "input type mismatch",
                    InputPreparationFailureKind::SourceUnavailable => "input source unavailable",
                    InputPreparationFailureKind::StagingUnavailable => "input staging unavailable",
                    InputPreparationFailureKind::LiveLimitExceeded => "input live limit",
                };
                failure.input_identity().map_or_else(
                    || code.to_owned(),
                    |input| format!("{code} · input {input}"),
                )
            }
            StepStartFailure::OutputsUnsupported => "outputs unsupported".to_owned(),
            StepStartFailure::WorkingDirectory(failure) => match failure {
                WorkingDirectoryFailure::ExecutionRootRebound => "execution root rebound",
                WorkingDirectoryFailure::Unavailable => "working directory unavailable",
                WorkingDirectoryFailure::EscapesExecutionRoot => "working directory escape",
                WorkingDirectoryFailure::NotDirectory => "working directory not directory",
            }
            .to_owned(),
            StepStartFailure::UnsupportedBody(kind) => match kind {
                StepBodyKind::Command => "command body unsupported",
                StepBodyKind::Agent => "agent body unsupported",
            }
            .to_owned(),
            StepStartFailure::CommandPreparation(failure) => match failure {
                CommandPreparationFailure::InvalidArgv => "invalid command argv",
                CommandPreparationFailure::PathNotConfigured => "command PATH unconfigured",
                CommandPreparationFailure::ExecutableNotFound => "executable not found",
                CommandPreparationFailure::ExecutableUnavailable => "executable unavailable",
            }
            .to_owned(),
            StepStartFailure::CommandLaunch(failure) => match failure {
                CommandLaunchFailure::NotFound => "command launch not found",
                CommandLaunchFailure::PermissionDenied => "command launch permission denied",
                CommandLaunchFailure::InvalidInput => "command launch invalid input",
                CommandLaunchFailure::Other => "command launch failed",
            }
            .to_owned(),
        },
        StepFailureCause::Execution(StepExecutionFailure::Command(failure)) => match failure {
            CommandExecutionFailure::UnsuccessfulExit { code: Some(code) } => {
                format!("exit {code}")
            }
            CommandExecutionFailure::UnsuccessfulExit { code: None } => {
                "unsuccessful exit without status".to_owned()
            }
            CommandExecutionFailure::Wait => "command wait failed".to_owned(),
        },
        StepFailureCause::OutputCapture(failure) => match failure {
            OutputCaptureFailure::StepUnavailable => "step unavailable".to_owned(),
            OutputCaptureFailure::UnsupportedOutput => "output unsupported".to_owned(),
            OutputCaptureFailure::TaskUnavailable => "capture task unavailable".to_owned(),
            OutputCaptureFailure::Capture(failure) => {
                let code = match failure.kind() {
                    CaptureFailureKind::AbsolutePath => "output path absolute",
                    CaptureFailureKind::LexicalEscape => "output path escape",
                    CaptureFailureKind::EmptyPath => "output path empty",
                    CaptureFailureKind::Missing => "output missing",
                    CaptureFailureKind::SymbolicLink => "output symbolic link",
                    CaptureFailureKind::NotDirectory => "output parent not directory",
                    CaptureFailureKind::NotRegularFile => "output not regular file",
                    CaptureFailureKind::SourceUnavailable => "output source unavailable",
                    CaptureFailureKind::FileCountLimitExceeded => "captured file count limit",
                    CaptureFailureKind::FileSizeLimitExceeded => "captured file size limit",
                    CaptureFailureKind::TotalSizeLimitExceeded => "captured total size limit",
                    CaptureFailureKind::StagingUnavailable => "output staging unavailable",
                };
                format!("{code} · output {}", failure.output_identity())
            }
        },
    }
}

fn completion_detail(detail: String, duration: Option<Duration>) -> String {
    match duration {
        Some(duration) => format!("{detail} after {}", human_duration(duration)),
        None => detail,
    }
}

fn success_detail(presentation: StepSuccessPresentation, output_count: usize) -> String {
    let base = match presentation {
        StepSuccessPresentation::Command => "exit 0",
        StepSuccessPresentation::AgentResponse => "response captured",
        StepSuccessPresentation::AgentResult => "result captured",
        StepSuccessPresentation::AgentWithoutValue => "no requested agent value",
    };
    match output_count {
        0 => base.to_owned(),
        1 => format!("{base} · 1 output"),
        count => format!("{base} · {count} outputs"),
    }
}

fn summary_step(
    step: &WorkflowRunStep,
    success: StepSuccessPresentation,
) -> Option<(&'static str, String, TokenRole)> {
    match &step.state {
        StepState::Succeeded { outputs } => Some((
            "succeeded",
            success_detail(success, outputs.len()),
            TokenRole::Success,
        )),
        StepState::Failed { phase, cause } => {
            Some(("failed", failure_detail(*phase, cause), TokenRole::Failure))
        }
        StepState::Blocked { dependency } => {
            Some(("blocked", format!("by {dependency}"), TokenRole::Blocked))
        }
        StepState::NotRun {
            reason: NotRunReason::FailureStop,
        } => Some(("not-run", "failure stop".to_owned(), TokenRole::Neutral)),
        StepState::Cancelled { reason } => Some((
            "cancelled",
            cancellation_reason(*reason).to_owned(),
            TokenRole::Blocked,
        )),
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Cancelling { .. } => None,
    }
}

fn terminal_counts(run: &WorkflowRunResult) -> [(&'static str, usize); 5] {
    let mut counts = [
        ("succeeded", 0),
        ("failed", 0),
        ("blocked", 0),
        ("not-run", 0),
        ("cancelled", 0),
    ];
    for step in &run.steps {
        let index = match step.state {
            StepState::Succeeded { .. } => Some(0),
            StepState::Failed { .. } => Some(1),
            StepState::Blocked { .. } => Some(2),
            StepState::NotRun { .. } => Some(3),
            StepState::Cancelled { .. } => Some(4),
            StepState::Pending
            | StepState::Starting
            | StepState::Running
            | StepState::CapturingOutputs
            | StepState::Cancelling { .. } => None,
        };
        if let Some(index) = index {
            counts[index].1 += 1;
        }
    }
    counts
}

fn human_duration(duration: Duration) -> String {
    let milliseconds = duration.as_millis();
    if milliseconds < 1000 {
        return format!("{milliseconds}ms");
    }
    if duration.as_secs() < 60 {
        return format!("{:.1}s", duration.as_secs_f64());
    }
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = total_seconds % 3600 / 60;
    let seconds = total_seconds % 60;
    if hours == 0 {
        format!("{minutes}m{seconds:02}s")
    } else {
        format!("{hours}h{minutes:02}m{seconds:02}s")
    }
}

#[cfg(test)]
mod tests;
