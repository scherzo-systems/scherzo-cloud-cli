use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::future::{Future, ready};
use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::Serialize;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};
use tokio::sync::watch;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::admission::{AdmissionFailure, CancellationReason};
use super::artifact::CaptureFailureKind;
use super::document::FailurePolicy;
use super::git_capture::GitCaptureFailure;
use super::input::InputPreparationFailureKind;
use super::local_run::{LocalRetryRejection, RetryIneligibilityReason};
use super::observation::{ExecutionObservation, ExecutionObserver, ObservedStepTransition};
use super::presentation_feed::{
    AcceptedRecordOrder, AgentPresentationHarness, DisplayDeadline, NormalizedAgentObservation,
    NormalizedChildOutput, PresentationRecord, PresentationRecordKind, PresentationTransition,
    WorkflowPresentationFeed, WorkflowPresentationStep, is_unicode_control,
};
use super::publication::{
    LocalPublicationError, WorkflowRunResult, WorkflowRunStep, WorkflowRunTerminalResultV1,
};
use super::rejection::{RejectionDiagnostic, human_resolution_remedy};
use super::resolution::{ResolutionFailure, ResolutionFailureKind, ResolvedWorkflow};
use super::run_timing::{ObservationClock, ObservationTime};
use super::runtime::{
    ActiveStepInvocation, FailurePhase, NotRunReason, RecoveryDecisionKind,
    RecoveryHandlerActivity, RecoveryHandlerKind, RunOutcome, StepState, StepStateKind,
    TransitionEvent,
};
use super::step_runtime::{
    CommandExecutionFailure, CommandLaunchFailure, CommandPreparationFailure, OutputCaptureFailure,
    StepExecutionFailure, StepFailureCause, StepStartFailure, WorkingDirectoryFailure,
};
use super::validated::ValidatedStep;
use crate::execution::AgentHarnessInstallationFailure;

const RUN_COMMAND: &str = "scherzo-cloud workflow run";
const RETRY_COMMAND: &str = "scherzo-cloud workflow retry";
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

pub(crate) fn styled_terminal_text(value: &str, style: &str, color: bool) -> String {
    if color {
        format!("\u{1b}[{style}m{value}\u{1b}[0m")
    } else {
        value.to_owned()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestedPresentationMode {
    Automatic,
    Plain,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PresentationMode {
    Tui,
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
    pub(crate) stdin_is_terminal: bool,
    pub(crate) stdout_is_terminal: bool,
    pub(crate) stderr_is_terminal: bool,
    pub(crate) stdout_width: Option<usize>,
    pub(crate) stderr_width: Option<usize>,
    pub(crate) term: Option<OsString>,
    pub(crate) no_color: Option<OsString>,
}

impl TerminalCapabilities {
    pub(crate) fn detect() -> Self {
        let stdin = io::stdin();
        let stdout = io::stdout();
        let stderr = io::stderr();
        let stdout_is_terminal = stdout.is_terminal();
        let stderr_is_terminal = stderr.is_terminal();
        Self {
            stdin_is_terminal: stdin.is_terminal(),
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
    pub(crate) standard_input_reserved: bool,
}

impl PresentationConfig {
    pub(crate) fn mode(&self) -> PresentationMode {
        match self.requested_mode {
            RequestedPresentationMode::Automatic
                if !self.standard_input_reserved
                    && self.capabilities.stdin_is_terminal
                    && self.capabilities.stdout_is_terminal
                    && usable_term(self.capabilities.term.as_deref()) =>
            {
                PresentationMode::Tui
            }
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
                    PresentationMode::Tui | PresentationMode::Plain => {
                        self.capabilities.stdout_is_terminal
                    }
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
            PresentationMode::Tui => None,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PresentationFailureOperation {
    HeaderWriter,
    LineWriter,
    DiagnosticWriter,
    TerminalJsonWriter,
    TerminalSetup,
    TerminalInput,
    TerminalDraw,
    TerminalRestore,
    TerminalTask,
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

    pub(crate) fn operation(operation: PresentationFailureOperation) -> Self {
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
    Rejected {
        human_diagnostic: Option<String>,
    },
    Published {
        exit_status: u16,
        result_directory: String,
    },
    PublicationFailed(LocalPublicationError),
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
    command: &'static str,
    retry_run_directory: Option<String>,
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
            command: RUN_COMMAND,
            retry_run_directory: None,
        }
    }

    pub(crate) fn for_retry(mut self, run_directory: &Path) -> Self {
        self.command = RETRY_COMMAND;
        self.retry_run_directory = run_directory.to_str().map(str::to_owned);
        self
    }

    pub(crate) fn render_resolution_rejection(
        self,
        failure: &ResolutionFailure,
    ) -> WorkflowRunPresentationResult {
        let diagnostic = RejectionDiagnostic::from_resolution(failure);
        let human_subject = match (failure.kind(), failure.source_path()) {
            (ResolutionFailureKind::SourceUnavailable, Some(_)) => {
                Some("workflow source unavailable")
            }
            (ResolutionFailureKind::SourceNotRegularFile, Some(_)) => {
                Some("workflow source is not a regular file")
            }
            _ => None,
        };
        let human_message = human_subject.map(|subject| {
            format!(
                "{subject} at {}\n\n{}",
                diagnostic.location,
                human_resolution_remedy(failure)
            )
        });
        self.write_workflow_rejection(
            "resolution",
            failure.workflow_path(),
            diagnostic,
            human_message,
        )
    }

    pub(crate) fn render_admission_rejection(
        self,
        workflow: &ResolvedWorkflow,
        failure: &AdmissionFailure,
    ) -> WorkflowRunPresentationResult {
        let Some(diagnostic) = RejectionDiagnostic::from_admission(failure) else {
            return WorkflowRunPresentationResult::Failed(PresentationFailure::operation(
                PresentationFailureOperation::UnsupportedRejection,
            ));
        };
        self.write_workflow_rejection(
            "admission",
            Some(&workflow.source.workflow_path),
            diagnostic,
            None,
        )
    }

    pub(crate) fn render_agent_harness_installation_rejection(
        self,
        workflow: &ResolvedWorkflow,
        failure: &AgentHarnessInstallationFailure,
    ) -> WorkflowRunPresentationResult {
        self.write_workflow_rejection(
            "installation",
            Some(&workflow.source.workflow_path),
            RejectionDiagnostic::from_agent_harness_installation(failure),
            None,
        )
    }

    fn write_workflow_rejection(
        mut self,
        phase: &'static str,
        workflow_path: Option<&str>,
        diagnostic: RejectionDiagnostic<'_>,
        human_message: Option<String>,
    ) -> WorkflowRunPresentationResult {
        let retry_run_directory = self.retry_run_directory.clone();
        let rejection = TerminalRejectionV1 {
            schema_version: 1,
            command: self.command,
            outcome: "rejected",
            exit_status: crate::exit_code::ExitCode::GeneralFailure.as_u8(),
            phase,
            run_directory: retry_run_directory.as_deref(),
            workflow: workflow_path.map(|path| RejectedWorkflowV1 { path }),
            diagnostics: [diagnostic.clone()],
        };
        let human_message = human_message.unwrap_or_else(|| {
            format!(
                "workflow rejected: {} at {}: {}",
                diagnostic.code, diagnostic.location, diagnostic.message
            )
        });
        self.write_rejection(&rejection, &human_message)
    }

    pub(crate) fn render_retry_rejection(
        mut self,
        rejection: &LocalRetryRejection,
    ) -> WorkflowRunPresentationResult {
        let Some(run_directory) = rejection.run_directory().to_str() else {
            return WorkflowRunPresentationResult::Failed(PresentationFailure::operation(
                PresentationFailureOperation::InvalidTerminalResult,
            ));
        };
        let message = retry_rejection_message(rejection.reason());
        let diagnostic = RetryDiagnosticV1 {
            code: rejection.reason().as_str(),
            message,
            location: RetryLocationV1 {
                kind: "attempt",
                attempt_number: rejection.attempt_number(),
                guard_ids: rejection.guard_ids(),
                ownership_reason: rejection.ownership_reason().map(|reason| reason.as_str()),
            },
        };
        let terminal = RetryRejectionV1 {
            schema_version: 1,
            command: RETRY_COMMAND,
            outcome: "rejected",
            exit_status: crate::exit_code::ExitCode::GeneralFailure.as_u8(),
            phase: "retry",
            run_directory,
            attempt_number: rejection.attempt_number(),
            diagnostics: [diagnostic],
        };
        self.write_rejection(&terminal, &human_retry_rejection(rejection))
    }

    fn write_rejection(
        &mut self,
        rejection: &impl Serialize,
        human_message: &str,
    ) -> WorkflowRunPresentationResult {
        let mode = self.config.mode();
        let result = match mode {
            PresentationMode::Tui | PresentationMode::Plain => Ok(()),
            PresentationMode::Json => write_pretty_json(&mut self.standard_output, rejection)
                .map_err(|error| {
                    PresentationFailure::writer(
                        PresentationFailureOperation::TerminalJsonWriter,
                        &error,
                    )
                }),
        };
        match result {
            Ok(()) => WorkflowRunPresentationResult::Rejected {
                human_diagnostic: (mode != PresentationMode::Json)
                    .then(|| human_message.to_owned()),
            },
            Err(failure) => WorkflowRunPresentationResult::Failed(failure),
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

    pub(crate) fn render_standard_summary(
        self,
        workflow: &ResolvedWorkflow,
        run: &WorkflowRunResult,
        publication: PublicationPresentation<'_>,
    ) -> WorkflowRunPresentationResult {
        let mut config = self.config;
        config.requested_mode = RequestedPresentationMode::Plain;
        let result_directory = match &publication {
            PublicationPresentation::Published(terminal) => Some(terminal.result_directory()),
            PublicationPresentation::Failed(_) => None,
        };
        let (failure_sender, _) = watch::channel(None);
        let mut state = PresentationState {
            mode: PresentationMode::Plain,
            color: config.color_enabled(),
            terminal_width: config.wrapping_width(),
            standard_output: self.standard_output,
            standard_error: self.standard_error,
            definition: PresentationDefinition::from_workflow(
                workflow,
                result_directory.unwrap_or("result"),
            ),
            feed: WorkflowPresentationFeed::new(workflow),
            last_accepted_order: None,
            step_starts: BTreeMap::new(),
            failure: None,
            failure_sender,
            finished: true,
        };
        if let Err(failure) =
            state.write_summary_or_report_failure(run, &publication, result_directory)
        {
            return failure;
        }
        state.present_publication_result(publication, false)
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
    #[serde(skip_serializing_if = "Option::is_none")]
    run_directory: Option<&'a str>,
    workflow: Option<RejectedWorkflowV1<'a>>,
    diagnostics: [RejectionDiagnostic<'a>; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RetryRejectionV1<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    phase: &'static str,
    run_directory: &'a str,
    attempt_number: u64,
    diagnostics: [RetryDiagnosticV1<'a>; 1],
}

#[derive(Serialize)]
struct RetryDiagnosticV1<'a> {
    code: &'static str,
    message: &'static str,
    location: RetryLocationV1<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RetryLocationV1<'a> {
    kind: &'static str,
    attempt_number: u64,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    guard_ids: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    ownership_reason: Option<&'static str>,
}

fn human_retry_rejection(rejection: &LocalRetryRejection) -> String {
    let attempt = rejection.attempt_number();
    match rejection.reason() {
        RetryIneligibilityReason::RunLocked => format!(
            "retry blocked: attempt {attempt} has an active execution owner\n\nWait for the current attempt to finish, then retry."
        ),
        RetryIneligibilityReason::LatestAttemptSucceeded => format!(
            "cannot retry run: attempt {attempt} succeeded\n\nA succeeded run cannot be retried. Start a new run instead:\n  scherzo-cloud workflow run --run-dir <NEW_DIR> <WORKFLOW>"
        ),
        RetryIneligibilityReason::LatestAttemptRejected => format!(
            "cannot retry run: attempt {attempt} was rejected\n\nStart a new run with a corrected workflow definition instead:\n  scherzo-cloud workflow run --run-dir <NEW_DIR> <WORKFLOW>"
        ),
        RetryIneligibilityReason::OwnershipUnproven => format!(
            "retry blocked: process ownership for attempt {attempt} is unproven\n\nNo safe retry remedy is available for this run."
        ),
    }
}

const fn retry_rejection_message(reason: RetryIneligibilityReason) -> &'static str {
    match reason {
        RetryIneligibilityReason::RunLocked => {
            "The current attempt still has an active execution owner."
        }
        RetryIneligibilityReason::LatestAttemptSucceeded => {
            "A succeeded run cannot be retried; create a new run."
        }
        RetryIneligibilityReason::LatestAttemptRejected => {
            "A rejected run cannot be retried; create a new run with a usable specification."
        }
        RetryIneligibilityReason::OwnershipUnproven => {
            "The predecessor process groups cannot be proven safe to quiesce."
        }
    }
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
    opened_at: ObservationTime,
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
            opened_at: self.opened_at,
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
            feed: WorkflowPresentationFeed::new(workflow),
            last_accepted_order: None,
            step_starts: BTreeMap::new(),
            failure: None,
            failure_sender,
            finished: false,
        };
        let opened_at = clock.sample();
        state.write_header(opened_at.utc, maximum_parallel_steps)?;
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            clock,
            opened_at,
        })
    }

    pub(crate) const fn opened_at(&self) -> ObservationTime {
        self.opened_at
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
            return WorkflowRunPresentationResult::Failed(failure);
        }

        let observed_at = self.clock.sample();
        if let Err(failure) = state.finish_child_streams(observed_at) {
            let failure = failure.with_result_directory(result_directory);
            return WorkflowRunPresentationResult::Failed(failure);
        }
        if let Err(failure) =
            state.write_summary_or_report_failure(run, &publication, result_directory)
        {
            return failure;
        }

        state.present_publication_result(publication, emit_terminal_json)
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
    feed: WorkflowPresentationFeed,
    last_accepted_order: Option<AcceptedRecordOrder>,
    step_starts: BTreeMap<String, Instant>,
    failure: Option<PresentationFailure>,
    failure_sender: watch::Sender<Option<PresentationFailure>>,
    finished: bool,
}

#[derive(Clone)]
struct PresentationDefinition {
    result_directory: String,
    steps: BTreeMap<String, PresentationStep>,
    scope_width: usize,
}

#[derive(Clone)]
struct PresentationStep {
    success: StepSuccessPresentation,
    outputs: BTreeMap<String, String>,
}

#[derive(Clone, Copy)]
enum StepSuccessPresentation {
    Command,
    Agent,
}

impl PresentationDefinition {
    fn from_workflow(workflow: &ResolvedWorkflow, result_directory: &str) -> Self {
        let steps = workflow
            .definition
            .steps
            .iter()
            .chain(
                workflow
                    .definition
                    .finalizers
                    .iter()
                    .map(|(id, finalizer)| (id, &finalizer.body)),
            )
            .map(|(id, step)| {
                let presentation = match step {
                    ValidatedStep::Command(command) => PresentationStep {
                        success: StepSuccessPresentation::Command,
                        outputs: presentation_outputs(&command.common.outputs),
                    },
                    ValidatedStep::Agent(agent) => PresentationStep {
                        success: StepSuccessPresentation::Agent,
                        outputs: presentation_outputs(&agent.common.outputs),
                    },
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
            result_directory: result_directory.to_owned(),
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
            let detail = match output.value_type {
                super::validated::WorkflowValueType::Text => "text",
                super::validated::WorkflowValueType::Json => "json",
                super::validated::WorkflowValueType::File => "file",
                super::validated::WorkflowValueType::GitBranch => "git_branch",
                super::validated::WorkflowValueType::AttachmentCollection => {
                    unreachable!("outputs cannot be attachment collections")
                }
            }
            .to_owned();
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
        let feed_definition = self.feed.definition();
        let step_count = feed_definition.steps.len();
        let step_label = if step_count == 1 { "step" } else { "steps" };
        let identity = visible_text(&self.definition.result_directory);
        let metadata = format!(
            " · {} · {step_count} {step_label} · concurrency {maximum_parallel_steps}",
            visible_text(&feed_definition.workflow_path)
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
        let records = self.feed.accept(observed_at.utc, observation);
        for record in records {
            self.render_record(record, observed_at.monotonic)?;
        }
        self.flush_line_writer()
    }

    fn render_record(
        &mut self,
        record: PresentationRecord,
        observed_monotonic: Instant,
    ) -> Result<(), PresentationFailure> {
        if let Some(previous) = self.last_accepted_order {
            debug_assert!(record.accepted_order > previous);
        }
        self.last_accepted_order = Some(record.accepted_order);
        match record.kind {
            PresentationRecordKind::Transition(transition) => self.render_transition(
                ObservationTime {
                    utc: record.observed_at,
                    monotonic: observed_monotonic,
                },
                transition,
            ),
            PresentationRecordKind::ChildOutput(output) => {
                self.render_child_output(record.observed_at, output)
            }
            PresentationRecordKind::AgentObservation(observation) => {
                self.render_agent_observation(record.observed_at, observation)
            }
        }
    }

    fn render_transition(
        &mut self,
        observed_at: ObservationTime,
        transition: PresentationTransition,
    ) -> Result<(), PresentationFailure> {
        match transition.event {
            TransitionEvent::CancellationAccepted {
                reason, deadline, ..
            } => {
                let deadline = rfc3339(deadline)?;
                self.write_event(
                    observed_at.utc,
                    "@workflow",
                    "cancelling",
                    &format!("{} · force stop at {deadline}", cancellation_reason(reason)),
                    TokenRole::Blocked,
                )
            }
            TransitionEvent::FinalizationCancellationAccepted {
                reason, deadline, ..
            } => {
                let deadline = rfc3339(deadline)?;
                self.write_event(
                    observed_at.utc,
                    "@workflow",
                    "finalization cancelling",
                    &format!("{} · force stop at {deadline}", cancellation_reason(reason)),
                    TokenRole::Blocked,
                )
            }
            TransitionEvent::ForceAbortAccepted { reason, .. } => self.write_event(
                observed_at.utc,
                "@workflow",
                "force abort",
                cancellation_reason(reason),
                TokenRole::Failure,
            ),
            TransitionEvent::Step {
                step,
                failure_policy,
                to,
                ..
            } => match to {
                StepStateKind::Starting => {
                    self.step_starts
                        .entry(step.clone())
                        .or_insert(observed_at.monotonic);
                    let detail = observed_recovery_detail(transition.step).unwrap_or_else(|| {
                        self.feed
                            .definition()
                            .steps
                            .get(&step)
                            .map_or_else(|| "cmd".to_owned(), start_detail)
                    });
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
                    let detail = issue_detail(detail, failure_policy);
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
                        Some(ObservedStepTransition::InputUnavailable { references }) => {
                            format!("inputs unavailable: {}", references.join(", "))
                        }
                        _ => "dependency did not succeed".to_owned(),
                    };
                    let detail = issue_detail(detail, failure_policy);
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
                    let detail = match transition.step {
                        Some(ObservedStepTransition::NotRun {
                            reason: NotRunReason::FinalizerTriggerNotSelected,
                        }) => "finalizer trigger not selected",
                        Some(ObservedStepTransition::NotRun {
                            reason: NotRunReason::FailureStop,
                        })
                        | None => "failure stop",
                        Some(_) => "not authorized",
                    };
                    self.write_event(
                        observed_at.utc,
                        &step,
                        "not-run",
                        detail,
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
                StepStateKind::Recovering => {
                    let detail = observed_recovery_detail(transition.step)
                        .unwrap_or_else(|| "recovery active".to_owned());
                    self.write_event(
                        observed_at.utc,
                        &step,
                        "recovering",
                        &detail,
                        TokenRole::Active,
                    )
                }
                StepStateKind::Pending
                | StepStateKind::Running
                | StepStateKind::CapturingOutputs => Ok(()),
            },
            TransitionEvent::Workflow {
                to: super::runtime::WorkflowState::Finalizing { trigger, .. },
                ..
            } => self.write_event(
                observed_at.utc,
                "@workflow",
                "finalizing",
                finalization_trigger(trigger),
                TokenRole::Active,
            ),
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
        output: NormalizedChildOutput,
    ) -> Result<(), PresentationFailure> {
        let NormalizedChildOutput {
            step,
            invocation: _,
            source,
            source_sequence: _,
            payload,
            continuation,
        } = output;
        let token = match source {
            super::observation::CommandOutputSource::StandardOutput => "stdout",
            super::observation::CommandOutputSource::StandardError => "stderr",
        };
        let detail = if continuation {
            format!("{SAFETY_CONTINUATION_MARKER} {payload}")
        } else {
            payload
        };
        self.write_event(observed_at, &step, token, &detail, TokenRole::Neutral)
    }

    fn render_agent_observation(
        &mut self,
        observed_at: OffsetDateTime,
        observation: NormalizedAgentObservation,
    ) -> Result<(), PresentationFailure> {
        let detail = if observation.continuation {
            format!(
                "{} · {SAFETY_CONTINUATION_MARKER} {}",
                observation.kind.as_str(),
                observation.payload
            )
        } else {
            format!("{} · {}", observation.kind.as_str(), observation.payload)
        };
        self.write_event(
            observed_at,
            &observation.step,
            "event",
            &detail,
            TokenRole::Neutral,
        )
    }

    fn finish_child_streams(
        &mut self,
        observed_at: ObservationTime,
    ) -> Result<(), PresentationFailure> {
        for record in self.feed.finish_child_streams(observed_at.utc) {
            self.render_record(record, observed_at.monotonic)?;
        }
        Ok(())
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

    fn write_summary_or_report_failure(
        &mut self,
        run: &WorkflowRunResult,
        publication: &PublicationPresentation<'_>,
        result_directory: Option<&str>,
    ) -> Result<(), WorkflowRunPresentationResult> {
        match self.write_summary(run, publication) {
            Ok(()) => Ok(()),
            Err(failure) => {
                let failure = failure.with_result_directory(result_directory);
                Err(WorkflowRunPresentationResult::Failed(failure))
            }
        }
    }

    fn write_summary(
        &mut self,
        run: &WorkflowRunResult,
        publication: &PublicationPresentation<'_>,
    ) -> Result<(), PresentationFailure> {
        let run_nodes = run.steps.iter().chain(
            run.finalization
                .iter()
                .flat_map(|finalization| &finalization.finalizers),
        );
        let steps = run_nodes
            .map(|step| (step.id.as_str(), step))
            .collect::<BTreeMap<_, _>>();
        if steps.len() != self.feed.definition().steps.len()
            || steps.len() != self.definition.steps.len()
            || steps.len()
                != run.steps.len()
                    + run
                        .finalization
                        .as_ref()
                        .map_or(0, |finalization| finalization.finalizers.len())
        {
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
        let order = self.feed.definition().presentation_order.clone();
        for id in order {
            let Some(step) = steps.get(id.as_str()) else {
                return Err(PresentationFailure::operation(
                    PresentationFailureOperation::InvalidTerminalResult,
                ));
            };
            let Some(definition) = self.feed.definition().steps.get(&id) else {
                return Err(PresentationFailure::operation(
                    PresentationFailureOperation::InvalidTerminalResult,
                ));
            };
            let Some(details) = self.definition.steps.get(&id) else {
                return Err(PresentationFailure::operation(
                    PresentationFailureOperation::InvalidTerminalResult,
                ));
            };
            let Some((state, detail, role)) = summary_step(step, details.success) else {
                return Err(PresentationFailure::operation(
                    PresentationFailureOperation::InvalidTerminalResult,
                ));
            };
            rows.push(SummaryRow {
                step: visible_text(&id),
                kind: step_kind(definition),
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
            "node", "kind", "state", "duration"
        );
        let header = self.styled_text(&header, TextTone::Muted);
        self.write_line_bytes(
            format!(
                "{}\n{header}\n",
                self.styled_text("ordinary phase", TextTone::Secondary)
            )
            .as_bytes(),
        )?;
        let finalization_start = self.feed.definition().finalization_start;
        for (index, row) in rows.into_iter().enumerate() {
            if finalization_start == Some(index) {
                let trigger = run.finalization.as_ref().map_or("unknown", |finalization| {
                    finalization_trigger(finalization.trigger)
                });
                let heading = self.styled_text(
                    &format!("finalization phase · trigger {trigger}"),
                    TextTone::Secondary,
                );
                self.write_line_bytes(format!("\n{heading}\n{header}\n").as_bytes())?;
            }
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
                            "{} {} · {}",
                            match primary_failure.role {
                                crate::execution::workflow::validated::WorkflowNodeRole::Step => "step",
                                crate::execution::workflow::validated::WorkflowNodeRole::Finalizer => "finalizer",
                            },
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

        if let Some(finalization) = &run.finalization {
            let issue_count = finalization
                .finalizers
                .iter()
                .filter(|finalizer| {
                    matches!(
                        finalizer.state,
                        StepState::Failed { .. } | StepState::InputUnavailable { .. }
                    )
                })
                .count();
            let status = if finalization.force_abort {
                "cleanup incomplete · force abort accepted".to_owned()
            } else if let Some(cancellation) = &finalization.cancellation {
                format!(
                    "cleanup incomplete · cancelled {}",
                    cancellation_reason(cancellation.reason)
                )
            } else if issue_count == 0 {
                "cleanup complete".to_owned()
            } else {
                format!("cleanup complete · {issue_count} issues")
            };
            let tone = if finalization.force_abort || finalization.cancellation.is_some() {
                TokenRole::Blocked
            } else if issue_count == 0 {
                TokenRole::Success
            } else {
                TokenRole::Failure
            };
            let line = format!(
                "{} {}\n",
                self.styled_token("finalization:", tone),
                self.styled_text(&status, TextTone::Secondary),
            );
            self.write_line_bytes(line.as_bytes())?;
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

    fn present_publication_result(
        &mut self,
        publication: PublicationPresentation<'_>,
        emit_terminal_json: bool,
    ) -> WorkflowRunPresentationResult {
        match publication {
            PublicationPresentation::Failed(error) => {
                WorkflowRunPresentationResult::PublicationFailed(error.clone())
            }
            PublicationPresentation::Published(terminal) => {
                if emit_terminal_json
                    && self.mode == PresentationMode::Json
                    && let Err(error) = write_pretty_json(&mut self.standard_output, terminal)
                {
                    let failure = PresentationFailure::writer(
                        PresentationFailureOperation::TerminalJsonWriter,
                        &error,
                    )
                    .with_result_directory(Some(terminal.result_directory()));
                    return WorkflowRunPresentationResult::Failed(failure);
                }
                WorkflowRunPresentationResult::Published {
                    exit_status: terminal.exit_status(),
                    result_directory: terminal.result_directory().to_owned(),
                }
            }
        }
    }

    fn write_line_bytes(&mut self, bytes: &[u8]) -> Result<(), PresentationFailure> {
        self.line_writer().write_all(bytes).map_err(|error| {
            PresentationFailure::writer(PresentationFailureOperation::LineWriter, &error)
        })
    }

    fn line_writer(&mut self) -> &mut dyn Write {
        match self.mode {
            PresentationMode::Tui | PresentationMode::Plain => &mut self.standard_output,
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

pub(crate) fn header_timestamp(value: OffsetDateTime) -> String {
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

fn start_detail(step: &WorkflowPresentationStep) -> String {
    let detail = match step {
        WorkflowPresentationStep::Command { argv, cwd, .. } => {
            let argv = argv
                .iter()
                .map(|argument| shell_quote(argument))
                .collect::<Vec<_>>()
                .join(" ");
            cwd.as_ref().map_or_else(
                || format!("cmd · {argv}"),
                |cwd| format!("cmd · {cwd} $ {argv}"),
            )
        }
        WorkflowPresentationStep::Agent {
            profile, harness, ..
        } => match harness {
            AgentPresentationHarness::Pi { model, thinking } => {
                let thinking = format!("{thinking:?}").to_ascii_lowercase();
                format!("agent · {profile} · pi · {model} · thinking={thinking}")
            }
            AgentPresentationHarness::ClaudeCode { model, effort } => {
                format!(
                    "agent · {profile} · claude code · {model} · effort={}",
                    effort.as_str()
                )
            }
            AgentPresentationHarness::Codex { model, effort } => {
                format!("agent · {profile} · codex · {model} · effort={effort}")
            }
        },
    };
    visible_text(&detail)
}

pub(crate) fn step_kind(step: &WorkflowPresentationStep) -> &'static str {
    match step {
        WorkflowPresentationStep::Command { .. } => "cmd",
        WorkflowPresentationStep::Agent { .. } => "agent",
    }
}

pub(crate) fn shell_quote(argument: &str) -> String {
    shell_quote_visible_argument(&visible_argument_text(argument))
}

pub(crate) fn shell_quote_visible_argument(argument: &str) -> String {
    if !argument.is_empty()
        && argument.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'_' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-'
                )
        })
    {
        return argument.to_owned();
    }
    format!("'{}'", argument.replace('\'', "'\\''"))
}

fn visible_argument_text(value: &str) -> String {
    visible_text_with_backslash(value, true)
}

pub(crate) fn visible_text(value: &str) -> String {
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

fn observed_recovery_detail(observed: Option<ObservedStepTransition>) -> Option<String> {
    let ObservedStepTransition::Recovery {
        active,
        configured_rounds,
        handler_kind,
        handler_state,
        decision,
        ..
    } = observed?
    else {
        return None;
    };
    Some(recovery_progress_detail(
        active,
        configured_rounds,
        handler_kind,
        handler_state,
        decision,
    ))
}

pub(crate) fn recovery_progress_detail(
    active: ActiveStepInvocation,
    configured_rounds: u8,
    handler_kind: Option<RecoveryHandlerKind>,
    handler_state: Option<RecoveryHandlerActivity>,
    decision: Option<RecoveryDecisionKind>,
) -> String {
    let latest_round = match active {
        ActiveStepInvocation::Target { execution_number } => {
            execution_number.get().saturating_sub(1).max(1)
        }
        ActiveStepInvocation::RecoveryHandler { round } => round.get(),
    };
    let mut detail = match active {
        ActiveStepInvocation::Target { execution_number } => {
            format!(
                "target execution {} · round {latest_round}/{configured_rounds}",
                execution_number.get()
            )
        }
        ActiveStepInvocation::RecoveryHandler { round } => format!(
            "recovery_handler {} {} · round {}/{configured_rounds}",
            match handler_kind {
                Some(RecoveryHandlerKind::Command) => "cmd",
                Some(RecoveryHandlerKind::Agent) => "agent",
                None => "unknown",
            },
            match handler_state {
                Some(RecoveryHandlerActivity::Starting) => "starting",
                Some(RecoveryHandlerActivity::Running) => "running",
                None => "active",
            },
            round.get()
        ),
    };
    if let Some(decision) = decision {
        detail.push_str(" · decision ");
        detail.push_str(match decision {
            RecoveryDecisionKind::Recheck => "recheck",
            RecoveryDecisionKind::GaveUp => "gave_up",
        });
    }
    detail
}

pub(crate) fn cancellation_reason(reason: CancellationReason) -> &'static str {
    reason.as_str()
}

pub(crate) fn finalization_trigger(
    trigger: crate::execution::workflow::document::FinalizationTrigger,
) -> &'static str {
    match trigger {
        crate::execution::workflow::document::FinalizationTrigger::Succeeded => "succeeded",
        crate::execution::workflow::document::FinalizationTrigger::Failed => "failed",
        crate::execution::workflow::document::FinalizationTrigger::Cancelled => "cancelled",
    }
}

pub(crate) fn failure_detail(phase: FailurePhase, cause: &StepFailureCause) -> String {
    format!("{} · {}", phase.as_str(), failure_cause(cause))
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
            StepStartFailure::AgentInput(failure) => format!("agent input · {failure:?}"),
            StepStartFailure::Agent(failure) => failure.code().replace('_', " "),
            StepStartFailure::AgentRuntimeUnavailable => "agent runtime unavailable".to_owned(),
            StepStartFailure::OutputsUnsupported => "outputs unsupported".to_owned(),
            StepStartFailure::WorkingDirectory(failure) => match failure {
                WorkingDirectoryFailure::ExecutionRootRebound => "execution root rebound",
                WorkingDirectoryFailure::Unavailable => "working directory unavailable",
                WorkingDirectoryFailure::EscapesExecutionRoot => "working directory escape",
                WorkingDirectoryFailure::NotDirectory => "working directory not directory",
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
        StepFailureCause::Execution(StepExecutionFailure::Agent(failure)) => {
            failure.code().replace('_', " ")
        }
        StepFailureCause::RecoveryHandler(_) => "recovery handler failed".to_owned(),
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
                    CaptureFailureKind::InvalidTextEncoding => "output invalid UTF-8",
                    CaptureFailureKind::InvalidJson => "output invalid JSON",
                    CaptureFailureKind::DuplicateJsonMember => "output duplicate JSON member",
                    CaptureFailureKind::JsonSchemaMismatch => "output JSON schema mismatch",
                    CaptureFailureKind::FileCountLimitExceeded => "captured file count limit",
                    CaptureFailureKind::FileSizeLimitExceeded => "captured file size limit",
                    CaptureFailureKind::TotalSizeLimitExceeded => "captured total size limit",
                    CaptureFailureKind::GitCarrierCountLimitExceeded => {
                        "captured Git carrier count limit"
                    }
                    CaptureFailureKind::GitCarrierSizeLimitExceeded => {
                        "captured Git carrier size limit"
                    }
                    CaptureFailureKind::TotalGitCarrierSizeLimitExceeded => {
                        "captured total Git carrier size limit"
                    }
                    CaptureFailureKind::CarrierProducerUnavailable => {
                        "Git carrier producer unavailable"
                    }
                    CaptureFailureKind::StagingUnavailable => "output staging unavailable",
                };
                format!("{code} · output {}", failure.output_identity())
            }
            OutputCaptureFailure::Git { output, failure } => match failure {
                GitCaptureFailure::CommandTimedOut(_) => {
                    format!("{failure} · output {output}")
                }
                _ => format!("Git branch capture {failure:?} · output {output}"),
            },
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
    match (presentation, output_count) {
        (StepSuccessPresentation::Command, 0) => "exit 0".to_owned(),
        (StepSuccessPresentation::Command, 1) => "exit 0 · 1 output".to_owned(),
        (StepSuccessPresentation::Command, count) => format!("exit 0 · {count} outputs"),
        (StepSuccessPresentation::Agent, 1) => "1 output committed".to_owned(),
        (StepSuccessPresentation::Agent, count) => format!("{count} outputs committed"),
    }
}

fn summary_step(
    step: &WorkflowRunStep,
    success: StepSuccessPresentation,
) -> Option<(&'static str, String, TokenRole)> {
    let (state, mut detail, role) = match &step.state {
        StepState::Succeeded { outputs } => Some((
            "succeeded",
            success_detail(success, outputs.len()),
            TokenRole::Success,
        )),
        StepState::Failed { phase, cause } => Some((
            "failed",
            issue_detail(failure_detail(*phase, cause), step.failure_policy),
            TokenRole::Failure,
        )),
        StepState::Blocked { dependency } => Some((
            "blocked",
            issue_detail(format!("by {dependency}"), step.failure_policy),
            TokenRole::Blocked,
        )),
        StepState::InputUnavailable { references } => Some((
            "blocked",
            issue_detail(
                format!("inputs unavailable: {}", references.join(", ")),
                step.failure_policy,
            ),
            TokenRole::Blocked,
        )),
        StepState::NotRun {
            reason: NotRunReason::FailureStop,
        } => Some(("not-run", "failure stop".to_owned(), TokenRole::Neutral)),
        StepState::NotRun {
            reason: NotRunReason::FinalizerTriggerNotSelected,
        } => Some((
            "not-run",
            "finalizer trigger not selected".to_owned(),
            TokenRole::Neutral,
        )),
        StepState::Cancelled { reason } => Some((
            "cancelled",
            cancellation_reason(*reason).to_owned(),
            TokenRole::Blocked,
        )),
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => return None,
    }?;
    if let Some(recovery) = &step.recovery {
        let usage = super::publication::total_recovery_usage(&step.invocations);
        let terminal_failure = match &step.state {
            StepState::Failed { phase, cause } => Some(failure_detail(*phase, cause)),
            _ => None,
        };
        detail.push_str(&format!(
            " · {} · {} invocations · usage input {} output {}",
            terminal_recovery_detail(recovery, terminal_failure.as_deref()),
            step.invocations.len(),
            usage.input_tokens,
            usage.output_tokens,
        ));
    }
    Some((state, detail, role))
}

pub(crate) fn terminal_recovery_detail(
    recovery: &super::publication::StepRecoverySummaryV1,
    terminal_failure: Option<&str>,
) -> String {
    let mut detail = match &recovery.termination {
        super::publication::RecoveryTerminationV1::Recovered { execution_number } => {
            format!("recovered at target execution {execution_number}")
        }
        super::publication::RecoveryTerminationV1::Exhausted { execution_number } => {
            format!("recovery exhausted at target execution {execution_number}")
        }
        super::publication::RecoveryTerminationV1::GaveUp { round } => {
            format!("recovery gave_up at round {round}")
        }
        super::publication::RecoveryTerminationV1::HandlerFailed { round, .. } => {
            format!("recovery handler failed at round {round}")
        }
        super::publication::RecoveryTerminationV1::Cancelled { round, .. } => {
            format!("recovery cancelled at round {round}")
        }
    };
    let retained_failure = recovery
        .rounds
        .last()
        .map(|round| published_failure_detail(&round.failed_execution.failure));
    if let Some(failure) = terminal_failure.or(retained_failure.as_deref()) {
        detail.push_str(" · latest target failure ");
        detail.push_str(failure);
    }
    detail
}

fn published_failure_detail(failure: &super::publication::FailureV1) -> String {
    let phase = match failure.phase {
        super::publication::FailurePhaseV1::Start => "start",
        super::publication::FailurePhaseV1::Execution => "execution",
        super::publication::FailurePhaseV1::OutputCapture => "output_capture",
    };
    let mut cause = snake_case_debug(failure.cause.code);
    if let Some(input) = &failure.cause.input {
        cause.push_str(" · input ");
        cause.push_str(&visible_text(input));
    }
    if let Some(index) = failure.cause.collection_index {
        cause.push_str(&format!(" · collection index {index}"));
    }
    if let Some(output) = &failure.cause.output {
        cause.push_str(" · output ");
        cause.push_str(&visible_text(output));
    }
    if let Some(exit_code) = failure.cause.exit_code {
        cause.push_str(&format!(" · exit {exit_code}"));
    }
    format!("{phase} · {cause}")
}

pub(crate) fn snake_case_debug(value: impl std::fmt::Debug) -> String {
    let value = format!("{value:?}");
    let mut result = String::with_capacity(value.len());
    for (index, character) in value.chars().enumerate() {
        if character.is_ascii_uppercase() {
            if index != 0 {
                result.push('_');
            }
            result.push(character.to_ascii_lowercase());
        } else {
            result.push(character);
        }
    }
    result
}

fn issue_detail(detail: String, failure_policy: FailurePolicy) -> String {
    match failure_policy {
        FailurePolicy::Required => detail,
        FailurePolicy::Advisory => format!("{detail} · advisory"),
    }
}

fn terminal_counts(run: &WorkflowRunResult) -> [(&'static str, usize); 6] {
    let mut counts = [
        ("succeeded", 0),
        ("failed", 0),
        ("blocked", 0),
        ("not-run", 0),
        ("cancelled", 0),
        ("advisory issues", 0),
    ];
    for step in run.steps.iter().chain(
        run.finalization
            .iter()
            .flat_map(|finalization| &finalization.finalizers),
    ) {
        let index = match step.state {
            StepState::Succeeded { .. } => Some(0),
            StepState::Failed { .. } => Some(1),
            StepState::Blocked { .. } | StepState::InputUnavailable { .. } => Some(2),
            StepState::NotRun { .. } => Some(3),
            StepState::Cancelled { .. } => Some(4),
            StepState::Pending
            | StepState::Starting
            | StepState::Running
            | StepState::CapturingOutputs
            | StepState::Recovering { .. }
            | StepState::Cancelling { .. } => None,
        };
        if let Some(index) = index {
            counts[index].1 += 1;
        }
        if step.failure_policy == FailurePolicy::Advisory
            && matches!(
                step.state,
                StepState::Failed { .. }
                    | StepState::Blocked { .. }
                    | StepState::InputUnavailable { .. }
            )
        {
            counts[5].1 += 1;
        }
    }
    counts
}

pub(crate) fn human_duration(duration: Duration) -> String {
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
