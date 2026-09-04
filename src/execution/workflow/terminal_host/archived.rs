use std::io;

use super::*;
use crate::execution::workflow::archived_attempt::{
    ArchivedDiagnosticStream, ArchivedStep, ArchivedStepDetail, ArchivedStepState,
    ArchivedWorkflowOutcome, LocalArchivedAttempt,
};
use crate::execution::workflow::archived_presentation::{
    archived_cancellation_reason, archived_failure_detail, archived_finalization_trigger,
    blocked_detail, condition_false_detail, safe_path, safe_text,
};
use crate::execution::workflow::evidence::{NodeDetail, PrimaryIssueDetail};
use crate::execution::workflow::presentation_feed::{
    NormalizedRetainedRecord, normalize_retained_prefix, normalize_terminal_shell_argument,
};

const ARCHIVED_WORKFLOW_COLUMN_PERCENTAGE: u16 = 52;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArchivedTerminalHostExit {
    Quit,
    Interrupted,
    Terminated,
}

#[derive(Clone)]
pub(crate) struct ArchivedTerminalExitRequest {
    exit: tokio::sync::mpsc::UnboundedSender<ArchivedTerminalHostExit>,
}

impl ArchivedTerminalExitRequest {
    pub(crate) fn request(&self, exit: ArchivedTerminalHostExit) {
        let _ = self.exit.send(exit);
    }
}

pub(crate) struct ArchivedWorkflowTerminalHost {
    exit: tokio::sync::mpsc::UnboundedSender<ArchivedTerminalHostExit>,
    task: Option<tokio::task::JoinHandle<Result<ArchivedTerminalHostExit, PresentationFailure>>>,
}

impl ArchivedWorkflowTerminalHost {
    pub(crate) fn start(
        attempt: LocalArchivedAttempt,
        color: bool,
    ) -> Result<Self, PresentationFailure> {
        Self::start_with_boundary(attempt, color, SystemTerminalBoundary::new())
    }

    fn start_with_boundary<Boundary>(
        attempt: LocalArchivedAttempt,
        color: bool,
        boundary: Boundary,
    ) -> Result<Self, PresentationFailure>
    where
        Boundary: ArchivedTerminalBoundary,
    {
        let view = ArchivedTerminalView::new(attempt);
        let mut terminal = RestoringTerminal::new(boundary);
        let area = terminal.boundary.setup().map_err(|error| {
            presentation_failure(PresentationFailureOperation::TerminalSetup, &error)
        })?;
        let mut interaction = ArchivedHostInteraction {
            terminal_area: area,
            ..ArchivedHostInteraction::default()
        };
        if let Err(error) = terminal
            .boundary
            .draw_archived(&view, &mut interaction, color)
        {
            let failure = presentation_failure(PresentationFailureOperation::TerminalDraw, &error);
            let _ = terminal.restore();
            return Err(failure);
        }
        let _ = terminal
            .boundary
            .notify_lifecycle(TerminalLifecycleEvent::QuitEligible);

        let (exit, exit_receiver) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(run_archived_terminal(
            terminal,
            view,
            color,
            exit_receiver,
            interaction,
        ));
        Ok(Self {
            exit,
            task: Some(task),
        })
    }

    pub(crate) fn exit_request(&self) -> ArchivedTerminalExitRequest {
        ArchivedTerminalExitRequest {
            exit: self.exit.clone(),
        }
    }

    pub(crate) async fn wait(mut self) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
        archived_join_result(self.take_task()?.await)
    }

    fn take_task(
        &mut self,
    ) -> Result<
        tokio::task::JoinHandle<Result<ArchivedTerminalHostExit, PresentationFailure>>,
        PresentationFailure,
    > {
        self.task.take().ok_or_else(|| {
            PresentationFailure::operation(PresentationFailureOperation::TerminalTask)
        })
    }
}

impl Drop for ArchivedWorkflowTerminalHost {
    fn drop(&mut self) {
        let _ = self.exit.send(ArchivedTerminalHostExit::Terminated);
    }
}

fn archived_join_result(
    result: Result<Result<ArchivedTerminalHostExit, PresentationFailure>, tokio::task::JoinError>,
) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
    match result {
        Ok(result) => result,
        Err(_) => Err(PresentationFailure::operation(
            PresentationFailureOperation::TerminalTask,
        )),
    }
}

trait ArchivedTerminalBoundary: TerminalBoundary {
    fn draw_archived(
        &mut self,
        view: &ArchivedTerminalView,
        interaction: &mut ArchivedHostInteraction,
        color: bool,
    ) -> io::Result<()>;
}

impl ArchivedTerminalBoundary for SystemTerminalBoundary {
    fn draw_archived(
        &mut self,
        view: &ArchivedTerminalView,
        interaction: &mut ArchivedHostInteraction,
        color: bool,
    ) -> io::Result<()> {
        self.surface_mut()?.draw_archived(view, interaction, color)
    }
}

impl TerminalSurface {
    fn draw_archived(
        &mut self,
        view: &ArchivedTerminalView,
        interaction: &mut ArchivedHostInteraction,
        color: bool,
    ) -> io::Result<()> {
        clamp_step_selection(&mut interaction.selected, view.steps.len());
        let graph = self
            .graph
            .get_or_insert_with(|| DagLayout::for_steps(&view.steps));
        self.terminal
            .draw(|frame| render_archived(frame, view, graph, interaction, color))?;
        Ok(())
    }
}

async fn run_archived_terminal<Boundary: ArchivedTerminalBoundary>(
    mut terminal: RestoringTerminal<Boundary>,
    view: ArchivedTerminalView,
    color: bool,
    mut requested_exit: tokio::sync::mpsc::UnboundedReceiver<ArchivedTerminalHostExit>,
    mut interaction: ArchivedHostInteraction,
) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
    loop {
        tokio::select! {
            biased;
            exit = requested_exit.recv() => {
                return restore_archived_terminal(
                    &mut terminal,
                    exit.unwrap_or(ArchivedTerminalHostExit::Terminated),
                );
            }
            event = terminal.boundary.next_event() => {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        return fail_archived_terminal(
                            &mut terminal,
                            PresentationFailureOperation::TerminalInput,
                            &error,
                        );
                    }
                };
                if event == TerminalInputEvent::Resize {
                    match terminal.boundary.resize() {
                        Ok(area) => interaction.terminal_area = area,
                        Err(error) => {
                            return fail_archived_terminal(
                                &mut terminal,
                                PresentationFailureOperation::TerminalDraw,
                                &error,
                            );
                        }
                    }
                } else if let Some(exit) = interaction.handle_key(event, &view) {
                    return restore_archived_terminal(&mut terminal, exit);
                }
                if event == TerminalInputEvent::Help && interaction.help_visible {
                    let _ = terminal
                        .boundary
                        .notify_lifecycle(TerminalLifecycleEvent::HelpOpened);
                }
                if let Err(error) = terminal
                    .boundary
                    .draw_archived(&view, &mut interaction, color)
                {
                    return fail_archived_terminal(
                        &mut terminal,
                        PresentationFailureOperation::TerminalDraw,
                        &error,
                    );
                }
            }
        }
    }
}

fn restore_archived_terminal<Boundary: TerminalBoundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    exit: ArchivedTerminalHostExit,
) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
    terminal.restore().map_or_else(
        |error| {
            Err(presentation_failure(
                PresentationFailureOperation::TerminalRestore,
                &error,
            ))
        },
        |()| Ok(exit),
    )
}

fn fail_archived_terminal<Boundary: TerminalBoundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    operation: PresentationFailureOperation,
    error: &io::Error,
) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
    let failure = presentation_failure(operation, error);
    let _ = terminal.restore();
    Err(failure)
}

struct ArchivedTerminalView {
    summary: Vec<ArchivedSummaryLine>,
    steps: Vec<ArchivedTerminalStepView>,
    phase_boundary: Option<StepPhaseBoundary>,
}

struct ArchivedSummaryLine {
    text: String,
    tone: Tone,
}

struct ArchivedTerminalStepView {
    id: String,
    role: crate::execution::workflow::validated::WorkflowNodeRole,
    definition: WorkflowPresentationStep,
    command: Option<String>,
    state: StepStateKind,
    timing: Option<super::super::run_view_model::WorkflowRunElapsed>,
    detail: ArchivedStepDetail,
    output: ArchivedCommandOutputView,
    recovery: Option<super::super::publication::StepRecoverySummaryV1>,
    invocations: Vec<super::super::publication::RecoveryInvocationV1>,
}

#[derive(Clone)]
enum ArchivedCommandOutputView {
    Missing,
    Present {
        stdout: ArchivedStreamView,
        stderr: ArchivedStreamView,
    },
}

#[derive(Clone)]
struct ArchivedStreamView {
    source: ArchivedStreamSource,
    records: Vec<NormalizedRetainedRecord>,
    unterminated: bool,
    retained_bytes: u64,
    discarded_bytes: u64,
    truncated: bool,
    fully_drained: bool,
}

#[derive(Clone, Copy)]
enum ArchivedStreamSource {
    StandardOutput,
    StandardError,
}

impl ArchivedTerminalView {
    fn new(attempt: LocalArchivedAttempt) -> Self {
        let summary = archived_summary(&attempt);
        let phase_boundary = attempt
            .workflow
            .finalization_start
            .zip(attempt.finalization.as_ref())
            .map(|(finalization_start, finalization)| StepPhaseBoundary {
                finalization_start,
                trigger: Some(archived_finalization_trigger(finalization.trigger)),
            });
        let mut definitions = attempt.workflow.steps;
        let steps = attempt
            .steps
            .into_iter()
            .filter_map(|step| {
                let definition = definitions.remove(&step.id)?;
                Some(ArchivedTerminalStepView::new(step, definition))
            })
            .collect::<Vec<_>>();
        Self {
            summary,
            steps,
            phase_boundary,
        }
    }
}

impl ArchivedTerminalStepView {
    fn new(step: ArchivedStep, definition: WorkflowPresentationStep) -> Self {
        let command = archived_command(&definition);
        let timing = step
            .started_at
            .zip(step.duration)
            .map(
                |(started_at, duration)| super::super::run_view_model::WorkflowRunElapsed {
                    started_at,
                    duration,
                    frozen: true,
                },
            );
        let output =
            step.command_output
                .as_ref()
                .map_or(ArchivedCommandOutputView::Missing, |output| {
                    ArchivedCommandOutputView::Present {
                        stdout: ArchivedStreamView::new(
                            ArchivedStreamSource::StandardOutput,
                            &output.stdout,
                        ),
                        stderr: ArchivedStreamView::new(
                            ArchivedStreamSource::StandardError,
                            &output.stderr,
                        ),
                    }
                });
        Self {
            id: safe_text(&step.id),
            role: step.role,
            definition: safe_definition(definition),
            command,
            state: archived_step_state(step.state),
            timing,
            recovery: step.recovery,
            invocations: step.invocations,
            detail: step.detail,
            output,
        }
    }
}

impl ArchivedStreamView {
    fn new(source: ArchivedStreamSource, stream: &ArchivedDiagnosticStream) -> Self {
        let normalized = normalize_retained_prefix(&stream.bytes);
        Self {
            source,
            records: normalized.records,
            unterminated: normalized.unterminated,
            retained_bytes: stream.retained_bytes,
            discarded_bytes: stream.discarded_bytes,
            truncated: stream.truncated,
            fully_drained: stream.fully_drained,
        }
    }
}

impl StepProjection for ArchivedTerminalStepView {
    // These accessors deliberately keep the archived terminal projection separate from
    // the live observation-backed view model while sharing read-only renderer geometry.
    // jscpd:ignore-start
    fn id(&self) -> &str {
        &self.id
    }

    fn definition(&self) -> &WorkflowPresentationStep {
        &self.definition
    }

    fn state(&self) -> StepStateKind {
        self.state
    }

    fn timing(&self) -> Option<&super::super::run_view_model::WorkflowRunElapsed> {
        self.timing.as_ref()
    }

    // jscpd:ignore-end
    fn dag_detail(&self) -> Option<String> {
        match &self.detail {
            ArchivedStepDetail::Succeeded => {
                let output_count = self.definition.outputs().len();
                match &self.definition {
                    WorkflowPresentationStep::Command { .. } if output_count == 0 => {
                        Some(self.with_recovery_detail("exit 0".to_owned()))
                    }
                    WorkflowPresentationStep::Command { .. } => Some(self.with_recovery_detail(
                        format!("exit 0 · {}", output_count_detail(output_count)),
                    )),
                    WorkflowPresentationStep::Agent { .. } if output_count != 0 => {
                        Some(self.with_recovery_detail(output_count_detail(output_count)))
                    }
                    WorkflowPresentationStep::Agent { .. } => self
                        .recovery
                        .as_ref()
                        .map(|_| self.with_recovery_detail(String::new())),
                }
            }
            ArchivedStepDetail::Evidence(NodeDetail::Failed(failure)) => {
                Some(self.with_recovery_detail(issue_detail_for_step(
                    archived_failure_detail(failure),
                    &self.definition,
                    self.state,
                )))
            }
            ArchivedStepDetail::Evidence(NodeDetail::Blocked(detail)) => Some(
                issue_detail_for_step(blocked_detail(detail), &self.definition, self.state),
            ),
            ArchivedStepDetail::Evidence(NodeDetail::Skipped(detail)) => {
                Some(condition_false_detail(detail))
            }
            ArchivedStepDetail::Evidence(NodeDetail::NotRun(detail)) => Some(
                crate::execution::workflow::presentation::snake_case_debug(detail.code),
            ),
            ArchivedStepDetail::Evidence(NodeDetail::Cancellation(detail)) => {
                Some(self.with_recovery_detail(
                    crate::execution::workflow::presentation::snake_case_debug(detail.code),
                ))
            }
        }
    }

    fn inspector_command(&self) -> Option<String> {
        self.command.clone().map(|command| {
            if self.role == crate::execution::workflow::validated::WorkflowNodeRole::Finalizer {
                format!("finalizer · {command}")
            } else {
                command
            }
        })
    }

    fn inspector_fact(&self) -> Option<InspectorField> {
        if self.recovery.is_some() {
            return Some(InspectorField::new(
                "recovery",
                self.with_recovery_detail(String::new()),
                match self.state {
                    StepStateKind::Succeeded => Tone::Success,
                    StepStateKind::Failed => Tone::Failure,
                    StepStateKind::Cancelled => Tone::Blocked,
                    _ => Tone::Neutral,
                },
            ));
        }
        match &self.detail {
            ArchivedStepDetail::Succeeded => Some(InspectorField::new(
                "outputs",
                output_count_detail(self.definition.outputs().len()),
                Tone::Success,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Failed(failure)) => Some(InspectorField::new(
                "failure",
                archived_failure_detail(failure),
                Tone::Failure,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Blocked(detail)) => Some(InspectorField::new(
                "prerequisites",
                blocked_detail(detail),
                Tone::Blocked,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Skipped(detail)) => Some(InspectorField::new(
                "condition",
                condition_false_detail(detail),
                Tone::Muted,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::NotRun(detail)) => Some(InspectorField::new(
                "not run",
                crate::execution::workflow::presentation::snake_case_debug(detail.code),
                Tone::Muted,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Cancellation(detail)) => {
                Some(InspectorField::new(
                    "cancellation",
                    crate::execution::workflow::presentation::snake_case_debug(detail.code),
                    Tone::Blocked,
                ))
            }
        }
    }

    fn inspector_outputs(&self) -> Vec<InspectorOutput> {
        self.definition
            .outputs()
            .iter()
            .map(|(name, output)| {
                let (kind, detail) = archived_output_description(output);
                InspectorOutput::declaration(safe_text(name), kind, detail)
            })
            .collect()
    }

    fn show_empty_outputs(&self) -> bool {
        false
    }
}

impl ArchivedTerminalStepView {
    fn with_recovery_detail(&self, base: String) -> String {
        let Some(recovery) = &self.recovery else {
            return base;
        };
        let terminal_failure = match &self.detail {
            ArchivedStepDetail::Evidence(NodeDetail::Failed(failure)) => {
                Some(archived_failure_detail(failure))
            }
            _ => None,
        };
        let termination = super::super::presentation::terminal_recovery_detail(
            recovery,
            terminal_failure.as_deref(),
        );
        let usage = super::super::publication::total_recovery_usage(&self.invocations);
        let recovery_detail = format!(
            "{termination} · {} invocations · usage input {} output {}",
            self.invocations.len(),
            usage.input_tokens,
            usage.output_tokens
        );
        if base.is_empty() {
            recovery_detail
        } else {
            format!("{base} · {recovery_detail}")
        }
    }
}

fn archived_summary(attempt: &LocalArchivedAttempt) -> Vec<ArchivedSummaryLine> {
    let selection = if attempt.attempt_number == attempt.current_attempt_number {
        "current at snapshot"
    } else {
        "historical"
    };
    let trigger = match attempt.trigger {
        crate::execution::workflow::archived_attempt::ArchivedAttemptTrigger::Initial => "initial",
        crate::execution::workflow::archived_attempt::ArchivedAttemptTrigger::ExplicitRetry => {
            "explicit retry"
        }
    };
    let attempt_state = match attempt.state {
        crate::execution::workflow::archived_attempt::ArchivedAttemptState::Succeeded => {
            "succeeded"
        }
        crate::execution::workflow::archived_attempt::ArchivedAttemptState::WorkflowFailed => {
            "workflow_failed"
        }
        crate::execution::workflow::archived_attempt::ArchivedAttemptState::Cancelled => {
            "cancelled"
        }
    };
    let (outcome, outcome_tone) = archived_outcome_status(attempt.outcome);
    let mut lines = vec![
        ArchivedSummaryLine {
            text: format!("run {}", safe_path(&attempt.run_directory)),
            tone: Tone::Primary,
        },
        ArchivedSummaryLine {
            text: format!(
                "attempt {} of {} · {selection} · {trigger}",
                attempt.attempt_number, attempt.current_attempt_number
            ),
            tone: Tone::Neutral,
        },
        ArchivedSummaryLine {
            text: format!(
                "workflow {} · {} {} · concurrency {}",
                safe_text(&attempt.workflow_path),
                attempt.steps.len(),
                if attempt.steps.len() == 1 {
                    "step"
                } else {
                    "steps"
                },
                attempt.execution.maximum_parallel_steps,
            ),
            tone: Tone::Neutral,
        },
        ArchivedSummaryLine {
            text: format!(
                "attempt state {attempt_state} · outcome {outcome} · result {}",
                safe_path(&attempt.result_directory)
            ),
            tone: outcome_tone,
        },
        ArchivedSummaryLine {
            text: format!(
                "created {} · started {} · settled {}",
                header_timestamp(attempt.created_at),
                attempt
                    .started_at
                    .map_or_else(|| "—".to_owned(), header_timestamp),
                header_timestamp(attempt.settled_at),
            ),
            tone: Tone::Muted,
        },
        ArchivedSummaryLine {
            text: format!(
                "execution {} → {} · {}",
                header_timestamp(attempt.execution.started_at),
                header_timestamp(attempt.execution.finished_at),
                human_duration(attempt.execution.duration),
            ),
            tone: Tone::Muted,
        },
    ];
    if let Some(primary) = &attempt.primary_issue {
        let detail = match &primary.detail {
            PrimaryIssueDetail::Failed(detail) => archived_failure_detail(detail),
            PrimaryIssueDetail::Blocked(detail) => blocked_detail(detail),
        };
        lines.push(ArchivedSummaryLine {
            text: format!(
                "primary issue {} {} · {:?} · {}",
                match primary.node.role {
                    crate::execution::workflow::validated::WorkflowNodeRole::Step => "step",
                    crate::execution::workflow::validated::WorkflowNodeRole::Finalizer =>
                        "finalizer",
                },
                safe_text(&primary.node.id),
                primary.state,
                detail,
            ),
            tone: Tone::Failure,
        });
    }
    if let Some(cancellation) = &attempt.cancellation {
        lines.push(ArchivedSummaryLine {
            text: format!(
                "cancellation {} · requested {} · force-stop {}",
                archived_cancellation_reason(cancellation.reason),
                header_timestamp(cancellation.requested_at),
                header_timestamp(cancellation.force_stop_deadline),
            ),
            tone: Tone::Blocked,
        });
    }
    if let Some(finalization) = &attempt.finalization {
        let trigger = match finalization.trigger {
            crate::execution::workflow::publication::FinalizationTriggerV1::Succeeded => {
                "succeeded"
            }
            crate::execution::workflow::publication::FinalizationTriggerV1::Failed => "failed",
            crate::execution::workflow::publication::FinalizationTriggerV1::Cancelled => {
                "cancelled"
            }
        };
        let cleanup = if finalization.force_abort {
            "incomplete · force abort accepted".to_owned()
        } else if let Some(cancellation) = &finalization.cancellation {
            format!(
                "incomplete · cancelled {}",
                archived_cancellation_reason(cancellation.reason)
            )
        } else {
            "complete".to_owned()
        };
        lines.push(ArchivedSummaryLine {
            text: format!(
                "finalization trigger {trigger} · {} issues · cleanup {cleanup}",
                finalization.issues.len()
            ),
            tone: if finalization.force_abort || finalization.cancellation.is_some() {
                Tone::Blocked
            } else if finalization.issues.is_empty() {
                Tone::Success
            } else {
                Tone::Failure
            },
        });
    }
    lines
}

fn archived_outcome_status(outcome: ArchivedWorkflowOutcome) -> (&'static str, Tone) {
    match outcome {
        ArchivedWorkflowOutcome::Succeeded => ("succeeded", Tone::Success),
        ArchivedWorkflowOutcome::Failed => ("failed", Tone::Failure),
        ArchivedWorkflowOutcome::Cancelled => ("cancelled", Tone::Blocked),
    }
}

fn archived_step_state(state: ArchivedStepState) -> StepStateKind {
    match state {
        ArchivedStepState::Succeeded => StepStateKind::Succeeded,
        ArchivedStepState::Failed => StepStateKind::Failed,
        ArchivedStepState::Blocked => StepStateKind::Blocked,
        ArchivedStepState::Skipped => StepStateKind::Skipped,
        ArchivedStepState::NotRun => StepStateKind::NotRun,
        ArchivedStepState::Cancelled => StepStateKind::Cancelled,
    }
}

fn archived_output_description(output: &WorkflowOutput) -> (&'static str, String) {
    (super::semantic_output_kind(output), "—".to_owned())
}

fn archived_command(definition: &WorkflowPresentationStep) -> Option<String> {
    let WorkflowPresentationStep::Command { argv, .. } = definition else {
        return None;
    };
    Some(
        argv.iter()
            .map(|argument| {
                shell_quote_visible_argument(&normalize_terminal_shell_argument(
                    argument.as_bytes(),
                ))
            })
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn safe_definition(mut definition: WorkflowPresentationStep) -> WorkflowPresentationStep {
    match &mut definition {
        WorkflowPresentationStep::Command {
            argv,
            cwd,
            direct_dependencies,
            outputs,
            ..
        } => {
            for argument in argv {
                *argument = safe_text(argument);
            }
            if let Some(cwd) = cwd {
                *cwd = safe_text(cwd);
            }
            for dependency in direct_dependencies {
                *dependency = safe_text(dependency);
            }
            normalize_outputs(outputs);
        }
        WorkflowPresentationStep::Agent {
            profile,
            harness,
            direct_dependencies,
            outputs,
            ..
        } => {
            *profile = safe_text(profile);
            match harness {
                AgentPresentationHarness::Pi { model, .. }
                | AgentPresentationHarness::ClaudeCode { model, .. } => {
                    *model = safe_text(model);
                }
                AgentPresentationHarness::Codex { model, effort } => {
                    *model = safe_text(model);
                    *effort = safe_text(effort);
                }
            }
            for dependency in direct_dependencies {
                *dependency = safe_text(dependency);
            }
            normalize_outputs(outputs);
        }
    }
    definition
}

fn normalize_outputs(outputs: &mut std::collections::BTreeMap<String, WorkflowOutput>) {
    for output in outputs.values_mut() {
        match output {
            WorkflowOutput::TextAgentResponse | WorkflowOutput::GitBranchWorkspace => {}
            WorkflowOutput::TextPath { path } => *path = safe_text(path),
            WorkflowOutput::JsonPath { path, schema } => {
                *path = safe_text(path);
                *schema = safe_text(schema);
            }
            WorkflowOutput::JsonAgentResult { schema } => *schema = safe_text(schema),
            WorkflowOutput::FilePath {
                path, media_type, ..
            } => {
                *path = safe_text(path);
                *media_type = safe_text(media_type);
            }
        }
    }
}

#[derive(Default)]
struct ArchivedHostInteraction {
    selected: usize,
    surface: HostSurface,
    help_visible: bool,
    terminal_area: Rect,
    output: ArchivedOutputInteraction,
}

impl ArchivedHostInteraction {
    fn handle_key(
        &mut self,
        event: TerminalInputEvent,
        view: &ArchivedTerminalView,
    ) -> Option<ArchivedTerminalHostExit> {
        clamp_step_selection(&mut self.selected, view.steps.len());
        if event == TerminalInputEvent::Quit {
            return Some(ArchivedTerminalHostExit::Quit);
        }
        if event == TerminalInputEvent::Cancel {
            return Some(ArchivedTerminalHostExit::Interrupted);
        }
        if self.help_visible {
            if event == TerminalInputEvent::Escape {
                self.help_visible = false;
            }
            return None;
        }
        if event == TerminalInputEvent::Help {
            self.help_visible = true;
            return None;
        }
        if !operational_area(self.terminal_area) {
            return None;
        }

        if self.surface == HostSurface::FullLog {
            self.synchronize_output(view);
            if let Some(navigation) = vertical_navigation(event) {
                let row_count = self.selected_document(view).len();
                self.output.navigate(row_count, navigation);
                return None;
            }
        }

        match event {
            TerminalInputEvent::Enter
                if self.surface == HostSurface::Split && !view.steps.is_empty() =>
            {
                self.surface = HostSurface::FullLog;
                self.output = ArchivedOutputInteraction::default();
                self.synchronize_output(view);
            }
            TerminalInputEvent::Escape => self.surface = HostSurface::Split,
            TerminalInputEvent::Up if self.surface == HostSurface::Split => {
                self.selected = self.selected.saturating_sub(1);
            }
            TerminalInputEvent::Down if self.surface == HostSurface::Split => {
                if self.selected.saturating_add(1) < view.steps.len() {
                    self.selected += 1;
                }
            }
            TerminalInputEvent::PanLeft if self.surface == HostSurface::FullLog => {
                self.output.horizontal_offset = self.output.horizontal_offset.saturating_sub(1);
            }
            TerminalInputEvent::PanRight if self.surface == HostSurface::FullLog => {
                let document = self.selected_document(view);
                self.output.pan_right(&document);
            }
            _ => {}
        }
        None
    }

    fn synchronize_output(&mut self, view: &ArchivedTerminalView) {
        let Some(step) = view.steps.get(self.selected) else {
            return;
        };
        let (width, rows) = archived_output_dimensions(self.terminal_area, step);
        self.output.synchronize(&output_document(step), width, rows);
    }

    fn selected_document(&self, view: &ArchivedTerminalView) -> Vec<ArchivedOutputRow> {
        view.steps
            .get(self.selected)
            .map_or_else(Vec::new, output_document)
    }
}

#[derive(Default)]
struct ArchivedOutputInteraction {
    top: usize,
    horizontal_offset: usize,
    available_width: usize,
    available_rows: usize,
}

impl ArchivedOutputInteraction {
    fn synchronize(&mut self, document: &[ArchivedOutputRow], width: usize, rows: usize) {
        self.available_width = width;
        self.available_rows = rows;
        self.top = self.top.min(maximum_document_top(document.len(), rows));
        self.horizontal_offset = self
            .horizontal_offset
            .min(maximum_document_horizontal_offset(document, width));
    }

    fn navigate(&mut self, row_count: usize, navigation: VerticalNavigation) {
        let bottom = maximum_document_top(row_count, self.available_rows);
        let page = self.available_rows.max(1);
        let half_page = (page / 2).max(1);
        self.top = match navigation {
            VerticalNavigation::Up => self.top.saturating_sub(1),
            VerticalNavigation::Down => self.top.saturating_add(1).min(bottom),
            VerticalNavigation::PageUp => self.top.saturating_sub(page),
            VerticalNavigation::PageDown => self.top.saturating_add(page).min(bottom),
            VerticalNavigation::HalfPageUp => self.top.saturating_sub(half_page),
            VerticalNavigation::HalfPageDown => self.top.saturating_add(half_page).min(bottom),
            VerticalNavigation::Top => 0,
            VerticalNavigation::Bottom => bottom,
        };
    }

    fn pan_right(&mut self, document: &[ArchivedOutputRow]) {
        self.horizontal_offset =
            self.horizontal_offset
                .saturating_add(1)
                .min(maximum_document_horizontal_offset(
                    document,
                    self.available_width,
                ));
    }
}

fn maximum_document_top(row_count: usize, available_rows: usize) -> usize {
    row_count.saturating_sub(available_rows)
}

fn maximum_document_horizontal_offset(
    document: &[ArchivedOutputRow],
    available_width: usize,
) -> usize {
    document
        .iter()
        .map(|row| display_width(&row.text).saturating_sub(available_width))
        .max()
        .unwrap_or(0)
}

fn render_archived(
    frame: &mut Frame<'_>,
    view: &ArchivedTerminalView,
    graph: &DagLayout,
    interaction: &mut ArchivedHostInteraction,
    color: bool,
) {
    let area = frame.area();
    interaction.terminal_area = area;
    clamp_step_selection(&mut interaction.selected, view.steps.len());
    frame.render_widget(Clear, area);
    if !operational_area(area) {
        render_archived_too_small(frame, area, color);
        return;
    }

    let sections =
        Layout::vertical([Constraint::Min(0), Constraint::Length(FOOTER_HEIGHT)]).split(area);
    if interaction.surface == HostSurface::FullLog {
        let selected_step = view.steps.get(interaction.selected);
        if selected_step.is_some() {
            interaction.synchronize_output(view);
        }
        let full_sections =
            inspector_and_log_areas(sections[0], inspector_desired_height(selected_step));
        render_inspector(frame, full_sections[0], selected_step, color, Borders::NONE);
        render_archived_full_output(
            frame,
            full_sections[1],
            selected_step,
            &interaction.output,
            color,
        );
        render_archived_footer(
            frame,
            sections[1],
            color,
            "OUTPUT",
            &ARCHIVED_OUTPUT_FOOTER_OPTIONS,
        );
    } else {
        render_archived_split(frame, sections[0], view, graph, interaction, color);
        render_archived_footer(
            frame,
            sections[1],
            color,
            "DAG",
            &ARCHIVED_SPLIT_FOOTER_OPTIONS,
        );
        render_split_footer_junction(
            frame,
            sections[0],
            sections[1].y,
            interaction.help_visible,
            color,
            archived_wide_split_columns(sections[0]),
        );
    }

    if interaction.help_visible {
        render_help_overlay_groups(
            frame,
            sections[0],
            archived_help_groups(interaction.surface),
            color,
        );
    }
}

fn archived_wide_split_columns(area: Rect) -> [Rect; 2] {
    let columns = Layout::horizontal([
        Constraint::Percentage(ARCHIVED_WORKFLOW_COLUMN_PERCENTAGE),
        Constraint::Percentage(100 - ARCHIVED_WORKFLOW_COLUMN_PERCENTAGE),
    ])
    .split(area);
    [columns[0], columns[1]]
}

fn render_archived_split(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &ArchivedTerminalView,
    graph: &DagLayout,
    interaction: &ArchivedHostInteraction,
    color: bool,
) {
    let selected_step = view.steps.get(interaction.selected);
    let layout = split_body_layout(
        area,
        archived_summary_height(view),
        inspector_desired_height(selected_step),
        archived_wide_split_columns(area),
    );
    render_archived_summary(frame, layout.summary, view, color, layout.summary_borders());
    render_split_steps(
        frame,
        layout,
        &view.steps,
        graph,
        view.phase_boundary,
        interaction.selected,
        color,
    );
    render_inspector(
        frame,
        layout.inspector,
        selected_step,
        color,
        layout.inspector_borders(),
    );
    render_archived_output_preview(frame, layout.output, selected_step, color, Borders::TOP);
    render_split_body_junctions(frame, layout, color);
}

fn archived_summary_height(view: &ArchivedTerminalView) -> u16 {
    u16::try_from(view.summary.len())
        .unwrap_or(u16::MAX)
        .saturating_add(1)
}

fn render_archived_summary(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &ArchivedTerminalView,
    color: bool,
    borders: Borders,
) {
    let block = summary_block(borders, color);
    let content = block.inner(area);
    let lines = view
        .summary
        .iter()
        .map(|line| {
            Line::from(Span::styled(
                ellipsize(&line.text, usize::from(content.width)),
                tone_style(color, line.tone),
            ))
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_archived_too_small(frame: &mut Frame<'_>, area: Rect, color: bool) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Terminal too small",
                tone_style(color, Tone::Failure),
            )),
            Line::from(format!(
                "Resize to at least {MINIMUM_WIDTH}x{MINIMUM_HEIGHT}."
            )),
            Line::from("Press q to quit or Ctrl-C to interrupt the viewer."),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(separator_style(color))
                .title(" Scherzo archived workflow attempt "),
        ),
        area,
    );
}

fn render_archived_output_preview(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&ArchivedTerminalStepView>,
    color: bool,
    borders: Borders,
) {
    let block = section_block(borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let content = block.inner(area);
    frame.render_widget(block, area);
    let Some(step) = step else {
        frame.render_widget(Paragraph::new("No workflow steps."), content);
        return;
    };
    match &step.output {
        ArchivedCommandOutputView::Missing => {
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(Span::styled(
                        "RETAINED OUTPUT",
                        tone_style(color, Tone::Muted),
                    )),
                    Line::default(),
                    Line::from("No durable command-stream prefixes exist."),
                ]),
                content,
            );
        }
        ArchivedCommandOutputView::Present { stdout, stderr } => {
            let regions =
                Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(content);
            render_archived_stream_preview(frame, regions[0], stdout, color);
            render_archived_stream_preview(frame, regions[1], stderr, color);
        }
    }
}

fn render_archived_stream_preview(
    frame: &mut Frame<'_>,
    area: Rect,
    stream: &ArchivedStreamView,
    color: bool,
) {
    if area.is_empty() {
        return;
    }
    let mut lines = vec![Line::from(Span::styled(
        ellipsize(
            &archived_stream_summary(stream, area.width < 100),
            usize::from(area.width),
        ),
        tone_style(color, archived_stream_tone(stream)),
    ))];
    let payload_rows = usize::from(area.height).saturating_sub(1);
    if payload_rows != 0 {
        if stream.records.is_empty() {
            lines.push(Line::from(Span::styled(
                "empty retained prefix",
                tone_style(color, Tone::Muted),
            )));
        } else {
            let payload_width = usize::from(area.width).max(1);
            lines.extend(
                stream
                    .records
                    .iter()
                    .flat_map(|record| wrap_archived_record(record, payload_width))
                    .take(payload_rows)
                    .map(|payload| {
                        Line::from(Span::styled(payload, tone_style(color, Tone::Neutral)))
                    }),
            );
        }
    }
    frame.render_widget(Paragraph::new(lines), area);
}

fn wrap_archived_record(record: &NormalizedRetainedRecord, width: usize) -> Vec<String> {
    let prefix = if record.continuation { "↪ " } else { "" };
    let payload_width = width.saturating_sub(display_width(prefix)).max(1);
    wrap_log_payload(&record.payload, payload_width)
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            if index == 0 {
                format!("{prefix}{payload}")
            } else {
                format!("↳ {payload}")
            }
        })
        .collect()
}

fn archived_stream_summary(stream: &ArchivedStreamView, compact: bool) -> String {
    let source = match (stream.source, compact) {
        (ArchivedStreamSource::StandardOutput, false) => "STDOUT",
        (ArchivedStreamSource::StandardError, false) => "STDERR",
        (ArchivedStreamSource::StandardOutput, true) => "OUT",
        (ArchivedStreamSource::StandardError, true) => "ERR",
    };
    if compact {
        format!(
            "{source} r={}B d={}B trunc={} drain={}",
            stream.retained_bytes,
            stream.discarded_bytes,
            yes_no(stream.truncated),
            if stream.fully_drained {
                "EOF"
            } else {
                "incomplete"
            },
        )
    } else {
        format!(
            "{source} · retained {} B · discarded {} B · truncated {} · fully drained {}{}",
            stream.retained_bytes,
            stream.discarded_bytes,
            yes_no(stream.truncated),
            yes_no(stream.fully_drained),
            if stream.fully_drained {
                ""
            } else {
                " (incomplete drain)"
            },
        )
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn archived_stream_tone(stream: &ArchivedStreamView) -> Tone {
    if stream.truncated || !stream.fully_drained {
        Tone::Blocked
    } else {
        Tone::Muted
    }
}

struct ArchivedOutputRow {
    text: String,
    tone: Tone,
}

fn output_document(step: &ArchivedTerminalStepView) -> Vec<ArchivedOutputRow> {
    let ArchivedCommandOutputView::Present { stdout, stderr } = &step.output else {
        return vec![ArchivedOutputRow {
            text: "No durable command-stream prefixes exist for this step.".to_owned(),
            tone: Tone::Muted,
        }];
    };
    let mut rows = vec![ArchivedOutputRow {
        text: "Stdout is shown before stderr for layout only; cross-stream order is unavailable."
            .to_owned(),
        tone: Tone::Blocked,
    }];
    append_stream_document(&mut rows, stdout);
    rows.push(ArchivedOutputRow {
        text: String::new(),
        tone: Tone::Muted,
    });
    append_stream_document(&mut rows, stderr);
    rows
}

fn append_stream_document(rows: &mut Vec<ArchivedOutputRow>, stream: &ArchivedStreamView) {
    rows.push(ArchivedOutputRow {
        text: match stream.source {
            ArchivedStreamSource::StandardOutput => "RETAINED STDOUT PREFIX".to_owned(),
            ArchivedStreamSource::StandardError => "RETAINED STDERR PREFIX".to_owned(),
        },
        tone: Tone::Primary,
    });
    rows.push(ArchivedOutputRow {
        text: archived_stream_summary(stream, false),
        tone: archived_stream_tone(stream),
    });
    if stream.records.is_empty() {
        rows.push(ArchivedOutputRow {
            text: "empty retained prefix".to_owned(),
            tone: Tone::Muted,
        });
    } else {
        rows.extend(stream.records.iter().map(|record| ArchivedOutputRow {
            text: format!(
                "{}{}",
                if record.continuation { "↪ " } else { "" },
                record.payload
            ),
            tone: Tone::Neutral,
        }));
    }
    if stream.unterminated {
        rows.push(ArchivedOutputRow {
            text: "⟂ retained-prefix boundary (final fragment has no line ending)".to_owned(),
            tone: Tone::Muted,
        });
    }
}

fn archived_output_dimensions(area: Rect, step: &ArchivedTerminalStepView) -> (usize, usize) {
    let body =
        archived_output_block(Borders::TOP, false).inner(selected_lower_panel_area(area, step));
    let rows = archived_output_areas(body)[1];
    (usize::from(rows.width), usize::from(rows.height))
}

fn render_archived_full_output(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&ArchivedTerminalStepView>,
    interaction: &ArchivedOutputInteraction,
    color: bool,
) {
    let block = archived_output_block(Borders::TOP, color);
    let content = block.inner(area);
    frame.render_widget(block, area);
    let sections = archived_output_areas(content);
    let title = step.map_or_else(
        || "RETAINED OUTPUT".to_owned(),
        |step| format!("RETAINED OUTPUT · {}", step.id),
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            ellipsize(&title, usize::from(sections[0].width)),
            tone_style(color, Tone::Muted),
        )),
        sections[0],
    );
    let Some(step) = step else {
        frame.render_widget(Paragraph::new("No workflow steps."), sections[1]);
        return;
    };
    let document = output_document(step);
    let lines = document
        .iter()
        .skip(interaction.top)
        .take(usize::from(sections[1].height))
        .map(|row| Line::from(Span::styled(row.text.clone(), tone_style(color, row.tone))))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines).scroll((
            0,
            u16::try_from(interaction.horizontal_offset).unwrap_or(u16::MAX),
        )),
        sections[1],
    );
}

fn archived_output_block(borders: Borders, color: bool) -> Block<'static> {
    section_block(borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING))
}

fn archived_output_areas(area: Rect) -> [Rect; 2] {
    let rows =
        Layout::vertical([Constraint::Length(LOG_HEADER_HEIGHT), Constraint::Min(0)]).split(area);
    [rows[0], rows[1]]
}

const ARCHIVED_SPLIT_FOOTER_OPTIONS: [&[&str]; 3] = [
    &["↑/k up", "↓/j down", "↵ open"],
    &["↑/k up", "↓/j down", "↵ open"],
    &["↑/k", "↓/j", "↵"],
];

const ARCHIVED_OUTPUT_FOOTER_OPTIONS: [&[&str]; 3] = [
    &[
        "Esc back",
        "↑/k up",
        "↓/j down",
        "PgUp/b page-up",
        "PgDn/f page-down",
        "←/h left",
        "→/l right",
    ],
    &["Esc back", "↑/k", "↓/j", "PgUp/b", "PgDn/f"],
    &["Esc", "↑/k", "↓/j"],
];

fn render_archived_footer(
    frame: &mut Frame<'_>,
    area: Rect,
    color: bool,
    label: &'static str,
    options: &[&[&str]],
) {
    let options = options
        .iter()
        .map(|commands| {
            let mut commands = commands
                .iter()
                .map(|command| (*command).to_owned())
                .collect::<Vec<_>>();
            commands.push("q quit".to_owned());
            commands.push("? help".to_owned());
            commands.join("  ")
        })
        .collect::<Vec<_>>();
    let reserved_width = u16::try_from(display_width(label).saturating_add(4)).unwrap_or(u16::MAX);
    let text = fitting_footer(options, area.width.saturating_sub(reserved_width));
    render_footer_text(frame, area, label, text, color);
}

fn archived_help_groups(surface: HostSurface) -> Vec<HelpGroup> {
    let mut groups = surface_help_groups(surface, OutputHelpMode::Archived);
    groups.push(HelpGroup {
        title: "VIEWER",
        commands: vec![
            HelpCommand {
                keys: "q",
                description: "quit",
            },
            HelpCommand {
                keys: "^C",
                description: "interrupt",
            },
        ],
    });
    groups
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::execution::workflow::archived_attempt::{
        ArchivedAttemptState, ArchivedAttemptTrigger, ArchivedCommandOutput, ArchivedExecution,
        ArchivedFailure,
    };
    use crate::execution::workflow::document::Output;
    use crate::execution::workflow::evidence::{
        FailureCode, FailureDetail, FailurePhase, NodeDetail, PrimaryIssue,
    };
    use crate::execution::workflow::presentation_feed::WorkflowPresentationDefinition;
    use crate::execution::workflow::resolution::{ContentDigestAlgorithm, WorkflowContentDigest};
    use crate::execution::workflow::validated::WorkflowNodeRole;

    #[test]
    fn frozen_archive_renders_context_dag_inspector_and_declarations() {
        let view = ArchivedTerminalView::new(archived_attempt(Some(hostile_output())));
        let graph = DagLayout::for_steps(&view.steps);
        let mut interaction = ArchivedHostInteraction::default();
        let buffer = render_view(&view, &graph, &mut interaction, 300, 40);
        let rendered = buffer_text(&buffer);

        for expected in [
            "run /tmp/archive-run",
            "attempt 2 of 3 · historical · explicit retry",
            "workflow workflows/archive.yaml · 2 steps · concurrency 2",
            "attempt state workflow_failed · outcome failed",
            "result /tmp/archive-run/attempts/0002/r",
            "created 2026-08-06 12:00:00Z",
            "execution 2026-08-06 12:00:01Z → 2026-08-06 12:00:04Z · 3.0s",
            "primary issue step verify · Failed · execution · command_exit · exit 17",
            "▏ ✓ prepare",
            "× verify",
            "prepare   cmd",
            "succeeded · required · 1.0s",
            "command",
            "printf payload",
            "report",
            "file",
            "1 output committed",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected:?}: {rendered:?}"
            );
        }
        assert!(!rendered.contains("captured"));
        assert!(!rendered.contains("pending"));
        assert!(!rendered.contains("unavailable"));
        assert!(!rendered.contains('\u{1b}'));

        assert_eq!(
            interaction.handle_key(TerminalInputEvent::Down, &view),
            None
        );
        let selected = buffer_text(&render_view(&view, &graph, &mut interaction, 300, 40));
        assert!(selected.contains("▏ × verify"));
        assert!(
            selected.contains("failure       execution · command_exit · exit 17"),
            "missing selected failure: {selected:?}"
        );
    }

    #[test]
    fn retained_prefixes_are_independent_safe_documents_with_exact_facts() {
        let view = ArchivedTerminalView::new(archived_attempt(Some(hostile_output())));
        let step = &view.steps[0];
        let document = output_document(step);
        let text = document
            .iter()
            .map(|row| row.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let stdout = text.find("RETAINED STDOUT PREFIX").unwrap();
        let stderr = text.find("RETAINED STDERR PREFIX").unwrap();

        assert!(stdout < stderr);
        assert!(text.contains("cross-stream order is unavailable"));
        assert!(text.contains("retained 24 B · discarded 0 B · truncated no · fully drained yes"));
        assert!(text.contains(&format!(
            "retained {} B · discarded 9 B · truncated yes · fully drained no (incomplete drain)",
            super::super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM
        )));
        assert!(text.contains("stdout red\\xff  end"));
        assert!(text.contains("stderr\\x00warning"));
        assert!(text.contains("retained-prefix boundary"));
        assert!(!text.contains('\u{1b}'));
        assert_eq!(
            safe_text("left\u{1b}]0;hostile title\u{7}right\n"),
            "leftright\\x0a"
        );
        assert_eq!(
            safe_path(std::path::Path::new(std::ffi::OsStr::from_bytes(
                b"/tmp/\xff\x1b]0;title\x07safe",
            ))),
            "/tmp/\\xffsafe"
        );
        for prohibited in [
            "following",
            "paused",
            "observed",
            "discarded records",
            "12:00:",
        ] {
            assert!(
                !text.contains(prohibited),
                "invented archive fact {prohibited:?}"
            );
        }

        let graph = DagLayout::for_steps(&view.steps);
        let mut interaction = ArchivedHostInteraction {
            terminal_area: Rect::new(0, 0, 120, 30),
            ..ArchivedHostInteraction::default()
        };
        interaction.handle_key(TerminalInputEvent::Enter, &view);
        let full = buffer_text(&render_view(&view, &graph, &mut interaction, 120, 30));
        assert!(full.contains("cross-stream order is unavailable"));
        assert!(full.contains("RETAINED STDOUT PREFIX"));
        assert!(!full.contains("F follow"));
    }

    #[test]
    fn empty_and_missing_command_output_remain_distinct() {
        let empty = ArchivedCommandOutput {
            stdout: stream(Vec::new(), 0, true),
            stderr: stream(Vec::new(), 0, true),
        };
        let empty_view = ArchivedTerminalView::new(archived_attempt(Some(empty)));
        let empty_document = output_document(&empty_view.steps[0]);
        let empty_text = empty_document
            .iter()
            .map(|row| row.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(empty_text.contains("RETAINED STDOUT PREFIX"));
        assert!(
            empty_text.contains("retained 0 B · discarded 0 B · truncated no · fully drained yes")
        );
        assert_eq!(empty_text.matches("empty retained prefix").count(), 2);

        let missing_view = ArchivedTerminalView::new(archived_attempt(None));
        let missing = output_document(&missing_view.steps[0]);
        assert_eq!(missing.len(), 1);
        assert!(
            missing[0]
                .text
                .contains("No durable command-stream prefixes exist")
        );
        assert!(!missing[0].text.contains("STDOUT"));
        assert!(!missing[0].text.contains("0 B"));
    }

    #[test]
    fn archived_command_arguments_keep_scalar_normalization_unambiguous() {
        let mut attempt = archived_attempt(None);
        let original = vec!["line\nbreak".to_owned(), r"line\x0abreak".to_owned()];
        let WorkflowPresentationStep::Command { argv, .. } =
            attempt.workflow.steps.get_mut("prepare").unwrap()
        else {
            panic!("prepare must remain a command step");
        };
        *argv = original.clone();
        let view = ArchivedTerminalView::new(attempt);
        let fields = inspector_fields(&view.steps[0], 200, 5);
        let command = fields
            .iter()
            .find(|field| field.label == "command")
            .unwrap();
        let expected = original
            .iter()
            .map(|argument| shell_quote(argument))
            .collect::<Vec<_>>()
            .join(" ");

        assert_eq!(command.value, expected);
    }

    #[test]
    fn archived_navigation_help_and_resize_preserve_static_viewport() {
        let view = ArchivedTerminalView::new(archived_attempt(Some(multiline_output(80))));
        let graph = DagLayout::for_steps(&view.steps);
        let mut interaction = ArchivedHostInteraction {
            terminal_area: Rect::new(0, 0, 120, 30),
            ..ArchivedHostInteraction::default()
        };
        interaction.handle_key(TerminalInputEvent::Enter, &view);
        interaction.handle_key(TerminalInputEvent::PageDown, &view);
        interaction.handle_key(TerminalInputEvent::PanRight, &view);
        let top = interaction.output.top;
        let horizontal_offset = interaction.output.horizontal_offset;
        assert!(top > 0);
        assert!(horizontal_offset > 0);

        interaction.handle_key(TerminalInputEvent::Help, &view);
        let help = buffer_text(&render_view(&view, &graph, &mut interaction, 120, 30));
        for expected in [
            "? — all commands",
            "MOVE",
            "JUMP",
            "PgDn/f/Space",
            "top",
            "bottom",
            "VIEWER",
            "interrupt",
        ] {
            assert!(help.contains(expected), "missing {expected:?}");
        }
        assert!(!help.contains("follow latest"));
        assert!(!help.contains("FILTER"));

        let too_small = buffer_text(&render_view(&view, &graph, &mut interaction, 40, 8));
        assert!(too_small.contains("Terminal too small"));
        assert_eq!(interaction.output.top, top);
        assert_eq!(interaction.output.horizontal_offset, horizontal_offset);
        assert!(interaction.help_visible);

        let recovered = buffer_text(&render_view(&view, &graph, &mut interaction, 120, 30));
        assert!(recovered.contains("? — all commands"));
        assert_eq!(interaction.output.top, top);
        assert_eq!(interaction.output.horizontal_offset, horizontal_offset);
        assert_eq!(interaction.selected, 0);
        assert_eq!(interaction.surface, HostSurface::FullLog);
    }

    #[test]
    fn archived_help_requested_while_too_small_opens_after_resize() {
        let view = ArchivedTerminalView::new(archived_attempt(None));
        let graph = DagLayout::for_steps(&view.steps);
        let mut interaction = ArchivedHostInteraction {
            terminal_area: Rect::new(0, 0, 40, 8),
            ..ArchivedHostInteraction::default()
        };

        assert_eq!(
            interaction.handle_key(TerminalInputEvent::Help, &view),
            None
        );
        assert!(
            interaction.help_visible,
            "a help request made in the too-small view must be retained"
        );
        let resized = buffer_text(&render_view(&view, &graph, &mut interaction, 120, 30));
        assert!(resized.contains("? — all commands"));
    }

    #[tokio::test]
    async fn archived_host_quit_interrupt_and_termination_restore_without_a_summary() {
        for (input, expected) in [
            (
                Some(TerminalInputEvent::Quit),
                ArchivedTerminalHostExit::Quit,
            ),
            (
                Some(TerminalInputEvent::Cancel),
                ArchivedTerminalHostExit::Interrupted,
            ),
            (None, ArchivedTerminalHostExit::Terminated),
        ] {
            let (host, sender, mut actions) =
                start_scripted_archive_host(BoundaryFailures::default());
            wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 100, 30))).await;
            let result = if let Some(input) = input {
                sender.send(ScriptedInput::Event(input)).unwrap();
                host.wait().await.unwrap()
            } else {
                let request = host.exit_request();
                request.request(ArchivedTerminalHostExit::Terminated);
                host.wait().await.unwrap()
            };
            assert_eq!(result, expected);
            wait_for_action(&mut actions, BoundaryAction::Restore).await;
            assert!(actions.try_recv().is_err());
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "real time only bounds a regression that leaves the terminal task detached"
    )]
    #[tokio::test]
    async fn dropping_archived_host_restores_the_terminal() {
        let (host, _sender, mut actions) = start_scripted_archive_host(BoundaryFailures::default());
        wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 100, 30))).await;
        let cleanup = host.exit_request();

        drop(host);
        let restored = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_action(&mut actions, BoundaryAction::Restore),
        )
        .await;
        if restored.is_err() {
            cleanup.request(ArchivedTerminalHostExit::Terminated);
            wait_for_action(&mut actions, BoundaryAction::Restore).await;
        }
        assert!(
            restored.is_ok(),
            "dropping an active archived host must restore its terminal"
        );
    }

    #[tokio::test]
    async fn archived_host_resize_uses_boundary_area_and_redraws() {
        let initial = Rect::new(0, 0, 100, 30);
        let resized = Rect::new(0, 0, 80, 24);
        let (mut boundary, sender, mut actions) =
            ScriptedArchiveBoundary::new(initial, BoundaryFailures::default());
        boundary.resize_areas.push_back(resized);
        let host = ArchivedWorkflowTerminalHost::start_with_boundary(
            archived_attempt(None),
            false,
            boundary,
        )
        .unwrap();
        wait_for_action(&mut actions, BoundaryAction::Draw(initial)).await;

        sender
            .send(ScriptedInput::Event(TerminalInputEvent::Resize))
            .unwrap();
        wait_for_action(&mut actions, BoundaryAction::Draw(resized)).await;
        sender
            .send(ScriptedInput::Event(TerminalInputEvent::Quit))
            .unwrap();

        assert_eq!(host.wait().await.unwrap(), ArchivedTerminalHostExit::Quit);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
    }

    #[tokio::test]
    async fn archived_host_failures_restore_and_preserve_failure_precedence() {
        for (failures, input, expected_operation) in [
            (
                BoundaryFailures {
                    draw_at: Some(2),
                    ..BoundaryFailures::default()
                },
                ScriptedInput::Event(TerminalInputEvent::Other),
                PresentationFailureOperation::TerminalDraw,
            ),
            (
                BoundaryFailures::default(),
                ScriptedInput::Failure,
                PresentationFailureOperation::TerminalInput,
            ),
        ] {
            let (host, sender, mut actions) = start_scripted_archive_host(failures);
            sender.send(input).unwrap();
            let failure = host.wait().await.unwrap_err();
            assert_eq!(failure.operation, expected_operation);
            wait_for_action(&mut actions, BoundaryAction::Restore).await;
        }

        for (failures, expected_operation) in [
            (
                BoundaryFailures {
                    setup: true,
                    ..BoundaryFailures::default()
                },
                PresentationFailureOperation::TerminalSetup,
            ),
            (
                BoundaryFailures {
                    draw_at: Some(1),
                    ..BoundaryFailures::default()
                },
                PresentationFailureOperation::TerminalDraw,
            ),
        ] {
            let (boundary, _sender, mut actions) =
                ScriptedArchiveBoundary::new(Rect::new(0, 0, 100, 30), failures);
            let failure = ArchivedWorkflowTerminalHost::start_with_boundary(
                archived_attempt(None),
                false,
                boundary,
            )
            .err()
            .unwrap();
            assert_eq!(failure.operation, expected_operation);
            wait_for_action(&mut actions, BoundaryAction::Restore).await;
        }

        let (host, sender, mut actions) = start_scripted_archive_host(BoundaryFailures::default());
        sender.send(ScriptedInput::Panic).unwrap();
        let failure = host.wait().await.unwrap_err();
        assert_eq!(
            failure.operation,
            PresentationFailureOperation::TerminalTask
        );
        wait_for_action(&mut actions, BoundaryAction::Restore).await;

        let (host, sender, mut actions) = start_scripted_archive_host(BoundaryFailures {
            restore: true,
            ..BoundaryFailures::default()
        });
        sender
            .send(ScriptedInput::Event(TerminalInputEvent::Quit))
            .unwrap();
        let failure = host.wait().await.unwrap_err();
        assert_eq!(
            failure.operation,
            PresentationFailureOperation::TerminalRestore
        );
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
    }

    fn archived_attempt(command_output: Option<ArchivedCommandOutput>) -> LocalArchivedAttempt {
        let started = timestamp("2026-08-06T12:00:01Z");
        let failure: ArchivedFailure = FailureDetail::new(
            FailurePhase::Execution,
            FailureCode::CommandExit,
            None,
            None,
            None,
            Some(17),
        )
        .unwrap();
        let prepare_definition = WorkflowPresentationStep::Command {
            argv: vec![
                "printf".to_owned(),
                "\u{1b}]0;hostile title\u{7}payload".to_owned(),
            ],
            cwd: Some("work".to_owned()),
            failure_policy: FailurePolicy::Required,
            direct_dependencies: Vec::new(),
            outputs: BTreeMap::from([(
                "report".to_owned(),
                Output::FilePath {
                    path: "report.txt".to_owned(),
                    media_type: "text/plain".to_owned(),
                },
            )]),
        };
        let verify_definition = WorkflowPresentationStep::Command {
            argv: vec!["verify".to_owned()],
            cwd: None,
            failure_policy: FailurePolicy::Required,
            direct_dependencies: vec!["prepare".to_owned()],
            outputs: BTreeMap::new(),
        };
        LocalArchivedAttempt {
            run_directory: PathBuf::from("/tmp/archive-run"),
            current_attempt_number: 3,
            attempt_number: 2,
            prior_attempt_number: Some(1),
            result_directory: PathBuf::from("/tmp/archive-run/attempts/0002/result"),
            trigger: ArchivedAttemptTrigger::ExplicitRetry,
            state: ArchivedAttemptState::WorkflowFailed,
            created_at: timestamp("2026-08-06T12:00:00Z"),
            started_at: Some(started),
            settled_at: timestamp("2026-08-06T12:00:05Z"),
            workflow_path: "workflows/archive.yaml".to_owned(),
            source_root: PathBuf::from("/tmp/source"),
            workflow_digest: WorkflowContentDigest {
                algorithm: ContentDigestAlgorithm::Sha256,
                value: "a".repeat(64),
            },
            workflow: WorkflowPresentationDefinition {
                workflow_path: "workflows/archive.yaml".to_owned(),
                presentation_order: vec!["prepare".to_owned(), "verify".to_owned()],
                finalization_start: None,
                steps: BTreeMap::from([
                    ("prepare".to_owned(), prepare_definition),
                    ("verify".to_owned(), verify_definition),
                ]),
                node_roles: BTreeMap::from([
                    ("prepare".to_owned(), WorkflowNodeRole::Step),
                    ("verify".to_owned(), WorkflowNodeRole::Step),
                ]),
            },
            execution: ArchivedExecution {
                execution_root: PathBuf::from("/tmp/execution"),
                maximum_parallel_steps: 2,
                started_at: started,
                finished_at: timestamp("2026-08-06T12:00:04Z"),
                duration: Duration::from_secs(3),
            },
            outcome: ArchivedWorkflowOutcome::Failed,
            primary_issue: Some(PrimaryIssue::failed(
                crate::execution::workflow::validated::WorkflowNode {
                    id: "verify".to_owned(),
                    role: WorkflowNodeRole::Step,
                },
                failure.clone(),
            )),
            cancellation: None,
            finalization: None,
            steps: vec![
                ArchivedStep {
                    id: "prepare".to_owned(),
                    role: WorkflowNodeRole::Step,
                    failure_policy: FailurePolicy::Required,
                    state: ArchivedStepState::Succeeded,
                    started_at: Some(started),
                    duration: Some(Duration::from_secs(1)),
                    detail: ArchivedStepDetail::Succeeded,
                    command_output,
                    recovery: None,
                    invocations: Vec::new(),
                },
                ArchivedStep {
                    id: "verify".to_owned(),
                    role: WorkflowNodeRole::Step,
                    failure_policy: FailurePolicy::Required,
                    state: ArchivedStepState::Failed,
                    started_at: Some(started + Duration::from_secs(1)),
                    duration: Some(Duration::from_secs(2)),
                    detail: ArchivedStepDetail::Evidence(NodeDetail::Failed(failure)),
                    command_output: None,
                    recovery: None,
                    invocations: Vec::new(),
                },
            ],
        }
    }

    fn hostile_output() -> ArchivedCommandOutput {
        let stdout = b"stdout \x1b[31mred\x1b[0m\xff\tend".to_vec();
        assert_eq!(stdout.len(), 24);
        let mut stderr = b"stderr\0\x1b]0;title\x07warning\n".to_vec();
        stderr.resize(
            usize::try_from(super::super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM).unwrap(),
            b'x',
        );
        ArchivedCommandOutput {
            stdout: stream(stdout, 0, true),
            stderr: stream(stderr, 9, false),
        }
    }

    fn multiline_output(lines: usize) -> ArchivedCommandOutput {
        let stdout = (0..lines)
            .map(|index| format!("line-{index:03}-{}\n", "x".repeat(160)))
            .collect::<String>()
            .into_bytes();
        ArchivedCommandOutput {
            stdout: stream(stdout, 0, true),
            stderr: stream(b"stderr\n".to_vec(), 0, true),
        }
    }

    fn stream(
        bytes: Vec<u8>,
        discarded_bytes: u64,
        fully_drained: bool,
    ) -> ArchivedDiagnosticStream {
        ArchivedDiagnosticStream {
            retained_bytes: u64::try_from(bytes.len()).unwrap(),
            bytes: Arc::from(bytes),
            discarded_bytes,
            truncated: discarded_bytes != 0,
            fully_drained,
        }
    }

    fn timestamp(value: &str) -> time::OffsetDateTime {
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).unwrap()
    }

    fn render_view(
        view: &ArchivedTerminalView,
        graph: &DagLayout,
        interaction: &mut ArchivedHostInteraction,
        width: u16,
        height: u16,
    ) -> ratatui::buffer::Buffer {
        let backend = ratatui::backend::TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_archived(frame, view, graph, interaction, false))
            .unwrap();
        terminal.backend().buffer().clone()
    }

    // The archived renderer fixture owns a distinct view type from the live renderer.
    // jscpd:ignore-start
    fn buffer_text(buffer: &ratatui::buffer::Buffer) -> String {
        buffer
            .content()
            .iter()
            .fold(String::new(), |mut rendered, cell| {
                rendered.push_str(cell.symbol());
                rendered
            })
    }

    // jscpd:ignore-end

    // The archive mock has immediate viewer exits and no workflow cancellation source.
    // jscpd:ignore-start
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum BoundaryAction {
        Setup,
        Draw(Rect),
        Input,
        Restore,
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct BoundaryFailures {
        setup: bool,
        draw_at: Option<usize>,
        restore: bool,
    }

    enum ScriptedInput {
        Event(TerminalInputEvent),
        Failure,
        Panic,
    }

    // jscpd:ignore-end
    struct ScriptedArchiveBoundary {
        area: Rect,
        input: tokio::sync::mpsc::UnboundedReceiver<ScriptedInput>,
        actions: tokio::sync::mpsc::UnboundedSender<BoundaryAction>,
        failures: BoundaryFailures,
        draw_count: usize,
        resize_areas: VecDeque<Rect>,
    }

    impl ScriptedArchiveBoundary {
        fn new(
            area: Rect,
            failures: BoundaryFailures,
        ) -> (
            Self,
            tokio::sync::mpsc::UnboundedSender<ScriptedInput>,
            tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
        ) {
            let (sender, input) = tokio::sync::mpsc::unbounded_channel();
            let (actions, receiver) = tokio::sync::mpsc::unbounded_channel();
            (
                Self {
                    area,
                    input,
                    actions,
                    failures,
                    draw_count: 0,
                    resize_areas: VecDeque::new(),
                },
                sender,
                receiver,
            )
        }

        fn record(&self, action: BoundaryAction) {
            let _ = self.actions.send(action);
        }
    }

    impl TerminalBoundary for ScriptedArchiveBoundary {
        fn setup(&mut self) -> io::Result<Rect> {
            self.record(BoundaryAction::Setup);
            if self.failures.setup {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected setup failure",
                ))
            } else {
                Ok(self.area)
            }
        }

        // The separate mock records archived input without live execution side effects.
        // jscpd:ignore-start
        fn next_event(&mut self) -> impl Future<Output = io::Result<TerminalInputEvent>> + Send {
            let actions = self.actions.clone();
            async move {
                match self.input.recv().await {
                    Some(ScriptedInput::Event(event)) => {
                        let _ = actions.send(BoundaryAction::Input);
                        Ok(event)
                    }
                    Some(ScriptedInput::Failure) | None => Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "injected input failure",
                    )),
                    Some(ScriptedInput::Panic) => {
                        std::panic::panic_any("injected archived terminal input panic")
                    }
                }
            }
        }

        // jscpd:ignore-end
        fn resize(&mut self) -> io::Result<Rect> {
            if let Some(area) = self.resize_areas.pop_front() {
                self.area = area;
            }
            Ok(self.area)
        }

        fn restore(&mut self) -> io::Result<()> {
            self.record(BoundaryAction::Restore);
            if self.failures.restore {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected restore failure",
                ))
            } else {
                Ok(())
            }
        }
    }

    impl ArchivedTerminalBoundary for ScriptedArchiveBoundary {
        fn draw_archived(
            &mut self,
            _view: &ArchivedTerminalView,
            interaction: &mut ArchivedHostInteraction,
            _color: bool,
        ) -> io::Result<()> {
            self.draw_count = self.draw_count.saturating_add(1);
            self.record(BoundaryAction::Draw(interaction.terminal_area));
            if self.failures.draw_at == Some(self.draw_count) {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected draw failure",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn start_scripted_archive_host(
        failures: BoundaryFailures,
    ) -> (
        ArchivedWorkflowTerminalHost,
        tokio::sync::mpsc::UnboundedSender<ScriptedInput>,
        tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
    ) {
        let (boundary, sender, actions) =
            ScriptedArchiveBoundary::new(Rect::new(0, 0, 100, 30), failures);
        let host = ArchivedWorkflowTerminalHost::start_with_boundary(
            archived_attempt(None),
            false,
            boundary,
        )
        .unwrap();
        (host, sender, actions)
    }

    // The archive action channel is intentionally independent from the live host fixture.
    // jscpd:ignore-start
    async fn wait_for_action(
        actions: &mut tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
        expected: BoundaryAction,
    ) {
        loop {
            let action = actions.recv().await.unwrap();
            if action == expected {
                return;
            }
        }
    }
    // jscpd:ignore-end
}
