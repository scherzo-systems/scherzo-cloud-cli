pub(crate) mod archived;
mod dag_layout;

use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossterm::cursor::{Hide, Show};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{execute, queue};
use futures_util::StreamExt as _;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Padding, Paragraph};
use ratatui::{Frame, Terminal, TerminalOptions, Viewport};
use rustix::termios::{OptionalActions, Termios, tcgetattr, tcgetwinsize, tcsetattr};
use time::UtcOffset;
use tokio::sync::oneshot;
use unicode_segmentation::UnicodeSegmentation as _;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use self::dag_layout::DagLayout;
#[cfg(test)]
use super::admission::CancellationOperation;
use super::admission::{CancellationReason, CancellationSource};
use super::document::{FailurePolicy, Output as WorkflowOutput};
use super::observation::{CommandOutputSource, ObservedStepTransition};
use super::presentation::{
    PresentationFailure, PresentationFailureOperation, cancellation_reason,
    canonical_blocked_detail, canonical_failure_detail, finalization_trigger, header_timestamp,
    human_duration, recovery_progress_detail, shell_quote, shell_quote_visible_argument, step_kind,
    visible_text,
};
use super::presentation_feed::{
    AcceptedRecordOrder, AgentPresentationHarness, AgentPresentationObservationKind,
    WorkflowPresentationStep,
};
use super::run_view_model::{
    WorkflowRunCleanupResult, WorkflowRunCleanupState, WorkflowRunLogRecord, WorkflowRunLogSource,
    WorkflowRunOutputDisposition, WorkflowRunOutputUnavailableReason, WorkflowRunPublicationResult,
    WorkflowRunPublicationState, WorkflowRunStepLog, WorkflowRunStepView, WorkflowRunViewModel,
    WorkflowRunViewSnapshot,
};
use super::runtime::{SchedulingGate, StepStateKind, WorkflowState};
#[cfg(test)]
use super::step_runtime::StepFailureCause;

const MINIMUM_WIDTH: u16 = 64;
const MINIMUM_HEIGHT: u16 = 20;
const WIDE_LAYOUT_WIDTH: u16 = 100;
const WORKFLOW_SUMMARY_HEIGHT: u16 = 3;
const FOOTER_HEIGHT: u16 = 2;
const MINIMUM_INSPECTOR_HEIGHT: u16 = 8;
const INSPECTOR_HEADER_HEIGHT: u16 = 3;
const INSPECTOR_PANEL_PADDING: u16 = 2;
const MINIMUM_OUTPUT_PANEL_HEIGHT: u16 = 2;
const MINIMUM_LOG_HEIGHT: u16 = 4;
const REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const RUNNING_INDICATOR_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const KIND_COLUMN_WIDTH: usize = 5;
const MINIMUM_DETAIL_WIDTH: usize = 12;
const INSPECTOR_LABEL_WIDTH: usize = 14;
const LOG_HEADER_HEIGHT: u16 = 2;
const LOG_TIMESTAMP_WIDTH: usize = 12;
const LOG_SOURCE_WIDTH: usize = 6;
const LOG_SEPARATOR_WIDTH: usize = 3;
const LOG_SOURCE_GUTTER_WIDTH: usize = LOG_SOURCE_WIDTH + LOG_SEPARATOR_WIDTH;
const LOG_TIMESTAMPED_GUTTER_WIDTH: usize = LOG_TIMESTAMP_WIDTH + 1 + LOG_SOURCE_GUTTER_WIDTH;
const MINIMUM_TIMESTAMPED_LOG_CONTENT_WIDTH: usize = 12;
const TERMINAL_LIFECYCLE_HANDSHAKE_ENVIRONMENT: &str =
    "SCHERZO_INTERNAL_WORKFLOW_RUN_TUI_HANDSHAKE";

pub(crate) struct WorkflowTerminalHost {
    activation: Option<oneshot::Sender<()>>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<TerminalHostExit, PresentationFailure>>,
    cancellation: CancellationSource,
    execution_active: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalHostExit {
    Quit,
    Stopped,
}

impl WorkflowTerminalHost {
    pub(crate) fn start<Clock>(
        view: WorkflowRunViewModel<Clock>,
        cancellation: CancellationSource,
        color: bool,
    ) -> Result<Self, PresentationFailure>
    where
        Clock: super::run_timing::ObservationClock,
    {
        Self::start_with_boundary(view, cancellation, color, SystemTerminalBoundary::new())
    }

    fn start_with_boundary<Clock, Boundary>(
        view: WorkflowRunViewModel<Clock>,
        cancellation: CancellationSource,
        color: bool,
        boundary: Boundary,
    ) -> Result<Self, PresentationFailure>
    where
        Clock: super::run_timing::ObservationClock,
        Boundary: WorkflowTerminalBoundary,
    {
        let mut terminal = RestoringTerminal::new(boundary);
        let area = terminal.boundary.setup().map_err(|error| {
            presentation_failure(PresentationFailureOperation::TerminalSetup, &error)
        })?;
        let mut interaction = HostInteraction {
            terminal_area: area,
            ..HostInteraction::default()
        };
        if let Err(error) = terminal.boundary.draw_workflow(
            &view.snapshot_for_render(interaction.selected),
            &mut interaction,
            color,
        ) {
            let failure = presentation_failure(PresentationFailureOperation::TerminalDraw, &error);
            let _ = terminal.restore();
            return Err(failure);
        }

        let (activation, activation_receiver) = oneshot::channel();
        let (shutdown, mut shutdown_receiver) = oneshot::channel();
        let task_cancellation = cancellation.clone();
        let execution_active = Arc::new(AtomicBool::new(false));
        let task_execution_active = Arc::clone(&execution_active);
        let task = tokio::spawn(async move {
            let mut unwind_guard = TerminalTaskUnwindGuard::new(
                task_cancellation.clone(),
                Arc::clone(&task_execution_active),
            );
            let activated = tokio::select! {
                biased;
                _ = &mut shutdown_receiver => false,
                activation = activation_receiver => activation.is_ok(),
            };
            let result = if activated {
                run_terminal(
                    terminal,
                    view,
                    task_cancellation,
                    task_execution_active,
                    color,
                    shutdown_receiver,
                    interaction,
                )
                .await
            } else {
                restore_terminal(
                    &mut terminal,
                    TerminalHostExit::Stopped,
                    &task_cancellation,
                    false,
                )
            };
            unwind_guard.disarm();
            result
        });
        Ok(Self {
            activation: Some(activation),
            shutdown: Some(shutdown),
            task,
            cancellation,
            execution_active,
        })
    }

    pub(crate) fn activate_execution(&mut self) -> Result<(), PresentationFailure> {
        let Some(activation) = self.activation.take() else {
            return Err(PresentationFailure::operation(
                PresentationFailureOperation::TerminalTask,
            ));
        };
        self.execution_active.store(true, Ordering::SeqCst);
        if activation.send(()).is_err() {
            self.execution_active.store(false, Ordering::SeqCst);
            return Err(PresentationFailure::operation(
                PresentationFailureOperation::TerminalTask,
            ));
        }
        Ok(())
    }

    pub(crate) async fn wait(mut self) -> Result<TerminalHostExit, PresentationFailure> {
        drop(self.activation.take());
        let shutdown = self.shutdown.take();
        let cancellation = self.cancellation.clone();
        let result = self.task.await;
        drop(shutdown);
        Self::join_result(&cancellation, &self.execution_active, result)
    }

    pub(crate) async fn stop(mut self) -> Result<TerminalHostExit, PresentationFailure> {
        drop(self.activation.take());
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let cancellation = self.cancellation.clone();
        let result = self.task.await;
        Self::join_result(&cancellation, &self.execution_active, result)
    }

    fn join_result(
        cancellation: &CancellationSource,
        execution_active: &AtomicBool,
        result: Result<Result<TerminalHostExit, PresentationFailure>, tokio::task::JoinError>,
    ) -> Result<TerminalHostExit, PresentationFailure> {
        match result {
            Ok(result) => result,
            Err(_) => {
                if execution_active.load(Ordering::SeqCst) {
                    cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
                }
                Err(PresentationFailure {
                    operation: PresentationFailureOperation::TerminalTask,
                    error_kind: None,
                    result_directory: None,
                })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalLifecycleEvent {
    HelpOpened,
    QuitEligible,
}

trait TerminalBoundary: Send + 'static {
    fn setup(&mut self) -> io::Result<Rect>;

    fn next_event(&mut self) -> impl Future<Output = io::Result<TerminalInputEvent>> + Send;

    fn resize(&mut self) -> io::Result<Rect>;

    fn restore(&mut self) -> io::Result<()>;

    fn notify_lifecycle(&mut self, _event: TerminalLifecycleEvent) -> io::Result<()> {
        Ok(())
    }
}

trait WorkflowTerminalBoundary: TerminalBoundary {
    fn draw_workflow(
        &mut self,
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        color: bool,
    ) -> io::Result<()>;
}

struct TerminalTaskUnwindGuard {
    cancellation: CancellationSource,
    execution_active: Arc<AtomicBool>,
    armed: bool,
}

impl TerminalTaskUnwindGuard {
    fn new(cancellation: CancellationSource, execution_active: Arc<AtomicBool>) -> Self {
        Self {
            cancellation,
            execution_active,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TerminalTaskUnwindGuard {
    fn drop(&mut self) {
        if self.armed && self.execution_active.load(Ordering::SeqCst) {
            self.cancellation
                .request_cancellation(CancellationReason::CallerOutputFailure);
        }
    }
}

struct RestoringTerminal<Boundary: TerminalBoundary> {
    boundary: Boundary,
    restored: bool,
}

impl<Boundary: TerminalBoundary> RestoringTerminal<Boundary> {
    fn new(boundary: Boundary) -> Self {
        Self {
            boundary,
            restored: false,
        }
    }

    fn restore(&mut self) -> io::Result<()> {
        if !begin_restoration(&mut self.restored) {
            return Ok(());
        }
        self.boundary.restore()
    }
}

impl<Boundary: TerminalBoundary> Drop for RestoringTerminal<Boundary> {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

async fn run_terminal<Clock, Boundary>(
    mut terminal: RestoringTerminal<Boundary>,
    view: WorkflowRunViewModel<Clock>,
    cancellation: CancellationSource,
    execution_active: Arc<AtomicBool>,
    color: bool,
    mut shutdown: oneshot::Receiver<()>,
    mut interaction: HostInteraction,
) -> Result<TerminalHostExit, PresentationFailure>
where
    Clock: super::run_timing::ObservationClock,
    Boundary: WorkflowTerminalBoundary,
{
    let mut changes = view.subscribe();
    let mut redraw = redraw_interval();
    redraw.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = redraw.tick().await;
    // The execution may advance before this task subscribes, so the first timed tick
    // refreshes the setup-time frame even when no later notification is observed.
    let mut dirty = true;

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                return restore_terminal(
                    &mut terminal,
                    TerminalHostExit::Stopped,
                    &cancellation,
                    execution_active.load(Ordering::SeqCst),
                );
            }
            event = terminal.boundary.next_event() => {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        let snapshot = view.snapshot_for_render(interaction.selected);
                        let active = workflow_is_executing(&snapshot);
                        execution_active.store(active, Ordering::SeqCst);
                        return fail_terminal(
                            &mut terminal,
                            PresentationFailureOperation::TerminalInput,
                            &error,
                            &cancellation,
                            active,
                        );
                    }
                };
                let snapshot = view.snapshot_for_render(interaction.selected);
                notify_quit_eligibility(&mut terminal.boundary, &snapshot);
                let active = workflow_is_executing(&snapshot);
                execution_active.store(active, Ordering::SeqCst);
                if event == TerminalInputEvent::Resize {
                    match terminal.boundary.resize() {
                        Ok(area) => interaction.terminal_area = area,
                        Err(error) => {
                            return fail_terminal(
                                &mut terminal,
                                PresentationFailureOperation::TerminalDraw,
                                &error,
                                &cancellation,
                                active,
                            );
                        }
                    }
                } else {
                    let control = interaction.handle_key(event, &snapshot, &cancellation);
                    if event == TerminalInputEvent::Help && interaction.help_visible {
                        let _ = terminal
                            .boundary
                            .notify_lifecycle(TerminalLifecycleEvent::HelpOpened);
                    }
                    if control == HostControl::Quit {
                        return restore_terminal(
                            &mut terminal,
                            TerminalHostExit::Quit,
                            &cancellation,
                            false,
                        );
                    }
                }
                let snapshot = view.snapshot_for_render(interaction.selected);
                notify_quit_eligibility(&mut terminal.boundary, &snapshot);
                let active = workflow_is_executing(&snapshot);
                execution_active.store(active, Ordering::SeqCst);
                if let Err(error) = terminal
                    .boundary
                    .draw_workflow(&snapshot, &mut interaction, color)
                {
                    return fail_terminal(
                        &mut terminal,
                        PresentationFailureOperation::TerminalDraw,
                        &error,
                        &cancellation,
                        active,
                    );
                }
                dirty = false;
            }
            changed = changes.changed() => {
                if changed.is_ok() {
                    let _ = changes.borrow_and_update();
                    let snapshot = view.snapshot_for_render(interaction.selected);
                    notify_quit_eligibility(&mut terminal.boundary, &snapshot);
                    execution_active.store(workflow_is_executing(&snapshot), Ordering::SeqCst);
                    dirty = true;
                }
            }
            _ = redraw.tick() => {
                let snapshot = view.snapshot_for_render(interaction.selected);
                notify_quit_eligibility(&mut terminal.boundary, &snapshot);
                let active = workflow_is_executing(&snapshot);
                execution_active.store(active, Ordering::SeqCst);
                if dirty || !snapshot.timing.frozen {
                    if let Err(error) = terminal
                        .boundary
                        .draw_workflow(&snapshot, &mut interaction, color)
                    {
                        return fail_terminal(
                            &mut terminal,
                            PresentationFailureOperation::TerminalDraw,
                            &error,
                            &cancellation,
                            active,
                        );
                    }
                    dirty = false;
                }
            }
        }
    }
}

fn notify_quit_eligibility<Boundary: TerminalBoundary>(
    boundary: &mut Boundary,
    snapshot: &WorkflowRunViewSnapshot,
) {
    if snapshot.quit_eligible {
        let _ = boundary.notify_lifecycle(TerminalLifecycleEvent::QuitEligible);
    }
}

fn workflow_is_executing(snapshot: &WorkflowRunViewSnapshot) -> bool {
    matches!(snapshot.workflow, WorkflowState::Executing { .. })
}

#[expect(
    clippy::disallowed_methods,
    reason = "redraw_interval is the terminal host boundary for coalesced redraw timing"
)]
fn redraw_interval() -> tokio::time::Interval {
    tokio::time::interval(REDRAW_INTERVAL)
}

fn restore_terminal<Boundary: TerminalBoundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    exit: TerminalHostExit,
    cancellation: &CancellationSource,
    execution_active: bool,
) -> Result<TerminalHostExit, PresentationFailure> {
    terminal.restore().map_or_else(
        |error| {
            if execution_active {
                cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
            }
            Err(presentation_failure(
                PresentationFailureOperation::TerminalRestore,
                &error,
            ))
        },
        |()| Ok(exit),
    )
}

fn fail_terminal<Boundary: TerminalBoundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    operation: PresentationFailureOperation,
    error: &io::Error,
    cancellation: &CancellationSource,
    execution_active: bool,
) -> Result<TerminalHostExit, PresentationFailure> {
    if execution_active {
        cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
    }
    let failure = presentation_failure(operation, error);
    let _ = terminal.restore();
    Err(failure)
}

fn presentation_failure(
    operation: PresentationFailureOperation,
    error: &io::Error,
) -> PresentationFailure {
    PresentationFailure {
        operation,
        error_kind: Some(error.kind()),
        result_directory: None,
    }
}

struct SystemTerminalBoundary {
    surface: Option<TerminalSurface>,
    input: TerminalInput,
    restore: Option<TerminalRestore>,
    lifecycle_handshake: Option<UnixStream>,
    quit_eligibility_reported: bool,
}

impl SystemTerminalBoundary {
    fn new() -> Self {
        Self {
            surface: None,
            input: TerminalInput::new(),
            restore: None,
            lifecycle_handshake: None,
            quit_eligibility_reported: false,
        }
    }

    fn surface_mut(&mut self) -> io::Result<&mut TerminalSurface> {
        self.surface.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "terminal surface is not set up",
            )
        })
    }
}

impl TerminalBoundary for SystemTerminalBoundary {
    fn setup(&mut self) -> io::Result<Rect> {
        self.lifecycle_handshake = std::env::var_os(TERMINAL_LIFECYCLE_HANDSHAKE_ENVIRONMENT)
            .map(std::path::PathBuf::from)
            .map(UnixStream::connect)
            .transpose()?;
        self.restore = Some(TerminalRestore::enter_raw_mode()?);
        let area = selected_output_area()?;
        let mut output = io::stdout();
        if let Some(restore) = &mut self.restore {
            restore.alternate_screen = true;
        }
        execute!(output, EnterAlternateScreen, Hide)?;
        let terminal = Terminal::with_options(
            CrosstermBackend::new(output),
            TerminalOptions {
                viewport: Viewport::Fixed(area),
            },
        )?;
        self.surface = Some(TerminalSurface {
            terminal,
            graph: None,
        });
        Ok(area)
    }

    fn next_event(&mut self) -> impl Future<Output = io::Result<TerminalInputEvent>> + Send {
        self.input.next_event()
    }

    fn resize(&mut self) -> io::Result<Rect> {
        self.surface_mut()?.resize()
    }

    fn restore(&mut self) -> io::Result<()> {
        self.restore
            .as_mut()
            .map_or(Ok(()), TerminalRestore::restore)
    }

    fn notify_lifecycle(&mut self, event: TerminalLifecycleEvent) -> io::Result<()> {
        if event == TerminalLifecycleEvent::QuitEligible && self.quit_eligibility_reported {
            return Ok(());
        }
        let Some(handshake) = &mut self.lifecycle_handshake else {
            self.quit_eligibility_reported |= event == TerminalLifecycleEvent::QuitEligible;
            return Ok(());
        };
        let message = match event {
            TerminalLifecycleEvent::HelpOpened => b"help-open\n".as_slice(),
            TerminalLifecycleEvent::QuitEligible => b"quit-eligible\n".as_slice(),
        };
        handshake.write_all(message)?;
        self.quit_eligibility_reported |= event == TerminalLifecycleEvent::QuitEligible;
        Ok(())
    }
}

impl WorkflowTerminalBoundary for SystemTerminalBoundary {
    fn draw_workflow(
        &mut self,
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        color: bool,
    ) -> io::Result<()> {
        self.surface_mut()?.draw(snapshot, interaction, color)
    }
}

struct TerminalSurface {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
    graph: Option<DagLayout>,
}

impl TerminalSurface {
    fn draw(
        &mut self,
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        color: bool,
    ) -> io::Result<()> {
        clamp_step_selection(&mut interaction.selected, snapshot.steps.len());
        let graph = self
            .graph
            .get_or_insert_with(|| DagLayout::for_steps(&snapshot.steps));
        self.terminal
            .draw(|frame| render(frame, snapshot, graph, interaction, color))?;
        Ok(())
    }

    fn resize(&mut self) -> io::Result<Rect> {
        let area = selected_output_area()?;
        self.terminal.resize(area)?;
        Ok(area)
    }
}

struct TerminalInput {
    events: EventStream,
}

impl TerminalInput {
    fn new() -> Self {
        Self {
            events: EventStream::new(),
        }
    }

    async fn next_event(&mut self) -> io::Result<TerminalInputEvent> {
        match self.events.next().await {
            Some(Ok(event)) => Ok(terminal_input_event(event)),
            Some(Err(error)) => Err(error),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "terminal input closed",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalInputEvent {
    Up,
    Down,
    PageUp,
    PageDown,
    HalfPageUp,
    HalfPageDown,
    Top,
    Bottom,
    PanLeft,
    PanRight,
    Follow,
    ToggleLogChannel(char),
    Help,
    Enter,
    Escape,
    Quit,
    Cancel,
    Resize,
    Other,
}

fn terminal_input_event(event: Event) -> TerminalInputEvent {
    match event {
        Event::Key(key)
            if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
                && key.modifiers.contains(KeyModifiers::CONTROL)
                && !key.modifiers.contains(KeyModifiers::ALT) =>
        {
            match key.code {
                KeyCode::Char('c') => TerminalInputEvent::Cancel,
                KeyCode::Char('u') => TerminalInputEvent::HalfPageUp,
                KeyCode::Char('d') => TerminalInputEvent::HalfPageDown,
                _ => TerminalInputEvent::Other,
            }
        }
        Event::Key(key)
            if key.kind == KeyEventKind::Press
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                && matches!(key.code, KeyCode::Char('1'..='9')) =>
        {
            match key.code {
                KeyCode::Char(channel @ '1'..='9') => TerminalInputEvent::ToggleLogChannel(channel),
                _ => TerminalInputEvent::Other,
            }
        }
        Event::Key(key)
            if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
                && !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            match key.code {
                KeyCode::Up | KeyCode::Char('k') => TerminalInputEvent::Up,
                KeyCode::Down | KeyCode::Char('j') => TerminalInputEvent::Down,
                KeyCode::PageUp | KeyCode::Char('b') => TerminalInputEvent::PageUp,
                KeyCode::PageDown | KeyCode::Char('f') | KeyCode::Char(' ') => {
                    TerminalInputEvent::PageDown
                }
                KeyCode::Char('u') => TerminalInputEvent::HalfPageUp,
                KeyCode::Char('d') => TerminalInputEvent::HalfPageDown,
                KeyCode::Char('g') => TerminalInputEvent::Top,
                KeyCode::Char('G') => TerminalInputEvent::Bottom,
                KeyCode::Left | KeyCode::Char('h') => TerminalInputEvent::PanLeft,
                KeyCode::Right | KeyCode::Char('l') => TerminalInputEvent::PanRight,
                KeyCode::Char('F') => TerminalInputEvent::Follow,
                KeyCode::Char('?') => TerminalInputEvent::Help,
                KeyCode::Enter => TerminalInputEvent::Enter,
                KeyCode::Esc => TerminalInputEvent::Escape,
                KeyCode::Char('q') => TerminalInputEvent::Quit,
                _ => TerminalInputEvent::Other,
            }
        }
        Event::Resize(_, _) => TerminalInputEvent::Resize,
        _ => TerminalInputEvent::Other,
    }
}

struct TerminalRestore {
    original_input_mode: Termios,
    alternate_screen: bool,
    restored: bool,
}

fn begin_restoration(restored: &mut bool) -> bool {
    if *restored {
        return false;
    }
    *restored = true;
    true
}

impl TerminalRestore {
    fn enter_raw_mode() -> io::Result<Self> {
        let input = io::stdin();
        let original_input_mode = tcgetattr(&input).map_err(io::Error::from)?;
        let mut restore = Self {
            original_input_mode: original_input_mode.clone(),
            alternate_screen: false,
            restored: false,
        };
        let mut raw_input_mode = original_input_mode;
        raw_input_mode.make_raw();
        if let Err(error) = tcsetattr(&input, OptionalActions::Now, &raw_input_mode) {
            restore.restored = true;
            return Err(error.into());
        }
        Ok(restore)
    }

    fn restore(&mut self) -> io::Result<()> {
        if !begin_restoration(&mut self.restored) {
            return Ok(());
        }
        let mut output = io::stdout();
        let input = io::stdin();
        attempt_terminal_restoration(
            self.alternate_screen,
            &mut output,
            |output| queue!(output, LeaveAlternateScreen),
            |output| queue!(output, Show),
            Write::flush,
            || {
                tcsetattr(&input, OptionalActions::Now, &self.original_input_mode)
                    .map_err(io::Error::from)
            },
        )
    }
}

fn attempt_terminal_restoration<Output: Write>(
    alternate_screen: bool,
    output: &mut Output,
    mut leave_alternate_screen: impl FnMut(&mut Output) -> io::Result<()>,
    mut show_cursor: impl FnMut(&mut Output) -> io::Result<()>,
    mut flush_output: impl FnMut(&mut Output) -> io::Result<()>,
    mut restore_input_mode: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    let mut first_error = None;
    if alternate_screen {
        retain_first_error(leave_alternate_screen(output), &mut first_error);
        retain_first_error(show_cursor(output), &mut first_error);
        retain_first_error(flush_output(output), &mut first_error);
    }
    retain_first_error(restore_input_mode(), &mut first_error);
    first_error.map_or(Ok(()), Err)
}

impl Drop for TerminalRestore {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

fn selected_output_area() -> io::Result<Rect> {
    let size = tcgetwinsize(io::stdout()).map_err(io::Error::from)?;
    if size.ws_col == 0 || size.ws_row == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "terminal reported an empty window",
        ));
    }
    Ok(Rect::new(0, 0, size.ws_col, size.ws_row))
}

fn retain_first_error(result: io::Result<()>, first_error: &mut Option<io::Error>) {
    if let Err(error) = result
        && first_error.is_none()
    {
        *first_error = Some(error);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum HostSurface {
    #[default]
    Split,
    FullLog,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogChannel {
    StandardOutput,
    StandardError,
    Agent,
    Reasoning,
    Tools,
    System,
}

impl LogChannel {
    const fn bit(self) -> u8 {
        match self {
            Self::StandardOutput => 1 << 0,
            Self::StandardError => 1 << 1,
            Self::Agent => 1 << 2,
            Self::Reasoning => 1 << 3,
            Self::Tools => 1 << 4,
            Self::System => 1 << 5,
        }
    }

    const fn for_source(source: WorkflowRunLogSource) -> Self {
        match source {
            WorkflowRunLogSource::Command(CommandOutputSource::StandardOutput) => {
                Self::StandardOutput
            }
            WorkflowRunLogSource::Command(CommandOutputSource::StandardError) => {
                Self::StandardError
            }
            WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Assistant) => Self::Agent,
            WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Reasoning) => {
                Self::Reasoning
            }
            WorkflowRunLogSource::Agent(
                AgentPresentationObservationKind::ToolCall
                | AgentPresentationObservationKind::ToolResult,
            ) => Self::Tools,
            WorkflowRunLogSource::Agent(
                AgentPresentationObservationKind::Diagnostic
                | AgentPresentationObservationKind::Usage
                | AgentPresentationObservationKind::Model
                | AgentPresentationObservationKind::Lifecycle
                | AgentPresentationObservationKind::ValueRejected
                | AgentPresentationObservationKind::HarnessEvent,
            ) => Self::System,
        }
    }
}

#[derive(Clone, Copy)]
struct LogChannelOption {
    key: char,
    channel: LogChannel,
    label: &'static str,
    compact_label: &'static str,
}

const COMMAND_LOG_CHANNELS: [LogChannelOption; 2] = [
    LogChannelOption {
        key: '1',
        channel: LogChannel::StandardOutput,
        label: "stdout",
        compact_label: "out",
    },
    LogChannelOption {
        key: '2',
        channel: LogChannel::StandardError,
        label: "stderr",
        compact_label: "err",
    },
];

const AGENT_LOG_CHANNELS: [LogChannelOption; 4] = [
    LogChannelOption {
        key: '1',
        channel: LogChannel::Agent,
        label: "agent",
        compact_label: "agt",
    },
    LogChannelOption {
        key: '2',
        channel: LogChannel::Reasoning,
        label: "reasoning",
        compact_label: "rsn",
    },
    LogChannelOption {
        key: '3',
        channel: LogChannel::Tools,
        label: "tools",
        compact_label: "tool",
    },
    LogChannelOption {
        key: '4',
        channel: LogChannel::System,
        label: "system",
        compact_label: "sys",
    },
];

fn log_channel_options(step: &WorkflowRunStepView) -> &'static [LogChannelOption] {
    match &step.definition {
        WorkflowPresentationStep::Command { .. } => &COMMAND_LOG_CHANNELS,
        WorkflowPresentationStep::Agent { .. } => &AGENT_LOG_CHANNELS,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LogFilterState {
    enabled: u8,
}

impl Default for LogFilterState {
    fn default() -> Self {
        Self {
            enabled: LogChannel::StandardOutput.bit()
                | LogChannel::StandardError.bit()
                | LogChannel::Agent.bit()
                | LogChannel::Reasoning.bit()
                | LogChannel::Tools.bit()
                | LogChannel::System.bit(),
        }
    }
}

impl LogFilterState {
    const fn includes(self, channel: LogChannel) -> bool {
        self.enabled & channel.bit() != 0
    }

    fn toggle(&mut self, step: &WorkflowRunStepView, key: char) -> bool {
        let Some(option) = log_channel_options(step)
            .iter()
            .find(|option| option.key == key)
        else {
            return false;
        };
        self.enabled ^= option.channel.bit();
        true
    }
}

struct FilteredLog<'a> {
    records: Vec<&'a WorkflowRunLogRecord>,
    hidden_records: usize,
}

impl<'a> FilteredLog<'a> {
    fn new(log: &'a WorkflowRunStepLog, filters: LogFilterState) -> Self {
        let records = log
            .records
            .iter()
            .filter(|record| filters.includes(LogChannel::for_source(record.source)))
            .collect::<Vec<_>>();
        let hidden_records = log.records.len().saturating_sub(records.len());
        Self {
            records,
            hidden_records,
        }
    }
}

#[derive(Default)]
struct HostInteraction {
    selected: usize,
    surface: HostSurface,
    help_visible: bool,
    terminal_area: Rect,
    full_log: FullLogInteraction,
    log_filters: LogFilterState,
}

struct FullLogInteraction {
    follow: bool,
    anchor: Option<AcceptedRecordOrder>,
    anchor_clamped: bool,
    horizontal_offset: usize,
    available_width: usize,
    available_rows: usize,
}

impl Default for FullLogInteraction {
    fn default() -> Self {
        Self {
            follow: true,
            anchor: None,
            anchor_clamped: false,
            horizontal_offset: 0,
            available_width: 0,
            available_rows: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VerticalNavigation {
    Up,
    Down,
    PageUp,
    PageDown,
    HalfPageUp,
    HalfPageDown,
    Top,
    Bottom,
}

impl FullLogInteraction {
    fn synchronize(&mut self, log: &FilteredLog<'_>, available_width: usize, rows: usize) {
        self.available_width = available_width;
        self.available_rows = rows;
        if self.follow {
            self.anchor = None;
            self.anchor_clamped = false;
        } else if log.records.is_empty() {
            self.anchor = None;
        } else if let Some(anchor) = self.anchor {
            if let Err(insertion) = log
                .records
                .binary_search_by_key(&anchor, |record| record.accepted_order)
            {
                self.anchor = log
                    .records
                    .get(insertion)
                    .or_else(|| log.records.last())
                    .map(|record| record.accepted_order);
                self.anchor_clamped = true;
            }
        } else {
            self.anchor = log.records.first().map(|record| record.accepted_order);
        }
        self.horizontal_offset = self
            .horizontal_offset
            .min(maximum_horizontal_offset(&log.records, available_width));
    }

    fn navigate(&mut self, log: &FilteredLog<'_>, navigation: VerticalNavigation) {
        self.synchronize(log, self.available_width, self.available_rows);
        let current = self.top_index(log);
        let viewport_rows = self.available_rows.max(1);
        let bottom = log.records.len().saturating_sub(viewport_rows);
        let page = viewport_rows;
        let half_page = (viewport_rows / 2).max(1);
        let target = match navigation {
            VerticalNavigation::Up => current.saturating_sub(1),
            VerticalNavigation::Down => {
                if self.lines_behind_from(log, current) == 0 {
                    current
                } else {
                    current.saturating_add(1).min(bottom)
                }
            }
            VerticalNavigation::PageUp => current.saturating_sub(page),
            VerticalNavigation::PageDown => {
                if self.lines_behind_from(log, current) == 0 {
                    current
                } else {
                    current.saturating_add(page).min(bottom)
                }
            }
            VerticalNavigation::HalfPageUp => current.saturating_sub(half_page),
            VerticalNavigation::HalfPageDown => {
                if self.lines_behind_from(log, current) == 0 {
                    current
                } else {
                    current.saturating_add(half_page).min(bottom)
                }
            }
            VerticalNavigation::Top => 0,
            VerticalNavigation::Bottom => bottom,
        };
        self.follow = false;
        self.anchor = log.records.get(target).map(|record| record.accepted_order);
        self.anchor_clamped = false;
    }

    fn pan(&mut self, log: &FilteredLog<'_>, right: bool) {
        self.synchronize(log, self.available_width, self.available_rows);
        if right {
            self.horizontal_offset =
                self.horizontal_offset
                    .saturating_add(1)
                    .min(maximum_horizontal_offset(
                        &log.records,
                        self.available_width,
                    ));
        } else {
            self.horizontal_offset = self.horizontal_offset.saturating_sub(1);
        }
    }

    fn resume_follow(&mut self) {
        self.follow = true;
        self.anchor = None;
        self.anchor_clamped = false;
    }

    fn top_index(&self, log: &FilteredLog<'_>) -> usize {
        if self.follow {
            return log.records.len().saturating_sub(self.available_rows);
        }
        self.anchor
            .and_then(|anchor| {
                log.records
                    .binary_search_by_key(&anchor, |record| record.accepted_order)
                    .ok()
            })
            .unwrap_or(0)
    }

    fn lines_behind(&self, log: &FilteredLog<'_>) -> usize {
        self.lines_behind_from(log, self.top_index(log))
    }

    fn lines_behind_from(&self, log: &FilteredLog<'_>, top: usize) -> usize {
        log.records
            .len()
            .saturating_sub(top.saturating_add(self.available_rows))
    }
}

fn maximum_horizontal_offset(records: &[&WorkflowRunLogRecord], available_width: usize) -> usize {
    let gutter = LogGutter::for_width(available_width);
    records
        .iter()
        .map(|record| {
            let line_width = gutter
                .width()
                .saturating_add(display_width(&record.payload));
            let nominal_offset = line_width.saturating_sub(available_width);
            next_log_grapheme_boundary(&record.payload, gutter.width(), nominal_offset)
        })
        .max()
        .unwrap_or(0)
}

fn next_log_grapheme_boundary(payload: &str, payload_start: usize, target: usize) -> usize {
    if target <= payload_start {
        return target;
    }

    let mut boundary = payload_start;
    for grapheme in payload.graphemes(true) {
        if boundary >= target {
            break;
        }
        boundary = boundary.saturating_add(display_width(grapheme));
    }
    boundary
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HostControl {
    Continue,
    Quit,
}

fn clamp_step_selection(selected: &mut usize, step_count: usize) {
    if step_count == 0 {
        *selected = 0;
    } else if *selected >= step_count {
        *selected = step_count - 1;
    }
}

impl HostInteraction {
    fn handle_key(
        &mut self,
        event: TerminalInputEvent,
        snapshot: &WorkflowRunViewSnapshot,
        cancellation: &CancellationSource,
    ) -> HostControl {
        clamp_step_selection(&mut self.selected, snapshot.steps.len());

        if event == TerminalInputEvent::Cancel {
            match finalization_signal_action(snapshot) {
                FinalizationSignalAction::Graceful => {
                    cancellation.request_cancellation(CancellationReason::UserRequest);
                }
                FinalizationSignalAction::ForceAbort => {
                    cancellation.request_force_abort();
                }
                FinalizationSignalAction::Inert => {}
            }
            return HostControl::Continue;
        }
        if event == TerminalInputEvent::Quit && snapshot.quit_eligible {
            return HostControl::Quit;
        }
        if !operational_area(self.terminal_area) {
            return HostControl::Continue;
        }
        if self.help_visible {
            if event == TerminalInputEvent::Escape {
                self.help_visible = false;
            }
            return HostControl::Continue;
        }
        if event == TerminalInputEvent::Help {
            self.help_visible = true;
            return HostControl::Continue;
        }

        if let TerminalInputEvent::ToggleLogChannel(key) = event {
            if let Some(step) = snapshot.steps.get(self.selected)
                && self.log_filters.toggle(step, key)
                && self.surface == HostSurface::FullLog
            {
                let (width, rows) = full_log_record_dimensions(self.terminal_area, step);
                let log = FilteredLog::new(&step.log, self.log_filters);
                self.full_log.synchronize(&log, width, rows);
            }
            return HostControl::Continue;
        }

        if self.surface == HostSurface::FullLog
            && let Some(step) = snapshot.steps.get(self.selected)
        {
            let (width, rows) = full_log_record_dimensions(self.terminal_area, step);
            let log = FilteredLog::new(&step.log, self.log_filters);
            self.full_log.synchronize(&log, width, rows);
        }

        if self.surface == HostSurface::FullLog
            && let (Some(step), Some(navigation)) = (
                snapshot.steps.get(self.selected),
                vertical_navigation(event),
            )
        {
            let log = FilteredLog::new(&step.log, self.log_filters);
            self.full_log.navigate(&log, navigation);
            return HostControl::Continue;
        }

        match event {
            TerminalInputEvent::Enter
                if self.surface == HostSurface::Split && !snapshot.steps.is_empty() =>
            {
                self.surface = HostSurface::FullLog;
                self.full_log = FullLogInteraction::default();
                if let Some(step) = snapshot.steps.get(self.selected) {
                    let (width, rows) = full_log_record_dimensions(self.terminal_area, step);
                    let log = FilteredLog::new(&step.log, self.log_filters);
                    self.full_log.synchronize(&log, width, rows);
                }
            }
            TerminalInputEvent::Escape => {
                self.surface = HostSurface::Split;
            }
            TerminalInputEvent::Up if self.surface == HostSurface::Split => {
                self.selected = self.selected.saturating_sub(1);
            }
            TerminalInputEvent::Down if self.surface == HostSurface::Split => {
                if self.selected.saturating_add(1) < snapshot.steps.len() {
                    self.selected += 1;
                }
            }
            TerminalInputEvent::PanLeft | TerminalInputEvent::PanRight
                if self.surface == HostSurface::FullLog =>
            {
                if let Some(step) = snapshot.steps.get(self.selected) {
                    let log = FilteredLog::new(&step.log, self.log_filters);
                    self.full_log
                        .pan(&log, event == TerminalInputEvent::PanRight);
                }
            }
            TerminalInputEvent::Follow if self.surface == HostSurface::FullLog => {
                self.full_log.resume_follow();
            }
            _ => {}
        }
        HostControl::Continue
    }
}

fn vertical_navigation(event: TerminalInputEvent) -> Option<VerticalNavigation> {
    match event {
        TerminalInputEvent::Up => Some(VerticalNavigation::Up),
        TerminalInputEvent::Down => Some(VerticalNavigation::Down),
        TerminalInputEvent::PageUp => Some(VerticalNavigation::PageUp),
        TerminalInputEvent::PageDown => Some(VerticalNavigation::PageDown),
        TerminalInputEvent::HalfPageUp => Some(VerticalNavigation::HalfPageUp),
        TerminalInputEvent::HalfPageDown => Some(VerticalNavigation::HalfPageDown),
        TerminalInputEvent::Top => Some(VerticalNavigation::Top),
        TerminalInputEvent::Bottom => Some(VerticalNavigation::Bottom),
        TerminalInputEvent::PanLeft
        | TerminalInputEvent::PanRight
        | TerminalInputEvent::Follow
        | TerminalInputEvent::ToggleLogChannel(_)
        | TerminalInputEvent::Help
        | TerminalInputEvent::Enter
        | TerminalInputEvent::Escape
        | TerminalInputEvent::Quit
        | TerminalInputEvent::Cancel
        | TerminalInputEvent::Resize
        | TerminalInputEvent::Other => None,
    }
}

fn operational_area(area: Rect) -> bool {
    area.width >= MINIMUM_WIDTH && area.height >= MINIMUM_HEIGHT
}

fn selected_lower_panel_area<Step: StepProjection>(area: Rect, step: &Step) -> Rect {
    let content_area = Rect::new(
        area.x,
        area.y,
        area.width,
        area.height.saturating_sub(FOOTER_HEIGHT),
    );
    inspector_and_log_areas(content_area, inspector_desired_height(Some(step)))[1]
}

fn full_log_record_dimensions(area: Rect, step: &WorkflowRunStepView) -> (usize, usize) {
    let log_content = log_block(Borders::TOP, false).inner(selected_lower_panel_area(area, step));
    let records_area = log_content_areas(log_content)[1];
    let marker_rows = usize::from(step.log.discarded_records != 0);
    (
        usize::from(records_area.width),
        usize::from(records_area.height).saturating_sub(marker_rows),
    )
}

fn render(
    frame: &mut Frame<'_>,
    snapshot: &WorkflowRunViewSnapshot,
    graph: &DagLayout,
    interaction: &mut HostInteraction,
    color: bool,
) {
    let area = frame.area();
    interaction.terminal_area = area;
    frame.render_widget(Clear, area);
    if !operational_area(area) {
        render_too_small(frame, area, snapshot, color);
        return;
    }

    let sections =
        Layout::vertical([Constraint::Min(0), Constraint::Length(FOOTER_HEIGHT)]).split(area);
    if interaction.surface == HostSurface::FullLog {
        let selected_step = snapshot.steps.get(interaction.selected);
        if let Some(step) = selected_step {
            let (width, rows) = full_log_record_dimensions(area, step);
            let log = FilteredLog::new(&step.log, interaction.log_filters);
            interaction.full_log.synchronize(&log, width, rows);
        }
        let full_log_sections =
            inspector_and_log_areas(sections[0], inspector_desired_height(selected_step));
        render_inspector(
            frame,
            full_log_sections[0],
            selected_step,
            color,
            Borders::NONE,
        );
        render_full_log(
            frame,
            full_log_sections[1],
            selected_step,
            &interaction.full_log,
            interaction.log_filters,
            color,
        );
        render_contextual_footer(
            frame,
            sections[1],
            snapshot,
            color,
            "LOG",
            &FULL_LOG_FOOTER_OPTIONS,
        );
    } else {
        render_split_body(frame, sections[0], snapshot, graph, interaction, color);
        render_contextual_footer(
            frame,
            sections[1],
            snapshot,
            color,
            "DAG",
            &SPLIT_FOOTER_OPTIONS,
        );
        render_split_footer_junction(
            frame,
            sections[0],
            sections[1].y,
            interaction.help_visible,
            color,
            wide_split_columns(sections[0]),
        );
    }

    if interaction.help_visible {
        render_help_overlay(
            frame,
            sections[0],
            interaction.surface,
            lifecycle_control(snapshot),
            color,
        );
    }
}

#[derive(Clone, Copy)]
struct SplitBodyLayout {
    summary: Rect,
    dag: Rect,
    inspector: Rect,
    output: Rect,
    wide: bool,
}

impl SplitBodyLayout {
    fn summary_borders(self) -> Borders {
        if self.wide {
            Borders::BOTTOM | Borders::RIGHT
        } else {
            Borders::BOTTOM
        }
    }

    fn dag_borders(self) -> Borders {
        if self.wide {
            Borders::RIGHT
        } else {
            Borders::NONE
        }
    }

    fn inspector_borders(self) -> Borders {
        if self.wide {
            Borders::NONE
        } else {
            Borders::TOP
        }
    }
}

fn split_body_layout(
    area: Rect,
    summary_height: u16,
    desired_inspector_height: u16,
    wide_columns: [Rect; 2],
) -> SplitBodyLayout {
    if area.width >= WIDE_LAYOUT_WIDTH {
        let left = Layout::vertical([Constraint::Length(summary_height), Constraint::Min(0)])
            .split(wide_columns[0]);
        let right = inspector_and_log_areas(wide_columns[1], desired_inspector_height);
        SplitBodyLayout {
            summary: left[0],
            dag: left[1],
            inspector: right[0],
            output: right[1],
            wide: true,
        }
    } else {
        let body_height = area.height.saturating_sub(summary_height);
        let dag_height = (body_height / 3).clamp(5, 10);
        let remaining_height = body_height.saturating_sub(dag_height);
        let inspector_height = bounded_inspector_height(remaining_height, desired_inspector_height);
        let rows = Layout::vertical([
            Constraint::Length(summary_height),
            Constraint::Length(dag_height),
            Constraint::Length(inspector_height),
            Constraint::Min(MINIMUM_LOG_HEIGHT),
        ])
        .split(area);
        SplitBodyLayout {
            summary: rows[0],
            dag: rows[1],
            inspector: rows[2],
            output: rows[3],
            wide: false,
        }
    }
}

fn render_split_body(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    graph: &DagLayout,
    interaction: &HostInteraction,
    color: bool,
) {
    let selected_step = snapshot.steps.get(interaction.selected);
    let layout = split_body_layout(
        area,
        WORKFLOW_SUMMARY_HEIGHT,
        inspector_desired_height(selected_step),
        wide_split_columns(area),
    );
    render_workflow_summary(
        frame,
        layout.summary,
        snapshot,
        color,
        layout.summary_borders(),
    );
    render_split_steps(
        frame,
        layout,
        &snapshot.steps,
        graph,
        live_step_phase_boundary(snapshot),
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
    render_log(
        frame,
        layout.output,
        snapshot,
        interaction,
        color,
        Borders::TOP,
    );
    render_split_body_junctions(frame, layout, color);
}

fn render_split_body_junctions(frame: &mut Frame<'_>, layout: SplitBodyLayout, color: bool) {
    if layout.wide {
        let divider_x = layout.inspector.x.saturating_sub(1);
        render_junction(
            frame,
            divider_x,
            layout.summary.bottom().saturating_sub(1),
            "┼",
            color,
        );
        render_junction(frame, divider_x, layout.output.y, "├", color);
    }
}

fn render_split_footer_junction(
    frame: &mut Frame<'_>,
    body: Rect,
    footer_y: u16,
    help_visible: bool,
    color: bool,
    wide_columns: [Rect; 2],
) {
    if body.width >= WIDE_LAYOUT_WIDTH && !help_visible {
        render_junction(
            frame,
            wide_columns[1].x.saturating_sub(1),
            footer_y,
            "┴",
            color,
        );
    }
}

fn wide_split_columns(area: Rect) -> [Rect; 2] {
    let columns =
        Layout::horizontal([Constraint::Ratio(1, 3), Constraint::Ratio(2, 3)]).split(area);
    [columns[0], columns[1]]
}

fn render_junction(frame: &mut Frame<'_>, x: u16, y: u16, symbol: &'static str, color: bool) {
    frame.render_widget(
        Paragraph::new(Span::styled(symbol, separator_style(color))),
        Rect::new(x, y, 1, 1),
    );
}

fn inspector_and_log_areas(area: Rect, desired_inspector_height: u16) -> [Rect; 2] {
    let inspector_height = bounded_inspector_height(area.height, desired_inspector_height);
    let rows = Layout::vertical([
        Constraint::Length(inspector_height),
        Constraint::Min(MINIMUM_LOG_HEIGHT),
    ])
    .split(area);
    [rows[0], rows[1]]
}

fn bounded_inspector_height(available_height: u16, desired_height: u16) -> u16 {
    let maximum_height = available_height.saturating_sub(MINIMUM_LOG_HEIGHT);
    let minimum_height = MINIMUM_INSPECTOR_HEIGHT.min(maximum_height);
    desired_height.clamp(minimum_height, maximum_height)
}

pub(super) fn inspector_desired_height<Step: StepProjection>(step: Option<&Step>) -> u16 {
    let body_height = step.map_or(1, |step| {
        inspector_detail_row_count(step)
            .saturating_add(1)
            .saturating_add(inspector_outputs_desired_height(step))
    });
    u16::try_from(body_height)
        .unwrap_or(u16::MAX)
        .saturating_add(INSPECTOR_HEADER_HEIGHT)
}

fn inspector_outputs_desired_height<Step: StepProjection>(step: &Step) -> usize {
    let outputs = step.inspector_outputs();
    if outputs.is_empty() {
        usize::from(step.show_empty_outputs()) * 4
    } else {
        inspector_outputs_desired_height_for_count(outputs.len())
    }
}

fn section_block(borders: Borders, color: bool) -> Block<'static> {
    let has_side_border = borders.contains(Borders::LEFT) || borders.contains(Borders::RIGHT);
    let padding = u16::from(!has_side_border);
    Block::default()
        .borders(borders)
        .border_style(separator_style(color))
        .padding(Padding::horizontal(padding))
}

fn render_too_small(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    color: bool,
) {
    let mut lines = vec![
        Line::from(Span::styled(
            "Terminal too small",
            tone_style(color, Tone::Failure),
        )),
        Line::from(format!(
            "Resize to at least {MINIMUM_WIDTH}x{MINIMUM_HEIGHT}."
        )),
    ];
    match lifecycle_control(snapshot) {
        LifecycleControl::Cancel => lines.push(Line::from("Ctrl-C cancels the workflow.")),
        LifecycleControl::Quit => lines.push(Line::from("Press q to quit.")),
        LifecycleControl::None => lines.push(Line::from("Finishing workflow lifecycle…")),
    }
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(separator_style(color))
                .title(" Scherzo workflow run "),
        ),
        area,
    );
}

fn render_workflow_summary(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    color: bool,
    borders: Borders,
) {
    let block = summary_block(borders, color);
    let content = block.inner(area);
    frame.render_widget(block, area);

    let duration = human_duration(snapshot.timing.duration);
    let (status, status_tone) = workflow_header_status(snapshot);
    let status_width = display_width(status)
        .saturating_add(2)
        .saturating_add(display_width(&duration));
    let status_width = status_width.min(usize::from(content.width));
    let title_width = usize::from(content.width)
        .saturating_sub(status_width)
        .saturating_sub(2);
    let title = ellipsize(&workflow_display_name(&snapshot.workflow_path), title_width);

    if !title.is_empty() {
        frame.render_widget(
            Paragraph::new(Span::styled(
                title,
                tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
            )),
            Rect::new(
                content.x,
                content.y,
                u16::try_from(title_width).unwrap_or(u16::MAX),
                1,
            ),
        );
    }

    let status_width = u16::try_from(status_width).unwrap_or(content.width);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(status, tone_style(color, status_tone)),
            Span::raw("  "),
            Span::styled(duration, tone_style(color, Tone::Muted)),
        ])),
        Rect::new(
            content.right().saturating_sub(status_width),
            content.y,
            status_width,
            1,
        ),
    );

    let counts = step_count_summary(&step_counts(snapshot), snapshot.steps.len());
    frame.render_widget(
        Paragraph::new(Span::styled(
            ellipsize(&counts, usize::from(content.width)),
            tone_style(color, Tone::Muted),
        )),
        Rect::new(content.x, content.y.saturating_add(1), content.width, 1),
    );
}

fn summary_block(borders: Borders, color: bool) -> Block<'static> {
    Block::default()
        .borders(borders)
        .border_style(separator_style(color))
        .padding(Padding::horizontal(2))
}

fn workflow_display_name(workflow_path: &str) -> String {
    std::path::Path::new(workflow_path)
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(visible_text)
        .unwrap_or_else(|| visible_text(workflow_path))
}

pub(super) trait StepProjection {
    fn id(&self) -> &str;

    fn definition(&self) -> &WorkflowPresentationStep;

    fn state(&self) -> StepStateKind;

    fn timing(&self) -> Option<&super::run_view_model::WorkflowRunElapsed>;

    fn dag_detail(&self) -> Option<String>;

    fn inspector_command(&self) -> Option<String>;

    fn inspector_fact(&self) -> Option<InspectorField>;

    fn inspector_outputs(&self) -> Vec<InspectorOutput>;

    fn show_empty_outputs(&self) -> bool;
}

#[derive(Clone, Copy)]
pub(super) struct StepPanel {
    pub(super) borders: Borders,
    pub(super) show_title: bool,
    phase_boundary: Option<StepPhaseBoundary>,
}

#[derive(Clone, Copy)]
pub(super) struct StepPhaseBoundary {
    pub(super) finalization_start: usize,
    pub(super) trigger: Option<&'static str>,
}

fn live_step_phase_boundary(snapshot: &WorkflowRunViewSnapshot) -> Option<StepPhaseBoundary> {
    let finalization_start = snapshot.finalization_start?;
    let trigger = snapshot
        .finalization
        .as_ref()
        .map(|finalization| finalization.trigger)
        .or(match &snapshot.workflow {
            WorkflowState::Finalizing { trigger, .. } => Some(*trigger),
            WorkflowState::Executing { .. }
            | WorkflowState::Succeeded
            | WorkflowState::Failed { .. }
            | WorkflowState::Cancelled { .. } => None,
        })
        .map(finalization_trigger);
    Some(StepPhaseBoundary {
        finalization_start,
        trigger,
    })
}

fn render_split_steps<Step: StepProjection>(
    frame: &mut Frame<'_>,
    layout: SplitBodyLayout,
    steps: &[Step],
    graph: &DagLayout,
    phase_boundary: Option<StepPhaseBoundary>,
    selected: usize,
    color: bool,
) {
    render_projected_steps(
        frame,
        layout.dag,
        steps,
        graph,
        selected,
        color,
        StepPanel {
            borders: layout.dag_borders(),
            show_title: false,
            phase_boundary,
        },
    );
}

fn render_steps(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    graph: &DagLayout,
    interaction: &HostInteraction,
    color: bool,
    panel: StepPanel,
) {
    render_projected_steps(
        frame,
        area,
        &snapshot.steps,
        graph,
        interaction.selected,
        color,
        StepPanel {
            phase_boundary: live_step_phase_boundary(snapshot),
            ..panel
        },
    );
}

fn render_projected_steps<Step: StepProjection>(
    frame: &mut Frame<'_>,
    area: Rect,
    steps: &[Step],
    graph: &DagLayout,
    selected_step: usize,
    color: bool,
    panel: StepPanel,
) {
    let mut block = Block::default()
        .borders(panel.borders)
        .border_style(separator_style(color));
    if panel.show_title {
        block = block.title(format!(" Steps ({}) ", steps.len()));
    }
    let available_width = usize::from(block.inner(area).width);
    let columns = StepColumns::for_steps(available_width, graph.gutter_width(), steps);
    let connector_style = graph_connector_style(color);
    let phase_boundary = panel
        .phase_boundary
        .filter(|boundary| boundary.finalization_start < steps.len());
    let mut items = Vec::with_capacity(steps.len() + usize::from(phase_boundary.is_some()) * 2);
    if phase_boundary.is_some() {
        items.push(ListItem::new(Line::from(Span::styled(
            "  ordinary phase",
            tone_style(color, Tone::Muted).add_modifier(Modifier::BOLD),
        ))));
    }
    for (index, (step, graph_row)) in steps.iter().zip(graph.rows()).enumerate() {
        if phase_boundary.is_some_and(|boundary| boundary.finalization_start == index) {
            let trigger = phase_boundary
                .and_then(|boundary| boundary.trigger)
                .map_or_else(String::new, |trigger| format!(" · trigger {trigger}"));
            items.push(ListItem::new(Line::from(Span::styled(
                format!("  finalization phase{trigger}"),
                tone_style(color, Tone::Muted).add_modifier(Modifier::BOLD),
            ))));
        }
        let selected = index == selected_step;
        let marker = if selected { "▏ " } else { "  " };
        let id = padded_text(&visible_text(step.id()), columns.id_width);
        let duration = step
            .timing()
            .map(|timing| human_duration(timing.duration))
            .unwrap_or_else(|| "-".to_owned());
        let mut spans = vec![
            Span::styled(marker, selection_marker_style(color)),
            Span::styled(graph_row.before_node.clone(), connector_style),
            Span::styled(
                step_state_glyph(step),
                step_state_style(step.state(), color),
            ),
            Span::styled(graph_row.after_node.clone(), connector_style),
            Span::raw(" "),
            Span::styled(id, step_identity_style(step.state(), color)),
        ];
        if columns.kind {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(
                format!("{:<KIND_COLUMN_WIDTH$}", step_kind(step.definition())),
                tone_style(color, Tone::Muted),
            ));
        }
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            padded_text(&duration, columns.duration_width),
            step_duration_style(step.state(), color),
        ));
        if columns.detail {
            spans.push(Span::raw("  "));
            if let Some(detail) = step.dag_detail() {
                spans.push(Span::styled(
                    fit_text(&visible_text(&detail), columns.detail_width),
                    tone_style(color, Tone::Muted),
                ));
            }
        }
        let mut node_line = Line::from(spans);
        if selected {
            node_line = node_line.style(step_selection_style(color));
        }
        let connector_line = Line::from(vec![
            Span::raw("  "),
            Span::styled(graph_row.below_node.clone(), connector_style),
        ]);
        items.push(ListItem::new(vec![node_line, connector_line]));
    }
    let list = List::new(items).block(block);
    let mut state = ListState::default();
    if !steps.is_empty() {
        let phase_rows_before_selection = phase_boundary.map_or(0, |boundary| {
            1 + usize::from(selected_step >= boundary.finalization_start)
        });
        state.select(Some(selected_step + phase_rows_before_selection));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

#[derive(Clone)]
pub(super) struct InspectorField {
    label: &'static str,
    value: String,
    tone: Tone,
}

impl InspectorField {
    pub(super) fn new(label: &'static str, value: impl AsRef<str>, tone: Tone) -> Self {
        Self {
            label,
            value: visible_text(value.as_ref()),
            tone,
        }
    }
}

pub(super) struct InspectorOutput {
    marker: &'static str,
    marker_tone: Tone,
    name: String,
    kind: &'static str,
    detail: String,
    disposition: Option<String>,
    tone: Tone,
}

impl InspectorOutput {
    pub(super) fn declaration(name: String, kind: &'static str, detail: String) -> Self {
        Self {
            marker: "·",
            marker_tone: Tone::Muted,
            name,
            kind,
            detail,
            disposition: None,
            tone: Tone::Muted,
        }
    }
}

impl StepProjection for WorkflowRunStepView {
    fn id(&self) -> &str {
        &self.id
    }

    fn definition(&self) -> &WorkflowPresentationStep {
        &self.definition
    }

    fn state(&self) -> StepStateKind {
        self.state
    }

    fn timing(&self) -> Option<&super::run_view_model::WorkflowRunElapsed> {
        self.timing.as_ref()
    }

    fn dag_detail(&self) -> Option<String> {
        live_step_detail(self)
    }

    fn inspector_command(&self) -> Option<String> {
        let WorkflowPresentationStep::Command { argv, .. } = &self.definition else {
            return None;
        };
        let command = argv
            .iter()
            .map(|argument| shell_quote(argument))
            .collect::<Vec<_>>()
            .join(" ");
        Some(
            if self.role == crate::execution::workflow::validated::WorkflowNodeRole::Finalizer {
                format!("finalizer · {command}")
            } else {
                command
            },
        )
    }

    fn inspector_fact(&self) -> Option<InspectorField> {
        live_inspector_fact(self.fact.as_ref())
    }

    fn inspector_outputs(&self) -> Vec<InspectorOutput> {
        live_inspector_outputs(self)
    }

    fn show_empty_outputs(&self) -> bool {
        true
    }
}

pub(super) fn render_inspector<Step: StepProjection>(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&Step>,
    color: bool,
    borders: Borders,
) {
    let sections = Layout::vertical([
        Constraint::Length(INSPECTOR_HEADER_HEIGHT),
        Constraint::Min(0),
    ])
    .split(area);
    let mut header_borders = Borders::BOTTOM;
    for border in [Borders::TOP, Borders::LEFT, Borders::RIGHT] {
        if borders.contains(border) {
            header_borders |= border;
        }
    }

    let header =
        section_block(header_borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let header_content = header.inner(sections[0]);
    frame.render_widget(header, sections[0]);
    if let Some(step) = step {
        render_selected_step_header(frame, header_content, step, color);
    } else {
        frame.render_widget(
            Paragraph::new(Span::styled(
                "Selected step",
                tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
            )),
            header_content,
        );
    }

    let mut output_borders = Borders::NONE;
    for border in [Borders::LEFT, Borders::RIGHT, Borders::BOTTOM] {
        if borders.contains(border) {
            output_borders |= border;
        }
    }
    let Some(step) = step else {
        let block = section_block(output_borders, color)
            .padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
        frame.render_widget(
            Paragraph::new("No workflow steps.").block(block),
            sections[1],
        );
        return;
    };

    let outputs = step.inspector_outputs();
    let output_panel_visible = !outputs.is_empty() || step.show_empty_outputs();
    let available_body_height = sections[1].height;
    let desired_detail_height = u16::try_from(inspector_detail_row_count(step))
        .unwrap_or(u16::MAX)
        .saturating_add(u16::from(output_panel_visible));
    let reserved_output_height = if output_panel_visible {
        MINIMUM_OUTPUT_PANEL_HEIGHT.min(available_body_height)
    } else {
        0
    };
    let maximum_detail_height = available_body_height.saturating_sub(reserved_output_height);
    let detail_height = desired_detail_height.min(maximum_detail_height);
    let body_sections = Layout::vertical([Constraint::Length(detail_height), Constraint::Min(0)])
        .split(sections[1]);

    if detail_height != 0 {
        let mut detail_borders = if output_panel_visible {
            Borders::BOTTOM
        } else {
            Borders::NONE
        };
        for border in [Borders::LEFT, Borders::RIGHT] {
            if borders.contains(border) {
                detail_borders |= border;
            }
        }
        render_inspector_panel(
            frame,
            body_sections[0],
            color,
            detail_borders,
            |content_area| inspector_detail_lines(step, content_area, color),
        );
        if output_panel_visible && body_sections[0].x != 0 && !borders.contains(Borders::LEFT) {
            render_junction(
                frame,
                body_sections[0].x.saturating_sub(1),
                body_sections[0].bottom().saturating_sub(1),
                "├",
                color,
            );
        }
    }
    if output_panel_visible {
        render_inspector_panel(
            frame,
            body_sections[1],
            color,
            output_borders,
            |content_area| {
                inspector_output_lines(&outputs, content_area.width, content_area.height, color)
            },
        );
    }
}

fn render_inspector_panel(
    frame: &mut Frame<'_>,
    area: Rect,
    color: bool,
    borders: Borders,
    content: impl FnOnce(Rect) -> Vec<Line<'static>>,
) {
    let block = section_block(borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let content_area = block.inner(area);
    frame.render_widget(Paragraph::new(content(content_area)).block(block), area);
}

fn inspector_detail_lines<Step: StepProjection>(
    step: &Step,
    content_area: Rect,
    color: bool,
) -> Vec<Line<'static>> {
    let total_rows = inspector_detail_row_count(step);
    let total_items = inspector_fixed_field_count(step);
    let available_rows = usize::from(content_area.height);
    let overflowing = total_rows > available_rows;
    let regular_row_limit = if overflowing {
        available_rows.saturating_sub(1)
    } else {
        total_rows
    };
    let fields = inspector_fields_for_rows(step, content_area.width, regular_row_limit);
    let rendered_items = fields.len();
    let mut lines = fields
        .iter()
        .map(|field| inspector_field_line(field, content_area.width, color))
        .collect::<Vec<_>>();
    if overflowing && available_rows != 0 {
        let omitted = total_items.saturating_sub(rendered_items);
        lines.push(Line::from(Span::styled(
            format!("+{omitted} more"),
            tone_style(color, Tone::Muted),
        )));
    }
    lines
}

fn render_selected_step_header<Step: StepProjection>(
    frame: &mut Frame<'_>,
    area: Rect,
    step: &Step,
    color: bool,
) {
    if area.is_empty() {
        return;
    }
    let status = selected_step_status_title(step, color);
    let status_width = u16::try_from(status.width())
        .unwrap_or(u16::MAX)
        .min(area.width);
    let title_width = area.width.saturating_sub(status_width.saturating_add(2));
    if title_width != 0 {
        frame.render_widget(
            Paragraph::new(selected_step_title(step, color, usize::from(title_width))),
            Rect::new(area.x, area.y, title_width, 1),
        );
    }
    frame.render_widget(
        Paragraph::new(status).alignment(Alignment::Right),
        Rect::new(
            area.right().saturating_sub(status_width),
            area.y,
            status_width,
            1,
        ),
    );
}

fn selected_step_title<Step: StepProjection>(
    step: &Step,
    color: bool,
    maximum_width: usize,
) -> Line<'static> {
    let badge = format!(" {} ", step_kind(step.definition()));
    let fixed_width = 5_usize.saturating_add(display_width(&badge));
    let id = ellipsize(
        &visible_text(step.id()),
        maximum_width.saturating_sub(fixed_width),
    );
    Line::from(vec![
        Span::styled(
            step_state_glyph(step),
            step_state_style(step.state(), color),
        ),
        Span::raw("  "),
        Span::styled(
            id,
            tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(badge, step_kind_badge_style(color)),
    ])
}

fn step_kind_badge_style(color: bool) -> Style {
    let style = tone_style(color, Tone::Muted);
    if color {
        style.bg(Color::Rgb(49, 50, 68))
    } else {
        style
    }
}

fn selected_step_status_title<Step: StepProjection>(step: &Step, color: bool) -> Line<'static> {
    let style = tone_style(color, step_state_tone(step.state()));
    let mut spans = vec![Span::styled(step_state_label(step.state()), style)];
    spans.push(Span::styled(
        format!(
            " · {}",
            failure_policy_name(step.definition().failure_policy())
        ),
        style,
    ));
    if let Some(timing) = step.timing() {
        let duration = if timing.frozen && step_state_is_active(step.state()) {
            format!("{} interrupted", human_duration(timing.duration))
        } else {
            human_duration(timing.duration)
        };
        spans.push(Span::styled(" · ", style));
        spans.push(Span::styled(duration, style));
    }
    Line::from(spans)
}

fn inspector_detail_row_count<Step: StepProjection>(step: &Step) -> usize {
    inspector_fixed_field_count(step)
}

fn inspector_fixed_field_count<Step: StepProjection>(step: &Step) -> usize {
    let mut count = 3;
    if inspector_timing(step).is_some() {
        count += 1;
    }
    if step.inspector_fact().is_some() {
        count += 1;
    }
    count
}

fn inspector_fields_for_rows<Step: StepProjection>(
    step: &Step,
    width: u16,
    maximum_rows: usize,
) -> Vec<InspectorField> {
    let maximum_fields = maximum_rows.min(inspector_fixed_field_count(step));
    inspector_fields(step, usize::from(width), maximum_fields)
}

fn live_inspector_outputs(step: &WorkflowRunStepView) -> Vec<InspectorOutput> {
    let declarations = step.definition.outputs();
    step.outputs
        .iter()
        .map(|(name, disposition)| {
            let (disposition, tone, marker, marker_tone) = output_disposition(*disposition);
            let (kind, detail) = declarations
                .get(name)
                .map_or(("output", "—".to_owned()), output_description);
            InspectorOutput {
                marker,
                marker_tone,
                name: visible_text(name),
                kind,
                detail,
                disposition: Some(disposition),
                tone,
            }
        })
        .collect()
}

fn inspector_fields<Step: StepProjection>(
    step: &Step,
    content_width: usize,
    maximum_fields: usize,
) -> Vec<InspectorField> {
    let mut fields = Vec::with_capacity(maximum_fields);
    let direct_dependencies = match step.definition() {
        WorkflowPresentationStep::Command {
            cwd,
            direct_dependencies,
            ..
        } => {
            if let Some(command) = step.inspector_command() {
                push_inspector_field(&mut fields, maximum_fields, || {
                    InspectorField::new("command", command, Tone::Neutral)
                });
            }
            push_inspector_field(&mut fields, maximum_fields, || {
                InspectorField::new("cwd", cwd.as_deref().unwrap_or("."), Tone::Neutral)
            });
            direct_dependencies
        }
        WorkflowPresentationStep::Agent {
            profile,
            harness,
            direct_dependencies,
            ..
        } => {
            push_inspector_field(&mut fields, maximum_fields, || {
                InspectorField::new("profile", profile, Tone::Neutral)
            });
            push_inspector_field(&mut fields, maximum_fields, || {
                InspectorField::new("harness", harness_description(harness), Tone::Neutral)
            });
            direct_dependencies
        }
    };

    let timing = inspector_timing(step);
    if let Some(timing) = timing {
        push_inspector_field(&mut fields, maximum_fields, || {
            InspectorField::new(
                "started",
                header_timestamp(timing.started_at),
                Tone::Neutral,
            )
        });
    }
    push_inspector_field(&mut fields, maximum_fields, || {
        let dependency_width = content_width.saturating_sub(INSPECTOR_LABEL_WIDTH);
        InspectorField::new(
            "depends on",
            summarize_repeated_values(direct_dependencies, dependency_width),
            Tone::Neutral,
        )
    });
    if fields.len() < maximum_fields
        && let Some(fact) = step.inspector_fact()
    {
        fields.push(fact);
    }
    fields
}

fn harness_description(harness: &AgentPresentationHarness) -> String {
    match harness {
        AgentPresentationHarness::Pi { model, thinking } => {
            let thinking = format!("{thinking:?}").to_ascii_lowercase();
            format!("pi · {} · thinking={thinking}", visible_text(model))
        }
        AgentPresentationHarness::ClaudeCode { model, effort } => format!(
            "claude code · {} · effort={}",
            visible_text(model),
            effort.as_str()
        ),
        AgentPresentationHarness::Codex { model, effort } => {
            format!(
                "codex · {} · effort={}",
                visible_text(model),
                visible_text(effort)
            )
        }
    }
}

fn push_inspector_field(
    fields: &mut Vec<InspectorField>,
    maximum_fields: usize,
    field: impl FnOnce() -> InspectorField,
) {
    if fields.len() < maximum_fields {
        fields.push(field());
    }
}

fn inspector_timing<Step: StepProjection>(
    step: &Step,
) -> Option<&super::run_view_model::WorkflowRunElapsed> {
    if matches!(
        step.state(),
        StepStateKind::Pending
            | StepStateKind::Blocked
            | StepStateKind::Skipped
            | StepStateKind::NotRun
    ) {
        None
    } else {
        step.timing()
    }
}

fn live_inspector_fact(fact: Option<&ObservedStepTransition>) -> Option<InspectorField> {
    match fact? {
        ObservedStepTransition::Recovery {
            active,
            configured_rounds,
            handler_kind,
            handler_state,
            decision,
            ..
        } => Some(InspectorField::new(
            "recovery",
            recovery_progress_detail(
                *active,
                *configured_rounds,
                *handler_kind,
                *handler_state,
                *decision,
            ),
            Tone::Active,
        )),
        ObservedStepTransition::Failed { detail } => Some(InspectorField::new(
            "failure",
            canonical_failure_detail(detail),
            Tone::Failure,
        )),
        ObservedStepTransition::Blocked { detail } => Some(InspectorField::new(
            "prerequisites",
            canonical_blocked_detail(detail),
            Tone::Blocked,
        )),
        ObservedStepTransition::Skipped { detail } => Some(InspectorField::new(
            "condition",
            super::archived_presentation::condition_false_detail(detail),
            Tone::Muted,
        )),
        ObservedStepTransition::NotRun { detail } => Some(InspectorField::new(
            "not run",
            super::presentation::snake_case_debug(detail.code),
            Tone::Muted,
        )),
        ObservedStepTransition::Cancelling { detail }
        | ObservedStepTransition::Cancelled { detail } => Some(InspectorField::new(
            "cancellation",
            cancellation_reason(detail.code),
            Tone::Blocked,
        )),
        ObservedStepTransition::OutputsCommitted { .. } => None,
    }
}

fn output_disposition(
    disposition: WorkflowRunOutputDisposition,
) -> (String, Tone, &'static str, Tone) {
    match disposition {
        WorkflowRunOutputDisposition::Pending => {
            ("pending".to_owned(), Tone::Muted, "○", Tone::Muted)
        }
        WorkflowRunOutputDisposition::Committed => {
            ("captured".to_owned(), Tone::Success, "✓", Tone::Success)
        }
        WorkflowRunOutputDisposition::Unavailable(reason) => {
            let reason = match reason {
                WorkflowRunOutputUnavailableReason::Failed => "failed",
                WorkflowRunOutputUnavailableReason::Blocked => "blocked",
                WorkflowRunOutputUnavailableReason::Skipped => "skipped",
                WorkflowRunOutputUnavailableReason::NotRun => "not-run",
                WorkflowRunOutputUnavailableReason::Cancelled => "cancelled",
            };
            (
                format!("unavailable ({reason})"),
                Tone::Blocked,
                "–",
                Tone::Blocked,
            )
        }
    }
}

fn output_description(output: &WorkflowOutput) -> (&'static str, String) {
    (semantic_output_kind(output), "—".to_owned())
}

fn semantic_output_kind(output: &WorkflowOutput) -> &'static str {
    match output {
        WorkflowOutput::TextPath { .. } | WorkflowOutput::TextAgentResponse => "text",
        WorkflowOutput::JsonPath { .. } | WorkflowOutput::JsonAgentResult { .. } => "json",
        WorkflowOutput::FilePath { .. } => "file",
        WorkflowOutput::GitBranchWorkspace => "git_branch",
    }
}

fn summarize_repeated_values(values: &[String], maximum_width: usize) -> String {
    let Some(first) = values.first() else {
        return "none".to_owned();
    };
    if values.len() == 1 {
        return visible_text(first);
    }

    let mut complete = String::new();
    let mut complete_width = 0_usize;
    let mut prefix_boundaries = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let value = visible_text(value);
        if index != 0 {
            complete.push_str(", ");
            complete_width = complete_width.saturating_add(2);
        }
        complete.push_str(&value);
        complete_width = complete_width.saturating_add(display_width(&value));
        prefix_boundaries.push((complete.len(), complete_width));
        if complete_width > maximum_width {
            break;
        }
        if index + 1 == values.len() {
            return complete;
        }
    }

    for included_count in (1..prefix_boundaries.len()).rev() {
        let (byte_length, prefix_width) = prefix_boundaries[included_count - 1];
        let suffix = format!(", +{} more", values.len() - included_count);
        if prefix_width.saturating_add(display_width(&suffix)) <= maximum_width {
            complete.truncate(byte_length);
            complete.push_str(&suffix);
            return complete;
        }
    }

    let suffix = format!(", +{} more", values.len() - 1);
    let first = visible_text(first);
    let prefix_width = maximum_width.saturating_sub(display_width(&suffix));
    format!("{}{suffix}", ellipsize(&first, prefix_width))
}

fn inspector_field_line(field: &InspectorField, width: u16, color: bool) -> Line<'static> {
    Line::from(inspector_field_spans(field, usize::from(width), color))
}

fn inspector_field_spans(
    field: &InspectorField,
    maximum_width: usize,
    color: bool,
) -> Vec<Span<'static>> {
    let label_width = INSPECTOR_LABEL_WIDTH.min(maximum_width);
    let label = padded_text(field.label, label_width);
    let value = if label_width < maximum_width {
        ellipsize(&field.value, maximum_width - label_width)
    } else {
        String::new()
    };
    let mut spans = vec![Span::styled(label, tone_style(color, Tone::Muted))];
    if !value.is_empty() {
        spans.push(Span::styled(value, tone_style(color, field.tone)));
    }
    spans
}

fn inspector_output_lines(
    outputs: &[InspectorOutput],
    width: u16,
    height: u16,
    color: bool,
) -> Vec<Line<'static>> {
    let available_rows = usize::from(height);
    if available_rows == 0 {
        return Vec::new();
    }

    let mut lines = vec![Line::from(Span::styled(
        "OUTPUTS",
        tone_style(color, Tone::Muted),
    ))];
    if outputs.is_empty() {
        if available_rows >= 3 {
            lines.push(Line::default());
        }
        if lines.len() < available_rows {
            lines.push(Line::from(Span::styled(
                "·  —  none declared",
                tone_style(color, Tone::Muted),
            )));
        }
        if lines.len() < available_rows {
            lines.push(Line::default());
        }
        return lines;
    }

    if available_rows >= 4 {
        lines.push(Line::default());
    }
    let remaining_rows = available_rows.saturating_sub(lines.len());
    if remaining_rows == 1 {
        if outputs.len() == 1 {
            lines.push(inspector_output_summary_line(&outputs[0], width, color));
        } else {
            lines.push(inspector_outputs_omitted_line(outputs.len(), color));
        }
        return lines;
    }

    let include_gaps = inspector_outputs_desired_height_for_count(outputs.len()) <= available_rows;
    let all_fit = include_gaps || outputs.len().saturating_mul(2) <= remaining_rows;
    let rendered_count = if all_fit {
        outputs.len()
    } else {
        remaining_rows.saturating_sub(1) / 2
    };
    for (index, output) in outputs.iter().take(rendered_count).enumerate() {
        if include_gaps && index != 0 {
            lines.push(Line::default());
        }
        lines.push(inspector_output_summary_line(output, width, color));
        lines.push(inspector_output_detail_line(output, width, color));
    }
    if rendered_count < outputs.len() && lines.len() < available_rows {
        lines.push(inspector_outputs_omitted_line(
            outputs.len() - rendered_count,
            color,
        ));
    } else if lines.len() < available_rows {
        lines.push(Line::default());
    }
    lines
}

fn inspector_outputs_desired_height_for_count(output_count: usize) -> usize {
    3_usize
        .saturating_add(output_count.saturating_mul(2))
        .saturating_add(output_count.saturating_sub(1))
}

fn inspector_outputs_omitted_line(omitted: usize, color: bool) -> Line<'static> {
    Line::from(Span::styled(
        format!("+{omitted} more outputs"),
        tone_style(color, Tone::Muted),
    ))
}

fn inspector_output_summary_line(
    output: &InspectorOutput,
    width: u16,
    color: bool,
) -> Line<'static> {
    let available_width = usize::from(width);
    let disposition_width = output.disposition.as_deref().map_or(0, display_width);
    if disposition_width != 0 && available_width <= disposition_width {
        return Line::from(Span::styled(
            ellipsize(
                output.disposition.as_deref().unwrap_or_default(),
                available_width,
            ),
            tone_style(color, output.tone),
        ));
    }

    let gap_width = usize::from(disposition_width != 0)
        .saturating_mul(2)
        .min(available_width.saturating_sub(disposition_width));
    let summary_width = available_width.saturating_sub(disposition_width + gap_width);
    let marker = format!("{}  ", output.marker);
    let marker_width = display_width(&marker).min(summary_width);
    let mut spans = vec![Span::styled(
        ellipsize(&marker, marker_width),
        tone_style(color, output.marker_tone),
    )];
    let mut used_width = marker_width;
    if used_width < summary_width {
        let kind_width = display_width(output.kind);
        let remaining_width = summary_width - used_width;
        let name_width = if remaining_width > kind_width.saturating_add(2) {
            remaining_width.saturating_sub(kind_width.saturating_add(2))
        } else {
            remaining_width
        };
        let name = ellipsize(&output.name, name_width);
        used_width = used_width.saturating_add(display_width(&name));
        spans.push(Span::styled(name, tone_style(color, Tone::Primary)));
        if summary_width.saturating_sub(used_width) > kind_width.saturating_add(1) {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(output.kind, tone_style(color, Tone::Muted)));
            used_width = used_width.saturating_add(kind_width.saturating_add(2));
        }
    }
    if used_width < summary_width {
        spans.push(Span::raw(" ".repeat(summary_width - used_width)));
    }
    if let Some(disposition) = &output.disposition {
        spans.push(Span::raw(" ".repeat(gap_width)));
        spans.push(Span::styled(
            disposition.clone(),
            tone_style(color, output.tone),
        ));
    }
    Line::from(spans)
}

fn inspector_output_detail_line(
    output: &InspectorOutput,
    width: u16,
    color: bool,
) -> Line<'static> {
    let prefix = "   ";
    let detail_width = usize::from(width).saturating_sub(display_width(prefix));
    Line::from(vec![
        Span::raw(prefix),
        Span::styled(
            ellipsize(&output.detail, detail_width),
            tone_style(color, Tone::Muted),
        ),
    ])
}

fn ellipsize(value: &str, maximum_width: usize) -> String {
    if display_width(value) <= maximum_width {
        return value.to_owned();
    }
    if maximum_width == 0 {
        return String::new();
    }
    if maximum_width == 1 {
        return "…".to_owned();
    }

    let content_width = maximum_width - 1;
    let mut used_width = 0_usize;
    let mut result = String::new();
    for grapheme in value.graphemes(true) {
        let grapheme_width = display_width(grapheme);
        if used_width.saturating_add(grapheme_width) > content_width {
            break;
        }
        result.push_str(grapheme);
        used_width = used_width.saturating_add(grapheme_width);
    }
    result.push('…');
    result
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

fn step_state_is_active(state: StepStateKind) -> bool {
    matches!(
        state,
        StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Cancelling
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StepColumns {
    id_width: usize,
    kind: bool,
    duration_width: usize,
    detail: bool,
    detail_width: usize,
}

impl StepColumns {
    fn for_steps<Step: StepProjection>(
        available: usize,
        gutter_width: usize,
        steps: &[Step],
    ) -> Self {
        let id_width = steps
            .iter()
            .map(|step| display_width(&visible_text(step.id())))
            .max()
            .unwrap_or(0);
        let duration_width = steps
            .iter()
            .map(|step| {
                step.timing()
                    .map_or(1, |timing| display_width(&human_duration(timing.duration)))
            })
            .max()
            .unwrap_or(1);
        let prefix_width = 2_usize.saturating_add(gutter_width).saturating_add(1);
        let exact_with_kind = prefix_width
            .saturating_add(id_width)
            .saturating_add(2)
            .saturating_add(KIND_COLUMN_WIDTH)
            .saturating_add(2)
            .saturating_add(duration_width);
        let detail = available
            >= exact_with_kind
                .saturating_add(2)
                .saturating_add(MINIMUM_DETAIL_WIDTH);
        if detail {
            return Self {
                id_width,
                kind: true,
                duration_width,
                detail: true,
                detail_width: available.saturating_sub(exact_with_kind.saturating_add(2)),
            };
        }
        if available >= exact_with_kind {
            return Self {
                id_width,
                kind: true,
                duration_width,
                detail: false,
                detail_width: 0,
            };
        }
        let fixed_width = prefix_width
            .saturating_add(2)
            .saturating_add(duration_width);
        Self {
            id_width: id_width.min(available.saturating_sub(fixed_width)),
            kind: false,
            duration_width,
            detail: false,
            detail_width: 0,
        }
    }
}

fn live_step_detail(step: &WorkflowRunStepView) -> Option<String> {
    match &step.fact {
        Some(ObservedStepTransition::Recovery {
            active,
            configured_rounds,
            handler_kind,
            handler_state,
            decision,
            ..
        }) => Some(recovery_progress_detail(
            *active,
            *configured_rounds,
            *handler_kind,
            *handler_state,
            *decision,
        )),
        Some(ObservedStepTransition::OutputsCommitted { outputs }) => {
            Some(output_count_detail(outputs.len()))
        }
        Some(ObservedStepTransition::Failed { detail }) => Some(issue_detail_for_step(
            canonical_failure_detail(detail),
            &step.definition,
            step.state,
        )),
        Some(ObservedStepTransition::Blocked { detail }) => Some(issue_detail_for_step(
            canonical_blocked_detail(detail),
            &step.definition,
            step.state,
        )),
        Some(ObservedStepTransition::Skipped { detail }) => {
            Some(super::archived_presentation::condition_false_detail(detail))
        }
        Some(ObservedStepTransition::NotRun { detail }) => {
            Some(super::presentation::snake_case_debug(detail.code))
        }
        Some(ObservedStepTransition::Cancelling { detail })
        | Some(ObservedStepTransition::Cancelled { detail }) => {
            Some(cancellation_reason(detail.code).to_owned())
        }
        None if step.state == StepStateKind::Succeeded => {
            let committed_outputs = step
                .outputs
                .values()
                .filter(|disposition| **disposition == WorkflowRunOutputDisposition::Committed)
                .count();
            match &step.definition {
                WorkflowPresentationStep::Command { .. } if committed_outputs == 0 => {
                    Some("exit 0".to_owned())
                }
                WorkflowPresentationStep::Command { .. } => Some(format!(
                    "exit 0 · {}",
                    output_count_detail(committed_outputs)
                )),
                WorkflowPresentationStep::Agent { .. } if committed_outputs != 0 => {
                    Some(output_count_detail(committed_outputs))
                }
                WorkflowPresentationStep::Agent { .. } => None,
            }
        }
        None => None,
    }
}

#[cfg(test)]
fn step_detail(step: &WorkflowRunStepView) -> Option<String> {
    live_step_detail(step)
}

fn failure_policy_name(policy: FailurePolicy) -> &'static str {
    match policy {
        FailurePolicy::Required => "required",
        FailurePolicy::Advisory => "advisory",
    }
}

fn is_advisory_issue(definition: &WorkflowPresentationStep, state: StepStateKind) -> bool {
    definition.failure_policy() == FailurePolicy::Advisory
        && matches!(state, StepStateKind::Failed | StepStateKind::Blocked)
}

fn issue_detail_for_step(
    detail: String,
    definition: &WorkflowPresentationStep,
    state: StepStateKind,
) -> String {
    if is_advisory_issue(definition, state) {
        format!("{detail} · advisory")
    } else {
        detail
    }
}

pub(super) fn output_count_detail(count: usize) -> String {
    if count == 1 {
        "1 output committed".to_owned()
    } else {
        format!("{count} outputs committed")
    }
}

fn padded_text(value: &str, width: usize) -> String {
    let fitted = fit_text(value, width);
    let padding = width.saturating_sub(display_width(&fitted));
    format!("{fitted}{}", " ".repeat(padding))
}

fn fit_text(value: &str, width: usize) -> String {
    if display_width(value) <= width {
        return value.to_owned();
    }
    if width == 0 {
        return String::new();
    }
    let content_width = width.saturating_sub(1);
    let mut fitted = String::new();
    let mut used = 0_usize;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used.saturating_add(character_width) > content_width {
            break;
        }
        fitted.push(character);
        used = used.saturating_add(character_width);
    }
    fitted.push('…');
    fitted
}

fn graph_connector_style(color: bool) -> Style {
    let style = if color {
        Style::default().fg(Color::Rgb(127, 132, 156))
    } else {
        Style::default()
    };
    style.add_modifier(Modifier::DIM)
}

fn selection_marker_style(color: bool) -> Style {
    if color {
        tone_style(true, Tone::Active)
    } else {
        Style::default()
    }
}

fn step_selection_style(color: bool) -> Style {
    if color {
        Style::default().bg(Color::Rgb(49, 50, 68))
    } else {
        Style::default().add_modifier(Modifier::REVERSED)
    }
}

fn step_identity_style(state: StepStateKind, color: bool) -> Style {
    let tone = match state {
        StepStateKind::Pending
        | StepStateKind::Blocked
        | StepStateKind::NotRun
        | StepStateKind::Cancelled => Tone::Muted,
        _ => Tone::Primary,
    };
    tone_style(color, tone)
}

fn step_duration_style(state: StepStateKind, color: bool) -> Style {
    let tone = if step_state_is_active(state) {
        Tone::Active
    } else {
        Tone::Muted
    };
    tone_style(color, tone)
}

fn render_log(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    interaction: &HostInteraction,
    color: bool,
    borders: Borders,
) {
    let Some(step) = snapshot.steps.get(interaction.selected) else {
        render_missing_step_log(frame, area, borders, color);
        return;
    };
    let log = FilteredLog::new(&step.log, interaction.log_filters);
    let records_area = render_log_surface(
        frame,
        area,
        borders,
        Some(LogHeaderState {
            step,
            status: LogTitleStatus::Following,
            filters: interaction.log_filters,
            hidden_records: log.hidden_records,
        }),
        color,
    );
    let lines = log_tail_lines(
        step,
        &log,
        usize::from(records_area.width),
        usize::from(records_area.height),
        color,
    );
    frame.render_widget(Paragraph::new(Text::from(lines)), records_area);
}

fn render_missing_step_log(frame: &mut Frame<'_>, area: Rect, borders: Borders, color: bool) {
    let records_area = render_log_surface(frame, area, borders, None, color);
    frame.render_widget(Paragraph::new("No workflow steps."), records_area);
}

fn render_full_log(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&WorkflowRunStepView>,
    interaction: &FullLogInteraction,
    filters: LogFilterState,
    color: bool,
) {
    let Some(step) = step else {
        render_missing_step_log(frame, area, Borders::ALL, color);
        return;
    };
    let log = FilteredLog::new(&step.log, filters);
    let status = if interaction.follow {
        LogTitleStatus::Following
    } else {
        LogTitleStatus::Paused {
            lines_behind: interaction.lines_behind(&log),
        }
    };
    let mut records_area = render_log_surface(
        frame,
        area,
        Borders::TOP,
        Some(LogHeaderState {
            step,
            status,
            filters,
            hidden_records: log.hidden_records,
        }),
        color,
    );

    if step.log.discarded_records != 0 && records_area.height != 0 {
        let marker_area = Rect::new(records_area.x, records_area.y, records_area.width, 1);
        frame.render_widget(
            Paragraph::new(log_eviction_line(
                step.log.discarded_records,
                step.log.discarded_bytes,
                interaction.anchor_clamped,
                color,
            )),
            marker_area,
        );
        records_area.y = records_area.y.saturating_add(1);
        records_area.height = records_area.height.saturating_sub(1);
    }
    if records_area.is_empty() {
        return;
    }
    if log.records.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                filtered_empty_log_message(step, log.hidden_records),
                tone_style(color, Tone::Muted),
            ))),
            records_area,
        );
        return;
    }

    let available_width = usize::from(records_area.width);
    let top = interaction.top_index(&log);
    let lines = log
        .records
        .iter()
        .skip(top)
        .take(usize::from(records_area.height))
        .map(|record| log_record_line(record, available_width, color))
        .collect::<Vec<_>>();
    let horizontal_offset = u16::try_from(interaction.horizontal_offset).unwrap_or(u16::MAX);
    frame.render_widget(
        Paragraph::new(Text::from(lines)).scroll((0, horizontal_offset)),
        records_area,
    );
}

#[derive(Clone, Copy)]
enum LogTitleStatus {
    Following,
    Paused { lines_behind: usize },
}

#[derive(Clone, Copy)]
struct LogHeaderState<'a> {
    step: &'a WorkflowRunStepView,
    status: LogTitleStatus,
    filters: LogFilterState,
    hidden_records: usize,
}

fn render_log_surface(
    frame: &mut Frame<'_>,
    area: Rect,
    borders: Borders,
    header: Option<LogHeaderState<'_>>,
    color: bool,
) -> Rect {
    let block = log_block(borders, color);
    let content_area = block.inner(area);
    frame.render_widget(block, area);
    let sections = log_content_areas(content_area);
    render_log_header(frame, sections[0], header, color);
    sections[1]
}

fn log_block(borders: Borders, color: bool) -> Block<'static> {
    section_block(borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING))
}

fn log_content_areas(area: Rect) -> [Rect; 2] {
    let rows =
        Layout::vertical([Constraint::Length(LOG_HEADER_HEIGHT), Constraint::Min(0)]).split(area);
    [rows[0], rows[1]]
}

fn render_log_header(
    frame: &mut Frame<'_>,
    area: Rect,
    header: Option<LogHeaderState<'_>>,
    color: bool,
) {
    if area.is_empty() {
        return;
    }
    let Some(LogHeaderState {
        step,
        status,
        filters,
        hidden_records,
    }) = header
    else {
        frame.render_widget(
            Paragraph::new(Span::styled("LOG", tone_style(color, Tone::Muted))),
            Rect::new(area.x, area.y, area.width, 1),
        );
        return;
    };

    let full_channels = log_channel_title(step, filters, false, color);
    let compact_channels = log_channel_title(step, filters, true, color);
    let full_status = log_status_title(step, status, hidden_records, false, color);
    let compact_status = log_status_title(step, status, hidden_records, true, color);
    let available_width = usize::from(area.width);
    let fits = |channels: &Line<'_>, status: &Line<'_>| {
        channels
            .width()
            .saturating_add(2)
            .saturating_add(status.width())
            <= available_width
    };
    let (channels, status) = if fits(&full_channels, &full_status) {
        (full_channels, full_status)
    } else if fits(&compact_channels, &full_status) {
        (compact_channels, full_status)
    } else {
        (compact_channels, compact_status)
    };

    let status_width = u16::try_from(status.width())
        .unwrap_or(u16::MAX)
        .min(area.width);
    let channel_width = area.width.saturating_sub(status_width.saturating_add(2));
    if channel_width >= 3 {
        frame.render_widget(
            Paragraph::new(channels),
            Rect::new(area.x, area.y, channel_width, 1),
        );
    }
    frame.render_widget(
        Paragraph::new(status),
        Rect::new(
            area.right().saturating_sub(status_width),
            area.y,
            status_width,
            1,
        ),
    );
}

fn log_channel_title(
    step: &WorkflowRunStepView,
    filters: LogFilterState,
    compact: bool,
    color: bool,
) -> Line<'static> {
    let mut spans = vec![Span::styled("LOG", tone_style(color, Tone::Muted))];
    let separator = if compact { " " } else { "  " };
    for option in log_channel_options(step) {
        let enabled = filters.includes(option.channel);
        let style = if enabled {
            tone_style(color, Tone::Neutral).add_modifier(Modifier::UNDERLINED)
        } else {
            tone_style(color, Tone::Muted).add_modifier(Modifier::DIM)
        };
        let label = if compact {
            option.compact_label
        } else {
            option.label
        };
        spans.push(Span::raw(separator));
        spans.push(Span::styled(format!("{} {label}", option.key), style));
    }
    Line::from(spans)
}

fn log_status_title(
    step: &WorkflowRunStepView,
    status: LogTitleStatus,
    hidden_records: usize,
    compact: bool,
    color: bool,
) -> Line<'static> {
    let tone = match status {
        LogTitleStatus::Following => Tone::Active,
        LogTitleStatus::Paused { .. } => Tone::Blocked,
    };
    let text = if compact {
        if hidden_records != 0 {
            match status {
                LogTitleStatus::Following => format!("● {hidden_records} hidden"),
                LogTitleStatus::Paused { lines_behind } => {
                    format!("● {lines_behind} back · {hidden_records} hidden")
                }
            }
        } else if step.log.retained_records != step.log.observed_records {
            format!(
                "● {}/{} kept",
                step.log.retained_records, step.log.observed_records
            )
        } else {
            match status {
                LogTitleStatus::Following => {
                    let line_label = if step.log.observed_records == 1 {
                        "line"
                    } else {
                        "lines"
                    };
                    format!("● {} {line_label}", step.log.observed_records)
                }
                LogTitleStatus::Paused { lines_behind } => {
                    format!("● {lines_behind} behind")
                }
            }
        }
    } else {
        let status = match status {
            LogTitleStatus::Following => "following".to_owned(),
            LogTitleStatus::Paused { lines_behind } => {
                let line_label = if lines_behind == 1 { "line" } else { "lines" };
                format!("paused · {lines_behind} {line_label} behind")
            }
        };
        let count = if hidden_records != 0 {
            format!("{hidden_records} hidden")
        } else if step.log.retained_records == step.log.observed_records {
            let line_label = if step.log.observed_records == 1 {
                "line"
            } else {
                "lines"
            };
            format!("{} {line_label}", step.log.observed_records)
        } else {
            format!(
                "{} retained / {} total",
                step.log.retained_records, step.log.observed_records
            )
        };
        format!("● {status} · {count}")
    };
    Line::from(Span::styled(text, tone_style(color, tone)))
}

fn log_tail_lines(
    step: &WorkflowRunStepView,
    log: &FilteredLog<'_>,
    available_width: usize,
    available_rows: usize,
    color: bool,
) -> Vec<Line<'static>> {
    if available_rows == 0 {
        return Vec::new();
    }

    let mut lines = Vec::new();
    let tail_rows = if step.log.discarded_records == 0 {
        available_rows
    } else {
        lines.push(log_eviction_line(
            step.log.discarded_records,
            step.log.discarded_bytes,
            false,
            color,
        ));
        available_rows.saturating_sub(1)
    };
    if tail_rows == 0 {
        return lines;
    }

    if log.records.is_empty() {
        lines.push(Line::from(Span::styled(
            filtered_empty_log_message(step, log.hidden_records),
            tone_style(color, Tone::Muted),
        )));
        return lines;
    }

    let mut remaining_rows = tail_rows;
    let mut newest_first_record_lines = Vec::new();
    for record in log.records.iter().rev() {
        let record_lines = log_record_tail_lines(record, available_width, remaining_rows, color);
        remaining_rows = remaining_rows.saturating_sub(record_lines.len());
        newest_first_record_lines.push(record_lines);
        if remaining_rows == 0 {
            break;
        }
    }
    for record_lines in newest_first_record_lines.into_iter().rev() {
        lines.extend(record_lines);
    }
    lines
}

fn log_eviction_line(
    discarded_records: u64,
    discarded_bytes: u64,
    anchor_clamped: bool,
    color: bool,
) -> Line<'static> {
    let line_label = if discarded_records == 1 {
        "line"
    } else {
        "lines"
    };
    let byte_label = if discarded_bytes == 1 {
        "byte"
    } else {
        "bytes"
    };
    let clamp_notice = if anchor_clamped {
        " | clamped to retained top"
    } else {
        ""
    };
    Line::from(Span::styled(
        format!(
            "↑ {discarded_records} older {line_label} / {discarded_bytes} {byte_label} discarded{clamp_notice}"
        ),
        tone_style(color, Tone::Muted),
    ))
}

fn filtered_empty_log_message(step: &WorkflowRunStepView, hidden_records: usize) -> &'static str {
    if hidden_records != 0 {
        "All log channels hidden."
    } else {
        empty_log_message(step.state)
    }
}

fn empty_log_message(state: StepStateKind) -> &'static str {
    match state {
        StepStateKind::Pending => "Waiting for this step to start.",
        StepStateKind::Starting
        | StepStateKind::Running
        | StepStateKind::CapturingOutputs
        | StepStateKind::Recovering
        | StepStateKind::Cancelling => "Waiting for output…",
        StepStateKind::Succeeded
        | StepStateKind::Failed
        | StepStateKind::Blocked
        | StepStateKind::Skipped
        | StepStateKind::NotRun
        | StepStateKind::Cancelled => "No output received.",
    }
}

fn log_record_tail_lines(
    record: &WorkflowRunLogRecord,
    available_width: usize,
    maximum_rows: usize,
    color: bool,
) -> Vec<Line<'static>> {
    let gutter = LogGutter::for_width(available_width);
    let content_width = available_width.saturating_sub(gutter.width()).max(1);
    wrap_log_payload_tail(&record.payload, content_width, maximum_rows)
        .into_iter()
        .enumerate()
        .map(|(visible_index, (is_first_line, payload))| {
            let row_kind = if is_first_line {
                LogRowKind::for_record(record)
            } else if visible_index == 0 {
                LogRowKind::ClippedVisualContinuation
            } else {
                LogRowKind::VisualContinuation
            };
            log_line(record, payload, gutter, row_kind, color)
        })
        .collect()
}

fn log_record_line(
    record: &WorkflowRunLogRecord,
    available_width: usize,
    color: bool,
) -> Line<'static> {
    log_line(
        record,
        record.payload.to_string(),
        LogGutter::for_width(available_width),
        LogRowKind::for_record(record),
        color,
    )
}

fn log_line(
    record: &WorkflowRunLogRecord,
    payload: String,
    gutter: LogGutter,
    row_kind: LogRowKind,
    color: bool,
) -> Line<'static> {
    let mut spans = gutter.spans(record, row_kind, color);
    spans.push(Span::styled(
        payload,
        log_payload_style(record.source, color),
    ));
    Line::from(spans)
}

fn wrap_log_payload(payload: &str, maximum_width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    for_each_wrapped_log_payload(payload, maximum_width, |_, line| lines.push(line));
    lines
}

fn wrap_log_payload_tail(
    payload: &str,
    maximum_width: usize,
    maximum_rows: usize,
) -> VecDeque<(bool, String)> {
    if maximum_rows == 0 {
        return VecDeque::new();
    }

    let mut lines = VecDeque::with_capacity(maximum_rows);
    for_each_wrapped_log_payload(payload, maximum_width, |is_first_line, line| {
        if lines.len() == maximum_rows {
            lines.pop_front();
        }
        lines.push_back((is_first_line, line));
    });
    lines
}

fn for_each_wrapped_log_payload(
    payload: &str,
    maximum_width: usize,
    mut emit: impl FnMut(bool, String),
) {
    if payload.is_empty() {
        emit(true, String::new());
        return;
    }

    let mut line = String::new();
    let mut line_width = 0_usize;
    let mut is_first_line = true;
    for grapheme in payload.graphemes(true) {
        let grapheme_width = display_width(grapheme);
        if !line.is_empty() && line_width.saturating_add(grapheme_width) > maximum_width {
            emit(is_first_line, std::mem::take(&mut line));
            is_first_line = false;
            line_width = 0;
        }
        line.push_str(grapheme);
        line_width = line_width.saturating_add(grapheme_width);
    }
    if !line.is_empty() {
        emit(is_first_line, line);
    }
}

#[derive(Clone, Copy)]
enum LogRowKind {
    Record,
    SafetyContinuation,
    VisualContinuation,
    ClippedVisualContinuation,
}

impl LogRowKind {
    const fn for_record(record: &WorkflowRunLogRecord) -> Self {
        if record.continuation {
            Self::SafetyContinuation
        } else {
            Self::Record
        }
    }
}

#[derive(Clone, Copy)]
struct LogSourcePresentation {
    label: &'static str,
    source_tone: Tone,
    payload_tone: Tone,
    dim_payload: bool,
}

const fn log_source_presentation(source: WorkflowRunLogSource) -> LogSourcePresentation {
    let (label, source_tone, payload_tone, dim_payload) = match source {
        WorkflowRunLogSource::Command(CommandOutputSource::StandardOutput) => {
            ("stdout", Tone::Muted, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Command(CommandOutputSource::StandardError) => {
            ("stderr", Tone::Blocked, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Assistant) => {
            ("agent", Tone::Active, Tone::Primary, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Reasoning) => {
            ("reason", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::ToolCall) => {
            ("tool", Tone::Active, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::ToolResult) => {
            ("result", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Diagnostic) => {
            ("diag", Tone::Blocked, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Usage) => {
            ("usage", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Model) => {
            ("model", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Lifecycle) => {
            ("life", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::ValueRejected) => {
            ("reject", Tone::Failure, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::HarnessEvent) => {
            ("event", Tone::Muted, Tone::Muted, true)
        }
    };
    LogSourcePresentation {
        label,
        source_tone,
        payload_tone,
        dim_payload,
    }
}

fn log_payload_style(source: WorkflowRunLogSource, color: bool) -> Style {
    let presentation = log_source_presentation(source);
    let style = tone_style(color, presentation.payload_tone);
    if presentation.dim_payload {
        style.add_modifier(Modifier::DIM)
    } else {
        style
    }
}

fn log_source_style(source: WorkflowRunLogSource, color: bool) -> Style {
    tone_style(color, log_source_presentation(source).source_tone)
}

#[derive(Clone, Copy)]
struct LogGutter {
    timestamp: bool,
}

impl LogGutter {
    fn for_width(available_width: usize) -> Self {
        Self {
            timestamp: available_width
                >= LOG_TIMESTAMPED_GUTTER_WIDTH + MINIMUM_TIMESTAMPED_LOG_CONTENT_WIDTH,
        }
    }

    const fn width(self) -> usize {
        if self.timestamp {
            LOG_TIMESTAMPED_GUTTER_WIDTH
        } else {
            LOG_SOURCE_GUTTER_WIDTH
        }
    }

    fn spans(
        self,
        record: &WorkflowRunLogRecord,
        row_kind: LogRowKind,
        color: bool,
    ) -> Vec<Span<'static>> {
        let mut spans = Vec::new();
        let visual_continuation = matches!(
            row_kind,
            LogRowKind::VisualContinuation | LogRowKind::ClippedVisualContinuation
        );
        if self.timestamp {
            let timestamp = if visual_continuation {
                " ".repeat(LOG_TIMESTAMP_WIDTH)
            } else {
                log_timestamp(record.observed_at)
            };
            spans.push(Span::styled(timestamp, tone_style(color, Tone::Muted)));
            spans.push(Span::raw(" "));
        }
        let source = log_source_presentation(record.source);
        let source_style = log_source_style(record.source, color);
        let source_label = if matches!(row_kind, LogRowKind::VisualContinuation) {
            " ".repeat(LOG_SOURCE_WIDTH)
        } else {
            format!("{:<LOG_SOURCE_WIDTH$}", source.label)
        };
        spans.push(Span::styled(source_label, source_style));
        let marker = match row_kind {
            LogRowKind::Record => " │ ",
            LogRowKind::SafetyContinuation => " ↪ ",
            LogRowKind::VisualContinuation | LogRowKind::ClippedVisualContinuation => " ↳ ",
        };
        spans.push(Span::styled(marker, source_style));
        spans
    }
}

fn log_timestamp(observed_at: time::OffsetDateTime) -> String {
    let utc = observed_at.to_offset(UtcOffset::UTC);
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        utc.hour(),
        utc.minute(),
        utc.second(),
        utc.millisecond()
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleControl {
    Cancel,
    Quit,
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FinalizationSignalAction {
    Graceful,
    ForceAbort,
    Inert,
}

fn finalization_signal_action(snapshot: &WorkflowRunViewSnapshot) -> FinalizationSignalAction {
    match &snapshot.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open | SchedulingGate::FailureStopped { .. },
        }
        | WorkflowState::Finalizing {
            gate: super::runtime::FinalizationGate::Open,
            ..
        } => FinalizationSignalAction::Graceful,
        WorkflowState::Finalizing {
            gate:
                super::runtime::FinalizationGate::Cancelling {
                    force_abort: false, ..
                },
            ..
        } => FinalizationSignalAction::ForceAbort,
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        }
        | WorkflowState::Finalizing {
            gate:
                super::runtime::FinalizationGate::Cancelling {
                    force_abort: true, ..
                },
            ..
        }
        | WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. } => FinalizationSignalAction::Inert,
    }
}

fn cancellation_available(snapshot: &WorkflowRunViewSnapshot) -> bool {
    finalization_signal_action(snapshot) != FinalizationSignalAction::Inert
}

fn lifecycle_control(snapshot: &WorkflowRunViewSnapshot) -> LifecycleControl {
    if snapshot.quit_eligible {
        LifecycleControl::Quit
    } else if cancellation_available(snapshot) {
        LifecycleControl::Cancel
    } else {
        LifecycleControl::None
    }
}

const SPLIT_FOOTER_OPTIONS: [&[&str]; 3] = [
    &["↑/k up", "↓/j down", "↵ open"],
    &["↑/k up", "↓/j down", "↵ open"],
    &["↑/k", "↓/j", "↵"],
];

const FULL_LOG_FOOTER_OPTIONS: [&[&str]; 3] = [
    &[
        "Esc back",
        "↑/k up",
        "↓/j down",
        "PgUp/b page-up",
        "PgDn/f page-down",
        "←/h left",
        "→/l right",
        "F follow",
    ],
    &["Esc back", "↑/k", "↓/j", "PgUp/b", "PgDn/f", "F follow"],
    &["Esc", "↑/k", "↓/j", "F"],
];

fn render_contextual_footer(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    color: bool,
    label: &'static str,
    command_options: &[&[&str]],
) {
    let lifecycle = lifecycle_control(snapshot);
    let options = command_options
        .iter()
        .enumerate()
        .map(|(index, commands)| {
            footer_option(
                commands,
                lifecycle,
                index.saturating_add(1) == command_options.len(),
            )
        })
        .collect();
    let reserved_width = u16::try_from(display_width(label).saturating_add(4)).unwrap_or(u16::MAX);
    let text = fitting_footer(options, area.width.saturating_sub(reserved_width));
    render_footer_text(frame, area, label, text, color);
}

fn footer_option(
    commands: &[&str],
    lifecycle: LifecycleControl,
    abbreviate_lifecycle: bool,
) -> String {
    let mut parts = commands
        .iter()
        .map(|command| (*command).to_owned())
        .collect::<Vec<_>>();
    let lifecycle = match (lifecycle, abbreviate_lifecycle) {
        (LifecycleControl::Cancel, false) => Some("^C cancel run"),
        (LifecycleControl::Cancel, true) => Some("^C"),
        (LifecycleControl::Quit, _) => Some("q quit"),
        (LifecycleControl::None, _) => None,
    };
    if let Some(lifecycle) = lifecycle {
        parts.push(lifecycle.to_owned());
    }
    parts.push("? help".to_owned());
    parts.join("  ")
}

fn fitting_footer(options: Vec<String>, width: u16) -> String {
    let available = usize::from(width);
    options
        .iter()
        .find(|option| display_width(option) <= available)
        .cloned()
        .unwrap_or_else(|| ellipsize(options.last().map_or("? help", String::as_str), available))
}

fn render_footer_text(
    frame: &mut Frame<'_>,
    area: Rect,
    label: &'static str,
    text: String,
    color: bool,
) {
    let mut spans = vec![
        Span::styled(
            label,
            command_accent_style(color).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
    ];
    for (index, command) in text.split("  ").enumerate() {
        if index != 0 {
            spans.push(Span::raw("  "));
        }
        if let Some((keys, description)) = command.split_once(' ') {
            let key_style = if keys == "?" {
                command_accent_style(color)
            } else {
                footer_key_style(color)
            };
            spans.push(Span::styled(keys.to_owned(), key_style));
            spans.push(Span::styled(
                format!(" {description}"),
                tone_style(color, Tone::Muted),
            ));
        } else {
            spans.push(Span::styled(command.to_owned(), footer_key_style(color)));
        }
    }
    frame.render_widget(
        Paragraph::new(Line::from(spans)).block(
            section_block(Borders::TOP, color)
                .border_style(footer_separator_style(color))
                .padding(Padding::horizontal(INSPECTOR_PANEL_PADDING)),
        ),
        area,
    );
}

#[derive(Clone, Copy)]
pub(super) struct HelpCommand {
    pub(super) keys: &'static str,
    pub(super) description: &'static str,
}

pub(super) struct HelpGroup {
    pub(super) title: &'static str,
    pub(super) commands: Vec<HelpCommand>,
}

fn render_help_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    surface: HostSurface,
    lifecycle: LifecycleControl,
    color: bool,
) {
    render_help_overlay_groups(frame, area, help_groups(surface, lifecycle), color);
}

pub(super) fn render_help_overlay_groups(
    frame: &mut Frame<'_>,
    area: Rect,
    groups: Vec<HelpGroup>,
    color: bool,
) {
    let column_count = help_column_count(area.width);
    let grid_height = help_grid_height(&groups, column_count);
    let desired_height = u16::try_from(grid_height)
        .unwrap_or(u16::MAX)
        .saturating_add(3);
    let panel_height = desired_height.min(area.height);
    let panel_area = Rect::new(
        area.x,
        area.bottom().saturating_sub(panel_height),
        area.width,
        panel_height,
    );
    let block =
        section_block(Borders::TOP, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let content_area = block.inner(panel_area);

    frame.render_widget(Clear, panel_area);
    frame.render_widget(block, panel_area);
    if content_area.is_empty() {
        return;
    }
    render_help_heading(frame, content_area, color);
    let grid_area = Rect::new(
        content_area.x,
        content_area.y.saturating_add(2),
        content_area.width,
        content_area.height.saturating_sub(2),
    );
    render_help_groups(frame, grid_area, &groups, column_count, color);
}

#[derive(Clone, Copy)]
enum OutputHelpMode {
    Live,
    Archived,
}

fn surface_help_groups(surface: HostSurface, mode: OutputHelpMode) -> Vec<HelpGroup> {
    match surface {
        HostSurface::Split => vec![
            HelpGroup {
                title: "MOVE",
                commands: vec![
                    HelpCommand {
                        keys: "↑/k",
                        description: "previous step",
                    },
                    HelpCommand {
                        keys: "↓/j",
                        description: "next step",
                    },
                ],
            },
            HelpGroup {
                title: "OPEN",
                commands: vec![HelpCommand {
                    keys: "↵",
                    description: match mode {
                        OutputHelpMode::Live => "open step log",
                        OutputHelpMode::Archived => "open retained output",
                    },
                }],
            },
            HelpGroup {
                title: "VIEW",
                commands: vec![
                    HelpCommand {
                        keys: "?",
                        description: "this help",
                    },
                    HelpCommand {
                        keys: "Esc",
                        description: "dismiss",
                    },
                ],
            },
        ],
        HostSurface::FullLog => {
            let noun = match mode {
                OutputHelpMode::Live => "record",
                OutputHelpMode::Archived => "row",
            };
            let mut view_commands = Vec::new();
            if matches!(mode, OutputHelpMode::Live) {
                view_commands.push(HelpCommand {
                    keys: "F",
                    description: "follow latest",
                });
            }
            view_commands.extend([
                HelpCommand {
                    keys: "Esc",
                    description: "back / dismiss",
                },
                HelpCommand {
                    keys: "?",
                    description: "this help",
                },
            ]);
            vec![
                HelpGroup {
                    title: "MOVE",
                    commands: vec![
                        HelpCommand {
                            keys: "↑/k",
                            description: if noun == "record" {
                                "one record up"
                            } else {
                                "one row up"
                            },
                        },
                        HelpCommand {
                            keys: "↓/j",
                            description: if noun == "record" {
                                "one record down"
                            } else {
                                "one row down"
                            },
                        },
                        HelpCommand {
                            keys: "PgUp/b",
                            description: "one page up",
                        },
                        HelpCommand {
                            keys: "PgDn/f/Space",
                            description: "one page down",
                        },
                        HelpCommand {
                            keys: "u/^U",
                            description: "half page up",
                        },
                        HelpCommand {
                            keys: "d/^D",
                            description: "half page down",
                        },
                    ],
                },
                HelpGroup {
                    title: "JUMP",
                    commands: vec![
                        HelpCommand {
                            keys: "g",
                            description: match mode {
                                OutputHelpMode::Live => "first record",
                                OutputHelpMode::Archived => "top",
                            },
                        },
                        HelpCommand {
                            keys: "G",
                            description: match mode {
                                OutputHelpMode::Live => "retained bottom",
                                OutputHelpMode::Archived => "bottom",
                            },
                        },
                        HelpCommand {
                            keys: "←/h",
                            description: "pan left",
                        },
                        HelpCommand {
                            keys: "→/l",
                            description: "pan right",
                        },
                    ],
                },
                HelpGroup {
                    title: "VIEW",
                    commands: view_commands,
                },
            ]
        }
    }
}

fn help_groups(surface: HostSurface, lifecycle: LifecycleControl) -> Vec<HelpGroup> {
    let mut groups = surface_help_groups(surface, OutputHelpMode::Live);
    groups.push(HelpGroup {
        title: "FILTER",
        commands: vec![HelpCommand {
            keys: "1…n",
            description: "toggle log channels",
        }],
    });
    let command = match lifecycle {
        LifecycleControl::Cancel => Some(HelpCommand {
            keys: "^C",
            description: "cancel run",
        }),
        LifecycleControl::Quit => Some(HelpCommand {
            keys: "q",
            description: "quit",
        }),
        LifecycleControl::None => None,
    };
    if let Some(command) = command {
        groups.push(HelpGroup {
            title: "RUN",
            commands: vec![command],
        });
    }
    groups
}

fn help_column_count(width: u16) -> usize {
    if width >= WIDE_LAYOUT_WIDTH { 4 } else { 2 }
}

fn help_grid_height(groups: &[HelpGroup], column_count: usize) -> usize {
    groups
        .chunks(column_count)
        .enumerate()
        .map(|(index, groups)| {
            usize::from(index != 0).saturating_add(1).saturating_add(
                groups
                    .iter()
                    .map(|group| group.commands.len())
                    .max()
                    .unwrap_or(0),
            )
        })
        .sum()
}

fn render_help_heading(frame: &mut Frame<'_>, area: Rect, color: bool) {
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("?", command_accent_style(color)),
            Span::styled(" — all commands", tone_style(color, Tone::Muted)),
        ])),
        Rect::new(area.x, area.y, area.width, 1),
    );
    let dismissal = "esc to dismiss";
    let dismissal_width = u16::try_from(display_width(dismissal))
        .unwrap_or(u16::MAX)
        .min(area.width);
    frame.render_widget(
        Paragraph::new(Span::styled(dismissal, tone_style(color, Tone::Muted))),
        Rect::new(
            area.right().saturating_sub(dismissal_width),
            area.y,
            dismissal_width,
            1,
        ),
    );
}

fn render_help_groups(
    frame: &mut Frame<'_>,
    area: Rect,
    groups: &[HelpGroup],
    column_count: usize,
    color: bool,
) {
    let mut y = area.y;
    for (band_index, band) in groups.chunks(column_count).enumerate() {
        if band_index != 0 {
            y = y.saturating_add(1);
        }
        let row_count = band
            .iter()
            .map(|group| group.commands.len())
            .max()
            .unwrap_or(0);
        let height = u16::try_from(row_count.saturating_add(1)).unwrap_or(u16::MAX);
        let band_area = Rect::new(
            area.x,
            y,
            area.width,
            height.min(area.bottom().saturating_sub(y)),
        );
        for (group, column) in band.iter().zip(help_column_areas(band_area, column_count)) {
            render_help_group(frame, column, group, color);
        }
        y = y.saturating_add(height);
        if y >= area.bottom() {
            break;
        }
    }
}

fn help_column_areas(area: Rect, column_count: usize) -> Vec<Rect> {
    let gap_width = 2_u16;
    let gap_count = u16::try_from(column_count.saturating_sub(1)).unwrap_or(u16::MAX);
    let content_width = area
        .width
        .saturating_sub(gap_width.saturating_mul(gap_count));
    let column_count = u16::try_from(column_count).unwrap_or(1).max(1);
    let base_width = content_width / column_count;
    let mut remainder = content_width % column_count;
    let mut x = area.x;
    (0..column_count)
        .map(|_| {
            let width = base_width.saturating_add(u16::from(remainder != 0));
            remainder = remainder.saturating_sub(1);
            let column = Rect::new(x, area.y, width, area.height);
            x = x.saturating_add(width).saturating_add(gap_width);
            column
        })
        .collect()
}

fn render_help_group(frame: &mut Frame<'_>, area: Rect, group: &HelpGroup, color: bool) {
    if area.is_empty() {
        return;
    }
    frame.render_widget(
        Paragraph::new(Span::styled(group.title, tone_style(color, Tone::Muted))),
        Rect::new(area.x, area.y, area.width, 1),
    );
    let key_width = usize::from(area.width / 2).min(14);
    let lines = group.commands.iter().map(|command| {
        Line::from(vec![
            Span::styled(padded_text(command.keys, key_width), help_key_style(color)),
            Span::styled("→ ", tone_style(color, Tone::Muted)),
            Span::styled(command.description, tone_style(color, Tone::Neutral)),
        ])
    });
    frame.render_widget(
        Paragraph::new(Text::from_iter(lines)),
        Rect::new(
            area.x,
            area.y.saturating_add(1),
            area.width,
            area.height.saturating_sub(1),
        ),
    );
}

#[derive(Default)]
struct StepCounts {
    pending: usize,
    active: usize,
    succeeded: usize,
    failed: usize,
    blocked: usize,
    skipped: usize,
    not_run: usize,
    cancelled: usize,
}

fn step_counts(snapshot: &WorkflowRunViewSnapshot) -> StepCounts {
    let mut counts = StepCounts::default();
    for step in &snapshot.steps {
        match step.state {
            StepStateKind::Pending => counts.pending += 1,
            StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering
            | StepStateKind::Cancelling => counts.active += 1,
            StepStateKind::Succeeded => counts.succeeded += 1,
            StepStateKind::Failed => counts.failed += 1,
            StepStateKind::Blocked => counts.blocked += 1,
            StepStateKind::Skipped => counts.skipped += 1,
            StepStateKind::NotRun => counts.not_run += 1,
            StepStateKind::Cancelled => counts.cancelled += 1,
        }
    }
    counts
}

fn step_count_summary(counts: &StepCounts, total: usize) -> String {
    let step_label = if total == 1 { "step" } else { "steps" };
    let mut parts = vec![format!("{total} {step_label}")];
    for (count, label) in [
        (counts.succeeded, "ok"),
        (counts.active, "running"),
        (counts.failed, "failed"),
        (counts.blocked, "blocked"),
        (counts.skipped, "skipped"),
        (counts.pending, "pending"),
        (counts.not_run, "not-run"),
        (counts.cancelled, "cancelled"),
    ] {
        if count != 0 {
            parts.push(format!("{count} {label}"));
        }
    }
    parts.join(" · ")
}

fn workflow_header_status(snapshot: &WorkflowRunViewSnapshot) -> (&'static str, Tone) {
    match (&snapshot.publication, snapshot.cleanup) {
        (WorkflowRunPublicationState::Publishing, _) => ("publishing", Tone::Active),
        (WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Failed(_)), _) => {
            ("publication failed", Tone::Failure)
        }
        (
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                ..
            }),
            WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Failed),
        ) => ("cleanup failed", Tone::Failure),
        (
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                ..
            }),
            WorkflowRunCleanupState::NotStarted | WorkflowRunCleanupState::Cleaning,
        ) => ("cleaning", Tone::Active),
        _ => (
            workflow_status(&snapshot.workflow),
            workflow_tone(&snapshot.workflow),
        ),
    }
}

fn workflow_status<Deadline>(workflow: &WorkflowState<Deadline>) -> &'static str {
    match workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => "running",
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { .. },
        } => "failing",
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        } => "cancelling",
        WorkflowState::Finalizing { .. } => "finalizing",
        WorkflowState::Succeeded => "succeeded",
        WorkflowState::Failed { .. } => "failed",
        WorkflowState::Cancelled { .. } => "cancelled",
    }
}

fn workflow_tone<Deadline>(workflow: &WorkflowState<Deadline>) -> Tone {
    match workflow {
        WorkflowState::Succeeded => Tone::Success,
        WorkflowState::Failed { .. }
        | WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { .. },
        } => Tone::Failure,
        WorkflowState::Cancelled { .. }
        | WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        } => Tone::Blocked,
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        }
        | WorkflowState::Finalizing { .. } => Tone::Active,
    }
}

fn step_state_glyph<Step: StepProjection>(step: &Step) -> &'static str {
    match step.state() {
        StepStateKind::Pending => "○",
        StepStateKind::Starting => "◔",
        StepStateKind::Running => {
            let elapsed = step
                .timing()
                .map_or(Duration::ZERO, |timing| timing.duration);
            let frame = elapsed.as_millis() / REDRAW_INTERVAL.as_millis();
            let index = usize::try_from(frame).unwrap_or(0) % RUNNING_INDICATOR_FRAMES.len();
            RUNNING_INDICATOR_FRAMES[index]
        }
        StepStateKind::CapturingOutputs => "◕",
        StepStateKind::Recovering => "◑",
        StepStateKind::Cancelling => "◒",
        StepStateKind::Succeeded => "✓",
        StepStateKind::Failed => "×",
        StepStateKind::Blocked => "◐",
        StepStateKind::Skipped => "↷",
        StepStateKind::NotRun => "–",
        StepStateKind::Cancelled => "⊘",
    }
}

fn step_state_label(state: StepStateKind) -> &'static str {
    match state {
        StepStateKind::Pending => "pending",
        StepStateKind::Starting => "starting",
        StepStateKind::Running => "running",
        StepStateKind::CapturingOutputs => "capturing",
        StepStateKind::Recovering => "recovering",
        StepStateKind::Cancelling => "cancelling",
        StepStateKind::Succeeded => "succeeded",
        StepStateKind::Failed => "failed",
        StepStateKind::Blocked => "blocked",
        StepStateKind::Skipped => "skipped",
        StepStateKind::NotRun => "not-run",
        StepStateKind::Cancelled => "cancelled",
    }
}

fn step_state_style(state: StepStateKind, color: bool) -> Style {
    tone_style(color, step_state_tone(state)).add_modifier(Modifier::BOLD)
}

fn step_state_tone(state: StepStateKind) -> Tone {
    match state {
        StepStateKind::Starting
        | StepStateKind::Running
        | StepStateKind::CapturingOutputs
        | StepStateKind::Recovering => Tone::Active,
        StepStateKind::Succeeded => Tone::Success,
        StepStateKind::Failed => Tone::Failure,
        StepStateKind::Cancelling | StepStateKind::Blocked | StepStateKind::Cancelled => {
            Tone::Blocked
        }
        StepStateKind::Pending | StepStateKind::Skipped | StepStateKind::NotRun => Tone::Muted,
    }
}

#[derive(Clone, Copy)]
pub(super) enum Tone {
    Primary,
    Neutral,
    Muted,
    Active,
    Success,
    Failure,
    Blocked,
}

fn tone_style(color: bool, tone: Tone) -> Style {
    if !color {
        return Style::default();
    }
    let foreground = match tone {
        Tone::Primary => Color::Rgb(205, 214, 244),
        Tone::Neutral => Color::Rgb(186, 194, 222),
        Tone::Muted => Color::Rgb(108, 112, 134),
        Tone::Active => Color::Rgb(137, 180, 250),
        Tone::Success => Color::Rgb(166, 227, 161),
        Tone::Failure => Color::Rgb(243, 139, 168),
        Tone::Blocked => Color::Rgb(250, 179, 135),
    };
    Style::default().fg(foreground)
}

fn command_accent_style(color: bool) -> Style {
    fixed_color_style(color, Color::Rgb(203, 166, 247))
}

fn footer_key_style(color: bool) -> Style {
    fixed_color_style(color, Color::Rgb(180, 190, 254))
}

fn help_key_style(color: bool) -> Style {
    fixed_color_style(color, Color::Rgb(249, 226, 175))
}

fn footer_separator_style(color: bool) -> Style {
    fixed_color_style(color, Color::Rgb(49, 50, 68))
}

fn separator_style(color: bool) -> Style {
    fixed_color_style(color, Color::Rgb(69, 71, 90))
}

fn fixed_color_style(color: bool, foreground: Color) -> Style {
    if color {
        Style::default().fg(foreground)
    } else {
        Style::default()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{BTreeMap, VecDeque};
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::execution::workflow::document::Output;
    use crate::execution::workflow::observation::{
        CommandOutputObservation, ExecutionObservation, ExecutionObserver, SourceSequence,
    };
    use crate::execution::workflow::pi::Thinking;
    use crate::execution::workflow::presentation_feed::AcceptedRecordOrder;
    use crate::execution::workflow::publication::{
        WorkflowRunCancellation, WorkflowRunResult, WorkflowRunStep, WorkflowRunStepKind,
        WorkflowRunTiming, WorkflowStepTiming,
    };
    use crate::execution::workflow::resolution::{self, ResolvedWorkflow};
    use crate::execution::workflow::run_timing::{
        ObservationClock, ObservationTime, RunTimingObservation,
    };
    use crate::execution::workflow::run_view_model::{WorkflowRunElapsed, WorkflowRunStepLog};
    use crate::execution::workflow::runtime::{
        ActionId, FailurePhase, RunOutcome, StepState, TransitionSequence,
    };
    use crate::execution::workflow::step_runtime::{CommandExecutionFailure, StepExecutionFailure};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum BoundaryAction {
        Setup,
        Draw(Rect),
        Input(TerminalInputEvent),
        InputFailure,
        Resize(Rect),
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

    struct ScriptedTerminalBoundary {
        area: Rect,
        resize_areas: VecDeque<Rect>,
        input: tokio::sync::mpsc::UnboundedReceiver<ScriptedInput>,
        actions: tokio::sync::mpsc::UnboundedSender<BoundaryAction>,
        failures: BoundaryFailures,
        draw_count: usize,
    }

    impl ScriptedTerminalBoundary {
        fn new(
            area: Rect,
            resize_areas: impl IntoIterator<Item = Rect>,
            failures: BoundaryFailures,
        ) -> (
            Self,
            tokio::sync::mpsc::UnboundedSender<ScriptedInput>,
            tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
        ) {
            let (input_sender, input) = tokio::sync::mpsc::unbounded_channel();
            let (actions, action_receiver) = tokio::sync::mpsc::unbounded_channel();
            (
                Self {
                    area,
                    resize_areas: resize_areas.into_iter().collect(),
                    input,
                    actions,
                    failures,
                    draw_count: 0,
                },
                input_sender,
                action_receiver,
            )
        }

        fn record(&self, action: BoundaryAction) {
            let _ = self.actions.send(action);
        }
    }

    impl TerminalBoundary for ScriptedTerminalBoundary {
        fn setup(&mut self) -> io::Result<Rect> {
            self.record(BoundaryAction::Setup);
            if self.failures.setup {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected setup failure",
                ));
            }
            Ok(self.area)
        }

        fn next_event(&mut self) -> impl Future<Output = io::Result<TerminalInputEvent>> + Send {
            let actions = self.actions.clone();
            async move {
                match self.input.recv().await {
                    Some(ScriptedInput::Event(event)) => {
                        let _ = actions.send(BoundaryAction::Input(event));
                        Ok(event)
                    }
                    Some(ScriptedInput::Failure) => {
                        let _ = actions.send(BoundaryAction::InputFailure);
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "injected input failure",
                        ))
                    }
                    Some(ScriptedInput::Panic) => {
                        std::panic::panic_any("injected terminal input panic")
                    }
                    None => Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "scripted terminal input closed",
                    )),
                }
            }
        }

        fn resize(&mut self) -> io::Result<Rect> {
            if let Some(area) = self.resize_areas.pop_front() {
                self.area = area;
            }
            self.record(BoundaryAction::Resize(self.area));
            Ok(self.area)
        }

        fn restore(&mut self) -> io::Result<()> {
            self.record(BoundaryAction::Restore);
            if self.failures.restore {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected restore failure",
                ));
            }
            Ok(())
        }
    }

    impl WorkflowTerminalBoundary for ScriptedTerminalBoundary {
        fn draw_workflow(
            &mut self,
            _snapshot: &WorkflowRunViewSnapshot,
            interaction: &mut HostInteraction,
            _color: bool,
        ) -> io::Result<()> {
            self.draw_count = self.draw_count.saturating_add(1);
            self.record(BoundaryAction::Draw(interaction.terminal_area));
            if self.failures.draw_at == Some(self.draw_count) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected draw failure",
                ));
            }
            Ok(())
        }
    }

    #[test]
    fn selected_terminal_events_map_to_host_controls() {
        for (event, expected) in [
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Char('j'),
                    KeyModifiers::NONE,
                )),
                TerminalInputEvent::Down,
            ),
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Char('k'),
                    KeyModifiers::NONE,
                )),
                TerminalInputEvent::Up,
            ),
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Char('c'),
                    KeyModifiers::CONTROL,
                )),
                TerminalInputEvent::Cancel,
            ),
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Char('3'),
                    KeyModifiers::NONE,
                )),
                TerminalInputEvent::ToggleLogChannel('3'),
            ),
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Char('?'),
                    KeyModifiers::SHIFT,
                )),
                TerminalInputEvent::Help,
            ),
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Enter,
                    KeyModifiers::NONE,
                )),
                TerminalInputEvent::Enter,
            ),
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Esc,
                    KeyModifiers::NONE,
                )),
                TerminalInputEvent::Escape,
            ),
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Char('q'),
                    KeyModifiers::NONE,
                )),
                TerminalInputEvent::Quit,
            ),
            (
                Event::Key(crossterm::event::KeyEvent::new(
                    KeyCode::Up,
                    KeyModifiers::NONE,
                )),
                TerminalInputEvent::Up,
            ),
            (Event::Resize(100, 30), TerminalInputEvent::Resize),
        ] {
            assert_eq!(terminal_input_event(event), expected);
        }
    }

    #[test]
    fn enter_opens_the_selected_steps_full_screen_log() {
        let snapshot = direct_snapshot(direct_command_step(
            StepStateKind::Pending,
            None,
            None,
            WorkflowRunOutputDisposition::Pending,
        ));
        let graph = DagLayout::for_steps(&snapshot.steps);
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction {
            terminal_area: Rect::new(0, 0, 120, 24),
            ..HostInteraction::default()
        };
        interaction.handle_key(
            terminal_input_event(Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            ))),
            &snapshot,
            &cancellation,
        );
        let backend = ratatui::backend::TestBackend::new(120, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render(frame, &snapshot, &graph, &mut interaction, false))
            .unwrap();
        let rendered = buffer_text(terminal.backend().buffer());

        assert!(rendered.contains("○  selected-command   cmd"));
        assert!(rendered.contains("pending"));
        assert!(rendered.contains("LOG"));
        assert!(rendered.contains("● following · 0 lines"));
        assert!(!rendered.contains("stdout + stderr"));
        assert!(!rendered.contains("workflow.yaml"));
        let inspector = buffer_position(terminal.backend().buffer(), "○  selected-command   cmd");
        let log = buffer_position(terminal.backend().buffer(), "LOG");
        assert_eq!(inspector.1, 0);
        assert!(log.1 > inspector.1);

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Esc,
            KeyModifiers::NONE,
            &cancellation,
        );
        terminal
            .draw(|frame| render(frame, &snapshot, &graph, &mut interaction, false))
            .unwrap();
        let rendered = buffer_text(terminal.backend().buffer());
        assert_eq!(interaction.selected, 0);
        assert!(rendered.contains("workflow"));
        assert!(rendered.contains("▏ ○ selected-command"));
    }

    #[test]
    fn full_screen_log_keeps_each_record_on_one_row() {
        let snapshot = direct_snapshot(long_log_step());
        let graph = DagLayout::for_steps(&snapshot.steps);
        let mut interaction = HostInteraction {
            surface: HostSurface::FullLog,
            ..HostInteraction::default()
        };
        let backend = ratatui::backend::TestBackend::new(64, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render(frame, &snapshot, &graph, &mut interaction, false))
            .unwrap();
        let rows = buffer_rows(terminal.backend().buffer());
        let record_rows = rows
            .iter()
            .filter(|row| row.contains("stdout │") || row.contains("stdout ↳"))
            .collect::<Vec<_>>();

        assert_eq!(
            record_rows.len(),
            1,
            "the full-screen log must not soft-wrap retained records: {record_rows:#?}"
        );
    }

    #[test]
    fn full_log_key_bindings_navigate_deterministic_records() {
        let snapshot = direct_snapshot(numbered_log_step(50, 140));

        for code in [KeyCode::Up, KeyCode::Char('k')] {
            let (interaction, _) =
                run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(47));
            assert!(!interaction.full_log.follow);
        }
        for code in [KeyCode::Down, KeyCode::Char('j')] {
            let (interaction, _) =
                run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(48));
            assert!(!interaction.full_log.follow);
        }
        for code in [KeyCode::PageUp, KeyCode::Char('b')] {
            let (interaction, _) =
                run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(45));
        }
        for code in [KeyCode::PageDown, KeyCode::Char('f'), KeyCode::Char(' ')] {
            let (interaction, _) = run_full_log_keys(
                &snapshot,
                80,
                20,
                &[
                    (KeyCode::Char('g'), KeyModifiers::NONE),
                    (code, KeyModifiers::NONE),
                ],
            );
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(4));
        }
        for (code, modifiers) in [
            (KeyCode::Char('u'), KeyModifiers::NONE),
            (KeyCode::Char('u'), KeyModifiers::CONTROL),
        ] {
            let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, modifiers)]);
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(47));
        }
        for (code, modifiers) in [
            (KeyCode::Char('d'), KeyModifiers::NONE),
            (KeyCode::Char('d'), KeyModifiers::CONTROL),
        ] {
            let (interaction, _) = run_full_log_keys(
                &snapshot,
                80,
                20,
                &[(KeyCode::Char('g'), KeyModifiers::NONE), (code, modifiers)],
            );
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(2));
        }
        for (code, modifiers, expected) in [
            (KeyCode::Char('g'), KeyModifiers::NONE, 1),
            (KeyCode::Char('G'), KeyModifiers::SHIFT, 48),
        ] {
            let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, modifiers)]);
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(expected));
        }
        for code in [KeyCode::Right, KeyCode::Char('l')] {
            let (interaction, _) =
                run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
            assert_eq!(interaction.full_log.horizontal_offset, 1);
        }
        for code in [KeyCode::Left, KeyCode::Char('h')] {
            let (interaction, _) = run_full_log_keys(
                &snapshot,
                80,
                20,
                &[
                    (KeyCode::Right, KeyModifiers::NONE),
                    (code, KeyModifiers::NONE),
                ],
            );
            assert_eq!(interaction.full_log.horizontal_offset, 0);
        }

        let (interaction, _) = run_full_log_keys(
            &snapshot,
            80,
            20,
            &[
                (KeyCode::Up, KeyModifiers::NONE),
                (KeyCode::Char('F'), KeyModifiers::SHIFT),
            ],
        );
        assert!(interaction.full_log.follow);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(48));
    }

    #[test]
    fn paused_log_keeps_its_anchor_and_pan_as_output_arrives() {
        let mut snapshot = direct_snapshot(numbered_log_step(30, 120));
        let (mut interaction, cancellation) = run_full_log_keys(
            &snapshot,
            80,
            20,
            &[
                (KeyCode::Up, KeyModifiers::NONE),
                (KeyCode::Right, KeyModifiers::NONE),
                (KeyCode::Right, KeyModifiers::NONE),
                (KeyCode::Right, KeyModifiers::NONE),
            ],
        );
        let anchor = full_log_top_order(&interaction, &snapshot);
        let horizontal_offset = interaction.full_log.horizontal_offset;
        assert_eq!(anchor, Some(27));
        let log = FilteredLog::new(&snapshot.steps[0].log, interaction.log_filters);
        assert_eq!(interaction.full_log.lines_behind(&log), 1);

        append_log_record(&mut snapshot.steps[0].log, 31, "new output one");
        append_log_record(&mut snapshot.steps[0].log, 32, "new output two");
        let (width, rows) =
            full_log_record_dimensions(interaction.terminal_area, &snapshot.steps[0]);
        let log = FilteredLog::new(&snapshot.steps[0].log, interaction.log_filters);
        interaction.full_log.synchronize(&log, width, rows);

        assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
        assert_eq!(interaction.full_log.horizontal_offset, horizontal_offset);
        assert_eq!(interaction.full_log.lines_behind(&log), 3);
        let rendered = buffer_text(&render_full_log_snapshot(
            &snapshot,
            &mut interaction,
            120,
            20,
        ));
        assert!(rendered.contains("paused · 3 lines behind"));

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('F'),
            KeyModifiers::SHIFT,
            &cancellation,
        );
        assert!(interaction.full_log.follow);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(30));
        let log = FilteredLog::new(&snapshot.steps[0].log, interaction.log_filters);
        assert_eq!(interaction.full_log.lines_behind(&log), 0);
        assert_eq!(interaction.full_log.horizontal_offset, horizontal_offset);
    }

    #[test]
    fn paused_log_anchor_survives_terminal_resize() {
        let snapshot = direct_snapshot(numbered_log_step(40, 80));
        let (mut interaction, _) =
            run_full_log_keys(&snapshot, 80, 20, &[(KeyCode::Up, KeyModifiers::NONE)]);
        let anchor = full_log_top_order(&interaction, &snapshot);
        assert_eq!(anchor, Some(37));

        let _ = render_full_log_snapshot(&snapshot, &mut interaction, 100, 24);
        assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
        assert_eq!(interaction.full_log.available_rows, 7);

        let _ = render_full_log_snapshot(&snapshot, &mut interaction, 64, 20);
        assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
        assert_eq!(interaction.full_log.available_rows, 3);
    }

    #[test]
    fn clamped_log_keeps_retained_and_total_counts_visible_at_minimum_width() {
        let (snapshot, mut interaction) = clamped_log_snapshot(MINIMUM_WIDTH, MINIMUM_HEIGHT);

        let rendered = buffer_text(&render_full_log_snapshot(
            &snapshot,
            &mut interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
        ));
        assert!(
            rendered.contains("30/38 kept"),
            "counts disappeared after clamping: {rendered:?}"
        );
    }

    #[test]
    fn eviction_clamps_a_paused_anchor_and_marks_the_clamp() {
        let (snapshot, mut interaction) = clamped_log_snapshot(140, 20);

        let buffer = render_full_log_snapshot(&snapshot, &mut interaction, 140, 20);
        let rendered = buffer_text(&buffer);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(9));
        assert!(interaction.full_log.anchor_clamped);
        assert!(
            rendered.contains("↑ 8 older lines / 400 bytes discarded | clamped to retained top")
        );
        assert!(rendered.contains("30 retained / 38 total"));
        assert!(rendered.contains("12:34:56.000 stderr │ record 09"));
    }

    #[test]
    fn full_log_horizontal_pan_reaches_the_end_of_wide_graphemes() {
        let payload = format!("START{}END", "界".repeat(80));
        let snapshot = direct_snapshot(direct_log_step(
            StepStateKind::Running,
            vec![direct_log_record(
                1,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56Z",
                &payload,
                false,
            )],
            1,
            0,
        ));
        let (mut interaction, cancellation) = entered_full_log(&snapshot, 64, 20);
        for _ in 0..300 {
            press_key(
                &mut interaction,
                &snapshot,
                KeyCode::Right,
                KeyModifiers::NONE,
                &cancellation,
            );
        }

        let rendered = buffer_text(&render_full_log_snapshot(
            &snapshot,
            &mut interaction,
            64,
            20,
        ));
        assert!(
            rendered.contains("END"),
            "far-right payload is not reachable: {rendered:?}"
        );
    }

    #[test]
    fn horizontal_pan_is_bounded_and_clamps_only_when_content_requires_it() {
        let long_payload = format!("START-{}-END", "x".repeat(120));
        let mut snapshot = direct_snapshot(direct_log_step(
            StepStateKind::Running,
            vec![direct_log_record(
                1,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56Z",
                &long_payload,
                false,
            )],
            1,
            0,
        ));
        let (mut interaction, cancellation) =
            run_full_log_keys(&snapshot, 64, 20, &[(KeyCode::Up, KeyModifiers::NONE)]);
        for _ in 0..200 {
            press_key(
                &mut interaction,
                &snapshot,
                KeyCode::Right,
                KeyModifiers::NONE,
                &cancellation,
            );
        }
        let (available_width, _) =
            full_log_record_dimensions(interaction.terminal_area, &snapshot.steps[0]);
        let log = FilteredLog::new(&snapshot.steps[0].log, interaction.log_filters);
        let maximum = maximum_horizontal_offset(&log.records, available_width);
        assert_eq!(interaction.full_log.horizontal_offset, maximum);
        let rendered = buffer_text(&render_full_log_snapshot(
            &snapshot,
            &mut interaction,
            64,
            20,
        ));
        assert!(rendered.contains("-END"));
        assert!(!rendered.contains("START-"));

        append_log_record(&mut snapshot.steps[0].log, 2, "short new output");
        let _ = render_full_log_snapshot(&snapshot, &mut interaction, 64, 20);
        assert_eq!(interaction.full_log.horizontal_offset, maximum);

        snapshot.steps[0].log.records.remove(0);
        snapshot.steps[0].log.retained_records = 1;
        snapshot.steps[0].log.discarded_records = 1;
        let _ = render_full_log_snapshot(&snapshot, &mut interaction, 64, 20);
        assert_eq!(interaction.full_log.horizontal_offset, 0);
    }

    #[test]
    fn log_preview_preserves_merged_order_and_accents_stderr_without_tinting_content() {
        let step = direct_log_step(
            StepStateKind::Running,
            vec![
                direct_log_record(
                    1,
                    CommandOutputSource::StandardOutput,
                    "2026-08-04T12:34:56.100Z",
                    "first from stdout",
                    false,
                ),
                direct_log_record(
                    2,
                    CommandOutputSource::StandardError,
                    "2026-08-04T12:34:56.200Z",
                    "then from stderr",
                    false,
                ),
                direct_log_record(
                    3,
                    CommandOutputSource::StandardOutput,
                    "2026-08-04T12:34:56.300Z",
                    "last from stdout",
                    false,
                ),
            ],
            3,
            0,
        );
        let buffer = render_direct_log(&step, 80, 7, true);
        let rows = buffer_rows(&buffer);
        let first = row_containing(&rows, "first from stdout");
        let second = row_containing(&rows, "then from stderr");
        let third = row_containing(&rows, "last from stdout");

        assert!(first < second && second < third);
        assert!(rows[first].contains("stdout │ first from stdout"));
        assert!(rows[second].contains("stderr │ then from stderr"));
        assert!(rows[third].contains("stdout │ last from stdout"));

        let payload_column = column_of(&rows[second], "then from stderr");
        let second = u16::try_from(second).unwrap();
        assert_eq!(
            buffer[(payload_column, second)].fg,
            tone_style(true, Tone::Neutral).fg.unwrap()
        );
        let source_column = column_of(&rows[usize::from(second)], "stderr");
        assert_eq!(
            buffer[(source_column, second)].fg,
            tone_style(true, Tone::Blocked).fg.unwrap()
        );
        assert_ne!(
            buffer[(source_column, second)].fg,
            tone_style(true, Tone::Failure).fg.unwrap()
        );
    }

    #[test]
    fn numbered_channels_filter_command_logs_and_report_hidden_records() {
        let step = direct_log_step(
            StepStateKind::Running,
            vec![
                direct_log_record(
                    1,
                    CommandOutputSource::StandardOutput,
                    "2026-08-04T12:34:56Z",
                    "visible stdout payload",
                    false,
                ),
                direct_log_record(
                    2,
                    CommandOutputSource::StandardError,
                    "2026-08-04T12:34:57Z",
                    "hidden stderr payload",
                    false,
                ),
            ],
            2,
            0,
        );
        let snapshot = direct_snapshot(step);
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction {
            terminal_area: Rect::new(0, 0, 120, 24),
            ..HostInteraction::default()
        };

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('2'),
            KeyModifiers::NONE,
            &cancellation,
        );
        let filtered = render_snapshot(&snapshot, &mut interaction, 120, 24, true);
        let rendered = buffer_text(&filtered);
        assert!(rendered.contains("visible stdout payload"));
        assert!(!rendered.contains("hidden stderr payload"));
        assert!(rendered.contains("● following · 1 hidden"));
        let (stdout_x, stdout_y) = buffer_position(&filtered, "1 stdout");
        assert!(
            filtered[(stdout_x, stdout_y)]
                .modifier
                .contains(Modifier::UNDERLINED)
        );
        let (stderr_x, stderr_y) = buffer_position(&filtered, "2 stderr");
        assert!(
            filtered[(stderr_x, stderr_y)]
                .modifier
                .contains(Modifier::DIM)
        );

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('1'),
            KeyModifiers::NONE,
            &cancellation,
        );
        let all_hidden = buffer_text(&render_snapshot(&snapshot, &mut interaction, 120, 24, true));
        assert!(all_hidden.contains("● following · 2 hidden"));
        assert!(all_hidden.contains("All log channels hidden."));
    }

    #[test]
    fn full_log_navigation_skips_filtered_records() {
        let snapshot = direct_snapshot(numbered_log_step(30, 20));
        let (mut interaction, _) = run_full_log_keys(
            &snapshot,
            120,
            24,
            &[
                (KeyCode::Char('2'), KeyModifiers::NONE),
                (KeyCode::Char('g'), KeyModifiers::NONE),
                (KeyCode::Down, KeyModifiers::NONE),
            ],
        );

        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(4));
        let rendered = buffer_text(&render_full_log_snapshot(
            &snapshot,
            &mut interaction,
            120,
            24,
        ));
        assert!(rendered.contains("15 hidden"));
    }

    #[test]
    fn agent_channels_group_observations_and_dim_secondary_rows() {
        let step = direct_agent_log_step(vec![
            direct_agent_log_record(
                1,
                AgentPresentationObservationKind::Assistant,
                "agent message",
            ),
            direct_agent_log_record(
                2,
                AgentPresentationObservationKind::Reasoning,
                "reasoning message",
            ),
            direct_agent_log_record(3, AgentPresentationObservationKind::ToolCall, "tool call"),
            direct_agent_log_record(
                4,
                AgentPresentationObservationKind::ToolResult,
                "tool result",
            ),
            direct_agent_log_record(
                5,
                AgentPresentationObservationKind::Diagnostic,
                "diagnostic message",
            ),
            direct_agent_log_record(6, AgentPresentationObservationKind::Usage, "usage message"),
        ]);
        let snapshot = direct_snapshot(step.clone());
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction {
            terminal_area: Rect::new(0, 0, 180, 24),
            ..HostInteraction::default()
        };
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('3'),
            KeyModifiers::NONE,
            &cancellation,
        );
        let rendered = render_snapshot(&snapshot, &mut interaction, 180, 24, true);
        let text = buffer_text(&rendered);
        for channel in ["1 agent", "2 reasoning", "3 tools", "4 system"] {
            assert!(text.contains(channel), "missing channel {channel:?}");
        }
        assert!(!text.contains("tool call"));
        assert!(!text.contains("tool result"));
        assert!(text.contains("2 hidden"));
        let (tools_x, tools_y) = buffer_position(&rendered, "3 tools");
        assert!(
            rendered[(tools_x, tools_y)]
                .modifier
                .contains(Modifier::DIM)
        );

        let mut filters = LogFilterState::default();
        assert!(filters.toggle(&step, '3'));
        let without_tools = FilteredLog::new(&step.log, filters);
        assert_eq!(without_tools.hidden_records, 2);
        assert!(
            without_tools
                .records
                .iter()
                .all(|record| !matches!(LogChannel::for_source(record.source), LogChannel::Tools))
        );

        assert!(filters.toggle(&step, '4'));
        let without_tools_or_system = FilteredLog::new(&step.log, filters);
        assert_eq!(without_tools_or_system.hidden_records, 4);
        assert_eq!(
            without_tools_or_system
                .records
                .iter()
                .map(|record| record.payload.as_ref())
                .collect::<Vec<_>>(),
            ["agent message", "reasoning message"]
        );
        assert!(filters.includes(LogChannel::StandardOutput));
        assert!(filters.includes(LogChannel::StandardError));

        assert!(
            log_payload_style(
                WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Reasoning),
                false,
            )
            .add_modifier
            .contains(Modifier::DIM)
        );
        assert!(
            log_payload_style(
                WorkflowRunLogSource::Agent(AgentPresentationObservationKind::ToolResult),
                false,
            )
            .add_modifier
            .contains(Modifier::DIM)
        );
        assert_eq!(
            log_source_style(
                WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Diagnostic),
                true,
            )
            .fg,
            tone_style(true, Tone::Blocked).fg
        );
    }

    #[test]
    fn log_timestamps_are_utc_with_milliseconds_and_elide_before_sources() {
        let step = direct_log_step(
            StepStateKind::Running,
            vec![direct_log_record(
                1,
                CommandOutputSource::StandardError,
                "2026-08-04T12:34:56.789123+02:00",
                "message",
                false,
            )],
            1,
            0,
        );

        let wide = buffer_rows(&render_direct_log(&step, 50, 5, false));
        assert!(
            wide.iter()
                .any(|row| row.contains("10:34:56.789 stderr │ message"))
        );

        let timestamp_boundary = buffer_rows(&render_direct_log(&step, 40, 5, false));
        assert!(
            timestamp_boundary
                .iter()
                .any(|row| row.contains("10:34:56.789 stderr │ message"))
        );

        let narrow = buffer_rows(&render_direct_log(&step, 39, 5, false));
        assert!(narrow.iter().any(|row| row.contains("stderr │ message")));
        assert!(narrow.iter().all(|row| !row.contains("10:34:56.789")));
    }

    #[test]
    fn log_preview_preserves_safety_continuation_record_metadata() {
        let step = direct_log_step(
            StepStateKind::Running,
            vec![
                direct_log_record(
                    1,
                    CommandOutputSource::StandardError,
                    "2026-08-04T12:34:56.100Z",
                    "first fragment",
                    false,
                ),
                direct_log_record(
                    2,
                    CommandOutputSource::StandardError,
                    "2026-08-04T12:34:56.200Z",
                    "continued fragment",
                    true,
                ),
            ],
            2,
            0,
        );

        let rows = buffer_rows(&render_direct_log(&step, 80, 5, false));
        assert!(
            rows.iter()
                .any(|row| row.contains("12:34:56.200 stderr ↪ continued fragment")),
            "a safety-continuation record must retain its own timestamp and remain distinct from a visual wrap: {rows:#?}"
        );
    }

    #[test]
    fn log_preview_rewraps_deterministically_and_keeps_the_visual_tail() {
        let step = long_log_step();

        let complete = inner_buffer_rows(&render_direct_log(&step, 34, 8, false));
        assert_eq!(
            complete[2..],
            [
                "  stdout │ abcdefghijklmnopqrs",
                "         ↳ tuvwxyzABCDEFGHIJKL",
                "         ↳ MNOPQRSTUVWXYZ01234",
                "         ↳ 56789",
            ]
        );

        let wide = inner_buffer_rows(&render_direct_log(&step, 34, 7, false));
        assert!(wide[0].trim_start().starts_with("LOG"));
        assert_eq!(
            wide[2..],
            [
                "  stdout ↳ tuvwxyzABCDEFGHIJKL",
                "         ↳ MNOPQRSTUVWXYZ01234",
                "         ↳ 56789",
            ]
        );

        let narrow = inner_buffer_rows(&render_direct_log(&step, 27, 7, false));
        assert!(narrow[0].contains("● 1 line"));
        assert_eq!(
            narrow[2..],
            [
                "  stdout ↳ KLMNOPQRSTUV",
                "         ↳ WXYZ01234567",
                "         ↳ 89",
            ]
        );
        assert_eq!(
            inner_buffer_rows(&render_direct_log(&step, 27, 7, false)),
            narrow
        );
    }

    #[test]
    fn large_history_preview_keeps_only_the_visible_visual_tail() {
        use crate::execution::workflow::presentation_feed::MAX_NORMALIZED_CHILD_RECORD_BYTES;

        let records = (1..=256)
            .map(|order| {
                let payload = if order == 256 {
                    format!(
                        "{}NEWEST",
                        "z".repeat(MAX_NORMALIZED_CHILD_RECORD_BYTES - "NEWEST".len())
                    )
                } else {
                    "x".repeat(MAX_NORMALIZED_CHILD_RECORD_BYTES)
                };
                direct_log_record(
                    order,
                    CommandOutputSource::StandardOutput,
                    "2026-08-04T12:34:56Z",
                    &payload,
                    false,
                )
            })
            .collect();
        let step = direct_log_step(StepStateKind::Running, records, 256, 0);

        let rows = inner_buffer_rows(&render_direct_log(&step, 34, 7, false));
        assert_eq!(rows.len(), 5);
        assert!(rows[4].ends_with("NEWEST"), "unexpected tail: {rows:#?}");
        assert!(rows[2].contains("stdout"));
        assert!(rows[3..].iter().all(|row| !row.contains("stdout")));
    }

    #[test]
    fn log_preview_reports_counts_following_and_evicted_history() {
        let mut step = direct_log_step(
            StepStateKind::Succeeded,
            vec![
                direct_log_record(
                    3,
                    CommandOutputSource::StandardOutput,
                    "2026-08-04T12:34:56Z",
                    "retained one",
                    false,
                ),
                direct_log_record(
                    4,
                    CommandOutputSource::StandardError,
                    "2026-08-04T12:34:57Z",
                    "retained two",
                    false,
                ),
                direct_log_record(
                    5,
                    CommandOutputSource::StandardOutput,
                    "2026-08-04T12:34:58Z",
                    "retained three",
                    false,
                ),
            ],
            5,
            2,
        );
        step.log.discarded_bytes = 37;
        let buffer = render_direct_log(&step, 100, 8, false);
        let rendered = buffer_text(&buffer);
        let rows = inner_buffer_rows(&buffer);

        assert!(rendered.contains("● following · 3 retained / 5 total"));
        assert!(rows[0].trim_start().starts_with("LOG"));
        assert!(rows[0].contains("● following · 3 retained / 5 total"));
        assert!(rows[1].trim().is_empty());
        assert_eq!(rows[2].trim(), "↑ 2 older lines / 37 bytes discarded");
        assert!(rows[3].ends_with("retained one"));
        assert!(rows[4].ends_with("retained two"));
        assert!(rows[5].ends_with("retained three"));

        let minimum_height = inner_buffer_rows(&render_direct_log(&step, 100, 6, false));
        assert!(minimum_height[0].trim_start().starts_with("LOG"));
        assert!(minimum_height[1].trim().is_empty());
        assert_eq!(
            minimum_height[2].trim(),
            "↑ 2 older lines / 37 bytes discarded"
        );
        assert!(minimum_height[3].ends_with("retained three"));
        assert!(
            !minimum_height
                .iter()
                .any(|row| row.ends_with("retained two"))
        );
    }

    #[test]
    fn empty_log_preview_distinguishes_waiting_and_no_output() {
        for (state, expected) in [
            (StepStateKind::Pending, "Waiting for this step to start."),
            (StepStateKind::Running, "Waiting for output…"),
            (StepStateKind::Succeeded, "No output received."),
        ] {
            let step = direct_log_step(state, Vec::new(), 0, 0);
            let rows = inner_buffer_rows(&render_direct_log(&step, 70, 5, false));
            assert!(rows[0].trim_start().starts_with("LOG"));
            assert!(rows[1].trim().is_empty());
            assert_eq!(rows[2].trim_start(), expected);
        }
    }

    #[test]
    fn navigation_is_bounded_and_ctrl_c_uses_user_request() {
        let cancellation = CancellationSource::new();
        let mut snapshot = direct_snapshot(long_log_step());
        snapshot.steps.push(snapshot.steps[0].clone());
        let mut interaction = HostInteraction {
            terminal_area: Rect::new(0, 0, MINIMUM_WIDTH, MINIMUM_HEIGHT),
            ..HostInteraction::default()
        };
        assert_eq!(
            interaction.handle_key(TerminalInputEvent::Down, &snapshot, &cancellation),
            HostControl::Continue
        );
        assert_eq!(interaction.selected, 1);
        interaction.handle_key(TerminalInputEvent::Down, &snapshot, &cancellation);
        assert_eq!(interaction.selected, 1);
        interaction.handle_key(TerminalInputEvent::Up, &snapshot, &cancellation);
        interaction.handle_key(TerminalInputEvent::Up, &snapshot, &cancellation);
        assert_eq!(interaction.selected, 0);

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
            &cancellation,
        );
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::UserRequest)
        );
    }

    #[test]
    fn finalization_ctrl_c_escalates_from_graceful_to_force_abort() {
        let cancellation = CancellationSource::new();
        assert!(cancellation.begin_finalization_arm());
        assert!(cancellation.complete_finalization_arm());
        let mut operations = cancellation.subscribe_operations();
        let mut snapshot = direct_snapshot(long_log_step());
        let trigger = crate::execution::workflow::document::FinalizationTrigger::Succeeded;
        snapshot.workflow = WorkflowState::Finalizing {
            trigger,
            gate: crate::execution::workflow::runtime::FinalizationGate::Open,
            primary_issue: None,
        };
        let mut interaction = HostInteraction::default();

        interaction.handle_key(TerminalInputEvent::Cancel, &snapshot, &cancellation);
        assert!(matches!(
            operations.next_operation(),
            Some(CancellationOperation::Graceful {
                reason: CancellationReason::UserRequest,
                ..
            })
        ));

        snapshot.workflow = WorkflowState::Finalizing {
            trigger,
            gate: crate::execution::workflow::runtime::FinalizationGate::Cancelling {
                reason: CancellationReason::UserRequest,
                deadline: Some(time::OffsetDateTime::UNIX_EPOCH),
                force_abort: false,
            },
            primary_issue: None,
        };
        interaction.handle_key(TerminalInputEvent::Cancel, &snapshot, &cancellation);
        assert!(matches!(
            operations.next_operation(),
            Some(CancellationOperation::ForceAbort { .. })
        ));
    }

    #[test]
    fn quit_requires_adapter_completion_on_every_surface() {
        let cancellation = CancellationSource::new();
        let mut snapshot = direct_snapshot(long_log_step());
        snapshot.workflow = WorkflowState::Succeeded;
        snapshot.authoritative_result = true;
        snapshot.quiescent = true;
        snapshot.publication =
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                result_directory: "results".to_owned(),
            });
        snapshot.cleanup = WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Succeeded);

        let operational = Rect::new(0, 0, MINIMUM_WIDTH, MINIMUM_HEIGHT);
        let mut interactions = [
            HostInteraction {
                terminal_area: operational,
                ..HostInteraction::default()
            },
            HostInteraction {
                surface: HostSurface::FullLog,
                terminal_area: operational,
                ..HostInteraction::default()
            },
            HostInteraction {
                help_visible: true,
                terminal_area: operational,
                ..HostInteraction::default()
            },
            HostInteraction {
                surface: HostSurface::FullLog,
                help_visible: true,
                terminal_area: operational,
                ..HostInteraction::default()
            },
            HostInteraction {
                terminal_area: Rect::new(0, 0, 40, 8),
                ..HostInteraction::default()
            },
        ];

        for interaction in &mut interactions {
            assert_quit_control(interaction, &snapshot, &cancellation, HostControl::Continue);
        }

        press_key(
            &mut interactions[0],
            &snapshot,
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
            &cancellation,
        );
        assert_eq!(cancellation.cancellation_reason(), None);

        snapshot.quit_eligible = true;
        for interaction in &mut interactions {
            assert_quit_control(interaction, &snapshot, &cancellation, HostControl::Quit);
        }
    }

    #[test]
    fn restoration_attempts_every_operation_after_cursor_failure() {
        let actions = RefCell::new(Vec::new());
        let mut output = io::sink();

        let failure = attempt_terminal_restoration(
            true,
            &mut output,
            |_| {
                actions.borrow_mut().push("leave alternate screen");
                Ok(())
            },
            |_| {
                actions.borrow_mut().push("show cursor");
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected cursor restoration failure",
                ))
            },
            |_| {
                actions.borrow_mut().push("flush output");
                Ok(())
            },
            || {
                actions.borrow_mut().push("restore input mode");
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(failure.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(
            actions.into_inner(),
            [
                "leave alternate screen",
                "show cursor",
                "flush output",
                "restore input mode"
            ]
        );
    }

    #[test]
    fn setup_and_initial_render_failures_attempt_restoration_before_execution() {
        for (failures, expected_operation, expected_actions) in [
            (
                BoundaryFailures {
                    setup: true,
                    ..BoundaryFailures::default()
                },
                PresentationFailureOperation::TerminalSetup,
                vec![BoundaryAction::Setup, BoundaryAction::Restore],
            ),
            (
                BoundaryFailures {
                    draw_at: Some(1),
                    ..BoundaryFailures::default()
                },
                PresentationFailureOperation::TerminalDraw,
                vec![
                    BoundaryAction::Setup,
                    BoundaryAction::Draw(Rect::new(0, 0, 80, 24)),
                    BoundaryAction::Restore,
                ],
            ),
        ] {
            let (_temporary, _workflow, view, _) = scripted_host_view();
            let cancellation = CancellationSource::new();
            let (boundary, _input, mut actions) =
                ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], failures);

            let failure = WorkflowTerminalHost::start_with_boundary(
                view,
                cancellation.clone(),
                false,
                boundary,
            )
            .err()
            .expect("injected setup must fail");

            assert_eq!(failure.operation, expected_operation);
            assert_eq!(cancellation.cancellation_reason(), None);
            assert_eq!(
                std::iter::from_fn(|| actions.try_recv().ok()).collect::<Vec<_>>(),
                expected_actions
            );
        }
    }

    #[tokio::test]
    async fn terminal_input_is_inert_until_execution_is_activated() {
        let (_temporary, _workflow, view, _) = scripted_host_view();
        let cancellation = CancellationSource::new();
        let (boundary, input, mut actions) =
            ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], BoundaryFailures::default());
        let host =
            WorkflowTerminalHost::start_with_boundary(view, cancellation.clone(), false, boundary)
                .unwrap();
        wait_for_action(&mut actions, BoundaryAction::Setup).await;
        wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 80, 24))).await;

        input.send(ScriptedInput::Failure).unwrap();
        assert_eq!(cancellation.cancellation_reason(), None);
        assert_eq!(host.stop().await.unwrap(), TerminalHostExit::Stopped);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
        assert_eq!(cancellation.cancellation_reason(), None);
        assert!(actions.try_recv().is_err());
    }

    #[tokio::test]
    async fn scripted_input_keeps_q_inert_during_execution_and_restores_after_cancellation() {
        let (_temporary, workflow, view, now) = scripted_host_view();
        let cancellation = CancellationSource::new();
        let (boundary, input, mut actions) = ScriptedTerminalBoundary::new(
            Rect::new(0, 0, 80, 24),
            [Rect::new(0, 0, 40, 8), Rect::new(0, 0, 100, 30)],
            BoundaryFailures::default(),
        );
        let host = start_active_scripted_host(view.clone(), cancellation.clone(), boundary);
        wait_for_action(&mut actions, BoundaryAction::Setup).await;
        wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 80, 24))).await;

        input
            .send(ScriptedInput::Event(TerminalInputEvent::Resize))
            .unwrap();
        wait_for_action(&mut actions, BoundaryAction::Resize(Rect::new(0, 0, 40, 8))).await;
        wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 40, 8))).await;

        input
            .send(ScriptedInput::Event(TerminalInputEvent::Quit))
            .unwrap();
        wait_for_action(
            &mut actions,
            BoundaryAction::Input(TerminalInputEvent::Quit),
        )
        .await;
        wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 40, 8))).await;
        assert_eq!(cancellation.cancellation_reason(), None);

        input
            .send(ScriptedInput::Event(TerminalInputEvent::Cancel))
            .unwrap();
        assert_eq!(
            cancellation.wait_for_cancellation().await,
            CancellationReason::UserRequest
        );
        wait_for_action(
            &mut actions,
            BoundaryAction::Input(TerminalInputEvent::Cancel),
        )
        .await;

        input
            .send(ScriptedInput::Event(TerminalInputEvent::Resize))
            .unwrap();
        wait_for_action(
            &mut actions,
            BoundaryAction::Resize(Rect::new(0, 0, 100, 30)),
        )
        .await;
        complete_scripted_view(&view, &workflow, now, Some(CancellationReason::UserRequest));
        input
            .send(ScriptedInput::Event(TerminalInputEvent::Quit))
            .unwrap();

        assert_eq!(host.wait().await.unwrap(), TerminalHostExit::Quit);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::UserRequest)
        );
    }

    #[tokio::test]
    async fn normal_stop_and_injected_runtime_failures_restore_the_terminal() {
        let (_temporary, _workflow, view, _) = scripted_host_view();
        let cancellation = CancellationSource::new();
        let (boundary, _input, mut actions) =
            ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], BoundaryFailures::default());
        let host =
            WorkflowTerminalHost::start_with_boundary(view, cancellation.clone(), false, boundary)
                .unwrap();
        assert_eq!(host.stop().await.unwrap(), TerminalHostExit::Stopped);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
        assert_eq!(cancellation.cancellation_reason(), None);

        assert_scripted_runtime_failure(
            BoundaryFailures {
                draw_at: Some(2),
                ..BoundaryFailures::default()
            },
            ScriptedInput::Event(TerminalInputEvent::Other),
            PresentationFailureOperation::TerminalDraw,
        )
        .await;
        assert_scripted_runtime_failure(
            BoundaryFailures::default(),
            ScriptedInput::Failure,
            PresentationFailureOperation::TerminalInput,
        )
        .await;
    }

    #[tokio::test]
    async fn terminal_task_unwind_requests_cancellation_before_application_join() {
        let (_temporary, _workflow, view, _) = scripted_host_view();
        let cancellation = CancellationSource::new();
        let (boundary, input, mut actions) =
            ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], BoundaryFailures::default());
        let host = start_active_scripted_host(view, cancellation.clone(), boundary);
        input.send(ScriptedInput::Panic).unwrap();

        while actions.recv().await.is_some() {}

        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::CallerOutputFailure),
            "an active workflow must be cancelled as soon as its terminal task unwinds"
        );
        let failure = host.wait().await.unwrap_err();
        assert_eq!(
            failure.operation,
            PresentationFailureOperation::TerminalTask
        );
    }

    #[tokio::test]
    async fn teardown_failure_and_task_unwind_attempt_restoration_with_closed_precedence() {
        let (_temporary, workflow, view, now) = scripted_host_view();
        let cancellation = CancellationSource::new();
        let (boundary, input, mut actions) = ScriptedTerminalBoundary::new(
            Rect::new(0, 0, 80, 24),
            [],
            BoundaryFailures {
                restore: true,
                ..BoundaryFailures::default()
            },
        );
        let host = start_active_scripted_host(view.clone(), cancellation.clone(), boundary);
        complete_scripted_view(&view, &workflow, now, None);
        input
            .send(ScriptedInput::Event(TerminalInputEvent::Quit))
            .unwrap();
        let failure = host.wait().await.unwrap_err();
        assert_eq!(
            failure.operation,
            PresentationFailureOperation::TerminalRestore
        );
        assert_eq!(cancellation.cancellation_reason(), None);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;

        assert_scripted_runtime_failure(
            BoundaryFailures::default(),
            ScriptedInput::Panic,
            PresentationFailureOperation::TerminalTask,
        )
        .await;
    }

    #[test]
    fn inspector_renders_authoritative_command_states_and_dispositions() {
        let timing = WorkflowRunElapsed {
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            duration: Duration::from_millis(1_250),
            frozen: true,
        };
        let cases = [
            (
                direct_command_step(
                    StepStateKind::Pending,
                    None,
                    None,
                    WorkflowRunOutputDisposition::Pending,
                ),
                ["pending", "file", "—"].as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Running,
                    None,
                    Some(WorkflowRunElapsed {
                        frozen: false,
                        ..timing.clone()
                    }),
                    WorkflowRunOutputDisposition::Pending,
                ),
                ["running", "1.2s", "1970-01-01 00:00:00Z"].as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Succeeded,
                    None,
                    Some(timing.clone()),
                    WorkflowRunOutputDisposition::Committed,
                ),
                ["succeeded", "captured"].as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Failed,
                    Some(ObservedStepTransition::Failed {
                        detail: super::super::evidence::failure_detail(
                            FailurePhase::Execution,
                            &StepFailureCause::Execution(StepExecutionFailure::Command(
                                CommandExecutionFailure::UnsuccessfulExit { code: Some(17) },
                            )),
                        )
                        .unwrap(),
                    }),
                    Some(timing.clone()),
                    WorkflowRunOutputDisposition::Unavailable(
                        WorkflowRunOutputUnavailableReason::Failed,
                    ),
                ),
                [
                    "failed",
                    "failure       execution · command_exit · exit 17",
                    "unavailable (failed)",
                ]
                .as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Blocked,
                    Some(ObservedStepTransition::Blocked {
                        detail: super::super::evidence::BlockedDetail::new([
                            super::super::evidence::Prerequisite::control("prepare").unwrap(),
                        ])
                        .unwrap(),
                    }),
                    Some(timing.clone()),
                    WorkflowRunOutputDisposition::Unavailable(
                        WorkflowRunOutputUnavailableReason::Blocked,
                    ),
                ),
                [
                    "blocked",
                    "prerequisites_unsatisfied · control prepare",
                    "unavailable (blocked)",
                ]
                .as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::NotRun,
                    Some(ObservedStepTransition::NotRun {
                        detail: super::super::evidence::NonExecutionDetail::for_role(
                            super::super::validated::WorkflowNodeRole::Step,
                            super::super::evidence::NonExecutionCode::FailureStop,
                        )
                        .unwrap(),
                    }),
                    Some(timing.clone()),
                    WorkflowRunOutputDisposition::Unavailable(
                        WorkflowRunOutputUnavailableReason::NotRun,
                    ),
                ),
                [
                    "not-run",
                    "not run       failure_stop",
                    "unavailable (not-run)",
                ]
                .as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Cancelled,
                    Some(ObservedStepTransition::Cancelled {
                        detail: super::super::evidence::CancellationDetail::new(
                            CancellationReason::UserRequest,
                        ),
                    }),
                    Some(timing),
                    WorkflowRunOutputDisposition::Unavailable(
                        WorkflowRunOutputUnavailableReason::Cancelled,
                    ),
                ),
                [
                    "cancelled",
                    "cancellation  user_request",
                    "unavailable (cancelled)",
                ]
                .as_slice(),
            ),
        ];

        for (step, expected) in cases {
            let rendered = render_direct_inspector(&step, 120, 14);
            assert!(rendered.contains("selected-command   cmd"));
            assert!(rendered.contains("command       build 'héllo world'"));
            assert!(rendered.contains("cwd           work"));
            assert!(rendered.contains("depends on    prepare"));
            assert!(rendered.contains("OUTPUTS"));
            assert!(rendered.contains("report"));
            assert!(!rendered.contains("ID:"));
            assert!(!rendered.contains("Kind:"));
            assert!(!rendered.contains("State:"));
            assert!(!rendered.contains("Duration:"));
            for value in expected {
                assert!(
                    rendered.contains(value),
                    "missing {value:?} in {rendered:?}"
                );
            }
            if matches!(
                step.state,
                StepStateKind::Pending | StepStateKind::Blocked | StepStateKind::NotRun
            ) {
                assert!(!rendered.contains("started"));
            }
        }
    }

    #[test]
    fn ellipsize_preserves_grapheme_clusters() {
        assert_eq!(ellipsize("e\u{301}clair", 2), "e\u{301}…");
        assert_eq!(ellipsize("a👩‍🚀bc", 4), "a👩‍🚀…");
    }

    #[test]
    fn repeated_values_keep_fitting_values_and_report_omissions() {
        let fitting = vec!["a".to_owned(), "b".to_owned()];
        assert_eq!(summarize_repeated_values(&fitting, 9), "a, b");

        let overflowing = (1..=100)
            .map(|index| format!("d{index}"))
            .collect::<Vec<_>>();
        assert_eq!(summarize_repeated_values(&overflowing, 12), "d1, +99 more");
    }

    #[test]
    fn compact_inspector_ellipsizes_unicode_and_preserves_log_space() {
        let mut step = direct_command_step(
            StepStateKind::Running,
            None,
            Some(WorkflowRunElapsed {
                started_at: time::OffsetDateTime::UNIX_EPOCH,
                duration: Duration::from_secs(3),
                frozen: false,
            }),
            WorkflowRunOutputDisposition::Pending,
        );
        step.id = "構築工程の識別子がとても長くても安全に表示される選択中の工程".repeat(2);
        let WorkflowPresentationStep::Command {
            direct_dependencies,
            outputs,
            ..
        } = &mut step.definition
        else {
            panic!("fixture presentation step was not a command");
        };
        *direct_dependencies = (1..=8).map(|index| format!("dependency-{index}")).collect();
        for index in 2..=8 {
            let name = format!("report-{index}");
            outputs.insert(
                name.clone(),
                Output::FilePath {
                    path: format!("{name}.txt"),
                    media_type: "text/plain".to_owned(),
                },
            );
            step.outputs
                .insert(name, WorkflowRunOutputDisposition::Pending);
        }
        let snapshot = direct_snapshot(step);
        let graph = DagLayout::for_steps(&snapshot.steps);
        let backend = ratatui::backend::TestBackend::new(MINIMUM_WIDTH, MINIMUM_HEIGHT);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut interaction = HostInteraction::default();
        terminal
            .draw(|frame| {
                render(frame, &snapshot, &graph, &mut interaction, false);
            })
            .unwrap();
        let rendered = buffer_text(terminal.backend().buffer());

        assert!(rendered.contains('構'));
        assert!(rendered.contains('…'));
        assert!(rendered.contains("+"));
        assert!(rendered.contains("more"));
        assert!(rendered.contains("Waiting for output…"));
        assert!(!rendered.contains('\u{fffd}'));
    }

    #[test]
    fn inspector_renders_outputs_in_a_dedicated_structured_panel() {
        let step = direct_command_step(
            StepStateKind::Succeeded,
            None,
            Some(WorkflowRunElapsed {
                started_at: time::OffsetDateTime::UNIX_EPOCH,
                duration: Duration::from_secs(3),
                frozen: true,
            }),
            WorkflowRunOutputDisposition::Committed,
        );
        let buffer = render_direct_inspector_buffer(&step, 120, 14, true);
        let rows = buffer_rows(&buffer);
        let command_y = row_containing(&rows, "command       build");
        let cwd_y = row_containing(&rows, "cwd");
        let started_y = row_containing(&rows, "started");
        let dependencies_y = row_containing(&rows, "depends on");
        let outputs_y = row_containing(&rows, "OUTPUTS");
        let summary_y = row_containing(&rows, "✓  report  file");
        let detail_y = summary_y + 1;

        assert_eq!(cwd_y, command_y + 1);
        assert_eq!(started_y, cwd_y + 1);
        assert_eq!(dependencies_y, started_y + 1);
        assert!(dependencies_y < outputs_y);
        assert!(rows[outputs_y - 1].contains('─'));
        assert_eq!(summary_y, outputs_y + 2);
        assert_eq!(detail_y, summary_y + 1);
        assert!(rows[detail_y].contains('—'));
        assert!(rows[detail_y + 1].replace('│', "").trim().is_empty());
        assert!(rows[summary_y].contains("captured"));
        let (marker_x, marker_y) = buffer_position(&buffer, "✓  report");
        let (status_x, status_y) = buffer_position(&buffer, "captured");
        assert_eq!(
            buffer[(marker_x, marker_y)].fg,
            tone_style(true, Tone::Success).fg.unwrap()
        );
        assert_eq!(
            buffer[(status_x, status_y)].fg,
            tone_style(true, Tone::Success).fg.unwrap()
        );
    }

    #[test]
    fn inspector_reports_when_a_step_declares_no_outputs() {
        let mut step = direct_command_step(
            StepStateKind::Pending,
            None,
            None,
            WorkflowRunOutputDisposition::Pending,
        );
        let WorkflowPresentationStep::Command { outputs, .. } = &mut step.definition else {
            panic!("fixture presentation step was not a command");
        };
        outputs.clear();
        step.outputs.clear();

        let buffer = render_direct_inspector_buffer(&step, 80, 12, false);
        let rows = buffer_rows(&buffer);
        let outputs_y = row_containing(&rows, "OUTPUTS");
        let empty_y = row_containing(&rows, "·  —  none declared");

        assert_eq!(empty_y, outputs_y + 2);
        assert!(rows[empty_y + 1].replace('│', "").trim().is_empty());
        assert!(!buffer_text(&buffer).contains("report"));
    }

    #[tokio::test]
    async fn full_view_renders_header_steps_inspector_and_selected_log_tail() {
        let temporary = tempfile::tempdir().unwrap();
        std::fs::write(
            temporary.path().join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  build:\n    kind: cmd\n    command:\n      argv: [\"build\"]\n",
        )
        .unwrap();
        let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
        let clock = FixedClock {
            now: ObservationTime {
                utc: time::OffsetDateTime::UNIX_EPOCH,
                monotonic: crate::timing::monotonic_now(),
            },
        };
        let view = WorkflowRunViewModel::new(
            &workflow,
            1,
            RunTimingObservation::new(clock.sample()),
            clock,
        );
        view.observe(ExecutionObservation::<time::OffsetDateTime>::CommandOutput(
            CommandOutputObservation {
                step: "build".to_owned(),
                invocation: ActionId {
                    transition_sequence: TransitionSequence::default(),
                },
                source: CommandOutputSource::StandardOutput,
                sequence: SourceSequence::first(),
                bytes: Arc::from(b"compiling workflow host\n".as_slice()),
            },
        ))
        .await;
        let snapshot = view.snapshot();
        let backend = ratatui::backend::TestBackend::new(90, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut interaction = HostInteraction::default();
        terminal
            .draw(|frame| {
                render(
                    frame,
                    &snapshot,
                    &DagLayout::for_steps(&snapshot.steps),
                    &mut interaction,
                    false,
                );
            })
            .unwrap();
        let rendered = buffer_text(terminal.backend().buffer());

        assert!(rendered.contains("workflow"));
        assert!(!rendered.contains("workflow.yaml"));
        assert!(rendered.contains("▏ ○ build"));
        assert!(rendered.contains("build"));
        assert!(rendered.contains("compiling workflow host"));
        assert!(rendered.contains("^C cancel run"));
    }

    #[test]
    fn responsive_application_composes_wide_stacked_and_too_small_views() {
        let mut snapshot = direct_snapshot(long_log_step());
        snapshot.steps[0].id = "first-step".to_owned();
        let mut second = long_log_step();
        second.id = "selected-second-step".to_owned();
        snapshot.steps.push(second);
        let mut interaction = HostInteraction {
            selected: 1,
            ..HostInteraction::default()
        };

        let wide = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
        let columns = wide_split_columns(Rect::new(0, 0, 120, 22));
        assert_eq!((columns[0].width, columns[1].width), (40, 80));
        let divider_x = columns[1].x.saturating_sub(1);
        let workflow = buffer_position(&wide, "workflow");
        let steps = buffer_position(&wide, "▏ ⠋ selected-second-step");
        let inspector = buffer_position(&wide, "⠋  selected-second-step   cmd");
        let outputs = buffer_position(&wide, "OUTPUTS");
        let log = buffer_position(&wide, "LOG");
        assert_eq!(workflow.1, inspector.1);
        assert!(steps.1 > workflow.1);
        assert!(steps.0 < inspector.0);
        assert_eq!(inspector.0, log.0);
        assert!(inspector.1 < outputs.1 && outputs.1 < log.1);
        assert!((columns[1].x..=columns[1].x + 6).contains(&inspector.0));
        assert_eq!(wide[(divider_x, 0)].symbol(), "│");
        assert_eq!(wide[(divider_x, 1)].symbol(), "│");
        assert_eq!(wide[(divider_x, 2)].symbol(), "┼");
        assert_eq!(wide[(divider_x, outputs.1.saturating_sub(1))].symbol(), "├");
        assert_eq!(wide[(divider_x, log.1.saturating_sub(1))].symbol(), "├");
        assert_eq!(wide[(divider_x, 22)].symbol(), "┴");
        assert_ne!(wide[(columns[1].x, 1)].symbol(), "│");
        assert!(
            wide[(divider_x.saturating_sub(1), steps.1)]
                .modifier
                .contains(Modifier::REVERSED)
        );
        assert!(
            !wide[(divider_x.saturating_sub(1), steps.1 + 1)]
                .modifier
                .contains(Modifier::REVERSED)
        );
        assert!(buffer_text(&wide).contains("selected-second-step"));
        assert!(buffer_text(&wide).contains("2 steps · 2 running"));
        assert!(!buffer_text(&wide).contains("pending 0"));

        let stacked = render_snapshot(&snapshot, &mut interaction, 90, 24, false);
        let steps = buffer_position(&stacked, "▏ ⠋ selected-second-step");
        let inspector_body = buffer_position(&stacked, "command       build");
        let outputs = buffer_position(&stacked, "OUTPUTS");
        let log = buffer_position(&stacked, "LOG");
        assert!(steps.1 < inspector_body.1 && inspector_body.1 < outputs.1 && outputs.1 < log.1);
        assert!(steps.0 < inspector_body.0);
        assert_eq!(inspector_body.0, log.0);

        let too_small = render_snapshot(&snapshot, &mut interaction, 50, 12, false);
        let too_small = buffer_text(&too_small);
        assert!(too_small.contains("Terminal too small"));
        assert!(too_small.contains("64x20"));
        assert!(!too_small.contains("selected-second-step"));
        assert_eq!(interaction.selected, 1);
        assert_eq!(interaction.surface, HostSurface::Split);

        let recovered = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
        let selected_row = buffer_rows(&recovered)
            .into_iter()
            .find(|row| row.contains("▏ ") && row.contains("selected-second-step"))
            .unwrap();
        assert!(selected_row.contains('▏'));
        assert_eq!(interaction.selected, 1);
    }

    #[test]
    fn workflow_header_separates_identity_status_duration_and_counts() {
        let mut snapshot = direct_snapshot(long_log_step());
        snapshot.workflow_path = "plans/plan-implement-test.yaml".to_owned();
        snapshot.timing.duration = Duration::from_secs(20_065);
        let mut interaction = HostInteraction::default();

        let buffer = render_snapshot(&snapshot, &mut interaction, 180, 24, false);
        let rows = buffer_rows(&buffer);
        let name = buffer_position(&buffer, "plan-implement-test");
        let status = buffer_position(&buffer, "running");
        let duration = buffer_position(&buffer, "5h34m25s");
        let divider_x = wide_split_columns(Rect::new(0, 0, 180, 22))[1]
            .x
            .saturating_sub(1);

        assert_eq!(name, (2, 0));
        assert_eq!(status.1, name.1);
        assert_eq!(duration.1, name.1);
        assert!(name.0 < status.0 && status.0 < duration.0);
        assert_eq!(
            duration
                .0
                .saturating_add(u16::try_from(display_width("5h34m25s")).unwrap()),
            divider_x.saturating_sub(2)
        );
        assert!(rows[1].starts_with("  1 step · 1 running"));
        assert!(!rows[0].contains(".yaml"));
        assert!(!rows[0].contains("concurrency"));
        assert!(!rows[0].contains("published"));
    }

    #[test]
    fn workflow_header_status_tracks_failure_and_publication_lifecycle() {
        let mut snapshot = direct_snapshot(long_log_step());
        snapshot.workflow = WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped {
                primary_issue: {
                    let cause = StepFailureCause::Execution(StepExecutionFailure::Command(
                        CommandExecutionFailure::UnsuccessfulExit { code: Some(17) },
                    ));
                    super::super::evidence::PrimaryIssue::failed(
                        super::super::validated::WorkflowNode {
                            id: "selected-command".to_owned(),
                            role: super::super::validated::WorkflowNodeRole::Step,
                        },
                        super::super::evidence::failure_detail(FailurePhase::Execution, &cause)
                            .unwrap(),
                    )
                },
            },
        };
        assert_eq!(workflow_header_status(&snapshot).0, "failing");

        snapshot.workflow = WorkflowState::Succeeded;
        snapshot.publication = WorkflowRunPublicationState::Publishing;
        assert_eq!(workflow_header_status(&snapshot).0, "publishing");

        snapshot.publication =
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                result_directory: "result".to_owned(),
            });
        snapshot.cleanup = WorkflowRunCleanupState::Cleaning;
        assert_eq!(workflow_header_status(&snapshot).0, "cleaning");

        snapshot.cleanup = WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Failed);
        assert_eq!(workflow_header_status(&snapshot).0, "cleanup failed");

        snapshot.cleanup = WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Succeeded);
        assert_eq!(workflow_header_status(&snapshot).0, "succeeded");
    }

    #[test]
    fn resize_sequence_preserves_log_surface_viewport_and_help() {
        let mut snapshot = direct_snapshot(numbered_log_step(40, 200));
        snapshot.steps[0].id = "first-step".to_owned();
        let mut selected = numbered_log_step(40, 200);
        selected.id = "selected-second-step".to_owned();
        snapshot.steps.push(selected);
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction {
            selected: 1,
            terminal_area: Rect::new(0, 0, 120, 24),
            ..HostInteraction::default()
        };
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Enter,
            KeyModifiers::NONE,
            &cancellation,
        );
        let _ = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Up,
            KeyModifiers::NONE,
            &cancellation,
        );
        for _ in 0..3 {
            press_key(
                &mut interaction,
                &snapshot,
                KeyCode::Right,
                KeyModifiers::NONE,
                &cancellation,
            );
        }
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('?'),
            KeyModifiers::SHIFT,
            &cancellation,
        );
        let anchor = interaction.full_log.anchor;
        let offset = interaction.full_log.horizontal_offset;

        let stacked = render_snapshot(&snapshot, &mut interaction, 90, 20, false);
        assert!(buffer_text(&stacked).contains("? — all commands"));
        assert_eq!(interaction.full_log.anchor, anchor);
        assert_eq!(interaction.full_log.horizontal_offset, offset);

        let too_small = render_snapshot(&snapshot, &mut interaction, 40, 8, false);
        assert!(buffer_text(&too_small).contains("Terminal too small"));
        assert!(!buffer_text(&too_small).contains("? — all commands"));
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Down,
            KeyModifiers::NONE,
            &cancellation,
        );
        assert_eq!(interaction.full_log.anchor, anchor);
        assert_eq!(interaction.full_log.horizontal_offset, offset);

        let recovered = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
        assert!(buffer_text(&recovered).contains("? — all commands"));
        assert_eq!(interaction.selected, 1);
        assert_eq!(interaction.surface, HostSurface::FullLog);
        assert!(interaction.help_visible);
        assert!(!interaction.full_log.follow);
        assert_eq!(interaction.full_log.anchor, anchor);
        assert_eq!(interaction.full_log.horizontal_offset, offset);

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Esc,
            KeyModifiers::NONE,
            &cancellation,
        );
        let log = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
        let log = buffer_text(&log);
        assert!(!interaction.help_visible);
        assert_eq!(interaction.surface, HostSurface::FullLog);
        assert!(log.contains("selected-second-step"));
        assert!(log.contains("● paused"));
        assert!(!log.contains("stdout + stderr"));
        assert!(!log.contains("workflow.yaml"));
    }

    #[test]
    fn too_small_view_freezes_hidden_split_and_log_interaction_state() {
        let mut snapshot = direct_snapshot(numbered_log_step(40, 200));
        snapshot.steps[0].id = "first-step".to_owned();
        let mut second = numbered_log_step(40, 200);
        second.id = "selected-second-step".to_owned();
        snapshot.steps.push(second);

        let cancellation = CancellationSource::new();
        let mut split = HostInteraction {
            selected: 1,
            ..HostInteraction::default()
        };
        let _ = render_snapshot(&snapshot, &mut split, 90, 20, false);
        let _ = render_snapshot(&snapshot, &mut split, 40, 8, false);
        for code in [KeyCode::Up, KeyCode::Enter, KeyCode::Char('?')] {
            press_key(
                &mut split,
                &snapshot,
                code,
                KeyModifiers::NONE,
                &cancellation,
            );
        }
        assert_eq!(split.selected, 1);
        assert_eq!(split.surface, HostSurface::Split);
        assert!(!split.help_visible);

        let (mut log, cancellation) = entered_full_log(&snapshot, 120, 24);
        log.selected = 1;
        let _ = render_snapshot(&snapshot, &mut log, 120, 24, false);
        press_key(
            &mut log,
            &snapshot,
            KeyCode::Up,
            KeyModifiers::NONE,
            &cancellation,
        );
        press_key(
            &mut log,
            &snapshot,
            KeyCode::Right,
            KeyModifiers::NONE,
            &cancellation,
        );
        let anchor = log.full_log.anchor;
        let horizontal_offset = log.full_log.horizontal_offset;
        assert!(!log.full_log.follow);

        let _ = render_snapshot(&snapshot, &mut log, 40, 8, false);
        for code in [
            KeyCode::Down,
            KeyCode::PageDown,
            KeyCode::Left,
            KeyCode::Char('F'),
            KeyCode::Esc,
            KeyCode::Char('?'),
        ] {
            press_key(&mut log, &snapshot, code, KeyModifiers::NONE, &cancellation);
        }
        assert_eq!(log.selected, 1);
        assert_eq!(log.surface, HostSurface::FullLog);
        assert!(!log.help_visible);
        assert!(!log.full_log.follow);
        assert_eq!(log.full_log.anchor, anchor);
        assert_eq!(log.full_log.horizontal_offset, horizontal_offset);

        let _ = render_snapshot(&snapshot, &mut log, 120, 24, false);
        assert_eq!(log.surface, HostSurface::FullLog);
        assert!(!log.full_log.follow);
        assert_eq!(log.full_log.anchor, anchor);
        assert_eq!(log.full_log.horizontal_offset, horizontal_offset);
    }

    #[test]
    fn contextual_footers_and_help_remain_discoverable_at_minimum_size() {
        let mut snapshot = direct_snapshot(long_log_step());
        let mut second = long_log_step();
        second.id = "second-step".to_owned();
        snapshot.steps.push(second);
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction::default();

        let wide = render_snapshot(&snapshot, &mut interaction, 140, 24, false);
        let wide_footer = buffer_rows(&wide).pop().unwrap();
        assert!(wide_footer.contains("↑/k up"));
        assert!(wide_footer.contains("↓/j down"));
        assert!(wide_footer.contains("↵ open"));
        assert!(wide_footer.contains("^C cancel run"));
        assert!(wide_footer.contains("? help"));

        let minimum = render_snapshot(
            &snapshot,
            &mut interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        );
        let minimum_footer = buffer_rows(&minimum).pop().unwrap();
        assert!(minimum_footer.contains("↑/k up"));
        assert!(minimum_footer.contains("↓/j down"));
        assert!(minimum_footer.contains("↵ open"));
        assert!(minimum_footer.contains("^C cancel run"));
        assert!(minimum_footer.contains("? help"));

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('?'),
            KeyModifiers::SHIFT,
            &cancellation,
        );
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Down,
            KeyModifiers::NONE,
            &cancellation,
        );
        let split_help = render_snapshot(
            &snapshot,
            &mut interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        );
        let split_help_rows = buffer_rows(&split_help);
        let split_help = buffer_text(&split_help);
        assert_eq!(interaction.selected, 0);
        for expected in [
            "? — all commands",
            "esc to dismiss",
            "MOVE",
            "OPEN",
            "VIEW",
            "FILTER",
            "RUN",
            "↑/k",
            "↓/j",
            "↵",
            "1…n",
            "^C",
        ] {
            assert!(split_help.contains(expected), "missing {expected:?}");
        }
        assert!(split_help.contains("toggle log"));
        assert!(split_help_rows.last().unwrap().contains("DAG"));
        assert!(split_help_rows.last().unwrap().contains("? help"));

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Esc,
            KeyModifiers::NONE,
            &cancellation,
        );
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Enter,
            KeyModifiers::NONE,
            &cancellation,
        );
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('?'),
            KeyModifiers::SHIFT,
            &cancellation,
        );
        let log_help = render_snapshot(
            &snapshot,
            &mut interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        );
        let log_help_rows = buffer_rows(&log_help);
        let log_help = buffer_text(&log_help);
        for expected in [
            "? — all commands",
            "MOVE",
            "JUMP",
            "VIEW",
            "FILTER",
            "RUN",
            "↑/k",
            "↓/j",
            "PgUp/b",
            "PgDn/f/Space",
            "u/^U",
            "d/^D",
            "←/h",
            "→/l",
            "F",
            "1…n",
            "^C",
        ] {
            assert!(log_help.contains(expected), "missing {expected:?}");
        }
        assert!(log_help_rows.last().unwrap().contains("LOG"));
        assert!(log_help_rows.last().unwrap().contains("? help"));

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Esc,
            KeyModifiers::NONE,
            &cancellation,
        );
        let log = render_snapshot(
            &snapshot,
            &mut interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        );
        let log_footer = buffer_rows(&log).pop().unwrap();
        for expected in ["Esc", "↑/k", "↓/j", "F", "^C", "? help"] {
            assert!(log_footer.contains(expected), "missing {expected:?}");
        }

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Esc,
            KeyModifiers::NONE,
            &cancellation,
        );
        snapshot.workflow = WorkflowState::Succeeded;
        snapshot.quit_eligible = true;
        let completed = render_snapshot(
            &snapshot,
            &mut interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        );
        let completed_footer = buffer_rows(&completed).pop().unwrap();
        assert!(completed_footer.contains("q quit"));
        assert!(completed_footer.contains("? help"));
        assert!(!completed_footer.contains("^C"));

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('?'),
            KeyModifiers::SHIFT,
            &cancellation,
        );
        let completed_help = render_snapshot(
            &snapshot,
            &mut interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        );
        let completed_help = buffer_text(&completed_help);
        assert!(completed_help.contains("q"));
        assert!(completed_help.contains("quit"));
        assert!(!completed_help.contains("^C"));
    }

    #[test]
    fn contextual_footer_and_help_use_the_command_palette() {
        let snapshot = direct_snapshot(long_log_step());
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction::default();
        let _ = render_snapshot(&snapshot, &mut interaction, 140, 24, true);
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('?'),
            KeyModifiers::SHIFT,
            &cancellation,
        );

        let buffer = render_snapshot(&snapshot, &mut interaction, 140, 24, true);
        let rows = buffer_rows(&buffer);
        for (needle, expected) in [
            ("? — all commands", Color::Rgb(203, 166, 247)),
            ("MOVE", Color::Rgb(108, 112, 134)),
            ("↑/k", Color::Rgb(249, 226, 175)),
            ("previous step", Color::Rgb(186, 194, 222)),
        ] {
            let y = rows
                .iter()
                .position(|row| row.contains(needle))
                .unwrap_or_else(|| panic!("missing {needle:?} in {}", rows.join("\n")));
            let x = column_of(&rows[y], needle);
            assert_eq!(
                buffer[(x, u16::try_from(y).unwrap())].fg,
                expected,
                "wrong color for {needle:?}"
            );
        }

        let footer_y = buffer.area.height.saturating_sub(1);
        let footer = &rows[usize::from(footer_y)];
        for (needle, expected) in [
            ("DAG", Color::Rgb(203, 166, 247)),
            ("↑/k", Color::Rgb(180, 190, 254)),
            ("? help", Color::Rgb(203, 166, 247)),
        ] {
            let x = column_of(footer, needle);
            assert_eq!(
                buffer[(x, footer_y)].fg,
                expected,
                "wrong footer color for {needle:?}"
            );
        }
        let separator_y = footer_y.saturating_sub(1);
        assert_eq!(buffer[(0, separator_y)].fg, Color::Rgb(49, 50, 68));
        assert!(
            !rows[usize::from(separator_y)].contains('┴'),
            "the covered split junction must not protrude through the help menu"
        );
    }

    #[test]
    fn terminal_lifecycle_hides_quit_until_adapter_completion() {
        let mut snapshot = direct_snapshot(long_log_step());
        snapshot.workflow = WorkflowState::Succeeded;
        snapshot.authoritative_result = true;
        snapshot.quiescent = true;
        snapshot.publication = WorkflowRunPublicationState::Publishing;
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction::default();

        let publishing = render_minimum_snapshot_text(&snapshot, &mut interaction);
        assert!(publishing.contains("publishing"));
        assert!(!minimum_footer(&snapshot, &mut interaction).contains("q quit"));

        assert_help_omits_quit(&mut interaction, &snapshot, &cancellation);

        press_unmodified_key(&mut interaction, &snapshot, KeyCode::Esc, &cancellation);
        press_unmodified_key(&mut interaction, &snapshot, KeyCode::Enter, &cancellation);
        assert!(!minimum_footer(&snapshot, &mut interaction).contains("q quit"));

        assert_help_omits_quit(&mut interaction, &snapshot, &cancellation);
        assert!(!render_too_small_text(&snapshot).contains("q to quit"));

        snapshot.publication =
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                result_directory: "results".to_owned(),
            });
        snapshot.cleanup = WorkflowRunCleanupState::Cleaning;
        interaction.surface = HostSurface::Split;
        interaction.help_visible = false;
        assert!(render_minimum_snapshot_text(&snapshot, &mut interaction).contains("cleaning"));
        assert!(!minimum_footer(&snapshot, &mut interaction).contains("q quit"));

        snapshot.cleanup = WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Succeeded);
        snapshot.quit_eligible = true;
        assert!(minimum_footer(&snapshot, &mut interaction).contains("q quit"));

        press_unmodified_key(&mut interaction, &snapshot, KeyCode::Enter, &cancellation);
        assert!(minimum_footer(&snapshot, &mut interaction).contains("q quit"));

        open_help(&mut interaction, &snapshot, &cancellation);
        let completed_help = render_minimum_snapshot_text(&snapshot, &mut interaction);
        assert!(completed_help.contains("RUN"));
        assert!(completed_help.contains("→ quit"));
        assert!(render_too_small_text(&snapshot).contains("q to quit"));
    }

    #[test]
    fn cancelling_workflow_does_not_advertise_an_inactive_cancel_command() {
        let mut snapshot = direct_snapshot(long_log_step());
        snapshot.workflow = WorkflowState::Executing {
            gate: SchedulingGate::Cancelling {
                reason: CancellationReason::UserRequest,
                prior_issue: None,
            },
        };
        snapshot.cancellation = Some(
            crate::execution::workflow::run_view_model::WorkflowRunCancellationView {
                reason: CancellationReason::UserRequest,
                force_stop_deadline: time::OffsetDateTime::UNIX_EPOCH,
            },
        );
        let mut interaction = HostInteraction::default();

        let buffer = render_snapshot(
            &snapshot,
            &mut interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        );

        assert!(buffer_text(&buffer).contains("cancelling"));
        let footer = buffer_rows(&buffer).pop().unwrap();
        assert!(
            !footer.contains("^C"),
            "an already-cancelling workflow must not advertise a no-op cancel command: {footer:?}"
        );
    }

    #[test]
    fn color_disabled_rendering_retains_symbols_labels_and_focus() {
        let snapshot = direct_snapshot(long_log_step());
        let mut interaction = HostInteraction::default();
        let buffer = render_snapshot(&snapshot, &mut interaction, 90, 20, false);
        let rendered = buffer_text(&buffer);

        for expected in [
            "▏",
            "command",
            "following",
            "stdout",
            "^C cancel run",
            "? help",
        ] {
            assert!(rendered.contains(expected), "missing {expected:?}");
        }
        assert!(
            buffer
                .content()
                .iter()
                .all(|cell| cell.fg == Color::Reset && cell.bg == Color::Reset)
        );
        let (x, y) = buffer_position(&buffer, "selected-command");
        assert!(buffer[(x, y)].modifier.contains(Modifier::REVERSED));
        assert!(!buffer[(x, y + 1)].modifier.contains(Modifier::REVERSED));
    }

    #[test]
    fn color_enabled_rendering_separates_structure_content_and_focus() {
        let mut step = long_log_step();
        step.timing = Some(WorkflowRunElapsed {
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            duration: Duration::from_secs(3),
            frozen: false,
        });
        let snapshot = direct_snapshot(step);
        let mut interaction = HostInteraction::default();
        let buffer = render_snapshot(&snapshot, &mut interaction, 120, 24, true);
        let divider_x = wide_split_columns(Rect::new(0, 0, 120, 22))[1]
            .x
            .saturating_sub(1);

        assert_eq!(buffer[(divider_x, 1)].fg, separator_style(true).fg.unwrap());
        let (title_x, title_y) = buffer_position(&buffer, "selected-command");
        assert_eq!(
            buffer[(title_x, title_y)].fg,
            tone_style(true, Tone::Primary).fg.unwrap()
        );
        assert!(buffer[(title_x, title_y)].modifier.contains(Modifier::BOLD));
        let (badge_x, badge_y) = buffer_position(&buffer, " cmd ");
        assert_eq!(buffer[(badge_x, badge_y)].bg, Color::Rgb(49, 50, 68));
        assert_eq!(
            buffer[(badge_x, badge_y)].fg,
            tone_style(true, Tone::Muted).fg.unwrap()
        );
        let rows = buffer_rows(&buffer);
        let header_row = &rows[usize::from(title_y)];
        let duration_byte = header_row.rfind("3.0s").unwrap();
        let duration_x = u16::try_from(display_width(&header_row[..duration_byte])).unwrap();
        assert_eq!(
            buffer[(duration_x, title_y)].fg,
            tone_style(true, Tone::Active).fg.unwrap()
        );
        let (payload_x, payload_y) = buffer_position(&buffer, "abcdefghijklmnopqrstuvwxyz");
        assert_eq!(
            buffer[(payload_x, payload_y)].fg,
            tone_style(true, Tone::Neutral).fg.unwrap()
        );
        let (footer_x, footer_y) = buffer_position(&buffer, "DAG");
        assert_eq!(
            buffer[(footer_x, footer_y)].fg,
            command_accent_style(true).fg.unwrap()
        );
        let selected_y = buffer_position(&buffer, "▏ ⠋ selected-command").1;
        assert_eq!(
            buffer[(divider_x.saturating_sub(1), selected_y)].bg,
            step_selection_style(true).bg.unwrap()
        );
        let step_row = &rows[usize::from(selected_y)];
        let duration_byte = step_row.find("3.0s").unwrap();
        let duration_x = u16::try_from(display_width(&step_row[..duration_byte])).unwrap();
        assert_eq!(
            buffer[(duration_x, selected_y)].fg,
            tone_style(true, Tone::Active).fg.unwrap()
        );
    }

    #[test]
    fn running_step_indicator_advances_with_elapsed_time() {
        let mut step = long_log_step();
        step.timing = Some(WorkflowRunElapsed {
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            duration: Duration::ZERO,
            frozen: false,
        });
        assert_eq!(step_state_glyph(&step), "⠋");

        step.timing.as_mut().unwrap().duration = REDRAW_INTERVAL;
        assert_eq!(step_state_glyph(&step), "⠙");

        step.timing.as_mut().unwrap().duration = REDRAW_INTERVAL * 10;
        assert_eq!(step_state_glyph(&step), "⠋");
    }

    #[test]
    fn live_dag_separates_ordinary_and_finalization_phases_after_trigger_commit() {
        let mut snapshot = snapshot_from_yaml(
            "schemaVersion: 1
steps:
  complete:
    kind: cmd
    command:
      argv: [\"true\"]
finalizers:
  cleanup:
    kind: cmd
    command:
      argv: [\"true\"]
",
        );

        let before_trigger =
            render_steps_lines(&snapshot, &HostInteraction::default(), 80, 10).join("\n");
        assert!(before_trigger.contains("ordinary phase"));
        assert!(before_trigger.contains("finalization phase"));
        assert!(!before_trigger.contains("finalization phase · trigger"));

        snapshot.workflow = WorkflowState::Finalizing {
            trigger: crate::execution::workflow::document::FinalizationTrigger::Succeeded,
            gate: crate::execution::workflow::runtime::FinalizationGate::Open,
            primary_issue: None,
        };
        let after_trigger =
            render_steps_lines(&snapshot, &HostInteraction::default(), 80, 10).join("\n");
        let ordinary = after_trigger.find("ordinary phase").unwrap();
        let ordinary_step = after_trigger.find("complete").unwrap();
        let finalization = after_trigger
            .find("finalization phase · trigger succeeded")
            .unwrap();
        let finalizer = after_trigger.find("cleanup").unwrap();
        assert!(ordinary < ordinary_step);
        assert!(ordinary_step < finalization);
        assert!(finalization < finalizer);
    }

    #[test]
    fn graph_layout_is_stable_when_step_state_and_timing_change() {
        let mut snapshot = snapshot_from_yaml(
            "schemaVersion: 1\nsteps:\n  root:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  left:\n    kind: cmd\n    dependsOn: [root]\n    command:\n      argv: [\"true\"]\n  right:\n    kind: cmd\n    dependsOn: [root]\n    command:\n      argv: [\"true\"]\n  join:\n    kind: cmd\n    dependsOn: [left, right]\n    command:\n      argv: [\"true\"]\n",
        );
        let pending = DagLayout::for_steps(&snapshot.steps);

        snapshot.steps[0].state = StepStateKind::Succeeded;
        snapshot.steps[0].timing = Some(super::super::run_view_model::WorkflowRunElapsed {
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            duration: Duration::from_secs(4),
            frozen: true,
        });
        snapshot.steps[1].state = StepStateKind::Running;
        snapshot.steps[1].timing = Some(super::super::run_view_model::WorkflowRunElapsed {
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            duration: Duration::from_millis(1250),
            frozen: false,
        });

        assert_eq!(DagLayout::for_steps(&snapshot.steps), pending);
        let rendered = render_steps_lines(&snapshot, &HostInteraction::default(), 80, 8);
        assert!(rendered.iter().any(|line| line.contains("4.0s")));
        assert!(rendered.iter().any(|line| line.contains("1.2s")));
    }

    #[test]
    fn responsive_rows_drop_detail_before_kind_and_then_ellipsize_identity() {
        let mut snapshot = snapshot_from_yaml(
            "schemaVersion: 1\nsteps:\n  buildartifact:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        );
        snapshot.steps[0].state = StepStateKind::Succeeded;
        let graph = DagLayout::for_steps(&snapshot.steps);
        let id_width = display_width("buildartifact");
        let exact_with_kind = 2 + graph.gutter_width() + 1 + id_width + 2 + 5 + 2 + 1;

        let wide = StepColumns::for_steps(
            exact_with_kind + 2 + MINIMUM_DETAIL_WIDTH,
            graph.gutter_width(),
            &snapshot.steps,
        );
        let medium = StepColumns::for_steps(exact_with_kind, graph.gutter_width(), &snapshot.steps);
        let narrow =
            StepColumns::for_steps(exact_with_kind - 1, graph.gutter_width(), &snapshot.steps);
        let compact_width = 2 + graph.gutter_width() + 1 + id_width + 2 + 1 - 1;
        let compact = StepColumns::for_steps(compact_width, graph.gutter_width(), &snapshot.steps);

        assert!(wide.detail && wide.kind);
        assert!(!medium.detail && medium.kind);
        assert!(!narrow.detail && !narrow.kind);
        assert!(!compact.detail && !compact.kind);
        assert!(compact.id_width < id_width);
        assert_eq!(step_detail(&snapshot.steps[0]).as_deref(), Some("exit 0"));
    }

    #[test]
    fn scrolling_and_resize_keep_selection_visible_with_boundary_connectors() {
        let snapshot = snapshot_from_yaml(
            "schemaVersion: 1\nsteps:\n  root:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middleone:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middletwo:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middlethree:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middlefour:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middlefive:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middlesix:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middleseven:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middleeight:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  branch:\n    kind: cmd\n    dependsOn: [root]\n    command:\n      argv: [\"true\"]\n",
        );
        let interaction = HostInteraction {
            selected: 8,
            ..HostInteraction::default()
        };

        let compact = render_steps_lines(&snapshot, &interaction, 70, 6);
        let selected = compact
            .iter()
            .find(|line| line.contains("middleeight"))
            .unwrap();
        let top = compact
            .iter()
            .find(|line| line.contains("middleseven"))
            .unwrap();
        assert!(selected.contains("▏ │"));
        assert!(top.contains("│"));
        assert_eq!(
            display_width(&selected[..selected.find("middleeight").unwrap()]),
            display_width(&top[..top.find("middleseven").unwrap()]),
        );
        assert!(!compact.iter().any(|line| line.contains("branch")));

        let resized = render_steps_lines(&snapshot, &interaction, 64, 8);
        assert!(
            resized
                .iter()
                .any(|line| line.contains("▏ │") && line.contains("middleeight"))
        );
        assert!(!resized.iter().any(|line| line.contains("branch")));
    }

    #[test]
    fn too_small_view_only_advertises_the_available_lifecycle_action() {
        let mut snapshot = direct_snapshot(long_log_step());
        let backend = ratatui::backend::TestBackend::new(40, 8);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_too_small(frame, frame.area(), &snapshot, false))
            .unwrap();
        let running = buffer_text(terminal.backend().buffer());

        assert!(running.contains("Terminal too small"));
        assert!(running.contains("Resize to at least 64x20"));
        assert!(running.contains("Ctrl-C cancels"));
        assert!(!running.contains("q to quit"));

        snapshot.workflow = WorkflowState::Succeeded;
        snapshot.quit_eligible = true;
        terminal
            .draw(|frame| render_too_small(frame, frame.area(), &snapshot, false))
            .unwrap();
        let terminal = buffer_text(terminal.backend().buffer());
        assert!(terminal.contains("Press q to quit"));
        assert!(!terminal.contains("Ctrl-C cancels"));
    }

    fn start_active_scripted_host(
        view: WorkflowRunViewModel<FixedClock>,
        cancellation: CancellationSource,
        boundary: ScriptedTerminalBoundary,
    ) -> WorkflowTerminalHost {
        let mut host =
            WorkflowTerminalHost::start_with_boundary(view, cancellation, false, boundary).unwrap();
        host.activate_execution().unwrap();
        host
    }

    async fn assert_scripted_runtime_failure(
        failures: BoundaryFailures,
        scripted_input: ScriptedInput,
        expected_operation: PresentationFailureOperation,
    ) {
        let (_temporary, _workflow, view, _) = scripted_host_view();
        let cancellation = CancellationSource::new();
        let (boundary, input, mut actions) =
            ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], failures);
        let host = start_active_scripted_host(view, cancellation.clone(), boundary);
        input.send(scripted_input).unwrap();

        let failure = host.wait().await.unwrap_err();

        assert_eq!(failure.operation, expected_operation);
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::CallerOutputFailure)
        );
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
    }

    async fn wait_for_action(
        actions: &mut tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
        expected: BoundaryAction,
    ) {
        loop {
            let action = actions
                .recv()
                .await
                .expect("terminal action channel closed");
            if action == expected {
                return;
            }
        }
    }

    fn scripted_host_view() -> (
        tempfile::TempDir,
        ResolvedWorkflow,
        WorkflowRunViewModel<FixedClock>,
        ObservationTime,
    ) {
        let temporary = tempfile::tempdir().unwrap();
        std::fs::write(
            temporary.path().join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
        let now = ObservationTime {
            utc: time::OffsetDateTime::UNIX_EPOCH,
            monotonic: crate::timing::monotonic_now(),
        };
        let clock = FixedClock { now };
        let view = WorkflowRunViewModel::new(&workflow, 1, RunTimingObservation::new(now), clock);
        (temporary, workflow, view, now)
    }

    fn complete_scripted_view(
        view: &WorkflowRunViewModel<FixedClock>,
        workflow: &ResolvedWorkflow,
        started: ObservationTime,
        cancellation: Option<CancellationReason>,
    ) {
        let duration = Duration::from_millis(20);
        let (outcome, cancellation_fact, step_state, step_timing) = match cancellation {
            Some(reason) => (
                RunOutcome::Cancelled { reason },
                Some(WorkflowRunCancellation {
                    reason,
                    force_stop_deadline: started.utc + Duration::from_secs(10),
                }),
                StepState::Cancelled {
                    detail: super::super::evidence::CancellationDetail::new(reason),
                },
                None,
            ),
            None => (
                RunOutcome::Succeeded,
                None,
                StepState::Succeeded {
                    outputs: BTreeMap::new(),
                },
                Some(WorkflowStepTiming {
                    started_at: started.utc,
                    duration,
                }),
            ),
        };
        let run = WorkflowRunResult {
            run_directory: workflow.source.source_root.clone(),
            attempt_number: 1,
            workflow_path: workflow.source.workflow_path.clone(),
            source_root: workflow.source.source_root.clone(),
            content_digest: workflow.content_digest.clone(),
            execution_root: workflow.source.source_root.clone(),
            maximum_parallel_steps: NonZeroUsize::new(1).unwrap(),
            cloud_capacity: None,
            timing: WorkflowRunTiming {
                started_at: started.utc,
                finished_at: started.utc + duration,
                duration,
            },
            outcome,
            cancellation: cancellation_fact,
            steps: vec![WorkflowRunStep {
                id: "complete".to_owned(),
                role: crate::execution::workflow::validated::WorkflowNodeRole::Step,
                kind: WorkflowRunStepKind::Command,
                failure_policy: FailurePolicy::Required,
                state: step_state,
                timing: step_timing,
                command_output: None,
                recovery: None,
                invocations: Vec::new(),
            }],
            finalization: None,
            exports: BTreeMap::new(),
            export_sources: BTreeMap::new(),
        };
        view.reconcile_terminal_result(&run).unwrap();
        view.mark_quiescent();
        view.begin_publication();
        view.complete_publication(WorkflowRunPublicationResult::Succeeded {
            result_directory: "result".to_owned(),
        });
        view.begin_cleanup();
        view.complete_cleanup(WorkflowRunCleanupResult::Succeeded);
        view.mark_adapter_lifecycle_completed();
        assert!(view.snapshot().quit_eligible);
    }

    #[derive(Clone, Copy)]
    struct FixedClock {
        now: ObservationTime,
    }

    impl ObservationClock for FixedClock {
        fn sample(&self) -> ObservationTime {
            self.now
        }
    }

    fn direct_log_record(
        order: u64,
        source: CommandOutputSource,
        observed_at: &str,
        payload: &str,
        continuation: bool,
    ) -> WorkflowRunLogRecord {
        direct_source_log_record(
            order,
            WorkflowRunLogSource::Command(source),
            observed_at,
            payload,
            continuation,
        )
    }

    fn direct_agent_log_record(
        order: u64,
        source: AgentPresentationObservationKind,
        payload: &str,
    ) -> WorkflowRunLogRecord {
        direct_source_log_record(
            order,
            WorkflowRunLogSource::Agent(source),
            "2026-08-04T12:34:56Z",
            payload,
            false,
        )
    }

    fn direct_source_log_record(
        order: u64,
        source: WorkflowRunLogSource,
        observed_at: &str,
        payload: &str,
        continuation: bool,
    ) -> WorkflowRunLogRecord {
        WorkflowRunLogRecord {
            accepted_order: AcceptedRecordOrder::for_test(order),
            observed_at: time::OffsetDateTime::parse(
                observed_at,
                &time::format_description::well_known::Rfc3339,
            )
            .unwrap(),
            invocation: ActionId {
                transition_sequence: TransitionSequence::default(),
            },
            source,
            source_sequence: SourceSequence::first().get(),
            payload: Arc::from(payload),
            continuation,
        }
    }

    fn clamped_log_snapshot(width: u16, height: u16) -> (WorkflowRunViewSnapshot, HostInteraction) {
        let mut snapshot = direct_snapshot(numbered_log_step(30, 40));
        let (interaction, _) = run_full_log_keys(
            &snapshot,
            width,
            height,
            &[(KeyCode::Char('g'), KeyModifiers::NONE)],
        );
        let discarded_bytes = snapshot.steps[0]
            .log
            .records
            .drain(0..8)
            .map(|record| u64::try_from(record.payload.len()).unwrap())
            .sum();
        for order in 31..=38 {
            append_log_record(
                &mut snapshot.steps[0].log,
                order,
                &format!("record {order}"),
            );
        }
        snapshot.steps[0].log.discarded_records = 8;
        snapshot.steps[0].log.discarded_bytes = discarded_bytes;
        snapshot.steps[0].log.retained_records = 30;
        snapshot.steps[0].log.observed_records = 38;
        (snapshot, interaction)
    }

    fn numbered_log_step(record_count: u64, payload_width: usize) -> WorkflowRunStepView {
        let records = (1..=record_count)
            .map(|order| {
                let payload = format!("record {order:02} {}", "x".repeat(payload_width));
                direct_log_record(
                    order,
                    if order.is_multiple_of(2) {
                        CommandOutputSource::StandardOutput
                    } else {
                        CommandOutputSource::StandardError
                    },
                    "2026-08-04T12:34:56Z",
                    &payload,
                    false,
                )
            })
            .collect();
        direct_log_step(StepStateKind::Running, records, record_count, 0)
    }

    fn append_log_record(log: &mut WorkflowRunStepLog, order: u64, payload: &str) {
        log.records.push(direct_log_record(
            order,
            if order.is_multiple_of(2) {
                CommandOutputSource::StandardOutput
            } else {
                CommandOutputSource::StandardError
            },
            "2026-08-04T12:34:56Z",
            payload,
            false,
        ));
        log.observed_records = log.observed_records.max(order);
        log.retained_records = u64::try_from(log.records.len()).unwrap();
        log.retained_bytes = log.records.iter().fold(0_u64, |total, record| {
            total.saturating_add(u64::try_from(record.payload.len()).unwrap())
        });
    }

    fn entered_full_log(
        snapshot: &WorkflowRunViewSnapshot,
        width: u16,
        height: u16,
    ) -> (HostInteraction, CancellationSource) {
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction {
            terminal_area: Rect::new(0, 0, width, height),
            ..HostInteraction::default()
        };
        press_key(
            &mut interaction,
            snapshot,
            KeyCode::Enter,
            KeyModifiers::NONE,
            &cancellation,
        );
        (interaction, cancellation)
    }

    fn run_full_log_keys(
        snapshot: &WorkflowRunViewSnapshot,
        width: u16,
        height: u16,
        keys: &[(KeyCode, KeyModifiers)],
    ) -> (HostInteraction, CancellationSource) {
        let (mut interaction, cancellation) = entered_full_log(snapshot, width, height);
        for &(code, modifiers) in keys {
            press_key(&mut interaction, snapshot, code, modifiers, &cancellation);
        }
        (interaction, cancellation)
    }

    fn assert_quit_control(
        interaction: &mut HostInteraction,
        snapshot: &WorkflowRunViewSnapshot,
        cancellation: &CancellationSource,
        expected: HostControl,
    ) {
        assert_eq!(
            press_unmodified_key(interaction, snapshot, KeyCode::Char('q'), cancellation),
            expected
        );
    }

    fn assert_help_omits_quit(
        interaction: &mut HostInteraction,
        snapshot: &WorkflowRunViewSnapshot,
        cancellation: &CancellationSource,
    ) {
        open_help(interaction, snapshot, cancellation);
        assert!(
            !render_minimum_snapshot_text(snapshot, interaction)
                .contains("Quit the completed workflow")
        );
    }

    fn open_help(
        interaction: &mut HostInteraction,
        snapshot: &WorkflowRunViewSnapshot,
        cancellation: &CancellationSource,
    ) {
        press_key(
            interaction,
            snapshot,
            KeyCode::Char('?'),
            KeyModifiers::SHIFT,
            cancellation,
        );
    }

    fn press_unmodified_key(
        interaction: &mut HostInteraction,
        snapshot: &WorkflowRunViewSnapshot,
        code: KeyCode,
        cancellation: &CancellationSource,
    ) -> HostControl {
        press_key(
            interaction,
            snapshot,
            code,
            KeyModifiers::NONE,
            cancellation,
        )
    }

    fn press_key(
        interaction: &mut HostInteraction,
        snapshot: &WorkflowRunViewSnapshot,
        code: KeyCode,
        modifiers: KeyModifiers,
        cancellation: &CancellationSource,
    ) -> HostControl {
        interaction.handle_key(
            terminal_input_event(Event::Key(crossterm::event::KeyEvent::new(code, modifiers))),
            snapshot,
            cancellation,
        )
    }

    fn full_log_top_order(
        interaction: &HostInteraction,
        snapshot: &WorkflowRunViewSnapshot,
    ) -> Option<u64> {
        let step = &snapshot.steps[interaction.selected];
        let log = FilteredLog::new(&step.log, interaction.log_filters);
        log.records
            .get(interaction.full_log.top_index(&log))
            .map(|record| record.accepted_order.get())
    }

    fn render_full_log_snapshot(
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        width: u16,
        height: u16,
    ) -> ratatui::buffer::Buffer {
        render_snapshot(snapshot, interaction, width, height, false)
    }

    fn render_minimum_snapshot_text(
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
    ) -> String {
        buffer_text(&render_snapshot(
            snapshot,
            interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        ))
    }

    fn minimum_footer(
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
    ) -> String {
        buffer_rows(&render_snapshot(
            snapshot,
            interaction,
            MINIMUM_WIDTH,
            MINIMUM_HEIGHT,
            false,
        ))
        .pop()
        .unwrap()
    }

    fn render_too_small_text(snapshot: &WorkflowRunViewSnapshot) -> String {
        let backend = ratatui::backend::TestBackend::new(40, 8);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_too_small(frame, frame.area(), snapshot, false))
            .unwrap();
        buffer_text(terminal.backend().buffer())
    }

    fn render_snapshot(
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        width: u16,
        height: u16,
        color: bool,
    ) -> ratatui::buffer::Buffer {
        let backend = ratatui::backend::TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        let graph = DagLayout::for_steps(&snapshot.steps);
        terminal
            .draw(|frame| render(frame, snapshot, &graph, interaction, color))
            .unwrap();
        terminal.backend().buffer().clone()
    }

    fn long_log_step() -> WorkflowRunStepView {
        direct_log_step(
            StepStateKind::Running,
            vec![direct_log_record(
                1,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56Z",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                false,
            )],
            1,
            0,
        )
    }

    fn direct_agent_log_step(records: Vec<WorkflowRunLogRecord>) -> WorkflowRunStepView {
        let observed_records = u64::try_from(records.len()).unwrap();
        let mut step = direct_log_step(StepStateKind::Running, records, observed_records, 0);
        step.definition = WorkflowPresentationStep::Agent {
            profile: "test".to_owned(),
            harness: AgentPresentationHarness::Pi {
                model: "test-model".to_owned(),
                thinking: Thinking::Medium,
            },
            failure_policy: FailurePolicy::Required,
            direct_dependencies: Vec::new(),
            outputs: BTreeMap::new(),
        };
        step.outputs.clear();
        step
    }

    fn direct_log_step(
        state: StepStateKind,
        records: Vec<WorkflowRunLogRecord>,
        observed_records: u64,
        discarded_records: u64,
    ) -> WorkflowRunStepView {
        let retained_records = u64::try_from(records.len()).unwrap();
        let retained_bytes = records.iter().fold(0_u64, |total, record| {
            total.saturating_add(u64::try_from(record.payload.len()).unwrap())
        });
        let mut step =
            direct_command_step(state, None, None, WorkflowRunOutputDisposition::Pending);
        step.log = WorkflowRunStepLog {
            records,
            observed_records,
            retained_records,
            retained_bytes,
            discarded_records,
            discarded_bytes: 0,
        };
        step
    }

    fn direct_command_step(
        state: StepStateKind,
        fact: Option<ObservedStepTransition>,
        timing: Option<WorkflowRunElapsed>,
        output_disposition: WorkflowRunOutputDisposition,
    ) -> WorkflowRunStepView {
        let output = Output::FilePath {
            path: "report.txt".to_owned(),
            media_type: "text/plain".to_owned(),
        };
        WorkflowRunStepView {
            id: "selected-command".to_owned(),
            role: crate::execution::workflow::validated::WorkflowNodeRole::Step,
            definition: WorkflowPresentationStep::Command {
                argv: vec!["build".to_owned(), "héllo world".to_owned()],
                cwd: Some("work".to_owned()),
                failure_policy: FailurePolicy::Required,
                direct_dependencies: vec!["prepare".to_owned()],
                outputs: BTreeMap::from([("report".to_owned(), output)]),
            },
            state,
            fact,
            timing,
            outputs: BTreeMap::from([("report".to_owned(), output_disposition)]),
            log: WorkflowRunStepLog {
                records: Vec::new(),
                observed_records: 0,
                retained_records: 0,
                retained_bytes: 0,
                discarded_records: 0,
                discarded_bytes: 0,
            },
        }
    }

    fn direct_snapshot(step: WorkflowRunStepView) -> WorkflowRunViewSnapshot {
        WorkflowRunViewSnapshot {
            generation: 0,
            workflow_path: "workflow.yaml".to_owned(),
            maximum_parallel_steps: 1,
            workflow: WorkflowState::Executing {
                gate: SchedulingGate::Open,
            },
            timing: WorkflowRunElapsed {
                started_at: time::OffsetDateTime::UNIX_EPOCH,
                duration: Duration::from_secs(3),
                frozen: false,
            },
            steps: vec![step],
            finalization_start: None,
            cancellation: None,
            finalization: None,
            authoritative_result: false,
            quiescent: false,
            publication: WorkflowRunPublicationState::NotStarted,
            cleanup: WorkflowRunCleanupState::NotStarted,
            quit_eligible: false,
        }
    }

    fn render_direct_inspector(step: &WorkflowRunStepView, width: u16, height: u16) -> String {
        buffer_text(&render_direct_inspector_buffer(step, width, height, false))
    }

    fn render_direct_inspector_buffer(
        step: &WorkflowRunStepView,
        width: u16,
        height: u16,
        color: bool,
    ) -> ratatui::buffer::Buffer {
        let backend = ratatui::backend::TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                render_inspector(frame, frame.area(), Some(step), color, Borders::ALL);
            })
            .unwrap();
        terminal.backend().buffer().clone()
    }

    fn render_direct_log(
        step: &WorkflowRunStepView,
        width: u16,
        height: u16,
        color: bool,
    ) -> ratatui::buffer::Buffer {
        let snapshot = direct_snapshot(step.clone());
        let backend = ratatui::backend::TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                render_log(
                    frame,
                    frame.area(),
                    &snapshot,
                    &HostInteraction::default(),
                    color,
                    Borders::ALL,
                );
            })
            .unwrap();
        terminal.backend().buffer().clone()
    }

    fn buffer_rows(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
        let area = buffer.area;
        (0..area.height)
            .map(|y| {
                (0..area.width).fold(String::new(), |mut line, x| {
                    line.push_str(buffer[(x, y)].symbol());
                    line
                })
            })
            .collect()
    }

    fn inner_buffer_rows(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
        let area = buffer.area;
        (1..area.height.saturating_sub(1))
            .map(|y| {
                (1..area.width.saturating_sub(1)).fold(String::new(), |mut line, x| {
                    line.push_str(buffer[(x, y)].symbol());
                    line
                })
            })
            .map(|line| line.trim_end().to_owned())
            .collect()
    }

    fn row_containing(rows: &[String], needle: &str) -> usize {
        rows.iter().position(|row| row.contains(needle)).unwrap()
    }

    fn column_of(row: &str, needle: &str) -> u16 {
        let byte_index = row.find(needle).unwrap();
        u16::try_from(display_width(&row[..byte_index])).unwrap()
    }

    fn snapshot_from_yaml(source: &str) -> WorkflowRunViewSnapshot {
        let temporary = tempfile::tempdir().unwrap();
        std::fs::write(temporary.path().join("workflow.yaml"), source).unwrap();
        let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
        let clock = super::super::presentation::SystemObservationClock;
        WorkflowRunViewModel::new(
            &workflow,
            1,
            RunTimingObservation::new(clock.sample()),
            clock,
        )
        .snapshot()
    }

    fn render_steps_lines(
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &HostInteraction,
        width: u16,
        height: u16,
    ) -> Vec<String> {
        let backend = ratatui::backend::TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        let graph = DagLayout::for_steps(&snapshot.steps);
        terminal
            .draw(|frame| {
                render_steps(
                    frame,
                    frame.area(),
                    snapshot,
                    &graph,
                    interaction,
                    false,
                    StepPanel {
                        borders: Borders::ALL,
                        show_title: true,
                        phase_boundary: None,
                    },
                );
            })
            .unwrap();
        buffer_rows(terminal.backend().buffer())
    }

    fn buffer_position(buffer: &ratatui::buffer::Buffer, needle: &str) -> (u16, u16) {
        let rows = buffer_rows(buffer);
        let y = rows.iter().position(|row| row.contains(needle)).unwrap();
        let x = column_of(&rows[y], needle);
        (x, u16::try_from(y).unwrap())
    }

    fn buffer_text(buffer: &ratatui::buffer::Buffer) -> String {
        buffer
            .content()
            .iter()
            .fold(String::new(), |mut rendered, cell| {
                rendered.push_str(cell.symbol());
                rendered
            })
    }
}
