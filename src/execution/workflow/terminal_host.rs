mod dag_layout;

use std::io::{self, Write};
use std::time::Duration;

use crossterm::cursor::{Hide, Show};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use futures_util::StreamExt as _;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph};
use ratatui::{Frame, Terminal, TerminalOptions, Viewport};
use rustix::termios::{OptionalActions, Termios, tcgetattr, tcgetwinsize, tcsetattr};
use time::UtcOffset;
use tokio::sync::oneshot;
use unicode_segmentation::UnicodeSegmentation as _;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use self::dag_layout::DagLayout;
use super::admission::{CancellationReason, CancellationSource};
use super::observation::{CommandOutputSource, ObservedStepTransition};
use super::presentation::{
    PresentationFailure, PresentationFailureOperation, cancellation_reason, failure_detail,
    header_timestamp, human_duration, shell_quote, step_kind, visible_text,
};
use super::presentation_feed::{AcceptedRecordOrder, WorkflowPresentationStep};
use super::run_view_model::{
    WorkflowRunCleanupResult, WorkflowRunCleanupState, WorkflowRunLogRecord,
    WorkflowRunOutputDisposition, WorkflowRunOutputUnavailableReason, WorkflowRunPublicationResult,
    WorkflowRunPublicationState, WorkflowRunStepLog, WorkflowRunStepView, WorkflowRunViewModel,
    WorkflowRunViewSnapshot,
};
use super::runtime::{NotRunReason, SchedulingGate, StepStateKind, WorkflowState};
use super::step_runtime::StepFailureCause;

const MINIMUM_WIDTH: u16 = 64;
const MINIMUM_HEIGHT: u16 = 20;
const WIDE_LAYOUT_WIDTH: u16 = 100;
const TWO_COLUMN_INSPECTOR_WIDTH: u16 = 72;
const MINIMUM_INSPECTOR_HEIGHT: u16 = 8;
const MINIMUM_LOG_HEIGHT: u16 = 4;
const REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const KIND_COLUMN_WIDTH: usize = 5;
const MINIMUM_DETAIL_WIDTH: usize = 12;
const LOG_TIMESTAMP_WIDTH: usize = 12;
const LOG_SOURCE_WIDTH: usize = 6;
const LOG_SEPARATOR_WIDTH: usize = 3;
const LOG_SOURCE_GUTTER_WIDTH: usize = LOG_SOURCE_WIDTH + LOG_SEPARATOR_WIDTH;
const LOG_TIMESTAMPED_GUTTER_WIDTH: usize = LOG_TIMESTAMP_WIDTH + 1 + LOG_SOURCE_GUTTER_WIDTH;
const MINIMUM_TIMESTAMPED_LOG_CONTENT_WIDTH: usize = 12;

pub(crate) struct WorkflowTerminalHost {
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<TerminalHostExit, PresentationFailure>>,
    cancellation: CancellationSource,
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
        let mut terminal = match TerminalSession::enter() {
            Ok(terminal) => terminal,
            Err(error) => {
                cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
                return Err(presentation_failure(
                    PresentationFailureOperation::TerminalSetup,
                    &error,
                ));
            }
        };
        let mut interaction = HostInteraction::default();
        if let Err(error) = terminal.draw(
            &view.snapshot_for_render(interaction.selected),
            &mut interaction,
            color,
        ) {
            cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
            let failure = presentation_failure(PresentationFailureOperation::TerminalDraw, &error);
            let _ = terminal.restore();
            return Err(failure);
        }

        let (shutdown, receiver) = oneshot::channel();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(run_terminal(
            terminal,
            view,
            task_cancellation,
            color,
            receiver,
            interaction,
        ));
        Ok(Self {
            shutdown: Some(shutdown),
            task,
            cancellation,
        })
    }

    pub(crate) async fn wait(mut self) -> Result<TerminalHostExit, PresentationFailure> {
        let shutdown = self.shutdown.take();
        let cancellation = self.cancellation.clone();
        let result = self.task.await;
        drop(shutdown);
        Self::join_result(&cancellation, result)
    }

    pub(crate) async fn stop(mut self) -> Result<TerminalHostExit, PresentationFailure> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let cancellation = self.cancellation.clone();
        let result = self.task.await;
        Self::join_result(&cancellation, result)
    }

    fn join_result(
        cancellation: &CancellationSource,
        result: Result<Result<TerminalHostExit, PresentationFailure>, tokio::task::JoinError>,
    ) -> Result<TerminalHostExit, PresentationFailure> {
        match result {
            Ok(result) => result,
            Err(_) => {
                cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
                Err(PresentationFailure {
                    operation: PresentationFailureOperation::TerminalTask,
                    error_kind: None,
                    result_directory: None,
                })
            }
        }
    }
}

async fn run_terminal<Clock>(
    terminal: TerminalSession,
    view: WorkflowRunViewModel<Clock>,
    cancellation: CancellationSource,
    color: bool,
    mut shutdown: oneshot::Receiver<()>,
    mut interaction: HostInteraction,
) -> Result<TerminalHostExit, PresentationFailure>
where
    Clock: super::run_timing::ObservationClock,
{
    let TerminalSession {
        mut surface,
        mut input,
        mut restore,
    } = terminal;
    let mut changes = view.subscribe();
    let mut redraw = redraw_interval();
    redraw.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = redraw.tick().await;
    // The execution may advance before this task subscribes, so the first tick must
    // refresh the setup-time frame even when no later notification is observed.
    let mut dirty = true;

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                return restore_terminal(&mut restore, TerminalHostExit::Stopped, &cancellation);
            }
            event = input.next_event() => {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        return fail_terminal(
                            &mut restore,
                            PresentationFailureOperation::TerminalInput,
                            &error,
                            &cancellation,
                        );
                    }
                };
                if event == TerminalInputEvent::Resize {
                    match surface.resize() {
                        Ok(area) => interaction.terminal_area = area,
                        Err(error) => {
                            return fail_terminal(
                                &mut restore,
                                PresentationFailureOperation::TerminalDraw,
                                &error,
                                &cancellation,
                            );
                        }
                    }
                } else {
                    let snapshot = view.snapshot_for_render(interaction.selected);
                    if interaction.handle_key(event, &snapshot, &cancellation) == HostControl::Quit
                    {
                        return restore_terminal(
                            &mut restore,
                            TerminalHostExit::Quit,
                            &cancellation,
                        );
                    }
                }
                dirty = true;
            }
            changed = changes.changed() => {
                if changed.is_ok() {
                    let _ = changes.borrow_and_update();
                    dirty = true;
                }
            }
            _ = redraw.tick() => {
                let snapshot = view.snapshot_for_render(interaction.selected);
                if dirty || !snapshot.timing.frozen {
                    if let Err(error) = surface.draw(&snapshot, &mut interaction, color) {
                        return fail_terminal(
                            &mut restore,
                            PresentationFailureOperation::TerminalDraw,
                            &error,
                            &cancellation,
                        );
                    }
                    dirty = false;
                }
            }
        }
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "redraw_interval is the terminal host boundary for coalesced redraw timing"
)]
fn redraw_interval() -> tokio::time::Interval {
    tokio::time::interval(REDRAW_INTERVAL)
}

fn restore_terminal(
    restore: &mut TerminalRestore,
    exit: TerminalHostExit,
    cancellation: &CancellationSource,
) -> Result<TerminalHostExit, PresentationFailure> {
    restore.restore().map_or_else(
        |error| {
            cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
            Err(presentation_failure(
                PresentationFailureOperation::TerminalRestore,
                &error,
            ))
        },
        |()| Ok(exit),
    )
}

fn fail_terminal(
    restore: &mut TerminalRestore,
    operation: PresentationFailureOperation,
    error: &io::Error,
    cancellation: &CancellationSource,
) -> Result<TerminalHostExit, PresentationFailure> {
    cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
    let failure = presentation_failure(operation, error);
    let _ = restore.restore();
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

struct TerminalSession {
    surface: TerminalSurface,
    input: TerminalInput,
    restore: TerminalRestore,
}

impl TerminalSession {
    fn enter() -> io::Result<Self> {
        let mut restore = TerminalRestore::enter_raw_mode()?;
        let input = TerminalInput::new();
        let area = selected_output_area()?;
        let mut output = io::stdout();
        restore.alternate_screen = true;
        execute!(output, EnterAlternateScreen, Hide)?;
        let terminal = Terminal::with_options(
            CrosstermBackend::new(output),
            TerminalOptions {
                viewport: Viewport::Fixed(area),
            },
        )?;
        Ok(Self {
            surface: TerminalSurface {
                terminal,
                graph: None,
            },
            input,
            restore,
        })
    }

    fn draw(
        &mut self,
        snapshot: &WorkflowRunViewSnapshot,
        interaction: &mut HostInteraction,
        color: bool,
    ) -> io::Result<()> {
        self.surface.draw(snapshot, interaction, color)
    }

    fn restore(&mut self) -> io::Result<()> {
        self.restore.restore()
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
        interaction.clamp_selection(snapshot.steps.len());
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
        if self.restored {
            return Ok(());
        }
        self.restored = true;
        let mut first_error = None;
        if self.alternate_screen {
            let mut output = io::stdout();
            retain_first_error(
                execute!(output, Show, LeaveAlternateScreen).and_then(|()| output.flush()),
                &mut first_error,
            );
        }
        let input = io::stdin();
        retain_first_error(
            tcsetattr(&input, OptionalActions::Now, &self.original_input_mode)
                .map_err(io::Error::from),
            &mut first_error,
        );
        first_error.map_or(Ok(()), Err)
    }
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

#[derive(Default)]
struct HostInteraction {
    selected: usize,
    surface: HostSurface,
    help_visible: bool,
    terminal_area: Rect,
    full_log: FullLogInteraction,
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
    fn synchronize(&mut self, log: &WorkflowRunStepLog, available_width: usize, rows: usize) {
        self.available_width = available_width;
        self.available_rows = rows;
        if self.follow {
            self.anchor = None;
            self.anchor_clamped = false;
        } else if log.records.is_empty() {
            self.anchor = None;
        } else if let Some(anchor) = self.anchor {
            if log
                .records
                .binary_search_by_key(&anchor, |record| record.accepted_order)
                .is_err()
            {
                self.anchor = log.records.first().map(|record| record.accepted_order);
                self.anchor_clamped = true;
            }
        } else {
            self.anchor = log.records.first().map(|record| record.accepted_order);
        }
        self.horizontal_offset = self
            .horizontal_offset
            .min(maximum_horizontal_offset(log, available_width));
    }

    fn navigate(&mut self, log: &WorkflowRunStepLog, navigation: VerticalNavigation) {
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

    fn pan(&mut self, log: &WorkflowRunStepLog, right: bool) {
        self.synchronize(log, self.available_width, self.available_rows);
        if right {
            self.horizontal_offset = self
                .horizontal_offset
                .saturating_add(1)
                .min(maximum_horizontal_offset(log, self.available_width));
        } else {
            self.horizontal_offset = self.horizontal_offset.saturating_sub(1);
        }
    }

    fn resume_follow(&mut self) {
        self.follow = true;
        self.anchor = None;
        self.anchor_clamped = false;
    }

    fn top_index(&self, log: &WorkflowRunStepLog) -> usize {
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

    fn lines_behind(&self, log: &WorkflowRunStepLog) -> usize {
        self.lines_behind_from(log, self.top_index(log))
    }

    fn lines_behind_from(&self, log: &WorkflowRunStepLog, top: usize) -> usize {
        log.records
            .len()
            .saturating_sub(top.saturating_add(self.available_rows))
    }
}

fn maximum_horizontal_offset(log: &WorkflowRunStepLog, available_width: usize) -> usize {
    let gutter = LogGutter::for_width(available_width);
    log.records
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

impl HostInteraction {
    fn clamp_selection(&mut self, step_count: usize) {
        if step_count == 0 {
            self.selected = 0;
        } else if self.selected >= step_count {
            self.selected = step_count - 1;
        }
    }

    fn handle_key(
        &mut self,
        event: TerminalInputEvent,
        snapshot: &WorkflowRunViewSnapshot,
        cancellation: &CancellationSource,
    ) -> HostControl {
        self.clamp_selection(snapshot.steps.len());

        if event == TerminalInputEvent::Cancel {
            if cancellation_available(snapshot) {
                cancellation.request_cancellation(CancellationReason::UserRequest);
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

        if self.surface == HostSurface::FullLog
            && let Some(step) = snapshot.steps.get(self.selected)
        {
            let (width, rows) = full_log_record_dimensions(self.terminal_area, &step.log);
            self.full_log.synchronize(&step.log, width, rows);
        }

        if self.surface == HostSurface::FullLog
            && let (Some(step), Some(navigation)) = (
                snapshot.steps.get(self.selected),
                vertical_navigation(event),
            )
        {
            self.full_log.navigate(&step.log, navigation);
            return HostControl::Continue;
        }

        match event {
            TerminalInputEvent::Enter
                if self.surface == HostSurface::Split && !snapshot.steps.is_empty() =>
            {
                self.surface = HostSurface::FullLog;
                self.full_log = FullLogInteraction::default();
                if let Some(step) = snapshot.steps.get(self.selected) {
                    let (width, rows) = full_log_record_dimensions(self.terminal_area, &step.log);
                    self.full_log.synchronize(&step.log, width, rows);
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
                    self.full_log
                        .pan(&step.log, event == TerminalInputEvent::PanRight);
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

fn full_log_record_dimensions(area: Rect, log: &WorkflowRunStepLog) -> (usize, usize) {
    let log_height = area.height.saturating_sub(5);
    let inner_rows = usize::from(log_height.saturating_sub(2));
    let marker_rows = usize::from(log.discarded_records != 0);
    (
        usize::from(area.width.saturating_sub(2)),
        inner_rows.saturating_sub(marker_rows),
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

    let sections = Layout::vertical([
        Constraint::Length(4),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .split(area);
    render_header(frame, sections[0], snapshot, color);
    if interaction.surface == HostSurface::FullLog {
        if let Some(step) = snapshot.steps.get(interaction.selected) {
            let (width, rows) = full_log_record_dimensions(area, &step.log);
            interaction.full_log.synchronize(&step.log, width, rows);
        }
        render_full_log(
            frame,
            sections[1],
            snapshot.steps.get(interaction.selected),
            &interaction.full_log,
            color,
        );
        render_contextual_footer(
            frame,
            sections[2],
            snapshot,
            color,
            &FULL_LOG_FOOTER_OPTIONS,
        );
    } else {
        render_split_body(frame, sections[1], snapshot, graph, interaction, color);
        render_contextual_footer(frame, sections[2], snapshot, color, &SPLIT_FOOTER_OPTIONS);
    }

    if interaction.help_visible {
        render_help_overlay(
            frame,
            area,
            interaction.surface,
            lifecycle_control(snapshot),
            color,
        );
    }
}

// Split-body composition and step-list rendering are distinct UI stages; sharing their
// identical render inputs would couple layout orchestration to widget rendering.
// jscpd:ignore-start
fn render_split_body(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    graph: &DagLayout,
    interaction: &HostInteraction,
    color: bool,
) {
    // jscpd:ignore-end
    let selected_step = snapshot.steps.get(interaction.selected);
    if area.width >= WIDE_LAYOUT_WIDTH {
        let columns = Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)])
            .split(area);
        let right = inspector_and_log_areas(
            columns[1],
            inspector_desired_height(selected_step, columns[1].width),
        );
        render_steps(frame, columns[0], snapshot, graph, interaction, color);
        render_inspector(frame, right[0], selected_step, color);
        render_log(frame, right[1], snapshot, interaction, color);
    } else {
        let dag_height = (area.height / 5).clamp(3, 8);
        let remaining_height = area.height.saturating_sub(dag_height);
        let inspector_height = bounded_inspector_height(
            remaining_height,
            inspector_desired_height(selected_step, area.width),
        );
        let rows = Layout::vertical([
            Constraint::Length(dag_height),
            Constraint::Length(inspector_height),
            Constraint::Min(MINIMUM_LOG_HEIGHT),
        ])
        .split(area);
        render_steps(frame, rows[0], snapshot, graph, interaction, color);
        render_inspector(frame, rows[1], selected_step, color);
        render_log(frame, rows[2], snapshot, interaction, color);
    }
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

fn inspector_desired_height(step: Option<&WorkflowRunStepView>, width: u16) -> u16 {
    let row_count = step.map_or(1, |step| inspector_row_count(step, width));
    u16::try_from(row_count)
        .unwrap_or(u16::MAX)
        .saturating_add(2)
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
                .title(" Scherzo workflow run "),
        ),
        area,
    );
}

fn render_header(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    color: bool,
) {
    let counts = step_counts(snapshot);
    let status = workflow_status(&snapshot.workflow);
    let first = Line::from(vec![
        Span::styled(
            visible_text(&snapshot.workflow_path),
            tone_style(color, Tone::Primary),
        ),
        Span::raw("  "),
        Span::styled(status, tone_style(color, workflow_tone(&snapshot.workflow))),
        Span::raw(format!(
            "  {}  concurrency {}  {}",
            human_duration(snapshot.timing.duration),
            snapshot.maximum_parallel_steps,
            publication_status(snapshot),
        )),
    ]);
    let second = Line::from(format!(
        "pending {} | active {} | succeeded {} | failed {} | blocked {} | not-run {} | cancelled {}",
        counts.pending,
        counts.active,
        counts.succeeded,
        counts.failed,
        counts.blocked,
        counts.not_run,
        counts.cancelled,
    ));
    frame.render_widget(
        Paragraph::new(vec![first, second])
            .block(Block::default().borders(Borders::ALL).title(" Workflow ")),
        area,
    );
}

fn render_steps(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    graph: &DagLayout,
    interaction: &HostInteraction,
    color: bool,
) {
    let available_width = usize::from(area.width.saturating_sub(2));
    let columns = StepColumns::for_steps(available_width, graph.gutter_width(), &snapshot.steps);
    let connector_style = graph_connector_style(color);
    let items =
        snapshot
            .steps
            .iter()
            .zip(graph.rows())
            .enumerate()
            .map(|(index, (step, graph_row))| {
                let selected = index == interaction.selected;
                let marker = if selected { "> " } else { "  " };
                let id = padded_text(&visible_text(&step.id), columns.id_width);
                let duration = step
                    .timing
                    .as_ref()
                    .map(|timing| human_duration(timing.duration))
                    .unwrap_or_else(|| "-".to_owned());
                let mut spans = vec![
                    Span::raw(marker),
                    Span::styled(graph_row.before_node.clone(), connector_style),
                    Span::styled(
                        step_state_glyph(step.state),
                        step_state_style(step.state, color),
                    ),
                    Span::styled(graph_row.after_node.clone(), connector_style),
                    Span::raw(" "),
                    Span::styled(id, tone_style(color, Tone::Primary)),
                ];
                if columns.kind {
                    spans.push(Span::raw("  "));
                    spans.push(Span::styled(
                        format!("{:<KIND_COLUMN_WIDTH$}", step_kind(&step.definition)),
                        tone_style(color, Tone::Muted),
                    ));
                }
                spans.push(Span::raw("  "));
                spans.push(Span::styled(
                    padded_text(&duration, columns.duration_width),
                    tone_style(color, Tone::Muted),
                ));
                if columns.detail {
                    spans.push(Span::raw("  "));
                    if let Some(detail) = step_detail(step) {
                        spans.push(Span::styled(
                            fit_text(&visible_text(&detail), columns.detail_width),
                            tone_style(color, Tone::Muted),
                        ));
                    }
                }
                let item = ListItem::new(Line::from(spans));
                if selected {
                    item.style(Style::default().add_modifier(Modifier::REVERSED))
                } else {
                    item
                }
            });
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Steps ({}) ", snapshot.steps.len())),
    );
    let mut state = ListState::default();
    if !snapshot.steps.is_empty() {
        state.select(Some(interaction.selected));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

#[derive(Clone)]
struct InspectorField {
    label: &'static str,
    value: String,
    tone: Tone,
}

impl InspectorField {
    fn new(label: &'static str, value: impl AsRef<str>, tone: Tone) -> Self {
        Self {
            label,
            value: visible_text(value.as_ref()),
            tone,
        }
    }
}

#[derive(Clone)]
enum InspectorRow {
    Fields(Vec<InspectorField>),
    Section(&'static str),
}

impl InspectorRow {
    fn item_count(&self) -> usize {
        match self {
            Self::Fields(fields) => fields.len(),
            Self::Section(_) => 0,
        }
    }
}

fn render_inspector(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&WorkflowRunStepView>,
    color: bool,
) {
    let Some(step) = step else {
        frame.render_widget(
            Paragraph::new("No workflow steps.").block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Selected step "),
            ),
            area,
        );
        return;
    };

    let total_rows = inspector_row_count(step, area.width);
    let total_items = inspector_item_count(step);
    let available_rows = usize::from(area.height.saturating_sub(2));
    let overflowing = total_rows > available_rows;
    let regular_row_limit = if overflowing {
        available_rows.saturating_sub(1)
    } else {
        total_rows
    };
    let rows = inspector_rows(step, area.width, regular_row_limit);
    let rendered_items = rows.iter().map(InspectorRow::item_count).sum::<usize>();
    let mut lines = rows
        .iter()
        .map(|row| inspector_row_line(row, area.width, color))
        .collect::<Vec<_>>();
    if overflowing && available_rows != 0 {
        let omitted = total_items.saturating_sub(rendered_items);
        lines.push(Line::from(Span::styled(
            format!("+{omitted} more"),
            tone_style(color, Tone::Muted),
        )));
    }

    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Selected step "),
        ),
        area,
    );
}

fn inspector_row_count(step: &WorkflowRunStepView, width: u16) -> usize {
    let column_count = inspector_column_count(width);
    let field_rows = inspector_fixed_field_count(step).div_ceil(column_count);
    if step.outputs.is_empty() {
        field_rows
    } else {
        field_rows
            .saturating_add(1)
            .saturating_add(step.outputs.len().div_ceil(column_count))
    }
}

fn inspector_item_count(step: &WorkflowRunStepView) -> usize {
    inspector_fixed_field_count(step).saturating_add(step.outputs.len())
}

fn inspector_fixed_field_count(step: &WorkflowRunStepView) -> usize {
    let mut count = 3;
    if inspector_timing(step).is_some() {
        count += 2;
    }
    if inspector_fact_is_visible(step.fact.as_ref()) {
        count += 1;
    }
    if matches!(step.definition, WorkflowPresentationStep::Command { .. }) {
        count += 3;
    }
    count
}

fn inspector_fact_is_visible(fact: Option<&ObservedStepTransition>) -> bool {
    matches!(
        fact,
        Some(
            ObservedStepTransition::Failed { .. }
                | ObservedStepTransition::Blocked { .. }
                | ObservedStepTransition::NotRun { .. }
                | ObservedStepTransition::Cancelling { .. }
                | ObservedStepTransition::Cancelled { .. }
        )
    )
}

fn inspector_rows(
    step: &WorkflowRunStepView,
    width: u16,
    maximum_rows: usize,
) -> Vec<InspectorRow> {
    let column_count = inspector_column_count(width);
    let column_width = inspector_column_width(width, column_count);
    let fixed_field_count = inspector_fixed_field_count(step);
    let fixed_row_count = fixed_field_count.div_ceil(column_count);
    let fixed_rows_to_render = fixed_row_count.min(maximum_rows);
    let maximum_fields = fixed_rows_to_render
        .saturating_mul(column_count)
        .min(fixed_field_count);
    let fields = inspector_fields(step, column_width, maximum_fields);
    let mut rows = fields
        .chunks(column_count)
        .map(|fields| InspectorRow::Fields(fields.to_vec()))
        .collect::<Vec<_>>();
    if rows.len() >= maximum_rows || step.outputs.is_empty() {
        return rows;
    }

    rows.push(InspectorRow::Section("Outputs"));
    if rows.len() >= maximum_rows {
        return rows;
    }

    let maximum_output_fields = maximum_rows
        .saturating_sub(rows.len())
        .saturating_mul(column_count);
    let output_fields = step
        .outputs
        .iter()
        .take(maximum_output_fields)
        .map(|(name, disposition)| {
            let (disposition, tone) = output_disposition(*disposition);
            InspectorField::new("Output", format!("{name} · {disposition}"), tone)
        })
        .collect::<Vec<_>>();
    rows.extend(
        output_fields
            .chunks(column_count)
            .map(|fields| InspectorRow::Fields(fields.to_vec())),
    );
    rows
}

fn inspector_fields(
    step: &WorkflowRunStepView,
    column_width: usize,
    maximum_fields: usize,
) -> Vec<InspectorField> {
    let mut fields = Vec::with_capacity(maximum_fields);
    push_inspector_field(&mut fields, maximum_fields, || {
        InspectorField::new("ID", &step.id, Tone::Primary)
    });
    push_inspector_field(&mut fields, maximum_fields, || {
        InspectorField::new("Kind", step_kind(&step.definition), Tone::Neutral)
    });
    push_inspector_field(&mut fields, maximum_fields, || {
        InspectorField::new(
            "State",
            step_state_label(step.state),
            step_state_tone(step.state),
        )
    });

    let timing = inspector_timing(step);
    if let Some(timing) = timing {
        push_inspector_field(&mut fields, maximum_fields, || {
            let duration = if timing.frozen && step_state_is_active(step.state) {
                format!("{} (interrupted)", human_duration(timing.duration))
            } else {
                human_duration(timing.duration)
            };
            InspectorField::new("Duration", duration, Tone::Neutral)
        });
    }
    if fields.len() < maximum_fields
        && let Some(fact) = inspector_fact(step.fact.as_ref())
    {
        fields.push(fact);
    }
    if let Some(timing) = timing {
        push_inspector_field(&mut fields, maximum_fields, || {
            InspectorField::new(
                "Started",
                header_timestamp(timing.started_at),
                Tone::Neutral,
            )
        });
    }
    if let WorkflowPresentationStep::Command {
        argv,
        cwd,
        direct_dependencies,
        ..
    } = &step.definition
    {
        push_inspector_field(&mut fields, maximum_fields, || {
            let command = argv
                .iter()
                .map(|argument| shell_quote(argument))
                .collect::<Vec<_>>()
                .join(" ");
            InspectorField::new("Command", command, Tone::Neutral)
        });
        push_inspector_field(&mut fields, maximum_fields, || {
            InspectorField::new("Directory", cwd.as_deref().unwrap_or("."), Tone::Neutral)
        });
        push_inspector_field(&mut fields, maximum_fields, || {
            let dependency_width = column_width.saturating_sub(14);
            InspectorField::new(
                "Dependencies",
                summarize_repeated_values(direct_dependencies, dependency_width),
                Tone::Neutral,
            )
        });
    }
    fields
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

fn inspector_column_count(width: u16) -> usize {
    if width >= TWO_COLUMN_INSPECTOR_WIDTH {
        2
    } else {
        1
    }
}

fn inspector_column_width(width: u16, column_count: usize) -> usize {
    let inner_width = usize::from(width.saturating_sub(2));
    if column_count == 2 {
        inner_width.saturating_sub(2) / 2
    } else {
        inner_width
    }
}

fn inspector_timing(
    step: &WorkflowRunStepView,
) -> Option<&super::run_view_model::WorkflowRunElapsed> {
    if matches!(
        step.state,
        StepStateKind::Pending | StepStateKind::Blocked | StepStateKind::NotRun
    ) {
        None
    } else {
        step.timing.as_ref()
    }
}

fn inspector_fact(fact: Option<&ObservedStepTransition>) -> Option<InspectorField> {
    match fact? {
        ObservedStepTransition::Failed { phase, cause } => Some(InspectorField::new(
            "Failure",
            failure_detail(*phase, cause),
            Tone::Failure,
        )),
        ObservedStepTransition::Blocked { dependency } => {
            Some(InspectorField::new("Blocked by", dependency, Tone::Blocked))
        }
        ObservedStepTransition::NotRun { .. } => {
            Some(InspectorField::new("Not run", "failure_stop", Tone::Muted))
        }
        ObservedStepTransition::Cancelling { reason }
        | ObservedStepTransition::Cancelled { reason } => Some(InspectorField::new(
            "Cancellation",
            cancellation_reason(*reason),
            Tone::Blocked,
        )),
        ObservedStepTransition::OutputsCommitted { .. } => None,
    }
}

fn output_disposition(disposition: WorkflowRunOutputDisposition) -> (String, Tone) {
    match disposition {
        WorkflowRunOutputDisposition::Pending => ("pending".to_owned(), Tone::Muted),
        WorkflowRunOutputDisposition::Committed => ("committed".to_owned(), Tone::Output),
        WorkflowRunOutputDisposition::Unavailable(reason) => {
            let reason = match reason {
                WorkflowRunOutputUnavailableReason::Failed => "failed",
                WorkflowRunOutputUnavailableReason::Blocked => "blocked",
                WorkflowRunOutputUnavailableReason::NotRun => "not-run",
                WorkflowRunOutputUnavailableReason::Cancelled => "cancelled",
            };
            (format!("unavailable ({reason})"), Tone::Blocked)
        }
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

fn inspector_row_line(row: &InspectorRow, width: u16, color: bool) -> Line<'static> {
    match row {
        InspectorRow::Section(title) => Line::from(Span::styled(
            (*title).to_owned(),
            tone_style(color, Tone::Primary),
        )),
        InspectorRow::Fields(fields) => {
            let column_count = inspector_column_count(width);
            let column_width = inspector_column_width(width, column_count);
            let mut spans = Vec::new();
            for (index, field) in fields.iter().enumerate() {
                spans.extend(inspector_field_spans(
                    field,
                    column_width,
                    index + 1 < fields.len(),
                    color,
                ));
                if index + 1 < fields.len() {
                    spans.push(Span::raw("  "));
                }
            }
            Line::from(spans)
        }
    }
}

fn inspector_field_spans(
    field: &InspectorField,
    maximum_width: usize,
    pad: bool,
    color: bool,
) -> Vec<Span<'static>> {
    let raw_label = format!("{}: ", field.label);
    let label = ellipsize(&raw_label, maximum_width);
    let label_width = display_width(&label);
    let value = if label_width < maximum_width {
        ellipsize(&field.value, maximum_width - label_width)
    } else {
        String::new()
    };
    let used_width = label_width.saturating_add(display_width(&value));
    let mut spans = vec![Span::styled(label, tone_style(color, Tone::Muted))];
    if !value.is_empty() {
        spans.push(Span::styled(value, tone_style(color, field.tone)));
    }
    if pad && used_width < maximum_width {
        spans.push(Span::raw(" ".repeat(maximum_width - used_width)));
    }
    spans
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
    fn for_steps(available: usize, gutter_width: usize, steps: &[WorkflowRunStepView]) -> Self {
        let id_width = steps
            .iter()
            .map(|step| display_width(&visible_text(&step.id)))
            .max()
            .unwrap_or(0);
        let duration_width = steps
            .iter()
            .map(|step| {
                step.timing
                    .as_ref()
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

fn step_detail(step: &WorkflowRunStepView) -> Option<String> {
    match &step.fact {
        Some(ObservedStepTransition::OutputsCommitted { outputs }) => {
            Some(output_count_detail(outputs.len()))
        }
        Some(ObservedStepTransition::Failed { phase, cause }) => {
            Some(failure_detail(*phase, cause))
        }
        Some(ObservedStepTransition::Blocked { dependency }) => {
            Some(format!("blocked by {}", visible_text(dependency)))
        }
        Some(ObservedStepTransition::NotRun {
            reason: NotRunReason::FailureStop,
        }) => Some("failure_stop".to_owned()),
        Some(ObservedStepTransition::Cancelling { reason })
        | Some(ObservedStepTransition::Cancelled { reason }) => {
            Some(cancellation_reason(*reason).to_owned())
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

fn output_count_detail(count: usize) -> String {
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

fn render_log(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    interaction: &HostInteraction,
    color: bool,
) {
    let Some(step) = snapshot.steps.get(interaction.selected) else {
        render_missing_step_log(frame, area);
        return;
    };
    let available_width = usize::from(area.width.saturating_sub(2));
    let available_rows = usize::from(area.height.saturating_sub(2));
    let lines = log_tail_lines(step, available_width, available_rows, color);
    frame.render_widget(
        Paragraph::new(Text::from(lines)).block(
            Block::default().borders(Borders::ALL).title(log_title(
                step,
                LogTitleStatus::Following,
                color,
            )),
        ),
        area,
    );
}

fn render_missing_step_log(frame: &mut Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new("No workflow steps.").block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Selected step log "),
        ),
        area,
    );
}

fn render_full_log(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&WorkflowRunStepView>,
    interaction: &FullLogInteraction,
    color: bool,
) {
    let Some(step) = step else {
        render_missing_step_log(frame, area);
        return;
    };
    let status = if interaction.follow {
        LogTitleStatus::Following
    } else {
        LogTitleStatus::Paused {
            lines_behind: interaction.lines_behind(&step.log),
        }
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .title(log_title(step, status, color));
    let mut records_area = block.inner(area);
    frame.render_widget(block, area);

    if step.log.discarded_records != 0 && records_area.height != 0 {
        let marker_area = Rect::new(records_area.x, records_area.y, records_area.width, 1);
        frame.render_widget(
            Paragraph::new(log_eviction_line(
                step.log.discarded_records,
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
    if step.log.records.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                empty_log_message(step.state),
                tone_style(color, Tone::Muted),
            ))),
            records_area,
        );
        return;
    }

    let available_width = usize::from(records_area.width);
    let top = interaction.top_index(&step.log);
    let lines = step
        .log
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

fn log_title(step: &WorkflowRunStepView, status: LogTitleStatus, color: bool) -> Line<'static> {
    let (status, tone) = match status {
        LogTitleStatus::Following => ("following".to_owned(), Tone::Active),
        LogTitleStatus::Paused { lines_behind } => {
            let line_label = if lines_behind == 1 { "line" } else { "lines" };
            (
                format!("paused | {lines_behind} {line_label} behind"),
                Tone::Blocked,
            )
        }
    };
    Line::from(vec![
        Span::raw(" "),
        Span::styled(status, tone_style(color, tone)),
        Span::styled(
            format!(
                " | lines: {} retained / {} total | ",
                step.log.retained_records, step.log.observed_records
            ),
            tone_style(color, Tone::Muted),
        ),
        Span::styled(
            format!("{} log ", visible_text(&step.id)),
            tone_style(color, Tone::Primary),
        ),
    ])
}

fn log_tail_lines(
    step: &WorkflowRunStepView,
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
        lines.push(log_eviction_line(step.log.discarded_records, false, color));
        available_rows.saturating_sub(1)
    };
    if tail_rows == 0 {
        return lines;
    }

    if step.log.records.is_empty() {
        lines.push(Line::from(Span::styled(
            empty_log_message(step.state),
            tone_style(color, Tone::Muted),
        )));
        return lines;
    }

    let record_lines = step
        .log
        .records
        .iter()
        .flat_map(|record| log_record_lines(record, available_width, color))
        .collect::<Vec<_>>();
    let first_visible_line = record_lines.len().saturating_sub(tail_rows);
    lines.extend(record_lines.into_iter().skip(first_visible_line));
    lines
}

fn log_eviction_line(discarded_records: u64, anchor_clamped: bool, color: bool) -> Line<'static> {
    let line_label = if discarded_records == 1 {
        "line"
    } else {
        "lines"
    };
    let clamp_notice = if anchor_clamped {
        " | clamped to retained top"
    } else {
        ""
    };
    Line::from(Span::styled(
        format!("↑ {discarded_records} older {line_label} discarded{clamp_notice}"),
        tone_style(color, Tone::Muted),
    ))
}

fn empty_log_message(state: StepStateKind) -> &'static str {
    match state {
        StepStateKind::Pending => "Waiting for this step to start.",
        StepStateKind::Starting
        | StepStateKind::Running
        | StepStateKind::CapturingOutputs
        | StepStateKind::Cancelling => "Waiting for output…",
        StepStateKind::Succeeded
        | StepStateKind::Failed
        | StepStateKind::Blocked
        | StepStateKind::NotRun
        | StepStateKind::Cancelled => "No output received.",
    }
}

fn log_record_lines(
    record: &WorkflowRunLogRecord,
    available_width: usize,
    color: bool,
) -> Vec<Line<'static>> {
    let gutter = LogGutter::for_width(available_width);
    let content_width = available_width.saturating_sub(gutter.width()).max(1);
    wrap_log_payload(&record.payload, content_width)
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let row_kind = if index == 0 {
                LogRowKind::for_record(record)
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
    spans.push(Span::styled(payload, tone_style(color, Tone::Neutral)));
    Line::from(spans)
}

fn wrap_log_payload(payload: &str, maximum_width: usize) -> Vec<String> {
    if payload.is_empty() {
        return vec![String::new()];
    }

    let mut lines = Vec::new();
    let mut line = String::new();
    let mut line_width = 0_usize;
    for grapheme in payload.graphemes(true) {
        let grapheme_width = display_width(grapheme);
        if !line.is_empty() && line_width.saturating_add(grapheme_width) > maximum_width {
            lines.push(std::mem::take(&mut line));
            line_width = 0;
        }
        line.push_str(grapheme);
        line_width = line_width.saturating_add(grapheme_width);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

#[derive(Clone, Copy)]
enum LogRowKind {
    Record,
    SafetyContinuation,
    VisualContinuation,
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
        if self.timestamp {
            let timestamp = if matches!(row_kind, LogRowKind::VisualContinuation) {
                " ".repeat(LOG_TIMESTAMP_WIDTH)
            } else {
                log_timestamp(record.observed_at)
            };
            spans.push(Span::styled(timestamp, tone_style(color, Tone::Muted)));
            spans.push(Span::raw(" "));
        }
        let source = match record.source {
            CommandOutputSource::StandardOutput => "stdout",
            CommandOutputSource::StandardError => "stderr",
        };
        spans.push(Span::styled(
            format!("{source:<LOG_SOURCE_WIDTH$}"),
            tone_style(color, Tone::Muted),
        ));
        let marker = match row_kind {
            LogRowKind::Record => " │ ",
            LogRowKind::SafetyContinuation => " ↪ ",
            LogRowKind::VisualContinuation => " ↳ ",
        };
        spans.push(Span::styled(marker, tone_style(color, Tone::Muted)));
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

fn cancellation_available(snapshot: &WorkflowRunViewSnapshot) -> bool {
    matches!(
        &snapshot.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open | SchedulingGate::FailureStopped { .. },
        }
    )
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
    &["↑/k previous", "↓/j next", "Enter inspect log"],
    &["↑/k up", "↓/j down", "Enter log"],
    &["↑/k", "↓/j", "Enter"],
];

const FULL_LOG_FOOTER_OPTIONS: [&[&str]; 3] = [
    &[
        "Esc back",
        "↑↓/jk move",
        "PgUp/b PgDn/f/Space",
        "u/Ctrl-U d/Ctrl-D half",
        "g/G ends",
        "←→/hl pan",
        "F follow",
    ],
    &["Esc back", "↑↓/jk", "PgUp/PgDn", "←→/hl pan", "F follow"],
    &["Esc back", "↑↓/jk", "F follow"],
];

fn render_contextual_footer(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    color: bool,
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
    render_footer_text(frame, area, fitting_footer(options, area.width), color);
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
        (LifecycleControl::Cancel, false) => Some("Ctrl-C cancel"),
        (LifecycleControl::Cancel, true) => Some("Ctrl-C"),
        (LifecycleControl::Quit, _) => Some("q quit"),
        (LifecycleControl::None, _) => None,
    };
    if let Some(lifecycle) = lifecycle {
        parts.push(lifecycle.to_owned());
    }
    parts.push("? help".to_owned());
    parts.join(" | ")
}

fn fitting_footer(options: Vec<String>, width: u16) -> String {
    let available = usize::from(width);
    options
        .iter()
        .find(|option| display_width(option) <= available)
        .cloned()
        .unwrap_or_else(|| ellipsize(options.last().map_or("? help", String::as_str), available))
}

fn render_footer_text(frame: &mut Frame<'_>, area: Rect, text: String, color: bool) {
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            text,
            tone_style(color, Tone::Muted),
        ))),
        area,
    );
}

fn render_help_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    surface: HostSurface,
    lifecycle: LifecycleControl,
    color: bool,
) {
    let area = Rect::new(
        area.x.saturating_add(2),
        area.y.saturating_add(1),
        area.width.saturating_sub(4),
        area.height.saturating_sub(2),
    );
    let (title, mut lines) = match surface {
        HostSurface::Split => (
            " Split view help · Esc closes ",
            vec![
                help_line("↑ / k", "Select previous step", color),
                help_line("↓ / j", "Select next step", color),
                help_line("Enter", "Inspect selected step log", color),
                help_line("?", "Open this help", color),
                help_line("Esc", "Close help; otherwise no action", color),
            ],
        ),
        HostSurface::FullLog => (
            " Full-screen log help · Esc closes ",
            vec![
                help_line("↑ / k, ↓ / j", "Move one record up / down", color),
                help_line("Page Up / b", "Move one viewport up", color),
                help_line("Page Down / f / Space", "Move one viewport down", color),
                help_line("u / Ctrl-U", "Move half viewport up", color),
                help_line("d / Ctrl-D", "Move half viewport down", color),
                help_line("g", "Go to first retained record", color),
                help_line("G", "Go to retained bottom (paused)", color),
                help_line("← / h, → / l", "Pan one column left / right", color),
                help_line("F", "Follow current retained bottom", color),
                help_line("Esc", "Close help; then return to split", color),
                help_line("?", "Open this help", color),
            ],
        ),
    };
    match lifecycle {
        LifecycleControl::Cancel => {
            lines.push(help_line("Ctrl-C", "Cancel the running workflow", color))
        }
        LifecycleControl::Quit => lines.push(help_line("q", "Quit the completed workflow", color)),
        LifecycleControl::None => {}
    }

    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(tone_style(color, Tone::Primary))
                .title(title),
        ),
        area,
    );
}

fn help_line(keys: &str, description: &str, color: bool) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            padded_text(keys, 22),
            tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
        ),
        Span::styled(description.to_owned(), tone_style(color, Tone::Neutral)),
    ])
}

#[derive(Default)]
struct StepCounts {
    pending: usize,
    active: usize,
    succeeded: usize,
    failed: usize,
    blocked: usize,
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
            | StepStateKind::Cancelling => counts.active += 1,
            StepStateKind::Succeeded => counts.succeeded += 1,
            StepStateKind::Failed => counts.failed += 1,
            StepStateKind::Blocked => counts.blocked += 1,
            StepStateKind::NotRun => counts.not_run += 1,
            StepStateKind::Cancelled => counts.cancelled += 1,
        }
    }
    counts
}

fn workflow_status(workflow: &WorkflowState<StepFailureCause>) -> &'static str {
    match workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => "running",
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { .. },
        } => "stopping after failure",
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        } => "cancelling",
        WorkflowState::Succeeded => "succeeded",
        WorkflowState::Failed { .. } => "failed",
        WorkflowState::Cancelled { .. } => "cancelled",
    }
}

fn workflow_tone(workflow: &WorkflowState<StepFailureCause>) -> Tone {
    match workflow {
        WorkflowState::Succeeded => Tone::Success,
        WorkflowState::Failed { .. } => Tone::Failure,
        WorkflowState::Cancelled { .. }
        | WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        } => Tone::Blocked,
        WorkflowState::Executing { .. } => Tone::Active,
    }
}

fn step_state_glyph(state: StepStateKind) -> &'static str {
    match state {
        StepStateKind::Pending => "○",
        StepStateKind::Starting => "◔",
        StepStateKind::Running => "●",
        StepStateKind::CapturingOutputs => "◕",
        StepStateKind::Cancelling => "◒",
        StepStateKind::Succeeded => "✓",
        StepStateKind::Failed => "×",
        StepStateKind::Blocked => "!",
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
        StepStateKind::Cancelling => "cancelling",
        StepStateKind::Succeeded => "succeeded",
        StepStateKind::Failed => "failed",
        StepStateKind::Blocked => "blocked",
        StepStateKind::NotRun => "not-run",
        StepStateKind::Cancelled => "cancelled",
    }
}

fn step_state_style(state: StepStateKind, color: bool) -> Style {
    tone_style(color, step_state_tone(state))
}

fn step_state_tone(state: StepStateKind) -> Tone {
    match state {
        StepStateKind::Starting | StepStateKind::Running | StepStateKind::CapturingOutputs => {
            Tone::Active
        }
        StepStateKind::Succeeded => Tone::Success,
        StepStateKind::Failed => Tone::Failure,
        StepStateKind::Cancelling | StepStateKind::Blocked | StepStateKind::Cancelled => {
            Tone::Blocked
        }
        StepStateKind::Pending | StepStateKind::NotRun => Tone::Muted,
    }
}

#[derive(Clone, Copy)]
enum Tone {
    Primary,
    Neutral,
    Muted,
    Active,
    Output,
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
        Tone::Neutral => return Style::default(),
        Tone::Muted => Color::Rgb(127, 132, 156),
        Tone::Active => Color::Rgb(137, 180, 250),
        Tone::Output => Color::Rgb(148, 226, 213),
        Tone::Success => Color::Rgb(166, 227, 161),
        Tone::Failure => Color::Rgb(243, 139, 168),
        Tone::Blocked => Color::Rgb(250, 179, 135),
    };
    Style::default().fg(foreground).add_modifier(Modifier::BOLD)
}

fn publication_status(snapshot: &WorkflowRunViewSnapshot) -> &'static str {
    match (&snapshot.publication, snapshot.cleanup) {
        (WorkflowRunPublicationState::NotStarted, _) => "not published",
        (WorkflowRunPublicationState::Publishing, _) => "publishing",
        (
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                ..
            }),
            WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Succeeded),
        ) => "published",
        (
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                ..
            }),
            WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Failed),
        ) => "cleanup failed",
        (
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                ..
            }),
            _,
        ) => "cleaning",
        (WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Failed(_)), _) => {
            "publication failed"
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::execution::workflow::document::Output;
    use crate::execution::workflow::observation::{
        CommandOutputObservation, ExecutionObservation, ExecutionObserver, SourceSequence,
    };
    use crate::execution::workflow::presentation_feed::AcceptedRecordOrder;
    use crate::execution::workflow::resolution;
    use crate::execution::workflow::run_timing::{
        ObservationClock, ObservationTime, RunTimingObservation,
    };
    use crate::execution::workflow::run_view_model::{WorkflowRunElapsed, WorkflowRunStepLog};
    use crate::execution::workflow::runtime::{ActionId, FailurePhase, TransitionSequence};
    use crate::execution::workflow::step_runtime::{CommandExecutionFailure, StepExecutionFailure};

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

        assert!(rendered.contains("selected-command log"));
        assert!(!rendered.contains("Steps (1)"));

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
        assert!(rendered.contains("Steps (1)"));
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
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(37));
            assert!(!interaction.full_log.follow);
        }
        for code in [KeyCode::Down, KeyCode::Char('j')] {
            let (interaction, _) =
                run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(38));
            assert!(!interaction.full_log.follow);
        }
        for code in [KeyCode::PageUp, KeyCode::Char('b')] {
            let (interaction, _) =
                run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(25));
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
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(14));
        }
        for (code, modifiers) in [
            (KeyCode::Char('u'), KeyModifiers::NONE),
            (KeyCode::Char('u'), KeyModifiers::CONTROL),
        ] {
            let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, modifiers)]);
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(32));
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
            assert_eq!(full_log_top_order(&interaction, &snapshot), Some(7));
        }
        for (code, modifiers, expected) in [
            (KeyCode::Char('g'), KeyModifiers::NONE, 1),
            (KeyCode::Char('G'), KeyModifiers::SHIFT, 38),
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
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(38));
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
        assert_eq!(anchor, Some(17));
        assert_eq!(interaction.full_log.lines_behind(&snapshot.steps[0].log), 1);

        append_log_record(&mut snapshot.steps[0].log, 31, "new output one");
        append_log_record(&mut snapshot.steps[0].log, 32, "new output two");
        let (width, rows) =
            full_log_record_dimensions(interaction.terminal_area, &snapshot.steps[0].log);
        interaction
            .full_log
            .synchronize(&snapshot.steps[0].log, width, rows);

        assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
        assert_eq!(interaction.full_log.horizontal_offset, horizontal_offset);
        assert_eq!(interaction.full_log.lines_behind(&snapshot.steps[0].log), 3);
        let rendered = buffer_text(&render_full_log_snapshot(
            &snapshot,
            &mut interaction,
            120,
            20,
        ));
        assert!(rendered.contains("paused | 3 lines behind"));

        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('F'),
            KeyModifiers::SHIFT,
            &cancellation,
        );
        assert!(interaction.full_log.follow);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(20));
        assert_eq!(interaction.full_log.lines_behind(&snapshot.steps[0].log), 0);
        assert_eq!(interaction.full_log.horizontal_offset, horizontal_offset);
    }

    #[test]
    fn paused_log_anchor_survives_terminal_resize() {
        let snapshot = direct_snapshot(numbered_log_step(40, 80));
        let (mut interaction, _) =
            run_full_log_keys(&snapshot, 80, 20, &[(KeyCode::Up, KeyModifiers::NONE)]);
        let anchor = full_log_top_order(&interaction, &snapshot);
        assert_eq!(anchor, Some(27));

        let _ = render_full_log_snapshot(&snapshot, &mut interaction, 100, 24);
        assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
        assert_eq!(interaction.full_log.available_rows, 17);

        let _ = render_full_log_snapshot(&snapshot, &mut interaction, 64, 20);
        assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
        assert_eq!(interaction.full_log.available_rows, 13);
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
            rendered.contains("30 retained / 38 total"),
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
        assert!(rendered.contains("↑ 8 older lines discarded | clamped to retained top"));
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
        let maximum = maximum_horizontal_offset(&snapshot.steps[0].log, 62);
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
    fn log_preview_preserves_merged_source_order_and_neutral_process_content() {
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
        let buffer = render_direct_log(&step, 80, 6, true);
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
        assert_eq!(buffer[(payload_column, second)].fg, Color::Reset);
        let source_column = column_of(&rows[usize::from(second)], "stderr");
        assert_eq!(
            buffer[(source_column, second)].fg,
            tone_style(true, Tone::Muted).fg.unwrap()
        );
        assert_ne!(
            buffer[(source_column, second)].fg,
            tone_style(true, Tone::Failure).fg.unwrap()
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

        let wide = buffer_rows(&render_direct_log(&step, 50, 4, false));
        assert!(
            wide.iter()
                .any(|row| row.contains("10:34:56.789 stderr │ message"))
        );

        let timestamp_boundary = buffer_rows(&render_direct_log(&step, 36, 4, false));
        assert!(
            timestamp_boundary
                .iter()
                .any(|row| row.contains("10:34:56.789 stderr │ message"))
        );

        let narrow = buffer_rows(&render_direct_log(&step, 35, 4, false));
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

        let rows = buffer_rows(&render_direct_log(&step, 80, 4, false));
        assert!(
            rows.iter()
                .any(|row| row.contains("12:34:56.200 stderr ↪ continued fragment")),
            "a safety-continuation record must retain its own timestamp and remain distinct from a visual wrap: {rows:#?}"
        );
    }

    #[test]
    fn log_preview_rewraps_deterministically_and_keeps_the_visual_tail() {
        let step = long_log_step();

        let wide = inner_buffer_rows(&render_direct_log(&step, 34, 5, false));
        assert_eq!(
            wide,
            [
                "stdout │ abcdefghijklmnopqrstuvw",
                "stdout ↳ xyzABCDEFGHIJKLMNOPQRST",
                "stdout ↳ UVWXYZ0123456789",
            ]
        );

        let narrow = inner_buffer_rows(&render_direct_log(&step, 27, 5, false));
        assert_eq!(
            narrow,
            [
                "stdout ↳ qrstuvwxyzABCDEF",
                "stdout ↳ GHIJKLMNOPQRSTUV",
                "stdout ↳ WXYZ0123456789",
            ]
        );
        assert_eq!(
            inner_buffer_rows(&render_direct_log(&step, 27, 5, false)),
            narrow
        );
    }

    #[test]
    fn log_preview_reports_counts_following_and_evicted_history() {
        let step = direct_log_step(
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
        let buffer = render_direct_log(&step, 100, 6, false);
        let rendered = buffer_text(&buffer);
        let rows = inner_buffer_rows(&buffer);

        assert!(rendered.contains("following | lines: 3 retained / 5 total"));
        assert_eq!(rows[0], "↑ 2 older lines discarded");
        assert!(rows[1].ends_with("retained one"));
        assert!(rows[2].ends_with("retained two"));
        assert!(rows[3].ends_with("retained three"));

        let minimum_height = inner_buffer_rows(&render_direct_log(&step, 100, 4, false));
        assert_eq!(minimum_height[0], "↑ 2 older lines discarded");
        assert!(minimum_height[1].ends_with("retained three"));
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
            let rows = inner_buffer_rows(&render_direct_log(&step, 70, 4, false));
            assert_eq!(rows[0], expected);
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
    fn quit_is_ignored_until_the_workflow_is_terminal() {
        let cancellation = CancellationSource::new();
        let mut interaction = HostInteraction::default();
        let mut snapshot = direct_snapshot(long_log_step());
        assert_eq!(
            press_key(
                &mut interaction,
                &snapshot,
                KeyCode::Char('q'),
                KeyModifiers::NONE,
                &cancellation,
            ),
            HostControl::Continue
        );
        snapshot.workflow = WorkflowState::Succeeded;
        snapshot.quit_eligible = true;
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
            &cancellation,
        );
        assert_eq!(cancellation.cancellation_reason(), None);
        assert_eq!(
            press_key(
                &mut interaction,
                &snapshot,
                KeyCode::Char('q'),
                KeyModifiers::NONE,
                &cancellation,
            ),
            HostControl::Quit
        );
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
                ["State: pending", "Output: report · pending"].as_slice(),
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
                ["State: running", "Duration: 1.2s", "1970-01-01 00:00:00Z"].as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Succeeded,
                    None,
                    Some(timing.clone()),
                    WorkflowRunOutputDisposition::Committed,
                ),
                ["State: succeeded", "Output: report · committed"].as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Failed,
                    Some(ObservedStepTransition::Failed {
                        phase: FailurePhase::Execution,
                        cause: StepFailureCause::Execution(StepExecutionFailure::Command(
                            CommandExecutionFailure::UnsuccessfulExit { code: Some(17) },
                        )),
                    }),
                    Some(timing.clone()),
                    WorkflowRunOutputDisposition::Unavailable(
                        WorkflowRunOutputUnavailableReason::Failed,
                    ),
                ),
                [
                    "State: failed",
                    "Failure: execution · exit 17",
                    "unavailable (failed)",
                ]
                .as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Blocked,
                    Some(ObservedStepTransition::Blocked {
                        dependency: "prepare".to_owned(),
                    }),
                    Some(timing.clone()),
                    WorkflowRunOutputDisposition::Unavailable(
                        WorkflowRunOutputUnavailableReason::Blocked,
                    ),
                ),
                [
                    "State: blocked",
                    "Blocked by: prepare",
                    "unavailable (blocked)",
                ]
                .as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::NotRun,
                    Some(ObservedStepTransition::NotRun {
                        reason: super::super::runtime::NotRunReason::FailureStop,
                    }),
                    Some(timing.clone()),
                    WorkflowRunOutputDisposition::Unavailable(
                        WorkflowRunOutputUnavailableReason::NotRun,
                    ),
                ),
                [
                    "State: not-run",
                    "Not run: failure_stop",
                    "unavailable (not-run)",
                ]
                .as_slice(),
            ),
            (
                direct_command_step(
                    StepStateKind::Cancelled,
                    Some(ObservedStepTransition::Cancelled {
                        reason: CancellationReason::UserRequest,
                    }),
                    Some(timing),
                    WorkflowRunOutputDisposition::Unavailable(
                        WorkflowRunOutputUnavailableReason::Cancelled,
                    ),
                ),
                [
                    "State: cancelled",
                    "Cancellation: user_request",
                    "unavailable (cancelled)",
                ]
                .as_slice(),
            ),
        ];

        for (step, expected) in cases {
            let rendered = render_direct_inspector(&step, 120, 14);
            assert!(rendered.contains("ID: selected-command"));
            assert!(rendered.contains("Kind: cmd"));
            assert!(rendered.contains("Command: build 'héllo world'"));
            assert!(rendered.contains("Directory: work"));
            assert!(rendered.contains("Dependencies: prepare"));
            assert!(rendered.contains("Outputs"));
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
                assert!(!rendered.contains("Duration:"));
                assert!(!rendered.contains("Started:"));
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
            unreachable!();
        };
        *direct_dependencies = (1..=8).map(|index| format!("dependency-{index}")).collect();
        for index in 2..=8 {
            let name = format!("report-{index}");
            outputs.insert(
                name.clone(),
                Output::File {
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
    fn inspector_omits_outputs_section_for_steps_without_declarations() {
        let mut step = direct_command_step(
            StepStateKind::Pending,
            None,
            None,
            WorkflowRunOutputDisposition::Pending,
        );
        let WorkflowPresentationStep::Command { outputs, .. } = &mut step.definition else {
            unreachable!();
        };
        outputs.clear();
        step.outputs.clear();

        let rendered = render_direct_inspector(&step, 80, 12);

        assert!(!rendered.contains("Outputs"));
        assert!(!rendered.contains("Output:"));
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
            super::super::run_view_model::StepLogCapacity::default(),
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

        assert!(rendered.contains("workflow.yaml"));
        assert!(rendered.contains("Steps (1)"));
        assert!(rendered.contains("build"));
        assert!(rendered.contains("compiling workflow host"));
        assert!(rendered.contains("Ctrl-C cancel"));
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
        let steps = buffer_position(&wide, "Steps (2)");
        let inspector = buffer_position(&wide, "Selected step");
        let log = buffer_position(&wide, "following | lines");
        assert_eq!(steps.1, inspector.1);
        assert!(steps.0 < inspector.0 && inspector.0 == log.0);
        assert!(log.1 > inspector.1);
        assert!(buffer_text(&wide).contains("selected-second-step"));

        let stacked = render_snapshot(&snapshot, &mut interaction, 90, 24, false);
        let steps = buffer_position(&stacked, "Steps (2)");
        let inspector = buffer_position(&stacked, "Selected step");
        let log = buffer_position(&stacked, "following | lines");
        assert!(steps.1 < inspector.1 && inspector.1 < log.1);
        assert_eq!(steps.0, inspector.0);
        assert_eq!(inspector.0, log.0);

        let too_small = render_snapshot(&snapshot, &mut interaction, 50, 12, false);
        let too_small = buffer_text(&too_small);
        assert!(too_small.contains("Terminal too small"));
        assert!(too_small.contains("64x20"));
        assert!(!too_small.contains("Steps (2)"));
        assert_eq!(interaction.selected, 1);
        assert_eq!(interaction.surface, HostSurface::Split);

        let recovered = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
        let selected_row = buffer_rows(&recovered)
            .into_iter()
            .find(|row| row.contains("> ") && row.contains("selected-second-step"))
            .unwrap();
        assert!(selected_row.contains('>'));
        assert_eq!(interaction.selected, 1);
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
        assert!(buffer_text(&stacked).contains("Full-screen log help"));
        assert_eq!(interaction.full_log.anchor, anchor);
        assert_eq!(interaction.full_log.horizontal_offset, offset);

        let too_small = render_snapshot(&snapshot, &mut interaction, 40, 8, false);
        assert!(buffer_text(&too_small).contains("Terminal too small"));
        assert!(!buffer_text(&too_small).contains("Full-screen log help"));
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
        assert!(buffer_text(&recovered).contains("Full-screen log help"));
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
        assert!(log.contains("selected-second-step log"));
        assert!(!log.contains("Steps (2)"));
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
        assert!(wide_footer.contains("↑/k previous"));
        assert!(wide_footer.contains("↓/j next"));
        assert!(wide_footer.contains("Enter inspect log"));
        assert!(wide_footer.contains("Ctrl-C cancel"));
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
        assert!(minimum_footer.contains("Enter log"));
        assert!(minimum_footer.contains("Ctrl-C cancel"));
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
        let split_help = buffer_text(&split_help);
        assert_eq!(interaction.selected, 0);
        for expected in [
            "Split view help",
            "↑ / k",
            "↓ / j",
            "Enter",
            "Ctrl-C",
            "?",
            "Esc",
        ] {
            assert!(split_help.contains(expected), "missing {expected:?}");
        }

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
        let log_help = buffer_text(&log_help);
        for expected in [
            "Full-screen log help",
            "↑ / k, ↓ / j",
            "Page Up / b",
            "Page Down / f / Space",
            "u / Ctrl-U",
            "d / Ctrl-D",
            "first retained record",
            "retained bottom (paused)",
            "← / h, → / l",
            "Follow current retained bottom",
            "Ctrl-C",
            "?",
            "Esc",
        ] {
            assert!(log_help.contains(expected), "missing {expected:?}");
        }

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
        for expected in ["Esc back", "↑↓/jk", "F follow", "Ctrl-C", "? help"] {
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
        assert!(!completed_footer.contains("Ctrl-C"));

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
        assert!(completed_help.contains("Quit the completed workflow"));
        assert!(!completed_help.contains("Ctrl-C"));
    }

    #[test]
    fn cancelling_workflow_does_not_advertise_an_inactive_cancel_command() {
        let mut snapshot = direct_snapshot(long_log_step());
        snapshot.workflow = WorkflowState::Executing {
            gate: SchedulingGate::Cancelling {
                reason: CancellationReason::UserRequest,
                prior_failure: None,
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
            !footer.contains("Ctrl-C"),
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
            ">",
            "State: running",
            "following",
            "stdout",
            "Ctrl-C cancel",
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
            .find(|line| line.contains("middlefive"))
            .unwrap();
        assert!(selected.contains("> │"));
        assert!(top.contains("│"));
        assert_eq!(selected.find("middleeight"), top.find("middlefive"));
        assert!(!compact.iter().any(|line| line.contains("branch")));

        let resized = render_steps_lines(&snapshot, &interaction, 64, 8);
        assert!(
            resized
                .iter()
                .any(|line| line.contains("> │") && line.contains("middleeight"))
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
            source_sequence: SourceSequence::first(),
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
        snapshot.steps[0].log.records.drain(0..8);
        for order in 31..=38 {
            append_log_record(
                &mut snapshot.steps[0].log,
                order,
                &format!("record {order}"),
            );
        }
        snapshot.steps[0].log.discarded_records = 8;
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
        let log = &snapshot.steps[interaction.selected].log;
        log.records
            .get(interaction.full_log.top_index(log))
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
        let output = Output::File {
            path: "report.txt".to_owned(),
            media_type: "text/plain".to_owned(),
        };
        WorkflowRunStepView {
            id: "selected-command".to_owned(),
            definition: WorkflowPresentationStep::Command {
                argv: vec!["build".to_owned(), "héllo world".to_owned()],
                cwd: Some("work".to_owned()),
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
            cancellation: None,
            authoritative_result: false,
            quiescent: false,
            publication: WorkflowRunPublicationState::NotStarted,
            cleanup: WorkflowRunCleanupState::NotStarted,
            quit_eligible: false,
        }
    }

    fn render_direct_inspector(step: &WorkflowRunStepView, width: u16, height: u16) -> String {
        let backend = ratatui::backend::TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_inspector(frame, frame.area(), Some(step), false))
            .unwrap();
        buffer_text(terminal.backend().buffer())
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
            super::super::run_view_model::StepLogCapacity::default(),
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
                render_steps(frame, frame.area(), snapshot, &graph, interaction, false);
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
