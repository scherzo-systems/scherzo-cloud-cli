use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use time::format_description::well_known::Rfc3339;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, CaptureLimits, EnvironmentSnapshot, ExecutionContext,
    ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::artifact::ArtifactStaging;
use crate::execution::workflow::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
use crate::execution::workflow::observation::{
    CommandOutputClosedObservation, CommandOutputObservation, CommandOutputSource, SourceSequence,
    TransitionObservation,
};
use crate::execution::workflow::publication::{
    WorkflowRunCancellation, WorkflowRunStepKind, WorkflowRunTiming, WorkflowStepTiming,
    publish_workflow_result,
};
use crate::execution::workflow::resolution::{self, WorkflowContentDigest};
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};
use crate::execution::workflow::validated::WorkflowNodeRole;
use crate::execution::workflow::value::CapturedValue;

const TEST_CHILD_FRAGMENT_BYTES: usize = 16 * 1024;

#[derive(Clone, Default)]
struct SharedWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
    failing: Arc<AtomicBool>,
}

impl SharedWriter {
    fn text(&self) -> String {
        String::from_utf8(
            self.bytes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
        )
        .unwrap()
    }

    fn fail(&self) {
        self.failing.store(true, Ordering::SeqCst);
    }
}

impl Write for SharedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "injected"));
        }
        self.bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failing.load(Ordering::SeqCst) {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "injected"))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Default)]
struct FlushFailWriter {
    failing: Arc<AtomicBool>,
}

impl FlushFailWriter {
    fn fail_flush(&self) {
        self.failing.store(true, Ordering::SeqCst);
    }
}

impl Write for FlushFailWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failing.load(Ordering::SeqCst) {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected flush failure",
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct TestClock {
    values: Arc<Mutex<VecDeque<ObservationTime>>>,
    fallback: ObservationTime,
}

impl TestClock {
    fn fixed(value: &str) -> Self {
        let fallback = ObservationTime {
            utc: OffsetDateTime::parse(value, &Rfc3339).unwrap(),
            monotonic: crate::timing::monotonic_now(),
        };
        Self {
            values: Arc::new(Mutex::new(VecDeque::new())),
            fallback,
        }
    }

    fn sequence(values: &[&str]) -> Self {
        let monotonic = crate::timing::monotonic_now();
        let values = values
            .iter()
            .map(|value| ObservationTime {
                utc: OffsetDateTime::parse(value, &Rfc3339).unwrap(),
                monotonic,
            })
            .collect::<VecDeque<_>>();
        let fallback = *values.back().unwrap();
        Self {
            values: Arc::new(Mutex::new(values)),
            fallback,
        }
    }

    fn sequence_with_elapsed(values: &[(&str, Duration)]) -> Self {
        let monotonic = crate::timing::monotonic_now();
        let values = values
            .iter()
            .map(|(value, elapsed)| ObservationTime {
                utc: OffsetDateTime::parse(value, &Rfc3339).unwrap(),
                monotonic: monotonic + *elapsed,
            })
            .collect::<VecDeque<_>>();
        let fallback = *values.back().unwrap();
        Self {
            values: Arc::new(Mutex::new(values)),
            fallback,
        }
    }
}

impl ObservationClock for TestClock {
    fn sample(&self) -> ObservationTime {
        self.values
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop_front()
            .unwrap_or(self.fallback)
    }
}

fn capabilities(
    stdout: bool,
    stderr: bool,
    term: Option<&str>,
    no_color: Option<&str>,
) -> TerminalCapabilities {
    TerminalCapabilities {
        stdin_is_terminal: stdout,
        stdout_is_terminal: stdout,
        stderr_is_terminal: stderr,
        stdout_width: None,
        stderr_width: None,
        term: term.map(OsString::from),
        no_color: no_color.map(OsString::from),
    }
}

fn config(mode: RequestedPresentationMode, color: ColorChoice) -> PresentationConfig {
    PresentationConfig {
        requested_mode: mode,
        color,
        capabilities: capabilities(false, false, Some("xterm-256color"), None),
        standard_input_reserved: false,
    }
}

fn plain_terminal_config(width: usize, color: ColorChoice) -> PresentationConfig {
    let mut config = config(RequestedPresentationMode::Plain, color);
    config.capabilities.stdout_is_terminal = true;
    config.capabilities.stdout_width = Some(width);
    config
}

struct Fixture {
    _temporary: tempfile::TempDir,
    source_root: PathBuf,
    execution_root: PathBuf,
    result_parent: PathBuf,
    artifacts: ArtifactStaging,
    workflow: ResolvedWorkflow,
    digest: WorkflowContentDigest,
}

impl Fixture {
    fn new(source: &str) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let private_staging = temporary.path().join("private");
        let result_parent = temporary.path().join("results");
        for directory in [
            &source_root,
            &execution_root,
            &private_staging,
            &result_parent,
        ] {
            std::fs::create_dir(directory).unwrap();
        }
        std::fs::write(source_root.join("workflow.yaml"), source).unwrap();
        let workflow = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let digest = workflow.content_digest.clone();
        let admitted = admit_workflow(
            workflow.clone(),
            ResolvedImports::default(),
            execution_context(execution_root.clone()),
        )
        .unwrap();
        let artifacts = ArtifactStaging::create(admitted.execution(), &private_staging).unwrap();
        Self {
            _temporary: temporary,
            source_root,
            execution_root,
            result_parent,
            artifacts,
            workflow,
            digest,
        }
    }

    fn destination(&self, name: &str) -> PathBuf {
        self.result_parent.join(name)
    }

    fn succeeded_run(&self) -> WorkflowRunResult {
        let started_at = timestamp("2026-08-02T12:01:44Z");
        WorkflowRunResult {
            run_directory: self.result_parent.clone(),
            attempt_number: 1,
            workflow_path: "workflow.yaml".to_owned(),
            source_root: self.source_root.clone(),
            content_digest: self.digest.clone(),
            execution_root: self.execution_root.clone(),
            maximum_parallel_steps: NonZeroUsize::new(2).unwrap(),
            cloud_capacity: None,
            timing: WorkflowRunTiming {
                started_at,
                finished_at: timestamp("2026-08-02T12:01:45.25Z"),
                duration: Duration::from_millis(1250),
            },
            outcome: RunOutcome::Succeeded,
            cancellation: None,
            steps: self
                .workflow
                .definition
                .steps
                .keys()
                .map(|id| WorkflowRunStep {
                    id: id.clone(),
                    role: crate::execution::workflow::validated::WorkflowNodeRole::Step,
                    kind: WorkflowRunStepKind::Command,
                    failure_policy: FailurePolicy::Required,
                    state: StepState::Succeeded {
                        outputs: BTreeMap::new(),
                    },
                    timing: Some(WorkflowStepTiming {
                        started_at,
                        duration: Duration::from_millis(250),
                    }),
                    command_output: Some(empty_command_output()),
                    recovery: None,
                    invocations: Vec::new(),
                })
                .collect(),
            finalization: None,
            exports: BTreeMap::new(),
            export_sources: BTreeMap::new(),
        }
    }
}

async fn render_start(source: &str, config: PresentationConfig) -> String {
    let fixture = Fixture::new(source);
    let stdout = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(config, stdout.clone(), SharedWriter::default())
        .start(
            &fixture.workflow,
            1,
            TestClock::fixed("2026-08-02T12:01:44Z"),
        )
        .unwrap();
    presentation
        .observe(step_transition(
            "expectedFailure",
            StepStateKind::Starting,
            None,
        ))
        .await;
    stdout.text()
}

fn wrapping_workflow_source() -> &'static str {
    r#"schemaVersion: 1
steps:
  expectedFailure:
    kind: cmd
    command:
      argv:
        - sh
        - -c
        - "set -eu; printf running; sleep 0.20; printf still-running; sleep 0.20; exit 17"
"#
}

fn execution_context(root: PathBuf) -> ExecutionContext {
    ExecutionContext::new(
        root,
        ExecutionRootLifecycle::CallerOwnedRetained,
        ExecutionPolicyLimits::new(
            2,
            CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
            InputLimits::new(16, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
            super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM,
        ),
        EnvironmentSnapshot::default(),
        CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
    )
}

fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).unwrap()
}

fn empty_command_output() -> StepDiagnostic {
    StepDiagnostic::from_streams(
        CapturedDiagnosticStream::from_parts(Arc::<[u8]>::from([]), 0, true),
        CapturedDiagnosticStream::from_parts(Arc::<[u8]>::from([]), 0, true),
    )
}

fn workflow_source() -> &'static str {
    "schemaVersion: 1
steps:
  a:
    kind: cmd
    command:
      argv: [\"printf\", \"hello world\"]
  b:
    kind: cmd
    dependsOn: [a]
    command:
      argv: [\"true\"]
  c:
    kind: cmd
    command:
      argv: [\"true\"]
"
}

fn action() -> ActionId {
    ActionId {
        transition_sequence: TransitionSequence::default(),
    }
}

fn step_transition(
    step: &str,
    to: StepStateKind,
    detail: Option<ObservedStepTransition>,
) -> ExecutionObservation<OffsetDateTime> {
    ExecutionObservation::Transition(TransitionObservation {
        event: TransitionEvent::Step {
            sequence: TransitionSequence::default(),
            step: step.to_owned(),
            role: WorkflowNodeRole::Step,
            failure_policy: FailurePolicy::Required,
            from: StepStateKind::Pending,
            to,
        },
        step: detail,
    })
}

#[test]
fn capability_matrix_selects_one_mode_without_consulting_runtime_state() {
    for stdin in [false, true] {
        for stdout in [false, true] {
            for stderr in [false, true] {
                let mut terminal = capabilities(stdout, stderr, Some("xterm-256color"), None);
                terminal.stdin_is_terminal = stdin;
                for (requested_mode, expected) in [
                    (
                        RequestedPresentationMode::Automatic,
                        if stdin && stdout {
                            PresentationMode::Tui
                        } else {
                            PresentationMode::Plain
                        },
                    ),
                    (RequestedPresentationMode::Plain, PresentationMode::Plain),
                    (RequestedPresentationMode::Json, PresentationMode::Json),
                ] {
                    assert_eq!(
                        PresentationConfig {
                            requested_mode,
                            color: ColorChoice::Auto,
                            capabilities: terminal.clone(),
                            standard_input_reserved: false,
                        }
                        .mode(),
                        expected,
                        "stdin={stdin}, stdout={stdout}, stderr={stderr}, requested={requested_mode:?}"
                    );
                }
            }
        }
    }

    for (term, no_color, standard_input_reserved) in [
        (None, None, false),
        (Some(""), None, false),
        (Some("dumb"), None, false),
        (Some("xterm-256color"), None, true),
        (Some("xterm-256color"), Some("1"), false),
    ] {
        let mut terminal = capabilities(true, true, term, no_color);
        terminal.stdin_is_terminal = true;
        let selected = PresentationConfig {
            requested_mode: RequestedPresentationMode::Automatic,
            color: ColorChoice::Auto,
            capabilities: terminal,
            standard_input_reserved,
        };
        let expected = if term == Some("xterm-256color") && !standard_input_reserved {
            PresentationMode::Tui
        } else {
            PresentationMode::Plain
        };
        assert_eq!(selected.mode(), expected);
    }
}

#[test]
fn color_selection_uses_the_human_destination_and_environment_matrix() {
    for (mode, stdout_terminal, stderr_terminal, expected) in [
        (RequestedPresentationMode::Automatic, true, false, true),
        (RequestedPresentationMode::Plain, false, true, false),
        (RequestedPresentationMode::Json, true, false, false),
        (RequestedPresentationMode::Json, false, true, true),
    ] {
        let selected = PresentationConfig {
            requested_mode: mode,
            color: ColorChoice::Auto,
            capabilities: capabilities(
                stdout_terminal,
                stderr_terminal,
                Some("xterm-256color"),
                None,
            ),
            standard_input_reserved: false,
        };
        assert_eq!(selected.color_enabled(), expected);
    }

    for term in [None, Some(""), Some("dumb")] {
        let selected = PresentationConfig {
            requested_mode: RequestedPresentationMode::Plain,
            color: ColorChoice::Auto,
            capabilities: capabilities(true, true, term, None),
            standard_input_reserved: false,
        };
        assert!(!selected.color_enabled());
    }
    let no_color = PresentationConfig {
        requested_mode: RequestedPresentationMode::Plain,
        color: ColorChoice::Auto,
        capabilities: capabilities(true, true, Some("xterm"), Some("1")),
        standard_input_reserved: false,
    };
    assert!(!no_color.color_enabled());
    assert!(
        PresentationConfig {
            color: ColorChoice::Always,
            ..no_color.clone()
        }
        .color_enabled()
    );
    assert!(
        !PresentationConfig {
            color: ColorChoice::Never,
            ..no_color
        }
        .color_enabled()
    );
}

#[test]
fn displayed_shell_arguments_are_unambiguous() {
    let newline = visible_text(&shell_quote("line\nbreak"));
    let literal_escape = visible_text(&shell_quote(r"line\x0abreak"));

    assert_ne!(newline, literal_escape);
}

#[test]
fn rejection_json_is_one_pretty_document_without_stderr_prose() {
    let temporary = tempfile::tempdir().unwrap();
    let failure = resolution::resolve(
        &temporary.path().join("missing"),
        Path::new("workflow.yaml"),
    )
    .unwrap_err();
    let stdout = SharedWriter::default();
    let stderr = SharedWriter::default();

    let result = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Json, ColorChoice::Never),
        stdout.clone(),
        stderr.clone(),
    )
    .render_resolution_rejection(&failure);

    assert_eq!(
        result,
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: None
        }
    );
    assert!(stderr.text().is_empty());
    let bytes = stdout.text();
    assert!(bytes.contains("\n  \"schemaVersion\""));
    let value: Value = serde_json::from_str(&bytes).unwrap();
    assert_eq!(value["outcome"], "rejected");
    assert_eq!(value["phase"], "resolution");
    assert_eq!(value["workflow"], Value::Null);
    assert_eq!(value["diagnostics"].as_array().unwrap().len(), 1);

    let human_stdout = SharedWriter::default();
    let human_stderr = SharedWriter::default();
    let result = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        human_stdout.clone(),
        human_stderr.clone(),
    )
    .render_resolution_rejection(&failure);
    let WorkflowRunPresentationResult::Rejected {
        human_diagnostic: Some(diagnostic),
    } = result
    else {
        panic!("human rejection should return its diagnostic");
    };
    assert!(human_stdout.text().is_empty());
    assert!(human_stderr.text().is_empty());
    assert!(diagnostic.contains("source_root_unavailable"));
}

#[test]
fn rejection_json_writer_failure_is_returned_without_local_rendering() {
    let temporary = tempfile::tempdir().unwrap();
    let failure = resolution::resolve(
        &temporary.path().join("missing"),
        Path::new("workflow.yaml"),
    )
    .unwrap_err();
    let stdout = SharedWriter::default();
    stdout.fail();
    let stderr = SharedWriter::default();

    let result = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Json, ColorChoice::Never),
        stdout,
        stderr.clone(),
    )
    .render_resolution_rejection(&failure);

    let WorkflowRunPresentationResult::Failed(failure) = result else {
        panic!("JSON rejection writer failure must be typed");
    };
    assert_eq!(
        failure.operation,
        PresentationFailureOperation::TerminalJsonWriter
    );
    assert_eq!(failure.error_kind, Some(io::ErrorKind::BrokenPipe));
    assert!(stderr.text().is_empty());
}

#[test]
fn contracted_admission_rejection_has_the_resolved_workflow_identity() {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    std::fs::create_dir(&source_root).unwrap();
    std::fs::create_dir(&execution_root).unwrap();
    std::fs::write(
        source_root.join("workflow.yaml"),
        "schemaVersion: 1
steps:
  prompt:
    kind: cmd
    inputs:
      prompt:
        ref: imports.prompt
    command:
      argv: [\"true\"]
",
    )
    .unwrap();
    let workflow = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
    let failure = admit_workflow(
        workflow.clone(),
        ResolvedImports::default(),
        execution_context(execution_root),
    )
    .unwrap_err();
    let stdout = SharedWriter::default();
    let stderr = SharedWriter::default();

    let result = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Json, ColorChoice::Always),
        stdout.clone(),
        stderr.clone(),
    )
    .render_admission_rejection(&workflow, &failure);

    assert_eq!(
        result,
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: None
        }
    );
    assert!(stderr.text().is_empty());
    let value: Value = serde_json::from_str(&stdout.text()).unwrap();
    assert_eq!(value["diagnostics"][0]["code"], "missing_required_prompt");
    assert_eq!(value["workflow"]["path"], "workflow.yaml");
    assert!(!stdout.text().contains('\u{1b}'));
}

#[tokio::test]
async fn live_stream_labels_normalized_output_and_orders_cancellation_acknowledgement() {
    let fixture = Fixture::new(workflow_source());
    let stdout = SharedWriter::default();
    let stderr = SharedWriter::default();
    let clock = TestClock::sequence(&[
        "2026-08-02T12:01:44.999999999Z",
        "2026-08-02T12:01:45.123999999Z",
        "2026-08-02T12:01:46.999999999Z",
        "2026-08-02T12:01:47Z",
        "2026-08-02T12:01:48Z",
        "2026-08-02T12:01:49Z",
    ]);
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        stdout.clone(),
        stderr.clone(),
    )
    .start(&fixture.workflow, 2, clock)
    .unwrap();
    assert_eq!(
        presentation.opened_at().utc,
        timestamp("2026-08-02T12:01:44.999999999Z")
    );

    presentation
        .observe(step_transition("a", StepStateKind::Starting, None))
        .await;
    presentation
        .observe(ExecutionObservation::<OffsetDateTime>::CommandOutput(
            CommandOutputObservation {
                step: "a".to_owned(),
                invocation: action(),
                source: CommandOutputSource::StandardError,
                sequence: SourceSequence::first(),
                bytes: Arc::from(b"warning\x1b[2J\nfinal".as_slice()),
            },
        ))
        .await;
    presentation
        .observe(ExecutionObservation::<OffsetDateTime>::CommandOutputClosed(
            CommandOutputClosedObservation {
                step: "a".to_owned(),
                invocation: action(),
                source: CommandOutputSource::StandardError,
                sequence: SourceSequence::first().next(),
            },
        ))
        .await;
    presentation
        .observe(ExecutionObservation::Transition(TransitionObservation {
            event: TransitionEvent::CancellationAccepted {
                sequence: TransitionSequence::default(),
                reason: CancellationReason::UserRequest,
                deadline: timestamp("2026-08-02T12:01:58Z"),
            },
            step: None,
        }))
        .await;
    presentation
        .observe(step_transition(
            "a",
            StepStateKind::Cancelling,
            Some(ObservedStepTransition::Cancelling {
                detail: crate::execution::workflow::evidence::CancellationDetail::new(
                    CancellationReason::UserRequest,
                ),
            }),
        ))
        .await;

    let output = stdout.text();
    assert!(output.contains(
        "run result · workflow.yaml · 3 steps · concurrency 2\nstarted 2026-08-02 12:01:44Z"
    ));
    assert!(output.contains("[12:01:45] a          start       cmd · printf 'hello world'"));
    assert!(output.contains("a          stderr      warning"));
    assert!(output.contains("a          stderr      final"));
    assert!(!output.contains("\u{1b}[2J"));
    let acknowledgement = output.find("@workflow  cancelling  user_request").unwrap();
    let step_cancelling = output.rfind("a          cancelling  user_request").unwrap();
    assert!(acknowledgement < step_cancelling);
    assert_eq!(output.matches("@workflow  cancelling").count(), 1);
    assert!(stderr.text().is_empty());
}

#[tokio::test]
async fn tty_events_wrap_details_with_hanging_and_stacked_layouts() {
    let inline = render_start(
        wrapping_workflow_source(),
        plain_terminal_config(70, ColorChoice::Never),
    )
    .await;
    let inline_lines = inline.lines().collect::<Vec<_>>();
    let event_index = inline_lines
        .iter()
        .position(|line| line.starts_with("[12:01:44]"))
        .unwrap();
    let event_lines = &inline_lines[event_index..];
    let detail_column = display_width(&event_lines[0][..event_lines[0].find("cmd ·").unwrap()]);
    assert!(event_lines.len() > 1);
    assert!(event_lines[0].contains("expectedFailure"));
    for continuation in &event_lines[1..] {
        let marker = continuation.find(VISUAL_CONTINUATION_MARKER).unwrap();
        assert_eq!(display_width(&continuation[..marker]), detail_column);
        assert!(!continuation.contains("[12:01:44]"));
    }
    assert!(event_lines.iter().all(|line| display_width(line) <= 70));

    let colored = render_start(
        wrapping_workflow_source(),
        plain_terminal_config(70, ColorChoice::Always),
    )
    .await;
    assert!(colored.contains(&format!(
        "\u{1b}[{STYLE_CONTINUATION}m{VISUAL_CONTINUATION_MARKER}"
    )));

    let stacked = render_start(
        wrapping_workflow_source(),
        plain_terminal_config(60, ColorChoice::Never),
    )
    .await;
    let stacked_lines = stacked.lines().collect::<Vec<_>>();
    let event_index = stacked_lines
        .iter()
        .position(|line| line.starts_with("[12:01:44]"))
        .unwrap();
    let event_lines = &stacked_lines[event_index..];
    assert_eq!(event_lines[0], "[12:01:44] expectedFailure start");
    assert!(event_lines[1].starts_with("  cmd ·"));
    assert!(
        event_lines[2..]
            .iter()
            .all(|line| line.starts_with("    ↳ "))
    );
    assert!(event_lines.iter().all(|line| display_width(line) <= 60));
}

#[tokio::test]
async fn redirected_events_remain_one_logical_record_per_line() {
    let mut redirected = config(RequestedPresentationMode::Plain, ColorChoice::Never);
    redirected.capabilities.stdout_width = Some(40);
    let output = render_start(wrapping_workflow_source(), redirected).await;
    let event_lines = output
        .lines()
        .filter(|line| line.starts_with("[12:01:44]"))
        .collect::<Vec<_>>();
    assert_eq!(event_lines.len(), 1);
    assert!(event_lines[0].contains("sleep 0.20; exit 17'"));
    assert!(!output.contains(VISUAL_CONTINUATION_MARKER));
}

#[tokio::test]
async fn safety_fragmentation_keeps_its_own_prefixed_records_when_redirected() {
    let fixture = Fixture::new(workflow_source());
    let stdout = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        stdout.clone(),
        SharedWriter::default(),
    )
    .start(
        &fixture.workflow,
        1,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    presentation
        .observe(ExecutionObservation::<OffsetDateTime>::CommandOutput(
            CommandOutputObservation {
                step: "a".to_owned(),
                invocation: action(),
                source: CommandOutputSource::StandardOutput,
                sequence: SourceSequence::first(),
                bytes: Arc::from(vec![b'x'; TEST_CHILD_FRAGMENT_BYTES + 1]),
            },
        ))
        .await;
    presentation
        .observe(ExecutionObservation::<OffsetDateTime>::CommandOutputClosed(
            CommandOutputClosedObservation {
                step: "a".to_owned(),
                invocation: action(),
                source: CommandOutputSource::StandardOutput,
                sequence: SourceSequence::first().next(),
            },
        ))
        .await;

    let output = stdout.text();
    assert_eq!(output.matches("stdout").count(), 2);
    assert!(output.contains(&format!("{SAFETY_CONTINUATION_MARKER} x")));
    assert!(!output.contains(VISUAL_CONTINUATION_MARKER));
}

#[test]
fn detail_wrapping_prefers_words_and_hard_wraps_by_display_cells() {
    assert_eq!(
        wrap_detail("alpha beta gamma", 10, 8),
        ("alpha beta".to_owned(), vec!["gamma".to_owned()])
    );
    assert_eq!(
        wrap_detail("abcdefgh", 3, 3),
        ("abc".to_owned(), vec!["def".to_owned(), "gh".to_owned()])
    );
    assert_eq!(
        wrap_detail("界界界", 4, 4),
        ("界界".to_owned(), vec!["界".to_owned()])
    );
}

#[tokio::test]
async fn committed_outputs_render_before_the_terminal_step_record() {
    let fixture = Fixture::new(
        "schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      report:
        kind: file
        from: path
        path: report.txt
        mediaType: text/plain
",
    );
    let stdout = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        stdout.clone(),
        SharedWriter::default(),
    )
    .start(
        &fixture.workflow,
        1,
        TestClock::sequence_with_elapsed(&[
            ("2026-08-02T12:01:44Z", Duration::ZERO),
            ("2026-08-02T12:01:45Z", Duration::from_secs(1)),
            ("2026-08-02T12:01:47.25Z", Duration::from_millis(3250)),
        ]),
    )
    .unwrap();

    presentation
        .observe(step_transition("produce", StepStateKind::Starting, None))
        .await;
    presentation
        .observe(step_transition(
            "produce",
            StepStateKind::Succeeded,
            Some(ObservedStepTransition::OutputsCommitted {
                outputs: vec!["report".to_owned()],
            }),
        ))
        .await;

    let output = stdout.text();
    assert!(
        output.find("produce    output      report · file").unwrap()
            < output
                .find("produce    done        exit 0 · 1 output after 2.2s")
                .unwrap()
    );
}

#[tokio::test]
async fn agent_success_uses_semantic_output_count_in_live_and_summary_details() {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    std::fs::create_dir(&source_root).unwrap();
    std::fs::write(
        source_root.join("workflow.yaml"),
        "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  plan:
    kind: agent
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text:
          - file: message.md
    outputs:
      plan:
        kind: json
        from: agent_result
        schema: schema.json
",
    )
    .unwrap();
    std::fs::write(source_root.join("system.md"), "Return a plan.").unwrap();
    std::fs::write(source_root.join("message.md"), "Plan the change.").unwrap();
    std::fs::write(
        source_root.join("schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
    )
    .unwrap();
    let workflow = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
    let stdout = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        stdout.clone(),
        SharedWriter::default(),
    )
    .start(&workflow, 1, TestClock::fixed("2026-08-02T12:01:44Z"))
    .unwrap();

    presentation
        .observe(step_transition(
            "plan",
            StepStateKind::Succeeded,
            Some(ObservedStepTransition::OutputsCommitted {
                outputs: vec!["plan".to_owned()],
            }),
        ))
        .await;

    let output = stdout.text();
    assert!(output.contains("plan       output      plan · json"));
    assert!(output.contains("plan       done        1 output committed"));

    let step = WorkflowRunStep {
        id: "plan".to_owned(),
        role: crate::execution::workflow::validated::WorkflowNodeRole::Step,
        kind: WorkflowRunStepKind::Agent,
        failure_policy: FailurePolicy::Required,
        state: StepState::Succeeded {
            outputs: BTreeMap::from([(
                "plan".to_owned(),
                CapturedValue::json_fixture(Arc::new(Value::Object(serde_json::Map::new()))),
            )]),
        },
        timing: None,
        command_output: None,
        recovery: None,
        invocations: Vec::new(),
    };
    let (_, summary_detail, _) = summary_step(&step, StepSuccessPresentation::Agent).unwrap();
    assert_eq!(summary_detail, "1 output committed");
}

#[tokio::test]
async fn plain_and_json_route_live_records_summaries_and_terminal_json() {
    let fixture = Fixture::new(workflow_source());
    let run = fixture.succeeded_run();
    let plain_terminal =
        publish_workflow_result(&fixture.destination("plain"), &fixture.artifacts, &run).unwrap();
    let json_terminal =
        publish_workflow_result(&fixture.destination("json"), &fixture.artifacts, &run).unwrap();

    let plain_stdout = SharedWriter::default();
    let plain_stderr = SharedWriter::default();
    let plain = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        plain_stdout.clone(),
        plain_stderr.clone(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    plain
        .observe(step_transition("a", StepStateKind::Starting, None))
        .await;
    plain
        .observe(ExecutionObservation::<OffsetDateTime>::CommandOutput(
            CommandOutputObservation {
                step: "a".to_owned(),
                invocation: action(),
                source: CommandOutputSource::StandardOutput,
                sequence: SourceSequence::first(),
                bytes: Arc::from(b"plain child\n".as_slice()),
            },
        ))
        .await;
    assert!(matches!(
        plain.finish(&run, PublicationPresentation::Published(&plain_terminal)),
        WorkflowRunPresentationResult::Published { exit_status: 0, .. }
    ));

    let json_stdout = SharedWriter::default();
    let json_stderr = SharedWriter::default();
    let json = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Json, ColorChoice::Never),
        json_stdout.clone(),
        json_stderr.clone(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    json.observe(step_transition("a", StepStateKind::Starting, None))
        .await;
    json.observe(ExecutionObservation::<OffsetDateTime>::CommandOutput(
        CommandOutputObservation {
            step: "a".to_owned(),
            invocation: action(),
            source: CommandOutputSource::StandardOutput,
            sequence: SourceSequence::first(),
            bytes: Arc::from(b"json child\n".as_slice()),
        },
    ))
    .await;
    assert!(matches!(
        json.finish(&run, PublicationPresentation::Published(&json_terminal)),
        WorkflowRunPresentationResult::Published { exit_status: 0, .. }
    ));

    let plain_view = plain_stdout.text();
    assert!(plain_view.contains("a          stdout      plain child"));
    assert!(plain_view.contains("── summary ─"));
    assert!(plain_view.contains("node  kind  state"));
    assert!(plain_view.find("a     cmd").unwrap() < plain_view.find("b     cmd").unwrap());
    assert!(plain_view.find("b     cmd").unwrap() < plain_view.find("c     cmd").unwrap());
    assert!(plain_view.contains("succeeded · exit 0 · 3 succeeded · 1.2s total"));
    assert!(plain_view.contains(plain_terminal.result_directory()));
    assert!(!plain_view.contains("result: "));
    assert!(!plain_view.contains('\u{1b}'));
    assert!(plain_stderr.text().is_empty());

    let json_view = json_stderr.text();
    assert!(json_view.contains("a          stdout      json child"));
    assert!(json_view.contains("node  kind  state"));
    assert!(json_view.contains("succeeded · exit 0 · 3 succeeded · 1.2s total"));
    let terminal_bytes = json_stdout.text();
    assert!(terminal_bytes.contains("\n  \"schemaVersion\""));
    assert!(!terminal_bytes.contains('\u{1b}'));
    let value: Value = serde_json::from_str(&terminal_bytes).unwrap();
    assert_eq!(value["outcome"], "succeeded");
    assert_eq!(
        value["result"],
        serde_json::from_slice::<Value>(
            &std::fs::read(fixture.destination("json").join("result.json")).unwrap()
        )
        .unwrap()
    );

    let cleanup_terminal = publish_workflow_result(
        &fixture.destination("cleanup-failure"),
        &fixture.artifacts,
        &run,
    )
    .unwrap();
    let cleanup_stdout = SharedWriter::default();
    let cleanup_stderr = SharedWriter::default();
    let cleanup = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Json, ColorChoice::Never),
        cleanup_stdout.clone(),
        cleanup_stderr.clone(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    assert!(matches!(
        cleanup.finish_without_terminal_json(
            &run,
            PublicationPresentation::Published(&cleanup_terminal),
        ),
        WorkflowRunPresentationResult::Published { exit_status: 0, .. }
    ));
    assert!(cleanup_stdout.text().is_empty());
    assert!(
        cleanup_stderr
            .text()
            .contains("succeeded · exit 0 · 3 succeeded · 1.2s total")
    );
    assert!(
        cleanup_stderr
            .text()
            .contains(cleanup_terminal.result_directory())
    );
}

#[test]
fn tui_handoff_uses_the_standard_summary_without_reopening_live_output() {
    let fixture = Fixture::new(workflow_source());
    let run = fixture.succeeded_run();
    let terminal = publish_workflow_result(
        &fixture.destination("tui-summary"),
        &fixture.artifacts,
        &run,
    )
    .unwrap();
    let stdout = SharedWriter::default();
    let mut tui_config = config(RequestedPresentationMode::Automatic, ColorChoice::Never);
    tui_config.capabilities.stdin_is_terminal = true;
    tui_config.capabilities.stdout_is_terminal = true;
    assert_eq!(tui_config.mode(), PresentationMode::Tui);

    let presented = WorkflowRunOutput::new(tui_config, stdout.clone(), SharedWriter::default())
        .render_standard_summary(
            &fixture.workflow,
            &run,
            PublicationPresentation::Published(&terminal),
        );

    assert!(matches!(
        presented,
        WorkflowRunPresentationResult::Published { exit_status: 0, .. }
    ));
    let summary = stdout.text();
    assert!(summary.starts_with("\n── summary ─"));
    assert!(summary.contains("node  kind  state"));
    assert!(summary.contains("succeeded · exit 0 · 3 succeeded"));
    assert!(!summary.contains("started 2026"));
}

#[test]
fn failed_and_cancelled_summaries_use_authoritative_terminal_facts() {
    let fixture = Fixture::new(workflow_source());
    let mut failed = fixture.succeeded_run();
    let cause = StepFailureCause::Execution(StepExecutionFailure::Command(
        CommandExecutionFailure::UnsuccessfulExit { code: Some(23) },
    ));
    let detail =
        crate::execution::workflow::evidence::failure_detail(FailurePhase::Execution, &cause)
            .unwrap();
    failed.steps[1].state = StepState::Failed {
        detail: detail.clone(),
    };
    failed.steps[2].state = StepState::NotRun {
        detail: crate::execution::workflow::evidence::NonExecutionDetail::for_role(
            WorkflowNodeRole::Step,
            crate::execution::workflow::evidence::NonExecutionCode::FailureStop,
        )
        .unwrap(),
    };
    failed.steps[2].timing = None;
    failed.steps[2].command_output = None;
    failed.outcome = RunOutcome::Failed {
        primary_issue: crate::execution::workflow::evidence::PrimaryIssue::failed(
            crate::execution::workflow::validated::WorkflowNode {
                id: "b".to_owned(),
                role: WorkflowNodeRole::Step,
            },
            detail,
        ),
        later_cancellation: None,
    };
    let failed_terminal =
        publish_workflow_result(&fixture.destination("failed"), &fixture.artifacts, &failed)
            .unwrap();
    let failed_output = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        failed_output.clone(),
        SharedWriter::default(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    presentation.finish(
        &failed,
        PublicationPresentation::Published(&failed_terminal),
    );
    let failed_view = failed_output.text();
    assert!(failed_view.contains("failed · exit 1"));
    assert!(
        failed_view.contains("primary issue: step b · Failed · execution · command_exit · exit 23")
    );
    let header = failed_view
        .lines()
        .find(|line| line.starts_with("node"))
        .unwrap();
    let not_run = failed_view
        .lines()
        .find(|line| line.starts_with("c "))
        .unwrap();
    assert_eq!(
        header.find("detail").unwrap(),
        not_run[..not_run.find("failure_stop").unwrap()]
            .chars()
            .count()
    );

    let mut cancelled = fixture.succeeded_run();
    for step in &mut cancelled.steps {
        step.state = StepState::Cancelled {
            detail: crate::execution::workflow::evidence::CancellationDetail::new(
                CancellationReason::TerminationRequest,
            ),
        };
    }
    cancelled.outcome = RunOutcome::Cancelled {
        reason: CancellationReason::TerminationRequest,
    };
    cancelled.cancellation = Some(WorkflowRunCancellation {
        reason: CancellationReason::TerminationRequest,
        force_stop_deadline: timestamp("2026-08-02T12:01:55Z"),
    });
    let cancelled_terminal = publish_workflow_result(
        &fixture.destination("cancelled"),
        &fixture.artifacts,
        &cancelled,
    )
    .unwrap();
    let cancelled_output = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        cancelled_output.clone(),
        SharedWriter::default(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    assert!(matches!(
        presentation.finish(
            &cancelled,
            PublicationPresentation::Published(&cancelled_terminal),
        ),
        WorkflowRunPresentationResult::Published {
            exit_status: 143,
            ..
        }
    ));
    assert!(cancelled_output.text().contains("cancelled · exit 143"));
    assert!(
        cancelled_output
            .text()
            .contains("cancellation: termination_request")
    );
}

#[test]
fn publication_failure_keeps_factual_human_summary_and_omits_json() {
    let fixture = Fixture::new(workflow_source());
    let run = fixture.succeeded_run();
    let destination = fixture.destination("exists");
    std::fs::create_dir(&destination).unwrap();
    let failure = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap_err();
    let stdout = SharedWriter::default();
    let stderr = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Json, ColorChoice::Never),
        stdout.clone(),
        stderr.clone(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();

    let result = presentation.finish(&run, PublicationPresentation::Failed(&failure));

    assert_eq!(
        result,
        WorkflowRunPresentationResult::PublicationFailed(failure)
    );
    assert!(stdout.text().is_empty());
    assert!(stderr.text().contains("node  kind  state"));
    assert!(stderr.text().contains("workflow succeeded"));
    assert!(stderr.text().contains("result publication failed"));
    assert!(!stderr.text().contains("run result succeeded · exit"));
}

#[test]
fn header_writer_failure_is_returned_without_local_rendering() {
    let fixture = Fixture::new(workflow_source());
    let stdout = SharedWriter::default();
    stdout.fail();
    let stderr = SharedWriter::default();

    let failure = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        stdout,
        stderr.clone(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .err()
    .unwrap();

    assert_eq!(
        failure.operation,
        PresentationFailureOperation::HeaderWriter
    );
    assert_eq!(failure.error_kind, Some(io::ErrorKind::BrokenPipe));
    assert!(stderr.text().is_empty());
}

#[tokio::test]
async fn live_writer_failure_is_detected_when_the_destination_flush_fails() {
    let fixture = Fixture::new(workflow_source());
    let stdout = FlushFailWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        stdout.clone(),
        SharedWriter::default(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    stdout.fail_flush();

    presentation
        .observe(step_transition("a", StepStateKind::Starting, None))
        .await;

    let failure = presentation
        .failure()
        .expect("the live record must be flushed so the adapter can cancel promptly");
    assert_eq!(failure.operation, PresentationFailureOperation::LineWriter);
    assert_eq!(failure.error_kind, Some(io::ErrorKind::BrokenPipe));
}

#[tokio::test]
async fn live_writer_failure_notifies_the_adapter_and_stops_further_writes() {
    let fixture = Fixture::new(workflow_source());
    let stdout = SharedWriter::default();
    let stderr = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Plain, ColorChoice::Never),
        stdout.clone(),
        stderr.clone(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    let failures = presentation.subscribe_failures();
    let before_failure = stdout.text();
    stdout.fail();

    presentation
        .observe(step_transition("a", StepStateKind::Starting, None))
        .await;
    presentation
        .observe(step_transition("b", StepStateKind::Starting, None))
        .await;

    let failure = failures.borrow().clone().unwrap();
    assert_eq!(failure.operation, PresentationFailureOperation::LineWriter);
    assert_eq!(failure.error_kind, Some(io::ErrorKind::BrokenPipe));
    assert_eq!(stdout.text(), before_failure);
    assert!(stderr.text().is_empty());
}

#[test]
fn terminal_json_writer_failure_reports_the_published_result_path() {
    let fixture = Fixture::new(workflow_source());
    let run = fixture.succeeded_run();
    let terminal = publish_workflow_result(
        &fixture.destination("json-write-failure"),
        &fixture.artifacts,
        &run,
    )
    .unwrap();
    let stdout = SharedWriter::default();
    let stderr = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Json, ColorChoice::Never),
        stdout.clone(),
        stderr.clone(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    stdout.fail();

    let result = presentation.finish(&run, PublicationPresentation::Published(&terminal));

    let WorkflowRunPresentationResult::Failed(failure) = result else {
        panic!("terminal JSON failure must be typed");
    };
    assert_eq!(
        failure.operation,
        PresentationFailureOperation::TerminalJsonWriter
    );
    assert_eq!(failure.error_kind, Some(io::ErrorKind::BrokenPipe));
    assert_eq!(
        failure.result_directory.as_deref(),
        Some(terminal.result_directory())
    );
    assert!(stderr.text().contains(terminal.result_directory()));
    assert!(stderr.text().contains("succeeded · exit 0"));
}

#[test]
fn durations_use_compound_units_after_one_minute() {
    assert_eq!(human_duration(Duration::from_millis(999)), "999ms");
    assert_eq!(human_duration(Duration::from_millis(4250)), "4.2s");
    assert_eq!(human_duration(Duration::from_secs(161)), "2m41s");
    assert_eq!(human_duration(Duration::from_secs(3723)), "1h02m03s");
}

#[test]
fn json_color_always_styles_only_the_stderr_presentation() {
    let fixture = Fixture::new(workflow_source());
    let run = fixture.succeeded_run();
    let terminal = publish_workflow_result(
        &fixture.destination("colored-json"),
        &fixture.artifacts,
        &run,
    )
    .unwrap();
    let stdout = SharedWriter::default();
    let stderr = SharedWriter::default();
    let presentation = WorkflowRunOutput::new(
        config(RequestedPresentationMode::Json, ColorChoice::Always),
        stdout.clone(),
        stderr.clone(),
    )
    .start(
        &fixture.workflow,
        2,
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();
    presentation.finish(&run, PublicationPresentation::Published(&terminal));

    let presentation = stderr.text();
    for style in [STYLE_PRIMARY, STYLE_SECONDARY, STYLE_MUTED, STYLE_SUCCESS] {
        assert!(presentation.contains(&format!("\u{1b}[{style}m")));
    }
    assert!(!presentation.contains("\u{1b}[32m"));
    assert!(!stdout.text().contains('\u{1b}'));
    let _: Value = serde_json::from_str(&stdout.text()).unwrap();
}
