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
use crate::execution::workflow::observation::{SourceSequence, TransitionObservation};
use crate::execution::workflow::publication::{
    WorkflowRunCancellation, WorkflowRunTiming, WorkflowStepTiming, publish_workflow_result,
};
use crate::execution::workflow::resolution::{self, WorkflowContentDigest};
use crate::execution::workflow::runtime::{ActionId, StepFailure, TransitionSequence};

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
    values: Arc<Mutex<VecDeque<OffsetDateTime>>>,
    fallback: OffsetDateTime,
}

impl TestClock {
    fn fixed(value: &str) -> Self {
        let fallback = OffsetDateTime::parse(value, &Rfc3339).unwrap();
        Self {
            values: Arc::new(Mutex::new(VecDeque::new())),
            fallback,
        }
    }

    fn sequence(values: &[&str]) -> Self {
        let values = values
            .iter()
            .map(|value| OffsetDateTime::parse(value, &Rfc3339).unwrap())
            .collect::<VecDeque<_>>();
        let fallback = *values.back().unwrap();
        Self {
            values: Arc::new(Mutex::new(values)),
            fallback,
        }
    }
}

impl ObservationClock for TestClock {
    fn now(&self) -> OffsetDateTime {
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
        stdout_is_terminal: stdout,
        stderr_is_terminal: stderr,
        term: term.map(OsString::from),
        no_color: no_color.map(OsString::from),
    }
}

fn config(mode: RequestedPresentationMode, color: ColorChoice) -> PresentationConfig {
    PresentationConfig {
        requested_mode: mode,
        color,
        capabilities: capabilities(false, false, Some("xterm-256color"), None),
    }
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
            workflow_path: "workflow.yaml".to_owned(),
            source_root: self.source_root.clone(),
            content_digest: self.digest.clone(),
            execution_root: self.execution_root.clone(),
            maximum_parallel_steps: NonZeroUsize::new(2).unwrap(),
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
                    state: StepState::Succeeded {
                        outputs: BTreeMap::new(),
                    },
                    timing: Some(WorkflowStepTiming {
                        started_at,
                        duration: Duration::from_millis(250),
                    }),
                    command_output: None,
                })
                .collect(),
            exports: BTreeMap::new(),
        }
    }
}

fn execution_context(root: PathBuf) -> ExecutionContext {
    ExecutionContext::new(
        root,
        ExecutionRootLifecycle::CallerOwnedRetained,
        ExecutionPolicyLimits::new(
            2,
            CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
            InputLimits::new(16, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
            65_536,
        ),
        EnvironmentSnapshot::default(),
        CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
    )
}

fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).unwrap()
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
            from: StepStateKind::Pending,
            to,
        },
        step: detail,
    })
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
        };
        assert_eq!(selected.color_enabled(), expected);
    }

    for term in [None, Some(""), Some("dumb")] {
        let selected = PresentationConfig {
            requested_mode: RequestedPresentationMode::Plain,
            color: ColorChoice::Auto,
            capabilities: capabilities(true, true, term, None),
        };
        assert!(!selected.color_enabled());
    }
    let no_color = PresentationConfig {
        requested_mode: RequestedPresentationMode::Plain,
        color: ColorChoice::Auto,
        capabilities: capabilities(true, true, Some("xterm"), Some("1")),
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
fn child_normalization_preserves_framing_and_exposes_untrusted_bytes() {
    let mut stream = ChildStream::default();
    let mut records = stream
        .push(b"a\r\n\rB\rC\tD\x1b[31mred\x1b[0m\x1b]title\x07\x1bPsecret\x1b\\\x1b7\xff\xe2");
    records.extend(stream.push(b"\x80\xae\x1b]incomplete"));
    records.extend(stream.close());
    let payloads = records
        .iter()
        .map(|record| record.payload.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        payloads,
        ["a", "", "B", "C       Dred\\xff\\u{202e}\\x1b]incomplete",]
    );
    assert!(
        records
            .iter()
            .all(|record| !record.payload.contains('\u{1b}'))
    );
}

#[test]
fn child_normalization_resets_utf8_after_abandoning_a_control_candidate() {
    let mut stream = ChildStream::default();
    let mut records = stream.push(b"\x1b[\xc2\xa2");
    records.extend(stream.close());

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, "\\x1b[\\xc2\\xa2");
}

#[test]
fn child_normalization_bounds_long_lines_and_control_candidates() {
    let long = vec![b'x'; CHILD_FRAGMENT_BYTES + 19];
    let mut stream = ChildStream::default();
    let mut records = stream.push(&long);
    records.extend(stream.close());
    assert_eq!(records.len(), 2);
    assert!(!records[0].continuation);
    assert!(records[1].continuation);
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.as_str())
            .collect::<String>(),
        String::from_utf8(long).unwrap()
    );

    let mut candidate = Vec::from(b"\x1b]".as_slice());
    candidate.extend(std::iter::repeat_n(b'a', CONTROL_SEQUENCE_BYTES - 2));
    candidate.push(b'z');
    let mut stream = ChildStream::default();
    let mut records = stream.push(&candidate);
    records.extend(stream.close());
    let exposed = records
        .iter()
        .map(|record| record.payload.as_str())
        .collect::<String>();
    assert!(exposed.starts_with("\\x1b]"));
    assert!(exposed.ends_with('z'));
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

    assert_eq!(result, WorkflowRunPresentationResult::Rejected);
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
    assert_eq!(result, WorkflowRunPresentationResult::Rejected);
    assert!(human_stdout.text().is_empty());
    assert!(human_stderr.text().contains("source_root_unavailable"));
}

#[test]
fn rejection_json_writer_failure_is_diagnosed_on_stderr() {
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
    assert!(stderr.text().contains("workflow run output failure"));
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

    assert_eq!(result, WorkflowRunPresentationResult::Rejected);
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
                reason: CancellationReason::UserRequest,
            }),
        ))
        .await;

    let output = stdout.text();
    assert!(output.contains("view opened 2026-08-02T12:01:44.999999999Z"));
    assert!(output.contains("[12:01:45.123] a  start  cmd · printf 'hello world'"));
    assert!(output.contains("a  stderr  warning"));
    assert!(output.contains("a  stderr  final"));
    assert!(!output.contains("\u{1b}[2J"));
    let acknowledgement = output.find("@workflow  cancelling  user_request").unwrap();
    let step_cancelling = output.rfind("a  cancelling  user_request").unwrap();
    assert!(acknowledgement < step_cancelling);
    assert_eq!(output.matches("@workflow  cancelling").count(), 1);
    assert!(stderr.text().is_empty());
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
        TestClock::fixed("2026-08-02T12:01:44Z"),
    )
    .unwrap();

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
        output.find("produce  output  report · committed").unwrap()
            < output.find("produce  done").unwrap()
    );
}

#[test]
fn plain_and_json_route_complete_summaries_and_terminal_json() {
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
    assert!(matches!(
        json.finish(&run, PublicationPresentation::Published(&json_terminal)),
        WorkflowRunPresentationResult::Published { exit_status: 0, .. }
    ));

    let plain_view = plain_stdout.text();
    assert!(plain_view.contains("step  kind  state  duration  detail"));
    assert!(plain_view.find("a  cmd").unwrap() < plain_view.find("b  cmd").unwrap());
    assert!(plain_view.find("b  cmd").unwrap() < plain_view.find("c  cmd").unwrap());
    assert!(plain_view.contains("workflow succeeded · 3 succeeded"));
    assert!(plain_view.contains("result: "));
    assert!(plain_stderr.text().is_empty());

    let json_view = json_stderr.text();
    assert!(json_view.contains("step  kind  state  duration  detail"));
    assert!(json_view.contains("workflow succeeded · 3 succeeded"));
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
}

#[test]
fn failed_and_cancelled_summaries_use_authoritative_terminal_facts() {
    let fixture = Fixture::new(workflow_source());
    let mut failed = fixture.succeeded_run();
    let cause = StepFailureCause::Execution(StepExecutionFailure::Command(
        CommandExecutionFailure::UnsuccessfulExit { code: Some(23) },
    ));
    failed.steps[1].state = StepState::Failed {
        phase: FailurePhase::Execution,
        cause: cause.clone(),
    };
    failed.outcome = RunOutcome::Failed {
        primary_failure: StepFailure {
            step: "b".to_owned(),
            phase: FailurePhase::Execution,
            cause,
        },
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
    assert!(failed_output.text().contains("workflow failed"));
    assert!(
        failed_output
            .text()
            .contains("failure: b · execution · exit 23")
    );

    let mut cancelled = fixture.succeeded_run();
    for step in &mut cancelled.steps {
        step.state = StepState::Cancelled {
            reason: CancellationReason::TerminationRequest,
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
    assert!(cancelled_output.text().contains("workflow cancelled"));
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

    assert_eq!(result, WorkflowRunPresentationResult::PublicationFailed);
    assert!(stdout.text().is_empty());
    assert!(stderr.text().contains("step  kind  state"));
    assert!(stderr.text().contains("workflow succeeded"));
    assert!(stderr.text().contains("result publication failed"));
    assert!(!stderr.text().contains("result: "));
}

#[test]
fn header_writer_failure_prevents_a_live_presentation_from_starting() {
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
    assert!(stderr.text().contains("workflow run output failure"));
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
    assert!(stderr.text().contains("workflow succeeded"));
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

    assert!(stderr.text().contains("\u{1b}[32m"));
    assert!(!stdout.text().contains('\u{1b}'));
    let _: Value = serde_json::from_str(&stdout.text()).unwrap();
}
