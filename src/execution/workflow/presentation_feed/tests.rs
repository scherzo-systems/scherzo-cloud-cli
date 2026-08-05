use std::path::Path;
use std::sync::Arc;

use time::format_description::well_known::Rfc3339;

use super::*;
use crate::execution::workflow::observation::{CommandOutputSource, ObservedStepTransition};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{FailurePhase, StepStateKind, TransitionSequence};
use crate::execution::workflow::step_runtime::{
    CommandExecutionFailure, StepExecutionFailure, StepFailureCause,
};

fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).unwrap()
}

fn resolved_workflow(source: &str) -> (tempfile::TempDir, ResolvedWorkflow) {
    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(temporary.path().join("workflow.yaml"), source).unwrap();
    if source.contains("kind: agent") {
        std::fs::write(temporary.path().join("system.md"), "System.\n").unwrap();
        std::fs::write(temporary.path().join("message.md"), "Message.\n").unwrap();
    }
    let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
    (temporary, workflow)
}

fn action() -> ActionId {
    ActionId {
        transition_sequence: TransitionSequence::default(),
    }
}

fn step_transition(
    from: StepStateKind,
    to: StepStateKind,
    detail: Option<ObservedStepTransition>,
) -> ExecutionObservation<OffsetDateTime> {
    ExecutionObservation::Transition(TransitionObservation {
        event: TransitionEvent::Step {
            sequence: TransitionSequence::default(),
            step: "a".to_owned(),
            from,
            to,
        },
        step: detail,
    })
}

#[test]
fn definition_retains_presentation_order_and_typed_step_metadata() {
    let (_temporary, workflow) = resolved_workflow(
        "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
steps:
  a:
    kind: cmd
    cwd: packages/a
    command:
      argv: [\"printf\", \"hello world\"]
  b:
    kind: cmd
    dependsOn: [a]
    command:
      argv: [\"true\"]
  c:
    kind: agent
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text:
          - file: message.md
",
    );

    let feed = WorkflowPresentationFeed::new(&workflow);
    let definition = feed.definition();

    assert_eq!(definition.workflow_path, "workflow.yaml");
    assert_eq!(definition.presentation_order, ["a", "b", "c"]);
    assert_eq!(
        definition.steps["a"],
        WorkflowPresentationStep::Command {
            argv: vec!["printf".to_owned(), "hello world".to_owned()],
            cwd: Some("packages/a".to_owned()),
            direct_dependencies: Vec::new(),
            outputs: BTreeMap::new(),
        }
    );
    assert_eq!(
        definition.steps["c"],
        WorkflowPresentationStep::Agent {
            profile: "coding".to_owned(),
            harness: AgentPresentationHarness::Pi {
                model: "openai/gpt-5".to_owned(),
                thinking: Thinking::XHigh,
            },
            direct_dependencies: Vec::new(),
            outputs: BTreeMap::new(),
        }
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
    let long = vec![b'x'; MAX_NORMALIZED_CHILD_RECORD_BYTES + 19];
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
fn feed_preserves_typed_transitions_and_normalized_child_record_order() {
    let (_temporary, workflow) = resolved_workflow(
        "schemaVersion: 1
steps:
  a:
    kind: cmd
    command:
      argv: [\"true\"]
",
    );
    let mut feed = WorkflowPresentationFeed::new(&workflow);
    let running_at = timestamp("2026-08-04T12:00:00.001Z");
    let capturing_at = timestamp("2026-08-04T12:00:00.002Z");
    let output_at = timestamp("2026-08-04T12:00:00.003Z");
    let failed_at = timestamp("2026-08-04T12:00:00.004Z");
    let closed_at = timestamp("2026-08-04T12:00:00.005Z");
    let invocation = action();

    let running = feed.accept(
        running_at,
        step_transition(StepStateKind::Starting, StepStateKind::Running, None),
    );
    let capturing = feed.accept(
        capturing_at,
        step_transition(
            StepStateKind::Running,
            StepStateKind::CapturingOutputs,
            None,
        ),
    );
    let output = feed.accept(
        output_at,
        ExecutionObservation::<OffsetDateTime>::CommandOutput(CommandOutputObservation {
            step: "a".to_owned(),
            invocation,
            source: CommandOutputSource::StandardError,
            sequence: SourceSequence::first(),
            bytes: Arc::from(b"warning\nsecond\npartial".as_slice()),
        }),
    );
    let cause = StepFailureCause::Execution(StepExecutionFailure::Command(
        CommandExecutionFailure::UnsuccessfulExit { code: Some(23) },
    ));
    let failed = feed.accept(
        failed_at,
        step_transition(
            StepStateKind::CapturingOutputs,
            StepStateKind::Failed,
            Some(ObservedStepTransition::Failed {
                phase: FailurePhase::Execution,
                cause: cause.clone(),
            }),
        ),
    );
    let closed = feed.accept(
        closed_at,
        ExecutionObservation::<OffsetDateTime>::CommandOutputClosed(
            CommandOutputClosedObservation {
                step: "a".to_owned(),
                invocation,
                source: CommandOutputSource::StandardError,
                sequence: SourceSequence::first().next(),
            },
        ),
    );

    assert_eq!(
        [
            running[0].accepted_order.get(),
            capturing[0].accepted_order.get(),
            output[0].accepted_order.get(),
            output[1].accepted_order.get(),
            failed[0].accepted_order.get(),
            closed[0].accepted_order.get(),
        ],
        [1, 2, 3, 4, 5, 6]
    );
    assert!(matches!(
        &running[0].kind,
        PresentationRecordKind::Transition(TransitionObservation {
            event: TransitionEvent::Step {
                to: StepStateKind::Running,
                ..
            },
            step: None,
        })
    ));
    assert!(matches!(
        &capturing[0].kind,
        PresentationRecordKind::Transition(TransitionObservation {
            event: TransitionEvent::Step {
                to: StepStateKind::CapturingOutputs,
                ..
            },
            step: None,
        })
    ));
    assert!(matches!(
        &failed[0].kind,
        PresentationRecordKind::Transition(TransitionObservation {
            event: TransitionEvent::Step {
                to: StepStateKind::Failed,
                ..
            },
            step: Some(ObservedStepTransition::Failed {
                phase: FailurePhase::Execution,
                cause: observed,
            }),
        }) if observed == &cause
    ));

    let PresentationRecordKind::ChildOutput(output_record) = &output[0].kind else {
        panic!("a framed child line must become a normalized child record");
    };
    assert_eq!(output[0].observed_at, output_at);
    assert_eq!(output_record.step, "a");
    assert_eq!(output_record.invocation, invocation);
    assert_eq!(output_record.source, CommandOutputSource::StandardError);
    assert_eq!(output_record.source_sequence.get(), 1);
    assert_eq!(output_record.payload, "warning");
    assert!(!output_record.continuation);
    let PresentationRecordKind::ChildOutput(second_record) = &output[1].kind else {
        panic!("one accepted chunk may produce multiple ordered child records");
    };
    assert_eq!(second_record.source_sequence.get(), 1);
    assert_eq!(second_record.payload, "second");

    let PresentationRecordKind::ChildOutput(closed_record) = &closed[0].kind else {
        panic!("stream close must flush the final normalized fragment");
    };
    assert_eq!(closed[0].observed_at, closed_at);
    assert_eq!(closed_record.invocation, invocation);
    assert_eq!(closed_record.source_sequence.get(), 2);
    assert_eq!(closed_record.payload, "partial");
}
