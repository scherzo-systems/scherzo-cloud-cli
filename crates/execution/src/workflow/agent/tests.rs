use std::future::{Future, ready};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::mpsc;

use super::dispatch::invoke_agent_dispatcher;
use super::scripted::{ScriptedAgentControl, ScriptedAgentValue, scripted_agent_dispatcher};
use super::*;
use crate::workflow::admission::{CancellationReason, EnvironmentSnapshot};
use crate::workflow::agent_input::ClosedAgentInvocation;
use crate::workflow::execution_root::AdmittedExecutionRoot;
use crate::workflow::pi::{PiConfig, Thinking};
use crate::workflow::pi_json_v1::PiJsonV1ProtocolLimits;
use crate::workflow::runtime::TransitionSequence;
use crate::workflow::validated::WorkflowValueType;

#[derive(Clone)]
struct AcceptThenBlockObservationSink {
    observations: mpsc::UnboundedSender<AgentObservationEnvelope>,
}

impl AgentObservationSink for AcceptThenBlockObservationSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        let observations = self.observations.clone();
        async move {
            let _ = observations.send(observation);
            std::future::pending().await
        }
    }
}

#[derive(Clone)]
struct RecordingObservationSink {
    observations: mpsc::UnboundedSender<AgentObservationEnvelope>,
}

impl AgentObservationSink for RecordingObservationSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        let _ = self.observations.send(observation);
        ready(())
    }
}

type TestInvocation = AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, RecordingObservationSink>;

#[derive(Clone, Copy)]
struct ReturningWithoutTerminalAdapter;

impl AgentAdapter<RecordingObservationSink> for ReturningWithoutTerminalAdapter {
    type NativeConfiguration = PiConfig;
    type ProtocolLimits = PiJsonV1ProtocolLimits;

    async fn invoke(
        &self,
        _invocation: TestInvocation,
        _started: AgentStartCallback,
        _terminal: AgentTerminalCallback,
    ) {
    }
}

struct InvocationFixture {
    _temporary: tempfile::TempDir,
    invocation: TestInvocation,
    cancellation: CancellationSource,
    observations: mpsc::UnboundedReceiver<AgentObservationEnvelope>,
    start_callback: AgentStartCallback,
    started: AgentStartReceiver,
    terminal_callback: AgentTerminalCallback,
    terminal: AgentTerminalReceiver,
}

fn invocation_fixture(value_mode: AgentValueMode) -> InvocationFixture {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    std::fs::create_dir_all(execution_root.join("worktree")).unwrap();
    let cwd = AdmittedExecutionRoot::admit(&execution_root)
        .unwrap()
        .select_working_directory(Some("worktree"))
        .unwrap();
    let cancellation = CancellationSource::new();
    let identity = AgentInvocationIdentity::new(
        WorkflowRunId::from(Arc::from("run-fixed")),
        Arc::from("agent-step"),
        ActionId {
            transition_sequence: TransitionSequence::default(),
        },
    );
    let (observation_sender, observations) = mpsc::unbounded_channel();
    let limits = AgentInvocationLimits::new(
        NonZeroU64::new(64 * 1024).unwrap(),
        NonZeroU64::new(64 * 1024).unwrap(),
        NonZeroUsize::new(256).unwrap(),
        NonZeroU64::new(256 * 1024 * 1024).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroU64::new(8 * 1024).unwrap(),
        PositiveDuration::new(Duration::from_secs(5)).unwrap(),
        PositiveDuration::new(Duration::from_secs(30)).unwrap(),
        PiJsonV1ProtocolLimits::profile(),
    );
    let invocation = AgentInvocation::new(
        identity,
        AdmittedAgentAdapter::new(
            AgentCompatibilityProfile::PiJsonV1,
            "/validated/pi".into(),
            Arc::from("0.84.2"),
            PiConfig {
                model: "openai/gpt-5".to_owned(),
                thinking: Thinking::XHigh,
            },
        ),
        AgentProcessContext::new(cwd, EnvironmentSnapshot::new([("PATH", "/runner/bin")])),
        AgentInvocationStaging::new("/staging/invocation/result-endpoint".into()),
        crate::workflow::agent_diagnostics::AgentDiagnosticSession::fixture(
            temporary.path().join("diagnostic-session"),
        ),
        AgentPrompt::new(Arc::from("system"), Arc::from("message")),
        Arc::from([StagedAgentAttachment::new(
            "/staging/invocation/000000".into(),
            Arc::from("text/plain"),
            Some(Arc::from("review.txt")),
        )]),
        value_mode.clone(),
        limits,
        cancellation.clone(),
        crate::workflow::process_group::ProcessGuardRegistry::default(),
        RecordingObservationSink {
            observations: observation_sender,
        },
    );
    let (start_callback, started) = agent_start_channel();
    let (terminal_callback, terminal) = agent_terminal_channel(&value_mode);
    InvocationFixture {
        _temporary: temporary,
        invocation,
        cancellation,
        observations,
        start_callback,
        started,
        terminal_callback,
        terminal,
    }
}

fn result_mode() -> AgentValueMode {
    let document = Arc::new(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object"
    }));
    AgentValueMode::Result {
        output: Arc::from("result"),
        schema: RetainedJsonSchema::compile(
            Arc::from(
                br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#
                    .as_slice(),
            ),
            document,
        )
        .unwrap(),
    }
}

async fn start_script(
    fixture: InvocationFixture,
) -> (
    ScriptedAgentControl,
    tokio::task::JoinHandle<()>,
    CancellationSource,
    mpsc::UnboundedReceiver<AgentObservationEnvelope>,
    AgentTerminalReceiver,
    AgentTerminalCallback,
) {
    let expected_value_kind = fixture.invocation.value_mode().kind();
    let terminal_probe = fixture.terminal_callback.clone();
    let (adapter, mut control) = scripted_agent_dispatcher();
    let task = tokio::spawn(async move {
        invoke_agent_dispatcher(
            &adapter,
            ClosedAgentInvocation::Pi(fixture.invocation),
            fixture.start_callback,
            fixture.terminal_callback,
        )
        .await;
    });
    let invocation = control.wait_until_started().await.unwrap();
    assert_eq!(invocation.identity().run().as_ref(), "run-fixed");
    assert_eq!(invocation.profile(), AgentCompatibilityProfile::PiJsonV1);
    assert_eq!(invocation.value_kind(), expected_value_kind);
    invocation.control().start().await.unwrap();
    fixture.started.receive().await.unwrap();
    (
        control,
        task,
        fixture.cancellation,
        fixture.observations,
        fixture.terminal,
        terminal_probe,
    )
}

#[tokio::test]
async fn adapter_return_without_terminal_report_becomes_protocol_failure() {
    let fixture = invocation_fixture(AgentValueMode::None);

    invoke_agent_adapter(
        &ReturningWithoutTerminalAdapter,
        fixture.invocation,
        fixture.start_callback,
        fixture.terminal_callback,
    )
    .await;

    assert_eq!(
        fixture.terminal.receive().await.unwrap(),
        failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[tokio::test]
async fn accepted_observation_keeps_its_sequence_when_delivery_is_cancelled() {
    let identity = AgentInvocationIdentity::new(
        WorkflowRunId::from(Arc::from("run-fixed")),
        Arc::from("agent-step"),
        ActionId {
            transition_sequence: TransitionSequence::default(),
        },
    );
    let (observations, mut accepted) = mpsc::unbounded_channel();
    let sink =
        OrderedAgentObservationSink::new(identity, AcceptThenBlockObservationSink { observations });

    let first_sink = sink.clone();
    let first_delivery = tokio::spawn(async move {
        first_sink
            .emit(AgentObservation::Model {
                name: Arc::from("first"),
            })
            .await
    });
    let first = accepted.recv().await.unwrap();
    first_delivery.abort();
    assert!(first_delivery.await.unwrap_err().is_cancelled());

    let second_delivery = tokio::spawn(async move {
        sink.emit(AgentObservation::Model {
            name: Arc::from("second"),
        })
        .await
    });
    let second = accepted.recv().await.unwrap();
    second_delivery.abort();
    assert!(second_delivery.await.unwrap_err().is_cancelled());

    assert!(
        first.sequence() < second.sequence(),
        "accepted observations must retain strictly increasing identities"
    );
}

#[tokio::test]
async fn scripted_adapter_completes_each_value_mode_with_its_typed_value() {
    let no_value = run_success(AgentValueMode::None, None).await;
    assert_eq!(
        no_value,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );

    let response = run_success(
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        Some(ScriptedAgentValue::Response(Arc::from(""))),
    )
    .await;
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = response else {
        panic!("response mode must produce a bounded response");
    };
    assert_eq!(response.as_str(), "");

    let result = run_success(
        result_mode(),
        Some(ScriptedAgentValue::Result(Arc::new(json!({
            "verdict": "accepted"
        })))),
    )
    .await;
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = result else {
        panic!("result mode must produce a schema-valid result");
    };
    assert_eq!(result.value_type(), WorkflowValueType::Json);
    assert_eq!(result.value()["verdict"], "accepted");
    assert_eq!(result.canonical_json(), br#"{"verdict":"accepted"}"#);
    assert_eq!(result.schema().document()["type"], "object");
}

async fn run_success(
    value_mode: AgentValueMode,
    proposal: Option<ScriptedAgentValue>,
) -> AgentOutcome {
    let expected_kind = value_mode.kind();
    let (control, task, _cancellation, _observations, terminal, _terminal_probe) =
        start_script(invocation_fixture(value_mode)).await;
    if let Some(proposal) = proposal {
        control.propose(proposal).await.unwrap();
    }
    control.complete().await.unwrap();
    let outcome = terminal.receive().await.unwrap();
    task.await.unwrap();
    assert!(matches!(
        (&outcome, expected_kind),
        (
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
            AgentValueKind::None
        ) | (
            AgentOutcome::Completed(CompletedAgentInvocation::Response(_)),
            AgentValueKind::Response
        ) | (
            AgentOutcome::Completed(CompletedAgentInvocation::Result(_)),
            AgentValueKind::Result
        )
    ));
    outcome
}

#[test]
fn validation_fatals_map_to_the_closed_agent_failure_causes() {
    let deadline = PositiveDuration::new(Duration::from_secs(5)).unwrap();
    assert_eq!(
        AgentFailureCause::from(ResultValidationFatal::LimitExceeded { deadline }),
        AgentFailureCause::ResultValidationLimitExceeded { deadline }
    );
    assert_eq!(
        AgentFailureCause::from(ResultValidationFatal::WorkerFailed),
        AgentFailureCause::HarnessProtocolFailed
    );
}

#[tokio::test]
async fn observations_are_repeatable_ordered_and_never_terminal() {
    let first = run_observation_transcript().await;
    let second = run_observation_transcript().await;
    assert_eq!(first, second);

    for (index, envelope) in first.iter().enumerate() {
        assert_eq!(envelope.run().as_ref(), "run-fixed");
        assert_eq!(envelope.step(), "agent-step");
        assert_eq!(
            envelope.invocation(),
            ActionId {
                transition_sequence: TransitionSequence::default(),
            }
        );
        assert_eq!(envelope.sequence().get(), u64::try_from(index).unwrap() + 1);
    }
    assert!(matches!(
        first[0].observation(),
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::HarnessStarted
        }
    ));
}

async fn run_observation_transcript() -> Vec<AgentObservationEnvelope> {
    let fixture = invocation_fixture(AgentValueMode::None);
    let (control, task, _cancellation, mut observations, terminal, terminal_probe) =
        start_script(fixture).await;
    let transcript = [
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::HarnessStarted,
        },
        AgentObservation::AssistantText {
            text: Arc::from("working"),
        },
        AgentObservation::ToolCall {
            call_id: Arc::from("call-fixed"),
            name: Arc::from("inspect"),
            phase: AgentToolCallPhase::Started,
        },
        AgentObservation::ToolResult {
            call_id: Arc::from("call-fixed"),
            is_error: false,
            content: Arc::from("done"),
        },
        AgentObservation::ValueRejected {
            kind: AgentValueKind::Result,
            feedback: Arc::from("correct and resubmit"),
        },
    ];

    for observation in transcript.iter().cloned() {
        control.observe(observation).await.unwrap();
    }
    assert!(!terminal_probe.has_reported());

    let mut recorded = Vec::new();
    for _ in 0..transcript.len() {
        recorded.push(observations.recv().await.unwrap());
    }
    control.complete().await.unwrap();
    assert!(terminal_probe.has_reported());
    assert_eq!(
        terminal.receive().await.unwrap(),
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
    task.await.unwrap();
    recorded
}

#[test]
fn start_callback_accepts_one_acknowledgement() {
    let (started, receiver) = agent_start_channel();
    let competing_callback = started.clone();

    assert_eq!(started.report(), Ok(()));
    assert_eq!(
        competing_callback.report(),
        Err(AgentStartReportError::AlreadyReported)
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert_eq!(runtime.block_on(receiver.receive()), Ok(()));
}

#[test]
fn terminal_callback_accepts_one_mode_matching_outcome() {
    let value_mode = AgentValueMode::Response {
        output: Arc::from("response"),
    };
    let (terminal, receiver) = agent_terminal_channel(&value_mode);
    let competing_callback = terminal.clone();

    assert_eq!(
        terminal.report(AgentOutcome::Completed(CompletedAgentInvocation::NoValue)),
        Err(AgentTerminalReportError::CompletionModeMismatch)
    );
    assert_eq!(
        terminal.report(AgentOutcome::Completed(CompletedAgentInvocation::Response(
            BoundedAgentResponse::from_bounded(Arc::from("winner")),
        ))),
        Ok(())
    );
    assert_eq!(
        competing_callback.report(AgentOutcome::Cancelled {
            reason: CancellationReason::TerminationRequest,
        }),
        Err(AgentTerminalReportError::AlreadyReported)
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert_eq!(
        runtime.block_on(receiver.receive()).unwrap(),
        AgentOutcome::Completed(CompletedAgentInvocation::Response(
            BoundedAgentResponse::from_bounded(Arc::from("winner"))
        ))
    );
}
