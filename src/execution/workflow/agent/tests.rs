use std::ffi::OsStr;
use std::future::{Future, ready};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::mpsc;

use super::dispatch::invoke_agent_dispatcher;
use super::scripted::{
    ScriptedAgentControl, ScriptedAgentError, ScriptedAgentValue, scripted_agent_dispatcher,
};
use super::*;
use crate::execution::workflow::admission::{CancellationReason, EnvironmentSnapshot};
use crate::execution::workflow::agent_input::ClosedAgentInvocation;
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::pi::{PiConfig, Thinking};
use crate::execution::workflow::pi_json_v1::PiJsonV1ProtocolLimits;
use crate::execution::workflow::runtime::TransitionSequence;

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
        crate::execution::workflow::agent_diagnostics::AgentDiagnosticSession::fixture(
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
        crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
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
        schema: RetainedResultSchema::compile(
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

#[test]
fn invocation_retains_the_complete_immutable_engine_input() {
    let fixture = invocation_fixture(result_mode());
    let invocation = &fixture.invocation;

    assert_eq!(invocation.identity().run().as_ref(), "run-fixed");
    assert_eq!(invocation.identity().step(), "agent-step");
    assert_eq!(
        invocation.identity().invocation(),
        ActionId {
            transition_sequence: TransitionSequence::default(),
        }
    );
    assert_eq!(
        invocation.adapter().profile(),
        AgentCompatibilityProfile::PiJsonV1
    );
    assert_eq!(
        invocation.adapter().executable(),
        Path::new("/validated/pi")
    );
    assert_eq!(invocation.adapter().version(), "0.84.2");
    assert_eq!(
        invocation.adapter().native_configuration(),
        &PiConfig {
            model: "openai/gpt-5".to_owned(),
            thinking: Thinking::XHigh,
        }
    );
    assert_eq!(
        invocation.process().cwd(),
        std::fs::canonicalize(fixture._temporary.path().join("execution/worktree")).unwrap()
    );
    assert!(invocation.process().execution_root_is_bound());
    let mut command = std::process::Command::new("unused");
    invocation.process().bind_command(&mut command).unwrap();
    assert_eq!(
        invocation.staging().result_endpoint_directory(),
        Path::new("/staging/invocation/result-endpoint")
    );
    assert_eq!(
        invocation
            .process()
            .environment()
            .variable(OsStr::new("PATH")),
        Some(OsStr::new("/runner/bin"))
    );
    assert_eq!(invocation.prompt().system_prompt(), "system");
    assert_eq!(invocation.prompt().message(), "message");
    assert_eq!(invocation.attachments().len(), 1);
    assert_eq!(
        invocation.attachments()[0].path(),
        Path::new("/staging/invocation/000000")
    );
    assert_eq!(invocation.attachments()[0].media_type(), "text/plain");
    assert_eq!(
        invocation.attachments()[0].diagnostic_source_name(),
        Some("review.txt")
    );
    assert_eq!(invocation.value_mode().output(), Some("result"));
    let AgentValueMode::Result { schema, .. } = invocation.value_mode() else {
        panic!("fixture must use result mode");
    };
    assert_eq!(schema.document()["type"], "object");
    assert_eq!(
        schema.bytes(),
        br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#
    );
    assert_eq!(
        invocation.limits().maximum_system_prompt_bytes().get(),
        64 * 1024
    );
    assert_eq!(invocation.limits().maximum_message_bytes().get(), 64 * 1024);
    assert_eq!(invocation.limits().maximum_attachments().get(), 256);
    assert_eq!(
        invocation.limits().maximum_attachment_bytes().get(),
        256 * 1024 * 1024
    );
    assert_eq!(
        invocation
            .limits()
            .maximum_result_rejection_feedback_bytes()
            .get(),
        8 * 1024
    );
    assert_eq!(
        invocation.limits().result_validation_deadline().get(),
        Duration::from_secs(5)
    );
    assert_eq!(
        invocation.limits().result_settlement_grace().get(),
        Duration::from_secs(30)
    );
    assert_eq!(
        invocation.limits().adapter_protocol(),
        &PiJsonV1ProtocolLimits::profile()
    );
    assert!(!invocation.cancellation().is_cancelled());
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
    assert_eq!(result.value()["verdict"], "accepted");
    assert_eq!(result.canonical_json(), br#"{"verdict":"accepted"}"#);
}

#[tokio::test]
async fn scripted_adapter_generates_missing_value_failures() {
    let cases = [
        (
            AgentValueMode::Response {
                output: Arc::from("response"),
            },
            AgentFailureCause::MissingResponse,
        ),
        (result_mode(), AgentFailureCause::MissingResult),
    ];

    for (value_mode, cause) in cases {
        let (control, task, _cancellation, _observations, terminal, _terminal_probe) =
            start_script(invocation_fixture(value_mode)).await;
        control.complete().await.unwrap();
        assert_eq!(
            terminal.receive().await.unwrap(),
            failed_agent_outcome(cause)
        );
        task.await.unwrap();
    }
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
async fn scripted_adapter_preserves_every_closed_failure_cause() {
    let failures = [
        AgentFailureCause::HarnessStartFailed,
        AgentFailureCause::HarnessInputTooLarge {
            input: AgentInputKind::SystemPrompt,
            admitted_bytes: NonZeroU64::new(64).unwrap(),
            observed_bytes: 65,
        },
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::ModelError,
        },
        AgentFailureCause::HarnessProtocolFailed,
        AgentFailureCause::MissingResponse,
        AgentFailureCause::MissingResult,
        AgentFailureCause::ResultValidationLimitExceeded {
            deadline: PositiveDuration::new(Duration::from_secs(5)).unwrap(),
        },
        AgentFailureCause::CapturedValueTooLarge,
        AgentFailureCause::ResultSettlementFailed,
    ];

    for cause in failures {
        let (control, task, _cancellation, _observations, terminal, _terminal_probe) =
            start_script(invocation_fixture(AgentValueMode::None)).await;
        control.fail(cause.clone()).await.unwrap();
        assert_eq!(
            terminal.receive().await.unwrap(),
            failed_agent_outcome(cause)
        );
        task.await.unwrap();
    }
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

#[tokio::test]
async fn scripted_adapter_observes_initial_and_idle_cancellation() {
    let initial = invocation_fixture(AgentValueMode::None);
    assert!(
        initial
            .cancellation
            .request_cancellation(CancellationReason::RunnerShutdown)
    );
    let (adapter, mut initial_control) = scripted_agent_dispatcher();
    let initial_task = tokio::spawn(async move {
        invoke_agent_dispatcher(
            &adapter,
            ClosedAgentInvocation::Pi(initial.invocation),
            initial.start_callback,
            initial.terminal_callback,
        )
        .await;
    });
    assert_eq!(
        initial.terminal.receive().await.unwrap(),
        AgentOutcome::Cancelled {
            reason: CancellationReason::RunnerShutdown
        }
    );
    initial_task.await.unwrap();
    assert!(matches!(
        initial_control.wait_until_started().await,
        Err(ScriptedAgentError::AdapterStopped)
    ));

    let (_control, task, cancellation, _observations, terminal, _terminal_probe) =
        start_script(invocation_fixture(AgentValueMode::None)).await;
    assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
    assert_eq!(
        terminal.receive().await.unwrap(),
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest
        }
    );
    task.await.unwrap();
}

#[tokio::test]
async fn scripted_adapter_rejects_values_after_cancellation() {
    let (control, task, cancellation, _observations, terminal, _terminal_probe) =
        start_script(invocation_fixture(AgentValueMode::Response {
            output: Arc::from("response"),
        }))
        .await;

    assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
    assert!(
        control
            .propose(ScriptedAgentValue::Response(Arc::from("too late")))
            .await
            .is_err(),
        "cancellation must stop the adapter from accepting provisional values"
    );

    assert_eq!(
        terminal.receive().await.unwrap(),
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest
        }
    );
    task.await.unwrap();
}

#[tokio::test]
async fn explicit_barriers_make_cancellation_close_races_deterministic() {
    let fixture = invocation_fixture(AgentValueMode::Response {
        output: Arc::from("response"),
    });
    let (control, task, cancellation, _observations, terminal, terminal_probe) =
        start_script(fixture).await;
    control
        .propose(ScriptedAgentValue::Response(Arc::from("provisional")))
        .await
        .unwrap();
    let mut barrier = control.block().unwrap();
    barrier.wait_until_blocked().await.unwrap();
    assert!(!terminal_probe.has_reported());
    assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
    barrier.release().unwrap();
    assert_eq!(
        terminal.receive().await.unwrap(),
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest
        }
    );
    assert!(terminal_probe.has_reported());
    task.await.unwrap();

    let (control, task, cancellation, _observations, terminal, _terminal_probe) =
        start_script(invocation_fixture(AgentValueMode::None)).await;
    control
        .fail(AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::UnsuccessfulExit,
        })
        .await
        .unwrap();
    assert!(cancellation.request_cancellation(CancellationReason::RunnerShutdown));
    assert_eq!(
        terminal.receive().await.unwrap(),
        failed_agent_outcome(AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::UnsuccessfulExit,
        })
    );
    task.await.unwrap();
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
