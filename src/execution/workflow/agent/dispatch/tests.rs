use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;
use crate::execution::workflow::admission::{CancellationSource, EnvironmentSnapshot};
use crate::execution::workflow::agent::{
    AdmittedAgentAdapter, AgentCompatibilityProfile, AgentInvocation, AgentInvocationIdentity,
    AgentInvocationLimits, AgentInvocationStaging, AgentOutcome, AgentProcessContext, AgentPrompt,
    AgentValueMode, CompletedAgentInvocation, NoopAgentObservationSink, PositiveDuration,
    WorkflowRunId, agent_start_channel, agent_terminal_channel,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::execution::workflow::agent_input::ClosedAgentInvocation;
use crate::execution::workflow::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use crate::execution::workflow::claude_code_stream_json_v1::ClaudeCodeStreamJsonV1ProtocolLimits;
use crate::execution::workflow::codex::CodexConfig;
use crate::execution::workflow::codex_app_server_v1::CodexAppServerV1ProtocolLimits;
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::pi::{PiConfig, Thinking};
use crate::execution::workflow::pi_json_v1::PiJsonV1ProtocolLimits;
use crate::execution::workflow::process_group::ProcessGuardRegistry;
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};

#[derive(Clone)]
struct RecordingPiAdapter(Arc<Mutex<Vec<PiConfig>>>);

impl AgentAdapter<NoopAgentObservationSink> for RecordingPiAdapter {
    type NativeConfiguration = PiConfig;
    type ProtocolLimits = PiJsonV1ProtocolLimits;

    async fn invoke(
        &self,
        invocation: AgentInvocation<
            Self::NativeConfiguration,
            Self::ProtocolLimits,
            NoopAgentObservationSink,
        >,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) {
        self.0
            .lock()
            .unwrap()
            .push(invocation.adapter().native_configuration().clone());
        started.report().unwrap();
        terminal
            .report(AgentOutcome::Completed(CompletedAgentInvocation::NoValue))
            .unwrap();
    }
}

#[derive(Clone)]
struct RecordingClaudeCodeAdapter(Arc<Mutex<Vec<ClaudeCodeConfig>>>);

impl AgentAdapter<NoopAgentObservationSink> for RecordingClaudeCodeAdapter {
    type NativeConfiguration = ClaudeCodeConfig;
    type ProtocolLimits = ClaudeCodeStreamJsonV1ProtocolLimits;

    async fn invoke(
        &self,
        invocation: AgentInvocation<
            Self::NativeConfiguration,
            Self::ProtocolLimits,
            NoopAgentObservationSink,
        >,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) {
        self.0
            .lock()
            .unwrap()
            .push(invocation.adapter().native_configuration().clone());
        started.report().unwrap();
        terminal
            .report(AgentOutcome::Completed(CompletedAgentInvocation::NoValue))
            .unwrap();
    }
}

#[derive(Clone)]
struct RecordingCodexAdapter(Arc<Mutex<Vec<CodexConfig>>>);

impl AgentAdapter<NoopAgentObservationSink> for RecordingCodexAdapter {
    type NativeConfiguration = CodexConfig;
    type ProtocolLimits = CodexAppServerV1ProtocolLimits;

    async fn invoke(
        &self,
        invocation: AgentInvocation<
            Self::NativeConfiguration,
            Self::ProtocolLimits,
            NoopAgentObservationSink,
        >,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) {
        self.0
            .lock()
            .unwrap()
            .push(invocation.adapter().native_configuration().clone());
        started.report().unwrap();
        terminal
            .report(AgentOutcome::Completed(CompletedAgentInvocation::NoValue))
            .unwrap();
    }
}

fn invocation<Configuration, ProtocolLimits>(
    temporary: &tempfile::TempDir,
    profile: AgentCompatibilityProfile,
    executable: &str,
    version: &str,
    configuration: Configuration,
    protocol_limits: ProtocolLimits,
) -> AgentInvocation<Configuration, ProtocolLimits, NoopAgentObservationSink> {
    let root = temporary.path().join(format!("root-{profile:?}"));
    std::fs::create_dir(&root).unwrap();
    let cwd = AdmittedExecutionRoot::admit(&root)
        .unwrap()
        .select_working_directory(None)
        .unwrap();
    let limits = AgentInvocationLimits::new(
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(512).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        protocol_limits,
    );
    let diagnostic_session_path = temporary.path().join(format!("diagnostics-{profile:?}"));
    let diagnostic_session = match profile {
        AgentCompatibilityProfile::PiJsonV1 => {
            AgentDiagnosticSession::fixture(diagnostic_session_path)
        }
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
            AgentDiagnosticSession::claude_code_fixture(diagnostic_session_path)
        }
        AgentCompatibilityProfile::CodexAppServerV1 => {
            AgentDiagnosticSession::codex_fixture(diagnostic_session_path)
        }
    };
    AgentInvocation::new(
        AgentInvocationIdentity::new(
            WorkflowRunId::from(Arc::from("run")),
            Arc::from("agent"),
            ActionId {
                transition_sequence: TransitionSequence::default(),
            },
        ),
        AdmittedAgentAdapter::new(
            profile,
            executable.into(),
            Arc::from(version),
            configuration,
        ),
        AgentProcessContext::new(cwd, EnvironmentSnapshot::default()),
        AgentInvocationStaging::new(temporary.path().join("result-endpoint")),
        diagnostic_session,
        AgentPrompt::new(Arc::from("system"), Arc::from("message")),
        Arc::from([]),
        AgentValueMode::None,
        limits,
        CancellationSource::new(),
        ProcessGuardRegistry::default(),
        NoopAgentObservationSink,
    )
}

async fn dispatch(
    dispatcher: &ClosedAgentDispatcher<
        RecordingPiAdapter,
        RecordingClaudeCodeAdapter,
        RecordingCodexAdapter,
    >,
    invocation: ClosedAgentInvocation<NoopAgentObservationSink>,
) {
    let value_mode = AgentValueMode::None;
    let (started, start) = agent_start_channel();
    let (terminal, outcome) = agent_terminal_channel(&value_mode);
    invoke_agent_dispatcher(dispatcher, invocation, started, terminal).await;
    start.receive().await.unwrap();
    assert_eq!(
        outcome.receive().await.unwrap(),
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
}

#[tokio::test]
async fn closed_dispatcher_routes_each_native_profile_without_translation_or_fallback() {
    let temporary = tempfile::tempdir().unwrap();
    let pi_calls = Arc::new(Mutex::new(Vec::new()));
    let claude_code_calls = Arc::new(Mutex::new(Vec::new()));
    let codex_calls = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = ClosedAgentDispatcher::new(
        RecordingPiAdapter(Arc::clone(&pi_calls)),
        RecordingClaudeCodeAdapter(Arc::clone(&claude_code_calls)),
        RecordingCodexAdapter(Arc::clone(&codex_calls)),
    );
    let pi_config = PiConfig {
        model: "openai/gpt-5".to_owned(),
        thinking: Thinking::Minimal,
    };
    let claude_code_config = ClaudeCodeConfig {
        model: "claude-opus-4-1".to_owned(),
        effort: ClaudeCodeEffort::High,
    };
    let codex_config = CodexConfig {
        model: "gpt-5.4".to_owned(),
        effort: "xhigh".to_owned(),
    };

    dispatch(
        &dispatcher,
        ClosedAgentInvocation::Pi(invocation(
            &temporary,
            AgentCompatibilityProfile::PiJsonV1,
            "/validated/pi",
            "0.84.2",
            pi_config.clone(),
            PiJsonV1ProtocolLimits::profile(),
        )),
    )
    .await;
    dispatch(
        &dispatcher,
        ClosedAgentInvocation::ClaudeCode(invocation(
            &temporary,
            AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
            "/validated/claude",
            "2.1.234",
            claude_code_config.clone(),
            ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
        )),
    )
    .await;
    dispatch(
        &dispatcher,
        ClosedAgentInvocation::Codex(invocation(
            &temporary,
            AgentCompatibilityProfile::CodexAppServerV1,
            "/validated/codex",
            "0.147.23",
            codex_config.clone(),
            CodexAppServerV1ProtocolLimits::profile(),
        )),
    )
    .await;

    assert_eq!(*pi_calls.lock().unwrap(), [pi_config]);
    assert_eq!(*claude_code_calls.lock().unwrap(), [claude_code_config]);
    assert_eq!(*codex_calls.lock().unwrap(), [codex_config]);
}
