use std::fs;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use super::adapter::ClaudeCodeStreamJsonV1Adapter;
use super::test_support::{
    LoopbackProvider, PendingClock, RecordingObservationSink, SyntheticClaudeCodeRoot,
    admitted_adapter, invocation_identity, version_probe_environment,
};
use super::*;
use crate::execution::workflow::admission::CancellationSource;
use crate::execution::workflow::agent::{
    AgentInvocation, AgentInvocationLimits, AgentInvocationStaging, AgentProcessContext,
    AgentPrompt, AgentValueMode, PositiveDuration, agent_start_channel, agent_terminal_channel,
    invoke_agent_adapter,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::observation::NoopExecutionObserver;
use crate::execution::workflow::process_group::ProcessGuardRegistry;

const MODEL: &str = "scherzo-loopback";
const EFFORT: &str = "xhigh";
const RESPONSE: &str = "loopback complete";
const WATCHDOG: Duration = Duration::from_secs(20);

fn conformance_executable() -> Option<PathBuf> {
    option_env!("SCHERZO_CLAUDE_CODE_CONFORMANCE_EXECUTABLE").map(PathBuf::from)
}

#[test]
fn pinned_claude_code_00_qualification_anchor_is_exact() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    for (_, value) in version_probe_environment(temporary.path()) {
        fs::create_dir_all(value).unwrap();
    }
    let output = std::process::Command::new(executable)
        .arg("--version")
        .env_clear()
        .envs(version_probe_environment(temporary.path()))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        output.stdout,
        format!("{CLAUDE_CODE_STREAM_JSON_V1_VERSION} (Claude Code)\n").as_bytes()
    );
    println!(
        "qualified Claude Code version={} profile=ClaudeCodeStreamJsonV1 host={}-{}",
        CLAUDE_CODE_STREAM_JSON_V1_VERSION,
        std::env::consts::OS,
        std::env::consts::ARCH,
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
async fn pinned_claude_code_01_normal_mode_loopback_conforms_from_a_synthetic_root() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let expected_cwd = fs::canonicalize(root.project()).unwrap();
        let message = "Complete the deterministic synthetic exchange.";

        let mut command = Command::new(executable);
        command
            .args(normal_mode_arguments(MODEL, EFFORT, root.system_prompt()))
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        root.configure_command(&mut command, &provider);
        let mut child = command.spawn().unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(&initial_user_text_frame(message).unwrap())
            .await
            .unwrap();
        stdin.shutdown().await.unwrap();
        drop(stdin);

        let request = provider.next_request().await;
        assert_eq!(request.path(), "/v1/messages?beta=true");
        assert!(request.used_placeholder_key());
        assert_eq!(request.body()["model"], MODEL);
        assert_eq!(request.body()["stream"], true);
        assert!(contains_exact_string(request.body(), message));
        request.release_text(RESPONSE);

        let output = child.wait_with_output().await.unwrap();
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert!(!provider.has_pending_request());

        let mut parser = ClaudeCodeStreamJsonV1Parser::profile(
            Arc::from(expected_cwd.to_str().unwrap()),
            Arc::from(MODEL),
            crate::execution::workflow::agent::AgentValueKind::Response,
            NonZeroU64::new(1024).unwrap(),
        );
        for chunk in output.stdout.chunks(7) {
            parser.push_stdout(chunk, drop).unwrap();
        }
        assert!(parser.session_id().is_some());
        assert_eq!(parser.completed_exchanges(), 1);
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            parser.finish(true)
        else {
            panic!("exact binary response must complete through the production parser");
        };
        assert_eq!(response.as_str(), RESPONSE);
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code conformance watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
async fn pinned_claude_code_02_production_driver_returns_one_normalized_response() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        let observations = RecordingObservationSink::default();
        let limits = AgentInvocationLimits::new(
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            NonZeroU64::new(4096).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(512).unwrap(),
            PositiveDuration::new(Duration::from_secs(1)).unwrap(),
            PositiveDuration::new(Duration::from_secs(1)).unwrap(),
            ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
        );
        let value_mode = AgentValueMode::Response {
            output: Arc::from("response"),
        };
        let invocation = AgentInvocation::new(
            invocation_identity("claude-code-conformance", "agent"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, root.environment_snapshot(&provider)),
            AgentInvocationStaging::new(root.private().to_owned()),
            AgentDiagnosticSession::fixture(root.private().join("diagnostics/session")),
            AgentPrompt::new(
                Arc::from(fs::read_to_string(root.system_prompt()).unwrap()),
                Arc::from("Complete through the production Scherzo adapter."),
            ),
            Arc::from([]),
            value_mode.clone(),
            limits,
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        let adapter = ClaudeCodeStreamJsonV1Adapter::new(
            StepDiagnosticLog::default(),
            NonZeroU64::new(1024).unwrap(),
            PendingClock,
            NoopExecutionObserver,
        );
        let (started, start) = agent_start_channel();
        let (terminal, outcome) = agent_terminal_channel(&value_mode);
        let execution = tokio::spawn(async move {
            invoke_agent_adapter(&adapter, invocation, started, terminal).await;
        });

        start.receive().await.unwrap();
        let request = provider.next_request().await;
        assert_eq!(request.path(), "/v1/messages?beta=true");
        assert!(request.used_placeholder_key());
        assert_eq!(request.body()["model"], MODEL);
        request.release_text(RESPONSE);
        execution.await.unwrap();

        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            outcome.receive().await.unwrap()
        else {
            panic!("production exact-binary adapter must return one response outcome");
        };
        assert_eq!(response.as_str(), RESPONSE);
        let observations = observations.snapshot();
        assert_eq!(
            observations
                .iter()
                .filter_map(|observation| match observation.observation() {
                    AgentObservation::AssistantText { text } => Some(text.as_ref()),
                    _ => None,
                })
                .collect::<String>(),
            RESPONSE
        );
        assert!(!observations.iter().any(|observation| matches!(
            observation.observation(),
            AgentObservation::UnrecognizedHarnessEvent { .. }
        )));
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code production-driver watchdog expired");
}

fn contains_exact_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| contains_exact_string(value, expected)),
        Value::Object(object) => object
            .values()
            .any(|value| contains_exact_string(value, expected)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}
