use std::fs;
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rustix::process::{Pid, getpgid};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use super::adapter::ClaudeCodeStreamJsonV1Adapter;
use super::test_support::{
    LoopbackProvider, PendingClock, RecordingObservationSink, SyntheticClaudeCodeRoot,
    admitted_adapter, invocation_identity, version_probe_environment,
};
use super::*;
use crate::execution::workflow::admission::{CancellationReason, CancellationSource};
use crate::execution::workflow::agent::{
    AgentInvocation, AgentInvocationLimits, AgentInvocationStaging, AgentProcessContext,
    AgentProcessControl, AgentPrompt, AgentStartReceiver, AgentTerminalReceiver, AgentValueMode,
    PositiveDuration, RetainedResultSchema, StagedAgentAttachment, agent_start_channel,
    agent_terminal_channel, invoke_agent_adapter,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::observation::NoopExecutionObserver;
use crate::execution::workflow::process_group::{ProcessGuardRegistry, process_group_is_quiescent};

const MODEL: &str = "scherzo-loopback";
const EFFORT: &str = "xhigh";
const RESPONSE: &str = "loopback complete";
const WATCHDOG: Duration = Duration::from_secs(20);

fn conformance_executable() -> Option<PathBuf> {
    option_env!("SCHERZO_CLAUDE_CODE_CONFORMANCE_EXECUTABLE").map(PathBuf::from)
}

fn conformance_limits() -> AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits> {
    AgentInvocationLimits::new(
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
    )
}

struct RunningProductionClaudeCode {
    task: tokio::task::JoinHandle<()>,
    started: AgentStartReceiver,
    terminal: AgentTerminalReceiver,
    cancellation: CancellationSource,
    process_control: AgentProcessControl,
}

impl RunningProductionClaudeCode {
    fn launch(
        executable: PathBuf,
        root: &SyntheticClaudeCodeRoot,
        provider: &LoopbackProvider,
        value_mode: AgentValueMode,
    ) -> Self {
        // Lifecycle cases deliberately build their own invocation instead of borrowing the
        // attachment case's resources, so native cancellation cannot contaminate another root.
        // jscpd:ignore-start
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        let cancellation = CancellationSource::new();
        let invocation = AgentInvocation::new(
            invocation_identity("claude-code-lifecycle-conformance", "agent"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, root.environment_snapshot(provider)),
            AgentInvocationStaging::new(root.private().to_owned()),
            AgentDiagnosticSession::fixture(root.private().join("diagnostics/session")),
            AgentPrompt::new(
                Arc::from(fs::read_to_string(root.system_prompt()).unwrap()),
                Arc::from("Complete the controlled lifecycle exchange."),
            ),
            Arc::from([]),
            value_mode.clone(),
            conformance_limits(),
            cancellation.clone(),
            ProcessGuardRegistry::default(),
            RecordingObservationSink::default(),
        );
        let process_control = invocation.process_control().clone();
        let adapter = ClaudeCodeStreamJsonV1Adapter::new(
            StepDiagnosticLog::default(),
            NonZeroU64::new(1024).unwrap(),
            PendingClock,
            NoopExecutionObserver,
        )
        .unwrap();
        let (started_callback, started) = agent_start_channel();
        let (terminal_callback, terminal) = agent_terminal_channel(&value_mode);
        let task = tokio::spawn(async move {
            invoke_agent_adapter(&adapter, invocation, started_callback, terminal_callback).await;
        });
        Self {
            task,
            started,
            terminal,
            cancellation,
            process_control,
        }
        // jscpd:ignore-end
    }

    async fn await_started(self) -> RunningStartedClaudeCode {
        self.started.receive().await.unwrap();
        RunningStartedClaudeCode {
            task: self.task,
            terminal: self.terminal,
            cancellation: self.cancellation,
            process_control: self.process_control,
        }
    }
}

struct RunningStartedClaudeCode {
    task: tokio::task::JoinHandle<()>,
    terminal: AgentTerminalReceiver,
    cancellation: CancellationSource,
    process_control: AgentProcessControl,
}

impl RunningStartedClaudeCode {
    async fn finish(self) -> AgentOutcome {
        let outcome = self.terminal.receive().await.unwrap();
        self.task.await.unwrap();
        outcome
    }
}

async fn launch_response_lifecycle(
    executable: PathBuf,
    root: &SyntheticClaudeCodeRoot,
    provider: &LoopbackProvider,
) -> RunningStartedClaudeCode {
    RunningProductionClaudeCode::launch(
        executable,
        root,
        provider,
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
    )
    .await_started()
    .await
}

async fn assert_user_cancelled(running: RunningStartedClaudeCode) {
    assert_eq!(
        running.finish().await,
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest,
        }
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time only keeps polling schedulable; file contents are the success evidence"
)]
async fn wait_for_path(path: &std::path::Path) {
    while !fs::metadata(path).is_ok_and(|metadata| metadata.len() > 0) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn process_id(path: &std::path::Path) -> Pid {
    let raw = fs::read_to_string(path).unwrap().trim().parse().unwrap();
    Pid::from_raw(raw).unwrap()
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
    // Every exact-binary case deliberately owns a fresh watchdog, loopback provider, and
    // synthetic root; sharing those resources would let one native case contaminate another.
    // jscpd:ignore-start
    let Some(executable) = conformance_executable() else {
        return;
    };
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        // jscpd:ignore-end
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
    // Production-adapter cases each need independent native process and provider state;
    // sharing their synthetic roots would invalidate same-process correction evidence.
    // jscpd:ignore-start
    let Some(executable) = conformance_executable() else {
        return;
    };
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        // jscpd:ignore-end
        let observations = RecordingObservationSink::default();
        let png_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl2xZcAAAAASUVORK5CYII=";
        let png = BASE64.decode(png_base64).unwrap();
        let attachment_specs: [(&str, &[u8]); 4] = [
            ("text/plain", b"native text attachment"),
            ("image/png", png.as_slice()),
            ("application/pdf", b"%PDF-native"),
            ("application/octet-stream", &[0xde, 0xad]),
        ];
        let attachments = attachment_specs
            .iter()
            .enumerate()
            .map(|(index, (media_type, bytes))| {
                let path = root.private().join(format!("{index:06}"));
                fs::write(&path, bytes).unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
                StagedAgentAttachment::new(
                    path,
                    Arc::from(*media_type),
                    Some(Arc::from("caller-controlled-name")),
                )
            })
            .collect::<Vec<_>>();
        let unsupported_sealed_path = attachments[3].path().to_str().unwrap().to_owned();
        let limits = conformance_limits();
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
            Arc::from(attachments),
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
        )
        .unwrap();
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
        assert!(contains_exact_string(
            request.body(),
            "Scherzo attachment 000000 (text/plain) follows:\nnative text attachment"
        ));
        assert!(contains_exact_value(
            request.body(),
            &json!({"type": "base64", "media_type": "image/png", "data": png_base64})
        ));
        assert!(contains_exact_value(
            request.body(),
            &json!({
                "type": "base64",
                "media_type": "application/pdf",
                "data": "JVBERi1uYXRpdmU="
            })
        ));
        assert!(contains_exact_string(
            request.body(),
            &format!(
                "Scherzo attachment 000003 has media type application/octet-stream and is available to runner tools at {unsupported_sealed_path}."
            )
        ));
        assert!(!contains_exact_string(
            request.body(),
            "caller-controlled-name"
        ));
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

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
async fn pinned_claude_code_03_corrects_a_result_in_one_production_conversation() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        let schema_document = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "integer",
            "minimum": 1,
        });
        let schema_bytes = Arc::<[u8]>::from(serde_json::to_vec(&schema_document).unwrap());
        let value_mode = AgentValueMode::Result {
            output: Arc::from("result"),
            schema: RetainedResultSchema::compile(schema_bytes, Arc::new(schema_document)).unwrap(),
        };
        let limits = conformance_limits();
        let observations = RecordingObservationSink::default();
        let invocation = AgentInvocation::new(
            invocation_identity("claude-code-result-conformance", "agent"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, root.environment_snapshot(&provider)),
            AgentInvocationStaging::new(root.private().to_owned()),
            AgentDiagnosticSession::fixture(root.private().join("diagnostics/session")),
            AgentPrompt::new(
                Arc::from(fs::read_to_string(root.system_prompt()).unwrap()),
                Arc::from("Return the requested structured result."),
            ),
            Arc::from([]),
            value_mode.clone(),
            limits,
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        let adapter = ClaudeCodeStreamJsonV1Adapter::with_validation_worker(
            StepDiagnosticLog::default(),
            NonZeroU64::new(1024).unwrap(),
            PendingClock,
            NoopExecutionObserver,
            super::adapter_tests::InlineValidationWorker,
        );
        let (started, start) = agent_start_channel();
        let (terminal, outcome) = agent_terminal_channel(&value_mode);
        let mut execution = tokio::spawn(async move {
            invoke_agent_adapter(&adapter, invocation, started, terminal).await;
        });

        start.receive().await.unwrap();
        let first = provider.next_request().await;
        assert!(contains_exact_string(first.body(), "StructuredOutput"));
        first.release_structured_output(json!({"result": -1}));

        let second = tokio::select! {
            request = provider.next_request() => request,
            joined = &mut execution => {
                joined.unwrap();
                panic!("adapter ended before correction request: {:?}", outcome.receive().await);
            }
        };
        assert!(contains_exact_value(second.body(), &json!({"result": -1})));
        assert!(contains_string_fragment(
            second.body(),
            "Result rejected by the workflow schema:"
        ));
        second.release_structured_output(json!({"result": 7}));
        execution.await.unwrap();

        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) =
            outcome.receive().await.unwrap()
        else {
            panic!("exact binary corrected result must complete");
        };
        assert_eq!(result.value(), &json!(7));
        let observations = observations.snapshot();
        assert_eq!(
            observations
                .iter()
                .filter(|observation| matches!(
                    observation.observation(),
                    AgentObservation::ValueRejected {
                        kind: AgentValueKind::Result,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(observations.iter().any(|observation| matches!(
            observation.observation(),
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessQuiescent,
            }
        )));
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code result-correction watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
async fn pinned_claude_code_04_production_no_value_and_native_failure_are_typed() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    tokio::time::timeout(WATCHDOG, async {
        let mut success_provider = LoopbackProvider::start().await;
        let success_root = SyntheticClaudeCodeRoot::new();
        let success = RunningProductionClaudeCode::launch(
            executable.clone(),
            &success_root,
            &success_provider,
            AgentValueMode::None,
        )
        .await_started()
        .await;
        success_provider.next_request().await.release_text(RESPONSE);
        assert_eq!(
            success.finish().await,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
        );
        assert!(!success_provider.has_pending_request());
        success_provider.shutdown().await;

        let mut failure_provider = LoopbackProvider::start().await;
        let failure_root = SyntheticClaudeCodeRoot::new();
        let failure = RunningProductionClaudeCode::launch(
            executable,
            &failure_root,
            &failure_provider,
            AgentValueMode::None,
        )
        .await_started()
        .await;
        failure_provider
            .next_request()
            .await
            .release_invalid_request();
        assert_eq!(
            failure.finish().await,
            AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessFailed {
                    detail: AgentHarnessFailureDetail::ModelError,
                },
            }
        );
        assert!(!failure_provider.has_pending_request());
        failure_provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code no-value/failure watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
async fn pinned_claude_code_05_cancels_a_blocked_provider_request() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let running = launch_response_lifecycle(executable, &root, &provider).await;
        let blocked_request = provider.next_request().await;

        assert!(
            running
                .cancellation
                .request_cancellation(CancellationReason::UserRequest)
        );
        running.process_control.interrupt();
        assert_user_cancelled(running).await;
        drop(blocked_request);
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code blocked-provider cancellation watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
async fn pinned_claude_code_06_cancels_a_stubborn_bash_descendant() {
    let Some(executable) = conformance_executable() else {
        return;
    };
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let child_path = root.private().join("stubborn-child.pid");
        let running = launch_response_lifecycle(executable, &root, &provider).await;
        let command = format!(
            "trap '' INT TERM; printf '%s\\n' \"$$\" > '{}'; while :; do :; done",
            child_path.display()
        );
        provider.next_request().await.release_tool_use(
            "Bash",
            json!({
                "command": command,
                "description": "Run a controlled stubborn descendant",
            }),
        );
        wait_for_path(&child_path).await;
        let child = process_id(&child_path);
        let process_group = getpgid(Some(child)).unwrap();

        assert!(
            running
                .cancellation
                .request_cancellation(CancellationReason::UserRequest)
        );
        running.process_control.interrupt();

        assert_user_cancelled(running).await;
        assert!(process_group_is_quiescent(process_group));
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code stubborn-child cancellation watchdog expired");
}

fn contains_exact_value(value: &Value, expected: &Value) -> bool {
    value == expected
        || match value {
            Value::Array(values) => values
                .iter()
                .any(|value| contains_exact_value(value, expected)),
            Value::Object(object) => object
                .values()
                .any(|value| contains_exact_value(value, expected)),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
        }
}

fn contains_string_fragment(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value.contains(expected),
        Value::Array(values) => values
            .iter()
            .any(|value| contains_string_fragment(value, expected)),
        Value::Object(object) => object
            .values()
            .any(|value| contains_string_fragment(value, expected)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
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
