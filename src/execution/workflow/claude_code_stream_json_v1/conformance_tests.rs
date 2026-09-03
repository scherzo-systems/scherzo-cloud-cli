use std::fs;
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use rustix::process::{Pid, getpgid};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use super::adapter::ClaudeCodeStreamJsonV1Adapter;
use super::test_support::{
    FixtureSignal, LoopbackBlock, LoopbackProvider, PendingClock, RecordingObservationSink,
    SyntheticClaudeCodeRoot, admitted_adapter, invocation_identity, version_probe_environment,
};
use super::*;
use crate::execution::workflow::admission::{CancellationReason, CancellationSource};
use crate::execution::workflow::agent::{
    AgentInvocation, AgentInvocationLimits, AgentInvocationStaging, AgentProcessContext,
    AgentProcessControl, AgentPrompt, AgentStartReceiver, AgentTerminalReceiver, AgentValueMode,
    PositiveDuration, RetainedJsonSchema, StagedAgentAttachment, agent_start_channel,
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
const DIRECT_SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";
const WATCHDOG: Duration = Duration::from_secs(20);

static EXACT_BINARY_CONFORMANCE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn conformance_executable() -> PathBuf {
    std::env::var_os("SCHERZO_CLAUDE_CODE_CONFORMANCE_EXECUTABLE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            panic!("SCHERZO_CLAUDE_CODE_CONFORMANCE_EXECUTABLE must name the pinned Claude Code executable")
        })
}

async fn exclusive_conformance_executable() -> (PathBuf, tokio::sync::MutexGuard<'static, ()>) {
    let executable = conformance_executable();
    // Native startup is resource-intensive enough that concurrent qualification cases can
    // exhaust their independent anti-hang watchdogs before reaching controlled provider I/O.
    let exclusive = EXACT_BINARY_CONFORMANCE.lock().await;
    (executable, exclusive)
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
        observations: RecordingObservationSink,
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
            AgentDiagnosticSession::claude_code_fixture(root.private().join("diagnostics/session")),
            AgentPrompt::new(
                Arc::from(fs::read_to_string(root.system_prompt()).unwrap()),
                Arc::from("Complete the controlled lifecycle exchange."),
            ),
            Arc::from([]),
            value_mode.clone(),
            conformance_limits(),
            cancellation.clone(),
            ProcessGuardRegistry::default(),
            observations,
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
    launch_recorded_response_lifecycle(
        executable,
        root,
        provider,
        &RecordingObservationSink::default(),
    )
    .await
}

async fn launch_recorded_response_lifecycle(
    executable: PathBuf,
    root: &SyntheticClaudeCodeRoot,
    provider: &LoopbackProvider,
    observations: &RecordingObservationSink,
) -> RunningStartedClaudeCode {
    RunningProductionClaudeCode::launch(
        executable,
        root,
        provider,
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        observations.clone(),
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

fn process_id(bytes: &[u8]) -> Pid {
    let raw = std::str::from_utf8(bytes).unwrap().trim().parse().unwrap();
    Pid::from_raw(raw).unwrap()
}

fn assert_retained_native_session(root: &SyntheticClaudeCodeRoot, expected: &[&str]) {
    let transcript = fs::read_to_string(root.retained_transcript()).unwrap();
    for fragment in expected {
        assert!(
            transcript.contains(fragment),
            "missing retained fragment: {fragment}"
        );
    }
    assert!(root.retained_resources().is_dir());
    let ambient_paths = root.ambient_session_paths(DIRECT_SESSION_ID);
    for ambient in &ambient_paths {
        assert!(
            !ambient.exists(),
            "ambient session residue: {}",
            ambient.display()
        );
    }
    let ambient_project = ambient_paths[0].parent().unwrap();
    assert_eq!(
        fs::read_dir(ambient_project).unwrap().count(),
        0,
        "ambient project history retained an unrelated entry"
    );
}

#[test]
#[ignore = "requires pinned harness"]
fn pinned_real_claude_code_00_qualification_anchor_is_exact() {
    let executable = conformance_executable();
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
        format!("{QUALIFICATION_VERSION} (Claude Code)\n").as_bytes()
    );
    println!(
        "qualified Claude Code version={} profile=ClaudeCodeStreamJsonV1 host={}-{}",
        QUALIFICATION_VERSION,
        std::env::consts::OS,
        std::env::consts::ARCH,
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_01_normal_mode_loopback_conforms_from_a_synthetic_root() {
    // Every exact-binary case deliberately owns a fresh watchdog, loopback provider, and
    // synthetic root; sharing those resources would let one native case contaminate another.
    // jscpd:ignore-start
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        // jscpd:ignore-end
        let expected_cwd = fs::canonicalize(root.project()).unwrap();
        let message = "Complete the deterministic synthetic exchange.";

        let mut command = Command::new(executable);
        command
            .args(normal_mode_arguments(
                MODEL,
                EFFORT,
                DIRECT_SESSION_ID,
                root.system_prompt(),
            ))
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
        // The pinned release diagnoses the synthetic model on stderr. Its bytes are an
        // actionable process diagnostic, not protocol, and their wording is not a contract.
        assert!(!output.stderr.is_empty());
        assert!(!provider.has_pending_request());

        let mut parser = ClaudeCodeStreamJsonV1Parser::profile(
            Arc::from(expected_cwd.to_str().unwrap()),
            Arc::from(MODEL),
            Arc::from(DIRECT_SESSION_ID),
            Arc::from(QUALIFICATION_VERSION),
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
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_02_production_driver_returns_one_normalized_response() {
    // Production-adapter cases each need independent native process and provider state;
    // sharing their synthetic roots would invalidate same-process correction evidence.
    // jscpd:ignore-start
    let (executable, _exclusive) = exclusive_conformance_executable().await;
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
            AgentDiagnosticSession::claude_code_fixture(
                root.private().join("diagnostics/session"),
            ),
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
        let diagnostics = StepDiagnosticLog::default();
        let adapter = ClaudeCodeStreamJsonV1Adapter::new(
            diagnostics.clone(),
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
        assert_eq!(observations.concatenated_text(assistant_text), RESPONSE);
        let diagnostic = diagnostics.get("agent").unwrap();
        assert!(diagnostic.standard_output().bytes().is_empty());
        assert!(!diagnostic.standard_error().bytes().is_empty());
        assert_eq!(diagnostic.standard_error().truncation(), None);
        assert!(diagnostic.standard_error().fully_drained());
        assert!(!observations.snapshot().iter().any(|observation| matches!(
            observation.observation(),
            AgentObservation::UnrecognizedHarnessEvent { .. }
        )));
        assert!(!provider.has_pending_request());
        assert_retained_native_session(
            &root,
            &["Complete through the production Scherzo adapter.", RESPONSE],
        );
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
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03_corrects_a_result_in_one_production_conversation() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
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
            schema: RetainedJsonSchema::compile(schema_bytes, Arc::new(schema_document)).unwrap(),
        };
        let limits = conformance_limits();
        let observations = RecordingObservationSink::default();
        let invocation = AgentInvocation::new(
            invocation_identity("claude-code-result-conformance", "agent"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, root.environment_snapshot(&provider)),
            AgentInvocationStaging::new(root.private().to_owned()),
            AgentDiagnosticSession::claude_code_fixture(root.private().join("diagnostics/session")),
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
        assert_retained_native_session(
            &root,
            &[
                "Return the requested structured result.",
                "Result rejected by the workflow schema:",
            ],
        );
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
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_04_production_no_value_and_native_failure_are_typed() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut success_provider = LoopbackProvider::start().await;
        let success_root = SyntheticClaudeCodeRoot::new();
        let success = RunningProductionClaudeCode::launch(
            executable.clone(),
            &success_root,
            &success_provider,
            AgentValueMode::None,
            RecordingObservationSink::default(),
        )
        .await_started()
        .await;
        success_provider.next_request().await.release_text(RESPONSE);
        assert_eq!(
            success.finish().await,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
        );
        assert!(!success_provider.has_pending_request());
        assert_retained_native_session(&success_root, &[RESPONSE]);
        success_provider.shutdown().await;

        let mut failure_provider = LoopbackProvider::start().await;
        let failure_root = SyntheticClaudeCodeRoot::new();
        let failure = RunningProductionClaudeCode::launch(
            executable,
            &failure_root,
            &failure_provider,
            AgentValueMode::None,
            RecordingObservationSink::default(),
        )
        .await_started()
        .await;
        failure_provider
            .next_request()
            .await
            .release_invalid_request();
        assert_eq!(
            failure.finish().await,
            failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelError,
            })
        );
        assert!(!failure_provider.has_pending_request());
        assert_retained_native_session(
            &failure_root,
            &["Complete the controlled lifecycle exchange."],
        );
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
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_05_cancels_a_blocked_provider_request() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
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
        assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
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
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06_cancels_a_stubborn_bash_descendant() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let child = FixtureSignal::create(root.private().join("stubborn-child.pid"));
        let blocker = root.private().join("stubborn-child.blocker");
        mkfifo(&blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let running = launch_response_lifecycle(executable, &root, &provider).await;
        let command = format!(
            "trap '' INT TERM; printf '%s\\n' \"$$\" > '{}'; IFS= read -r unexpected < '{}'",
            child.path().display(),
            blocker.display()
        );
        provider.next_request().await.release_tool_use(
            "Bash",
            json!({
                "command": command,
                "description": "Run a controlled stubborn descendant",
            }),
        );
        let child = process_id(&child.receive().await);
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
        assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code stubborn-child cancellation watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_07_retains_forwarded_subagent_activity() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let running = launch_response_lifecycle(executable, &root, &provider).await;

        provider.next_request().await.release_tool_use(
            "Agent",
            json!({
                "description": "Run a controlled subagent",
                "prompt": "Return the exact subagent retained marker.",
                "subagent_type": "general-purpose",
                "run_in_background": false,
            }),
        );
        provider
            .next_request()
            .await
            .release_text("subagent retained marker");
        provider.next_request().await.release_text(RESPONSE);

        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            running.finish().await
        else {
            panic!("subagent exchange must complete through the production adapter");
        };
        assert_eq!(response.as_str(), RESPONSE);
        assert_retained_native_session(&root, &["subagent retained marker", RESPONSE]);
        let subagents = root.retained_resources().join("subagents");
        let retained = fs::read_dir(subagents)
            .unwrap()
            .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        assert!(!retained.is_empty());
        assert!(
            retained
                .iter()
                .any(|transcript| transcript.contains("subagent retained marker"))
        );
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code subagent-retention watchdog expired");
}

/// Ordered thinking segments deliberately carry multibyte, newline, tab, and trailing
/// whitespace so the case fails if Claude Code trims or normalizes reconstructed
/// thinking before restating it in its nominal assistant envelope.
const THINKING_SEGMENTS: [&str; 3] = [
    "Step one: \u{e9}\u{4e2d}\u{6587} ",
    "line\nbreak\ttab  ",
    "trailing space   ",
];

fn assistant_text(observation: &AgentObservation) -> Option<&str> {
    match observation {
        AgentObservation::AssistantText { text } => Some(text.as_ref()),
        _ => None,
    }
}

fn reasoning_text(observation: &AgentObservation) -> Option<&str> {
    match observation {
        AgentObservation::Reasoning { text } => Some(text.as_ref()),
        _ => None,
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_08_correlates_a_nominal_thinking_envelope_before_text() {
    // Every exact-binary case deliberately owns a fresh watchdog, loopback provider, and
    // synthetic root; sharing those resources would let one native case contaminate another.
    // jscpd:ignore-start
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        // jscpd:ignore-end
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;

        provider.next_request().await.release_blocks(vec![
            LoopbackBlock::thinking(&THINKING_SEGMENTS),
            LoopbackBlock::text(RESPONSE),
        ]);

        // Claude Code 2.1.259 emits a nominal `assistant` envelope restating the thinking
        // block. `ActiveContentBlock::correlate_nominal` requires that envelope to be
        // byte-equal to the reconstructed `thinking_delta` stream, so reaching a response
        // at all proves the equality invariant holds for native thinking.
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            running.finish().await
        else {
            panic!("a thinking block must not prevent an ordinary response outcome");
        };
        assert_eq!(response.as_str(), RESPONSE);

        // Thinking is observational only: it never contributes to the response, and the
        // response is exactly the text block that followed it.
        assert_eq!(
            observations.concatenated_text(reasoning_text),
            THINKING_SEGMENTS.concat()
        );
        assert_eq!(observations.concatenated_text(assistant_text), RESPONSE);
        // Thinking additionally makes Claude Code interleave `system` events with subtype
        // `thinking_tokens` between content-block events. They carry no authority, so they
        // must degrade to ordered unrecognized observations without breaking correlation.
        assert!(observations.snapshot().iter().any(|observation| matches!(
            observation.observation(),
            AgentObservation::UnrecognizedHarnessEvent { event }
                if event["type"] == "system" && event["subtype"] == "thinking_tokens"
        )));
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &[RESPONSE]);
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code nominal-thinking watchdog expired");
}

/// Opaque payload of a native `redacted_thinking` block. The native API delivers this
/// block complete in its `content_block_start`, with no deltas.
const REDACTED_THINKING_DATA: &str = "EmwKAhgBEgy3va3scherzoredacted";

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09_every_block_kind_correlates_its_own_nominal_envelope() {
    // Fresh per-case native resources, as in every other exact-binary case.
    // jscpd:ignore-start
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        // jscpd:ignore-end
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;

        // One native message carrying every content-block kind. `redacted_thinking` is an
        // unrecognized kind, so it correlates through `ActiveContentBlock::Unknown`, whose
        // rule is stricter than the text and thinking rules: the nominal envelope must equal
        // the whole `content_block_start` block. A tool_use block additionally proves the
        // exchange survives block-kind interleaving and continues into a second message.
        provider.next_request().await.release_blocks(vec![
            LoopbackBlock::thinking(&["deciding to call a tool "]),
            LoopbackBlock::redacted_thinking(REDACTED_THINKING_DATA),
            LoopbackBlock::tool_use(
                "Bash",
                json!({"command": "echo probe-marker", "description": "controlled probe"}),
            ),
        ]);
        provider.next_request().await.release_text(RESPONSE);

        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            running.finish().await
        else {
            panic!("interleaved block kinds must still reach an ordinary response outcome");
        };
        assert_eq!(response.as_str(), RESPONSE);
        // Only the final message's text is the response; nothing from the tool-calling
        // message contributes to it.
        assert_eq!(observations.concatenated_text(assistant_text), RESPONSE);
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &[RESPONSE]);
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code block-kind correlation watchdog expired");
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
