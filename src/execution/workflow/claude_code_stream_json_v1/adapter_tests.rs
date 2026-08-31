use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::future::{Future, pending, ready};
use std::io::{BufRead as _, Write as _};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use rustix::process::{Pid, getpgid};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};

use super::adapter::{
    ClaudeCodeStreamJsonV1Adapter, NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC,
    native_project_slug, prepare_launch,
};
use super::test_support::{
    FixtureSignal, PendingClock, RecordingObservationSink, admitted_adapter, invocation_identity,
};
use super::*;
use crate::execution::workflow::admission::{
    CancellationReason, CancellationSource, EnvironmentSnapshot,
};
use crate::execution::workflow::agent::{
    AgentAdapter, AgentCompatibilityProfile, AgentInvocation, AgentInvocationLimits,
    AgentInvocationStaging, AgentProcessContext, AgentPrompt, AgentStartReceiver,
    AgentTerminalReceiver, AgentValueMode, PositiveDuration, RetainedJsonSchema,
    StagedAgentAttachment, agent_start_channel, agent_terminal_channel, invoke_agent_adapter,
};
use crate::execution::workflow::agent_diagnostics::{
    AgentDiagnosticSession, AgentDiagnosticSessionStore,
};
use crate::execution::workflow::claude_code::ClaudeCodeConfig;
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::observation::NoopExecutionObserver;
use crate::execution::workflow::private_staging::open_directory_path;
use crate::execution::workflow::process_group::{ProcessGuardRegistry, process_group_is_quiescent};
use crate::execution::workflow::result_validation::{
    ResultValidationWorker, RunningResultValidation, ValidationWorkerDecision,
    ValidationWorkerRequest,
};
use crate::execution::workflow::test_support::{
    process_fixture_interrupt_receiver, spawn_process_fixture, write_process_fixture_id,
    write_process_fixture_signal,
};

const MODEL: &str = "scherzo-loopback";
const RESPONSE: &str = "driver response";
const FAKE_CLAUDE: &str = r#"#!/bin/sh
set -eu
for argument in "$@"; do
  printf '%s\0' "$argument"
done > "$CLAUDE_FIXTURE_ARGUMENTS"
cat > "$CLAUDE_FIXTURE_INPUT"
prompt=
model=
session=
previous=
for argument in "$@"; do
  case "$previous" in
    --append-system-prompt-file) prompt=$argument ;;
    --model) model=$argument ;;
    --session-id) session=$argument ;;
  esac
  previous=$argument
done
native_project=$CLAUDE_FIXTURE_NATIVE_PROJECT
printf '{malformed retained transcript' > "$native_project/$session.jsonl"
mkdir -p "$native_project/$session/subagents"
printf 'retained subagent activity\n' > "$native_project/$session/subagents/agent-fixture.jsonl"
cat "$prompt" > "$CLAUDE_FIXTURE_PROMPT"
printf '%s\n' "$PWD" > "$CLAUDE_FIXTURE_CWD"
{
  printf 'runner=%s\n' "$ONLY_RUNNER_VALUE"
  printf 'config=%s\n' "$CLAUDE_CONFIG_DIR"
  printf 'project_dir_name=%s\n' "${CLAUDE_CODE_PROJECT_DIR_NAME-unset}"
  printf 'updates=%s\n' "$DISABLE_UPDATES"
  printf 'traffic=%s\n' "$CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"
  printf 'marketplace=%s\n' "$CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL"
  printf 'memory=%s\n' "$CLAUDE_CODE_DISABLE_AUTO_MEMORY"
  printf 'git=%s\n' "$CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS"
} > "$CLAUDE_FIXTURE_ENVIRONMENT"
printf 'fixture diagnostic\n' >&2
output=$(
printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.241"}\n' "$PWD" "$session" "$model"
printf '{"type":"system","subtype":"status","status":"requesting","session_id":"%s"}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_start","message":{"id":"msg-driver","type":"message","role":"assistant","content":[],"model":"%s","usage":{"input_tokens":1,"output_tokens":0}}},"session_id":"%s","parent_tool_use_id":null}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"driver response"}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"assistant","message":{"id":"msg-driver","type":"message","role":"assistant","content":[{"type":"text","text":"driver response"}],"model":"%s"},"parent_tool_use_id":null,"session_id":"%s"}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_stop","index":0},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_stop"},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"result","subtype":"success","is_error":false,"terminal_reason":"completed","result":"convenience duplicate","session_id":"%s"}\n' "$session"
)
printf '%s\n' "$output"
"#;

const CANCELLATION_FAKE_CLAUDE: &str = r#"#!/bin/sh
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact execution::workflow::claude_code_stream_json_v1::adapter_tests::cancellation_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null 2>&1
"#;

const STUBBORN_DESCENDANT_FAKE_CLAUDE: &str = r#"#!/bin/sh
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact execution::workflow::claude_code_stream_json_v1::adapter_tests::stubborn_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null 2>&1
"#;

type TestInvocation = AgentInvocation<
    ClaudeCodeConfig,
    ClaudeCodeStreamJsonV1ProtocolLimits,
    RecordingObservationSink,
>;

struct ProcessFixture {
    _temporary: tempfile::TempDir,
    attachment_directory: PathBuf,
    invocation: Option<TestInvocation>,
    observations: RecordingObservationSink,
    diagnostics: StepDiagnosticLog,
    arguments: PathBuf,
    input: PathBuf,
    prompt: PathBuf,
    cwd: PathBuf,
    environment: PathBuf,
    config: PathBuf,
    native_session: PathBuf,
    session_id: Arc<str>,
    ready: FixtureSignal,
    descendant_ready: FixtureSignal,
    interrupted: FixtureSignal,
    expected_cwd: PathBuf,
    diagnostic_directory: PathBuf,
}

struct AttachmentFixture<'a> {
    media_type: &'a str,
    bytes: &'a [u8],
    diagnostic_source_name: Option<&'a str>,
}

impl ProcessFixture {
    fn new(value_mode: AgentValueMode, maximum_response_bytes: u64) -> Self {
        Self::with_attachments(value_mode, maximum_response_bytes, &[])
    }

    fn with_attachments(
        value_mode: AgentValueMode,
        maximum_response_bytes: u64,
        attachment_fixtures: &[AttachmentFixture<'_>],
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let cwd = execution_root.join("worktree");
        let staging = temporary.path().join("staging");
        let attachment_directory = staging.join("attachments");
        let result_endpoint = staging.join("result-endpoint");
        let controls = temporary.path().join("controls");
        let config = temporary.path().join("runner-claude-config");
        let attempt = temporary.path().join("attempt");
        for directory in [
            &cwd,
            &staging,
            &attachment_directory,
            &result_endpoint,
            &controls,
            &config,
            &attempt,
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let executable = temporary.path().join("claude");
        std::fs::write(&executable, FAKE_CLAUDE).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

        let arguments = controls.join("arguments");
        let input = controls.join("input");
        let prompt = controls.join("prompt");
        let captured_cwd = controls.join("cwd");
        let environment = controls.join("environment");
        let ready = FixtureSignal::create(controls.join("ready"));
        let descendant_ready = FixtureSignal::create(controls.join("descendant-ready"));
        let interrupted = FixtureSignal::create(controls.join("interrupted"));
        let mut runner_environment = BTreeMap::from([
            (
                OsString::from("PATH"),
                std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin")),
            ),
            (
                OsString::from("ONLY_RUNNER_VALUE"),
                OsString::from("runner exact"),
            ),
            (
                OsString::from("CLAUDE_CONFIG_DIR"),
                config.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_CODE_PROJECT_DIR_NAME"),
                OsString::from("ambient-project-name"),
            ),
            (OsString::from("DISABLE_UPDATES"), OsString::from("0")),
            (
                OsString::from("CLAUDE_FIXTURE_ARGUMENTS"),
                arguments.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_INPUT"),
                input.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_PROMPT"),
                prompt.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_CWD"),
                captured_cwd.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_ENVIRONMENT"),
                environment.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_PROCESS_HELPER"),
                std::env::current_exe().unwrap().into_os_string(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_READY"),
                ready.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_DESCENDANT_READY"),
                descendant_ready.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_INTERRUPTED"),
                interrupted.path().as_os_str().to_owned(),
            ),
        ]);
        let attachments = attachment_fixtures
            .iter()
            .enumerate()
            .map(|(index, fixture)| {
                let path = attachment_directory.join(format!("{index:06}"));
                std::fs::write(&path, fixture.bytes).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
                StagedAgentAttachment::new(
                    path,
                    Arc::from(fixture.media_type),
                    fixture.diagnostic_source_name.map(Arc::from),
                )
            })
            .collect::<Vec<_>>();
        std::fs::set_permissions(
            &attachment_directory,
            std::fs::Permissions::from_mode(0o500),
        )
        .unwrap();

        let admitted_root = AdmittedExecutionRoot::admit(&execution_root).unwrap();
        let working_directory = admitted_root
            .select_working_directory(Some("worktree"))
            .unwrap();
        let expected_cwd = working_directory.protocol_path().unwrap();
        // Give the fake upstream writer the exact ambient directory so this fixture does
        // not duplicate Claude's bounded slug algorithm; that algorithm has its own test.
        runner_environment.insert(
            OsString::from("CLAUDE_FIXTURE_NATIVE_PROJECT"),
            config
                .join("projects")
                .join(native_project_slug(expected_cwd.to_str().unwrap()))
                .into_os_string(),
        );
        let observations = RecordingObservationSink::default();
        let diagnostics = StepDiagnosticLog::default();
        let identity = invocation_identity("run-claude-driver", "agent-step");
        let attempt_directory = open_directory_path(&attempt).unwrap();
        let diagnostic_session = AgentDiagnosticSessionStore::create(
            &attempt_directory,
            &attempt,
            Arc::from("00000000-0000-4000-8000-000000000001"),
            1,
        )
        .unwrap()
        .allocate(
            &identity,
            AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
            QUALIFICATION_VERSION,
        )
        .unwrap();
        let native_session = diagnostic_session
            .claude_code_native_session_directory()
            .unwrap()
            .to_owned();
        let session_id =
            Arc::<str>::from(diagnostic_session.claude_code_native_session_id().unwrap());
        runner_environment.insert(
            OsString::from("CLAUDE_FIXTURE_SESSION"),
            OsString::from(session_id.as_ref()),
        );
        let diagnostic_directory = diagnostic_session.directory().to_owned();
        // The normal-driver and result-driver fixtures intentionally construct distinct
        // production invocations so their stdin closure and correction scripts cannot mix.
        // jscpd:ignore-start
        let invocation = AgentInvocation::new(
            identity,
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(
                working_directory,
                EnvironmentSnapshot::new(runner_environment),
            ),
            AgentInvocationStaging::new(result_endpoint),
            diagnostic_session,
            AgentPrompt::new(
                Arc::from("exact system prompt\n"),
                Arc::from("exact message @literal\nsecond line"),
            ),
            Arc::from(attachments),
            value_mode,
            invocation_limits(maximum_response_bytes),
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        // jscpd:ignore-end
        Self {
            _temporary: temporary,
            attachment_directory,
            invocation: Some(invocation),
            observations,
            diagnostics,
            arguments,
            input,
            prompt,
            cwd: captured_cwd,
            environment,
            config,
            native_session,
            session_id,
            ready,
            descendant_ready,
            interrupted,
            expected_cwd,
            diagnostic_directory,
        }
    }
}

impl Drop for ProcessFixture {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(
            &self.attachment_directory,
            std::fs::Permissions::from_mode(0o700),
        );
    }
}

// These small fixture limits intentionally differ from exact-binary conformance limits;
// keeping the complete admitted envelope visible makes boundary tests auditable.
// jscpd:ignore-start
fn invocation_limits(
    maximum_response_bytes: u64,
) -> AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits> {
    AgentInvocationLimits::new(
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroUsize::new(16).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        NonZeroU64::new(maximum_response_bytes).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(512).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
    )
}
// jscpd:ignore-end

async fn run_fixture(fixture: ProcessFixture) -> (ProcessFixture, AgentOutcome) {
    let (fixture, outcome, started) = run_fixture_allowing_start_failure(fixture).await;
    assert!(started, "terminal outcome: {outcome:?}");
    (fixture, outcome)
}

async fn run_fixture_allowing_start_failure(
    mut fixture: ProcessFixture,
) -> (ProcessFixture, AgentOutcome, bool) {
    let invocation = fixture.invocation.take().unwrap();
    let (task, start, outcome) = start_process_fixture(invocation, fixture.diagnostics.clone());
    task.await.unwrap();
    (
        fixture,
        outcome.receive().await.unwrap(),
        start.receive().await.is_ok(),
    )
}

fn start_process_fixture(
    invocation: TestInvocation,
    diagnostics: StepDiagnosticLog,
) -> (
    tokio::task::JoinHandle<()>,
    AgentStartReceiver,
    AgentTerminalReceiver,
) {
    let value_mode = invocation.value_mode().clone();
    let adapter = ClaudeCodeStreamJsonV1Adapter::with_validation_worker(
        diagnostics,
        NonZeroU64::new(1024).unwrap(),
        PendingClock,
        NoopExecutionObserver,
        InlineValidationWorker,
    );
    let (started, start) = agent_start_channel();
    let (terminal, outcome) = agent_terminal_channel(&value_mode);
    let task = tokio::spawn(async move {
        invoke_agent_adapter(&adapter, invocation, started, terminal).await;
    });
    (task, start, outcome)
}

fn assert_user_cancelled(outcome: AgentOutcome) {
    assert_eq!(
        outcome,
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest,
        }
    );
}

fn assert_agent_failure(outcome: &AgentOutcome, cause: AgentFailureCause) {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected agent failure, got {outcome:?}");
    };
    assert_eq!(failure.cause(), &cause);
}

fn production_transcript_script(
    events: &[Value],
    malformed_tail: bool,
    exit_status: u8,
    session: &str,
) -> String {
    let mut output = String::new();
    for event in events {
        let event = serde_json::to_string(event).unwrap();
        assert!(!event.contains('\''));
        output.push_str(&format!("printf '%s\\n' '{event}'\n"));
    }
    if malformed_tail {
        output.push_str("printf 'not-json\\n'\n");
    }
    assert!(!session.contains('\''));
    format!(
        r#"#!/bin/sh
set -eu
model=
previous=
for argument in "$@"; do
  if [ "$previous" = --model ]; then model=$argument; fi
  previous=$argument
done
IFS= read -r _initial
session='{session}'
printf '{{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.241"}}\n' "$PWD" "$session" "$model"
{output}exit {exit_status}
"#
    )
}

async fn invoke_fixture_adapter<Adapter>(
    invocation: TestInvocation,
    adapter: Adapter,
) -> AgentOutcome
where
    Adapter: AgentAdapter<
            RecordingObservationSink,
            NativeConfiguration = ClaudeCodeConfig,
            ProtocolLimits = ClaudeCodeStreamJsonV1ProtocolLimits,
        >,
{
    let value_mode = invocation.value_mode().clone();
    let (started, start) = agent_start_channel();
    let (terminal, outcome) = agent_terminal_channel(&value_mode);
    invoke_agent_adapter(&adapter, invocation, started, terminal).await;
    let outcome = outcome.receive().await.unwrap();
    assert_eq!(
        start.receive().await,
        Ok(()),
        "terminal outcome: {outcome:?}"
    );
    outcome
}

const RESULT_FAKE_CLAUDE: &str = r#"#!/bin/sh
set -eu
printf '%s\n' "$$" > "$CLAUDE_RESULT_PROCESS"
printf 'ready\n' > "$CLAUDE_RESULT_PROCESS_READY"
IFS= read -r initial
printf '%s\n' "$initial" > "$CLAUDE_RESULT_INPUT"
cat "$CLAUDE_RESULT_FIRST"
if [ "${CLAUDE_RESULT_READ_FEEDBACK:-0}" = 1 ]; then
  IFS= read -r feedback
  printf '%s\n' "$feedback" >> "$CLAUDE_RESULT_INPUT"
fi
if [ -s "$CLAUDE_RESULT_SECOND" ]; then
  cat "$CLAUDE_RESULT_SECOND"
fi
if [ "${CLAUDE_RESULT_MODE:-settle}" = exit-after-feedback ]; then
  exit 0
fi
cat >/dev/null
case "${CLAUDE_RESULT_MODE:-settle}" in
  settle) ;;
  delayed-exit)
    printf 'ready\n' > "$CLAUDE_RESULT_SETTLEMENT_READY"
    IFS= read -r released < "$CLAUDE_RESULT_SETTLEMENT_RELEASE"
    ;;
  post-work) cat "$CLAUDE_RESULT_POST_WORK" ;;
  hang)
    trap '' INT TERM
    IFS= read -r unexpected < "$CLAUDE_RESULT_SETTLEMENT_RELEASE"
    exit 75
    ;;
  *) exit 74 ;;
esac
"#;

#[derive(Clone)]
struct ResultExchangeFixture {
    candidate: Option<Value>,
    structured_tool_count: usize,
    sibling_text: bool,
}

impl ResultExchangeFixture {
    fn standalone(candidate: Value) -> Self {
        Self {
            candidate: Some(candidate),
            structured_tool_count: 1,
            sibling_text: false,
        }
    }
}

#[derive(Clone, Copy)]
enum ResultFixtureMode {
    Settle,
    DelayedExit,
    ExitAfterFeedback,
    PostWork,
    Hang,
}

impl ResultFixtureMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Settle => "settle",
            Self::DelayedExit => "delayed-exit",
            Self::ExitAfterFeedback => "exit-after-feedback",
            Self::PostWork => "post-work",
            Self::Hang => "hang",
        }
    }
}

struct ResultProcessFixture {
    _temporary: tempfile::TempDir,
    invocation: Option<TestInvocation>,
    observations: RecordingObservationSink,
    captured_input: PathBuf,
    process: PathBuf,
    process_ready: FixtureSignal,
    settlement_ready: FixtureSignal,
    settlement_release: PathBuf,
}

impl ResultProcessFixture {
    fn new(
        schema: RetainedJsonSchema,
        first: ResultExchangeFixture,
        second: Option<ResultExchangeFixture>,
        mode: ResultFixtureMode,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let cwd = execution_root.join("worktree");
        let staging = temporary.path().join("staging");
        let result_endpoint = staging.join("result-endpoint");
        let controls = temporary.path().join("controls");
        let home = temporary.path().join("home");
        let config = temporary.path().join("claude-config");
        for directory in [&cwd, &staging, &result_endpoint, &controls, &home, &config] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let executable = temporary.path().join("claude");
        std::fs::write(&executable, RESULT_FAKE_CLAUDE).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

        let admitted_root = AdmittedExecutionRoot::admit(&execution_root).unwrap();
        let working_directory = admitted_root
            .select_working_directory(Some("worktree"))
            .unwrap();
        let expected_cwd = working_directory.protocol_path().unwrap();
        let first_path = controls.join("first.jsonl");
        let second_path = controls.join("second.jsonl");
        let post_work_path = controls.join("post-work.jsonl");
        std::fs::write(
            &first_path,
            result_exchange_transcript(&expected_cwd, 1, &first),
        )
        .unwrap();
        std::fs::write(
            &second_path,
            second.as_ref().map_or_else(Vec::new, |exchange| {
                result_exchange_transcript(&expected_cwd, 2, exchange)
            }),
        )
        .unwrap();
        std::fs::write(
            &post_work_path,
            framed_result_values(&[json!({
                "type": "stream_event",
                "event": {
                    "type": "message_start",
                    "message": {
                        "id": "msg-post-acceptance",
                        "type": "message",
                        "role": "assistant",
                        "content": [],
                        "model": MODEL,
                        "usage": {"input_tokens": 1, "output_tokens": 0},
                    },
                },
                "session_id": "00000000-0000-4000-8000-000000000001",
                "parent_tool_use_id": null,
            })]),
        )
        .unwrap();
        let captured_input = controls.join("input.jsonl");
        let process = controls.join("process.pid");
        let process_ready = FixtureSignal::create(controls.join("process.ready"));
        let settlement_ready = FixtureSignal::create(controls.join("settlement.ready"));
        let settlement_release = controls.join("settlement.release");
        mkfifo(&settlement_release, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let runner_environment = EnvironmentSnapshot::new([
            (
                OsString::from("PATH"),
                std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin")),
            ),
            (OsString::from("HOME"), home.into_os_string()),
            (OsString::from("CLAUDE_CONFIG_DIR"), config.into_os_string()),
            (
                OsString::from("CLAUDE_RESULT_INPUT"),
                captured_input.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_PROCESS"),
                process.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_PROCESS_READY"),
                process_ready.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_SETTLEMENT_READY"),
                settlement_ready.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_SETTLEMENT_RELEASE"),
                settlement_release.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_FIRST"),
                first_path.into_os_string(),
            ),
            (
                OsString::from("CLAUDE_RESULT_SECOND"),
                second_path.into_os_string(),
            ),
            (
                OsString::from("CLAUDE_RESULT_POST_WORK"),
                post_work_path.into_os_string(),
            ),
            (
                OsString::from("CLAUDE_RESULT_READ_FEEDBACK"),
                OsString::from(
                    if second.is_some() || matches!(mode, ResultFixtureMode::ExitAfterFeedback) {
                        "1"
                    } else {
                        "0"
                    },
                ),
            ),
            (
                OsString::from("CLAUDE_RESULT_MODE"),
                OsString::from(mode.as_str()),
            ),
        ]);
        let observations = RecordingObservationSink::default();
        let invocation = AgentInvocation::new(
            invocation_identity("run-claude-result", "result-step"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, runner_environment),
            AgentInvocationStaging::new(result_endpoint),
            AgentDiagnosticSession::claude_code_fixture(
                temporary.path().join("diagnostics/session"),
            ),
            AgentPrompt::new(Arc::from("system"), Arc::from("produce one result")),
            Arc::from([]),
            AgentValueMode::Result {
                output: Arc::from("result"),
                schema,
            },
            invocation_limits(1024),
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        Self {
            _temporary: temporary,
            invocation: Some(invocation),
            observations,
            captured_input,
            process,
            process_ready,
            settlement_ready,
            settlement_release,
        }
    }
}

fn result_exchange_transcript(
    expected_cwd: &std::path::Path,
    exchange: u64,
    fixture: &ResultExchangeFixture,
) -> Vec<u8> {
    const SESSION: &str = "00000000-0000-4000-8000-000000000001";
    let stream_event = |event| {
        json!({
            "type": "stream_event",
            "event": event,
            "session_id": SESSION,
            "parent_tool_use_id": null,
        })
    };
    let mut values = vec![json!({
        "type": "system",
        "subtype": "init",
        "cwd": expected_cwd,
        "session_id": SESSION,
        "model": MODEL,
        "permissionMode": "bypassPermissions",
        "claude_code_version": "2.1.241",
    })];
    if let Some(candidate) = &fixture.candidate {
        values.push(stream_event(json!({
            "type": "message_start",
            "message": {
                "id": format!("msg-result-{exchange}"),
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": MODEL,
                "usage": {"input_tokens": 1, "output_tokens": 0},
            },
        })));
        for index in 0..fixture.structured_tool_count {
            let call_id = format!("tool-result-{exchange}-{index}");
            values.extend([
                stream_event(json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "tool_use",
                        "id": call_id,
                        "name": "StructuredOutput",
                        "input": candidate,
                    },
                })),
                json!({
                    "type": "assistant",
                    "message": {
                        "id": format!("msg-result-{exchange}"),
                        "type": "message",
                        "role": "assistant",
                        "content": [{
                            "type": "tool_use",
                            "id": call_id,
                            "name": "StructuredOutput",
                            "input": candidate,
                        }],
                        "model": MODEL,
                    },
                    "parent_tool_use_id": null,
                    "session_id": SESSION,
                }),
                stream_event(json!({
                    "type": "content_block_stop",
                    "index": index,
                })),
            ]);
            values.push(json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": call_id,
                        "content": "structurally correlated acknowledgement",
                    }],
                },
                "parent_tool_use_id": null,
                "session_id": SESSION,
                "tool_use_result": "structurally correlated acknowledgement",
            }));
        }
        let next_index = fixture.structured_tool_count;
        if fixture.sibling_text {
            values.extend([
                stream_event(json!({
                    "type": "content_block_start",
                    "index": next_index,
                    "content_block": {"type": "text", "text": "sibling"},
                })),
                stream_event(json!({
                    "type": "content_block_stop",
                    "index": next_index,
                })),
            ]);
        }
        values.extend([
            stream_event(json!({
                "type": "message_delta",
                "delta": {"stop_reason": "tool_use"},
                "usage": {"output_tokens": 1},
            })),
            stream_event(json!({"type": "message_stop"})),
        ]);
    }
    let mut result = json!({
        "type": "result",
        "subtype": "success",
        "is_error": false,
        "terminal_reason": "completed",
        "result": "structured result convenience text",
        "session_id": SESSION,
    });
    if let Some(candidate) = &fixture.candidate {
        result["structured_output"] = candidate.clone();
    }
    values.push(result);
    framed_result_values(&values)
}

fn framed_result_values(values: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

fn retained_schema(schema: Value) -> RetainedJsonSchema {
    let bytes = Arc::<[u8]>::from(serde_json::to_vec(&schema).unwrap());
    RetainedJsonSchema::compile(bytes, Arc::new(schema)).unwrap()
}

fn type_schema(root_type: &str) -> RetainedJsonSchema {
    retained_schema(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": root_type,
    }))
}

#[derive(Clone, Copy)]
pub(super) struct InlineValidationWorker;

pub(super) struct InlineValidation(Option<Result<ValidationWorkerDecision, ()>>);

// Keep this inline worker local to the Claude transcript fixture; sharing Pi's test
// worker would couple otherwise independent native-adapter conformance modules.
// jscpd:ignore-start
impl ResultValidationWorker for InlineValidationWorker {
    type Running = InlineValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        Ok(InlineValidation(Some(request.evaluate())))
    }
}

impl RunningResultValidation for InlineValidation {
    fn wait(&mut self) -> impl Future<Output = Result<ValidationWorkerDecision, ()>> + Send {
        ready(self.0.take().unwrap())
    }

    fn request_stop(&mut self) {}

    fn quiesce(self) -> impl Future<Output = ()> + Send {
        ready(())
    }
}
// jscpd:ignore-end

#[derive(Clone)]
struct ControlledClock {
    registrations: mpsc::UnboundedSender<Duration>,
    release: watch::Receiver<bool>,
}

// This deterministic clock records Claude validation and settlement phases; Pi's clock
// has additional process-guard scheduling modes and should remain profile-local.
// jscpd:ignore-start
impl CoordinatorClock for ControlledClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        if deadline == super::adapter::PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL {
            let probe = tokio::spawn(async {});
            let _ = probe.await;
            return;
        }
        let _ = self.registrations.send(deadline);
        let mut release = self.release.clone();
        while !*release.borrow_and_update() {
            if release.changed().await.is_err() {
                return;
            }
        }
    }
}
// jscpd:ignore-end

fn controlled_clock() -> (
    ControlledClock,
    mpsc::UnboundedReceiver<Duration>,
    watch::Sender<bool>,
) {
    let (registrations, registered) = mpsc::unbounded_channel();
    let (release, released) = watch::channel(false);
    (
        ControlledClock {
            registrations,
            release: released,
        },
        registered,
        release,
    )
}

#[derive(Clone)]
struct PendingValidationWorker {
    stopped: Arc<AtomicBool>,
    quiesced: Arc<AtomicBool>,
}

struct PendingValidation {
    stopped: Arc<AtomicBool>,
    quiesced: Arc<AtomicBool>,
}

impl ResultValidationWorker for PendingValidationWorker {
    type Running = PendingValidation;

    fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        Ok(PendingValidation {
            stopped: Arc::clone(&self.stopped),
            quiesced: Arc::clone(&self.quiesced),
        })
    }
}

#[derive(Clone, Copy)]
struct FailingValidationWorker;

impl ResultValidationWorker for FailingValidationWorker {
    type Running = InlineValidation;

    fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        Err(())
    }
}

impl RunningResultValidation for PendingValidation {
    async fn wait(&mut self) -> Result<ValidationWorkerDecision, ()> {
        pending().await
    }

    fn request_stop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    async fn quiesce(self) {
        self.quiesced.store(true, Ordering::SeqCst);
    }
}

async fn run_result_fixture<Clock, Worker>(
    mut fixture: ResultProcessFixture,
    clock: Clock,
    worker: Worker,
) -> (ResultProcessFixture, AgentOutcome)
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    let invocation = fixture.invocation.take().unwrap();
    let adapter = ClaudeCodeStreamJsonV1Adapter::with_validation_worker(
        StepDiagnosticLog::default(),
        NonZeroU64::new(1024).unwrap(),
        clock,
        NoopExecutionObserver,
        worker,
    );
    let outcome = invoke_fixture_adapter(invocation, adapter).await;
    (fixture, outcome)
}

fn fixture_process(path: &std::path::Path) -> Pid {
    let process = std::fs::read_to_string(path)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    Pid::from_raw(process).unwrap()
}

fn write_fixture_init() {
    let mut initial = String::new();
    assert!(std::io::stdin().lock().read_line(&mut initial).unwrap() > 0);
    let mut protocol = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/fd/3")
        .unwrap();
    serde_json::to_writer(
        &mut protocol,
        &json!({
            "type": "system",
            "subtype": "init",
            "cwd": std::env::current_dir().unwrap(),
            "session_id": std::env::var("CLAUDE_FIXTURE_SESSION").unwrap(),
            "model": MODEL,
            "permissionMode": "bypassPermissions",
            "claude_code_version": "2.1.241",
        }),
    )
    .unwrap();
    protocol.write_all(b"\n").unwrap();
}

#[test]
#[ignore = "launched as the cooperative Claude Code cancellation process fixture"]
fn cancellation_process_fixture() {
    write_fixture_init();
    let interrupted = process_fixture_interrupt_receiver();
    write_process_fixture_id("CLAUDE_FIXTURE_ARGUMENTS");
    write_process_fixture_signal("CLAUDE_FIXTURE_READY", b"ready\n");
    interrupted.recv().unwrap();
    write_process_fixture_signal("CLAUDE_FIXTURE_INTERRUPTED", b"interrupted\n");
    std::process::exit(130);
}

#[test]
#[ignore = "launched as the stubborn Claude Code process fixture"]
fn stubborn_process_fixture() {
    write_fixture_init();
    let interrupted = process_fixture_interrupt_receiver();
    write_process_fixture_id("CLAUDE_FIXTURE_ARGUMENTS");
    let _descendant = spawn_process_fixture(
        "execution::workflow::claude_code_stream_json_v1::adapter_tests::stubborn_descendant_process_fixture",
    );
    interrupted.recv().unwrap();
    write_process_fixture_signal("CLAUDE_FIXTURE_INTERRUPTED", b"interrupted\n");
    loop {
        std::thread::park();
    }
}

#[test]
#[ignore = "launched as the interrupt-resistant Claude Code descendant fixture"]
fn stubborn_descendant_process_fixture() {
    ctrlc::set_handler(|| {}).unwrap();
    write_process_fixture_id("CLAUDE_FIXTURE_CWD");
    write_process_fixture_signal("CLAUDE_FIXTURE_DESCENDANT_READY", b"ready\n");
    loop {
        std::thread::park();
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
async fn with_watchdog<Output>(future: impl Future<Output = Output>) -> Output {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("ClaudeCodeStreamJsonV1 fixture watchdog expired")
}

#[test]
fn launch_plan_stages_exact_prompt_and_lf_delimited_input() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let plan = prepare_launch(fixture.invocation.as_ref().unwrap()).unwrap();
    assert_eq!(
        std::fs::read(plan.system_prompt_file()).unwrap(),
        b"exact system prompt\n"
    );
    assert_eq!(plan.input().last(), Some(&b'\n'));
    assert_eq!(
        serde_json::from_slice::<Value>(&plan.input()[..plan.input().len() - 1]).unwrap(),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": "exact message @literal\nsecond line",
                }],
            },
        })
    );
    assert_eq!(plan.arguments()[0], OsStr::new("-p"));
    let session_argument = plan
        .arguments()
        .windows(2)
        .find(|arguments| arguments[0] == OsStr::new("--session-id"))
        .unwrap();
    assert_eq!(session_argument[1], OsStr::new(plan.session_id()));
}

#[test]
fn an_existing_native_session_entry_is_never_selected_or_replaced() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let ambient_project = fixture
        .config
        .join("projects")
        .join(native_project_slug(fixture.expected_cwd.to_str().unwrap()));
    std::fs::create_dir_all(&ambient_project).unwrap();
    let existing = ambient_project.join(format!("{}.jsonl", fixture.session_id));
    std::fs::write(&existing, b"existing conversation sentinel").unwrap();

    assert!(matches!(
        prepare_launch(fixture.invocation.as_ref().unwrap()),
        Err(AgentFailureCause::HarnessStartFailed)
    ));
    assert_eq!(
        std::fs::read(existing).unwrap(),
        b"existing conversation sentinel"
    );
}

#[test]
fn native_project_slug_matches_the_pinned_claude_code_storage_format() {
    assert_eq!(native_project_slug("/tmp/emoji-😀"), "-tmp-emoji---");
    let long = format!("/{}", "a".repeat(201));
    assert_eq!(
        native_project_slug(&long),
        format!("-{}-85qkr6", "a".repeat(199))
    );
}

#[test]
fn attachment_transport_is_ordered_lossless_and_uses_only_sealed_identities() {
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"immutable source text").unwrap();
    let staged_source = std::fs::read(source.path()).unwrap();
    let fixtures = [
        AttachmentFixture {
            media_type: "text/plain",
            bytes: &staged_source,
            diagnostic_source_name: Some("../../caller-name.txt"),
        },
        AttachmentFixture {
            media_type: "Application/JSON; Charset=UTF-8",
            bytes: br#"{"a":1,"z":2}"#,
            diagnostic_source_name: Some("duplicate"),
        },
        AttachmentFixture {
            media_type: "IMAGE/PNG",
            bytes: &[0, 1, 2, 0xff],
            diagnostic_source_name: Some("duplicate"),
        },
        AttachmentFixture {
            media_type: "APPLICATION/PDF",
            bytes: b"%PDF-exact",
            diagnostic_source_name: Some("000999\n@escape"),
        },
        AttachmentFixture {
            media_type: "text/markdown",
            bytes: &[0xff],
            diagnostic_source_name: None,
        },
        AttachmentFixture {
            media_type: "application/octet-stream",
            bytes: &[0xde, 0xad],
            diagnostic_source_name: None,
        },
        AttachmentFixture {
            media_type: "text/plain; charset=utf-8",
            bytes: b"",
            diagnostic_source_name: None,
        },
        AttachmentFixture {
            media_type: "image/png; charset=binary",
            bytes: &[1, 2, 3],
            diagnostic_source_name: None,
        },
        AttachmentFixture {
            media_type: "application/pdf",
            bytes: b"",
            diagnostic_source_name: None,
        },
    ];
    let fixture = ProcessFixture::with_attachments(AgentValueMode::None, 1024, &fixtures);
    std::fs::write(source.path(), b"mutated after staging").unwrap();
    let invocation = fixture.invocation.as_ref().unwrap();
    let sealed_paths = invocation
        .attachments()
        .iter()
        .map(|attachment| attachment.path().to_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let plan = prepare_launch(invocation).unwrap();
    let frame = serde_json::from_slice::<Value>(&plan.input()[..plan.input().len() - 1]).unwrap();
    let content = frame["message"]["content"].as_array().unwrap();

    assert_eq!(content.len(), fixtures.len() + 1);
    assert_eq!(
        content[0],
        json!({
            "type": "text",
            "text": "exact message @literal\nsecond line",
        })
    );
    assert_eq!(
        content[1],
        json!({
            "type": "text",
            "text": "Scherzo attachment 000000 (text/plain) follows:\nimmutable source text",
        })
    );
    assert_eq!(
        content[2],
        json!({
            "type": "text",
            "text": "Scherzo attachment 000001 (Application/JSON; Charset=UTF-8) follows:\n{\"a\":1,\"z\":2}",
        })
    );
    assert_eq!(
        content[3],
        json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": "AAEC/w==",
            },
        })
    );
    assert_eq!(
        content[4],
        json!({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": "JVBERi1leGFjdA==",
            },
        })
    );
    for (content_index, attachment_index) in [(5, 4), (6, 5), (8, 7)] {
        let media_type = fixtures[attachment_index].media_type;
        assert_eq!(
            content[content_index],
            json!({
                "type": "text",
                "text": format!(
                    "Scherzo attachment {attachment_index:06} has media type {media_type} and is available to runner tools at {}.",
                    sealed_paths[attachment_index]
                ),
            })
        );
    }
    assert_eq!(
        content[7],
        json!({
            "type": "text",
            "text": "Scherzo attachment 000006 (text/plain; charset=utf-8) follows:\n",
        })
    );
    assert_eq!(
        content[9],
        json!({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": "",
            },
        })
    );
    let serialized = String::from_utf8(plan.input().to_vec()).unwrap();
    for diagnostic_name in ["../../caller-name.txt", "duplicate", "000999", "@escape"] {
        assert!(!serialized.contains(diagnostic_name));
    }
}

#[tokio::test]
async fn permissive_native_envelope_preserves_every_authoritatively_valid_json_root() {
    let cases = [
        ("object", json!({"value": 1})),
        ("array", json!([1, 2])),
        ("string", json!("value")),
        ("number", json!(7)),
        ("boolean", json!(true)),
        ("null", Value::Null),
    ];
    for (root_type, candidate) in cases {
        let fixture = ResultProcessFixture::new(
            type_schema(root_type),
            ResultExchangeFixture::standalone(json!({"result": candidate})),
            None,
            ResultFixtureMode::Settle,
        );
        let plan = prepare_launch(fixture.invocation.as_ref().unwrap()).unwrap();
        assert_eq!(
            plan.arguments()[plan.arguments().len() - 2..],
            [
                OsStr::new("--json-schema"),
                OsStr::new(RESULT_ENVELOPE_SCHEMA)
            ]
        );
        drop(plan);
        let (_, outcome) = run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("{root_type} root must complete through authoritative validation");
        };
        assert_eq!(result.value(), &candidate);
    }
}

#[tokio::test]
async fn rejected_candidate_is_corrected_in_the_same_process_and_conversation() {
    let schema = retained_schema(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "integer",
        "minimum": 1,
    }));
    let fixture = ResultProcessFixture::new(
        schema,
        ResultExchangeFixture::standalone(json!({"result": -1})),
        Some(ResultExchangeFixture::standalone(json!({"result": 7}))),
        ResultFixtureMode::Settle,
    );
    let (fixture, outcome) =
        run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
        panic!("corrected candidate must complete");
    };
    assert_eq!(result.value(), &json!(7));

    let input = std::fs::read_to_string(&fixture.captured_input).unwrap();
    let frames = input
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(frames.len(), 2);
    let feedback = frames[1]["message"]["content"][0]["text"].as_str().unwrap();
    assert!(feedback.starts_with("Result rejected by the workflow schema:"));
    assert!(feedback.len() <= 512);
    let rejections = fixture
        .observations
        .snapshot()
        .into_iter()
        .filter(|observation| {
            matches!(
                observation.observation(),
                AgentObservation::ValueRejected {
                    kind: AgentValueKind::Result,
                    ..
                }
            )
        })
        .count();
    assert_eq!(rejections, 1);
}

#[tokio::test]
async fn oversized_candidate_receives_bounded_feedback_before_a_valid_correction() {
    let fixture = ResultProcessFixture::new(
        type_schema("string"),
        ResultExchangeFixture::standalone(json!({"result": "x".repeat(1100)})),
        Some(ResultExchangeFixture::standalone(json!({"result": "ok"}))),
        ResultFixtureMode::Settle,
    );
    let (fixture, outcome) =
        run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
        panic!("bounded correction must complete");
    };
    assert_eq!(result.value(), &json!("ok"));
    let input = std::fs::read_to_string(&fixture.captured_input).unwrap();
    let feedback = serde_json::from_str::<Value>(input.lines().nth(1).unwrap()).unwrap();
    let feedback = feedback["message"]["content"][0]["text"].as_str().unwrap();
    assert!(feedback.contains("1024-byte limit"));
    assert!(feedback.len() <= 512);
}

#[tokio::test]
async fn ambiguous_duplicate_and_missing_candidates_never_become_results() {
    let ambiguous = [
        ResultExchangeFixture {
            candidate: Some(json!({"result": 1})),
            structured_tool_count: 1,
            sibling_text: true,
        },
        ResultExchangeFixture {
            candidate: Some(json!({"result": 1})),
            structured_tool_count: 2,
            sibling_text: false,
        },
    ];
    for first in ambiguous {
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            first,
            None,
            ResultFixtureMode::ExitAfterFeedback,
        );
        let (fixture, outcome) =
            run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
        assert!(matches!(
            outcome,
            AgentOutcome::Failed(failure)
                if matches!(failure.cause(), AgentFailureCause::HarnessProtocolFailed)
        ));
        let input = std::fs::read_to_string(&fixture.captured_input).unwrap();
        assert_eq!(input.lines().count(), 2);
        assert!(input.contains("standalone structured result candidate"));
    }

    let missing = ResultProcessFixture::new(
        type_schema("number"),
        ResultExchangeFixture {
            candidate: None,
            structured_tool_count: 0,
            sibling_text: false,
        },
        None,
        ResultFixtureMode::Settle,
    );
    let (_, outcome) = run_result_fixture(missing, PendingClock, InlineValidationWorker).await;
    assert_eq!(
        outcome,
        failed_agent_outcome(AgentFailureCause::MissingResult)
    );
}

#[tokio::test]
async fn validation_timeout_stops_its_worker_and_discards_the_candidate() {
    let stopped = Arc::new(AtomicBool::new(false));
    let quiesced = Arc::new(AtomicBool::new(false));
    let worker = PendingValidationWorker {
        stopped: Arc::clone(&stopped),
        quiesced: Arc::clone(&quiesced),
    };
    let (clock, mut registered, release) = controlled_clock();
    let fixture = ResultProcessFixture::new(
        type_schema("number"),
        ResultExchangeFixture::standalone(json!({"result": 1})),
        None,
        ResultFixtureMode::Settle,
    );
    let task = tokio::spawn(run_result_fixture(fixture, clock, worker));
    assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
    release.send_replace(true);
    let (_, outcome) = task.await.unwrap();
    assert_eq!(
        outcome,
        failed_agent_outcome(AgentFailureCause::ResultValidationLimitExceeded {
            deadline: PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        })
    );
    assert!(stopped.load(Ordering::SeqCst));
    assert!(quiesced.load(Ordering::SeqCst));
}

#[tokio::test]
async fn accepted_result_closes_input_and_remains_provisional_until_process_exit() {
    with_watchdog(async {
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::DelayedExit,
        );
        let ready = fixture.settlement_ready.clone();
        let release = fixture.settlement_release.clone();
        let captured_input = fixture.captured_input.clone();
        let task = tokio::spawn(run_result_fixture(
            fixture,
            PendingClock,
            InlineValidationWorker,
        ));

        assert_eq!(ready.receive().await, b"ready\n");
        assert_eq!(
            std::fs::read_to_string(captured_input)
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(
            !task.is_finished(),
            "an accepted candidate must remain provisional before native exit"
        );
        std::fs::write(release, b"release\n").unwrap();

        let (_, outcome) = task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("clean settlement must publish the accepted result");
        };
        assert_eq!(result.value(), &json!(1));
    })
    .await;
}

#[tokio::test]
async fn post_acceptance_work_and_failed_settlement_discard_the_provisional_result() {
    with_watchdog(async {
        let post_work = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::PostWork,
        );
        let (_, post_work_outcome) =
            run_result_fixture(post_work, PendingClock, InlineValidationWorker).await;
        assert_agent_failure(&post_work_outcome, AgentFailureCause::HarnessProtocolFailed);
        let AgentOutcome::Failed(post_work_failure) = post_work_outcome else {
            panic!("fixture outcome was not failed");
        };
        let rejection =
            serde_json::to_value(post_work_failure.protocol_rejection().unwrap()).unwrap();
        assert_eq!(
            rejection["detail"]["reason"],
            "terminal_drain_event_invalid"
        );

        let (clock, mut registered, release) = controlled_clock();
        let nonsettling = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::Hang,
        );
        let task = tokio::spawn(run_result_fixture(
            nonsettling,
            clock,
            InlineValidationWorker,
        ));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        release.send_replace(true);
        let (_, settlement_outcome) = task.await.unwrap();
        assert_eq!(
            settlement_outcome,
            failed_agent_outcome(AgentFailureCause::ResultSettlementFailed)
        );
    })
    .await;
}

#[tokio::test]
async fn cancellation_during_validation_stops_the_worker_and_quiesces_the_process_group() {
    with_watchdog(async {
        let stopped = Arc::new(AtomicBool::new(false));
        let quiesced = Arc::new(AtomicBool::new(false));
        let worker = PendingValidationWorker {
            stopped: Arc::clone(&stopped),
            quiesced: Arc::clone(&quiesced),
        };
        let (clock, mut registered, _release) = controlled_clock();
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::Settle,
        );
        let cancellation = fixture.invocation.as_ref().unwrap().cancellation().clone();
        let process_path = fixture.process.clone();
        let process_ready = fixture.process_ready.clone();
        let task = tokio::spawn(run_result_fixture(fixture, clock, worker));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(process_ready.receive().await, b"ready\n");
        let process = fixture_process(&process_path);

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        let (_, outcome) = task.await.unwrap();
        assert_user_cancelled(outcome);
        assert!(stopped.load(Ordering::SeqCst));
        assert!(quiesced.load(Ordering::SeqCst));
        assert!(process_group_is_quiescent(process));
    })
    .await;
}

#[tokio::test]
async fn cancellation_after_a_provisional_result_forces_containment_quiescent() {
    with_watchdog(async {
        let (clock, mut registered, _release) = controlled_clock();
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::Hang,
        );
        let invocation = fixture.invocation.as_ref().unwrap();
        let cancellation = invocation.cancellation().clone();
        let process_control = invocation.process_control().clone();
        let process_path = fixture.process.clone();
        let process_ready = fixture.process_ready.clone();
        let task = tokio::spawn(run_result_fixture(fixture, clock, InlineValidationWorker));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(process_ready.receive().await, b"ready\n");
        let process = fixture_process(&process_path);
        assert!(
            !task.is_finished(),
            "the accepted result must remain provisional during settlement"
        );

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        process_control.interrupt();
        process_control.force();

        let (_, outcome) = task.await.unwrap();
        assert_user_cancelled(outcome);
        assert!(process_group_is_quiescent(process));
    })
    .await;
}

#[tokio::test]
async fn normal_driver_preserves_runner_configuration_and_returns_only_final_response() {
    let fixture = ProcessFixture::new(
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        1024,
    );
    let (fixture, outcome) = run_fixture(fixture).await;
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("response driver must complete");
    };
    assert_eq!(response.as_str(), RESPONSE);

    let arguments = std::fs::read(&fixture.arguments).unwrap();
    let arguments = arguments
        .split(|byte| *byte == 0)
        .filter(|argument| !argument.is_empty())
        .map(OsStr::from_bytes)
        .collect::<Vec<_>>();
    assert_eq!(arguments.len(), 20);
    for rejected in [
        "--bare",
        "--continue",
        "--resume",
        "--fork-session",
        "--fallback-model",
        "--replay-user-messages",
        "--worktree",
        "--no-session-persistence",
    ] {
        assert!(!arguments.contains(&OsStr::new(rejected)));
    }
    assert_eq!(
        arguments[..19],
        [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--forward-subagent-text",
            "--session-id",
            fixture.session_id.as_ref(),
            "--permission-mode",
            "bypassPermissions",
            "--setting-sources",
            "user,project,local",
            "--model",
            MODEL,
            "--effort",
            "xhigh",
            "--append-system-prompt-file",
        ]
        .map(OsStr::new)
    );
    assert_eq!(
        std::fs::read(&fixture.prompt).unwrap(),
        b"exact system prompt\n"
    );
    assert_eq!(
        std::fs::read_to_string(&fixture.cwd).unwrap(),
        format!("{}\n", fixture.expected_cwd.display())
    );
    assert_eq!(
        std::fs::read_to_string(&fixture.environment).unwrap(),
        format!(
            "runner=runner exact\nconfig={}\nproject_dir_name=unset\nupdates=1\ntraffic=1\nmarketplace=1\nmemory=1\ngit=1\n",
            fixture
                ._temporary
                .path()
                .join("runner-claude-config")
                .display()
        )
    );
    let input = std::fs::read(&fixture.input).unwrap();
    assert_eq!(input.last(), Some(&b'\n'));
    assert_eq!(input.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert_eq!(
        std::fs::read(fixture.native_session.join("transcript.jsonl")).unwrap(),
        b"{malformed retained transcript"
    );
    assert_eq!(
        std::fs::read(
            fixture
                .native_session
                .join("resources/subagents/agent-fixture.jsonl")
        )
        .unwrap(),
        b"retained subagent activity\n"
    );
    let ambient_project = fixture
        .config
        .join("projects")
        .join(native_project_slug(fixture.expected_cwd.to_str().unwrap()));
    assert_eq!(std::fs::read_dir(ambient_project).unwrap().count(), 0);

    let observations = fixture.observations.snapshot();
    assert_eq!(
        observations
            .iter()
            .filter(|observation| matches!(
                observation.observation(),
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::HarnessStarted
                }
            ))
            .count(),
        1
    );
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
    let diagnostic = fixture.diagnostics.get("agent-step").unwrap();
    assert_eq!(diagnostic.standard_error().bytes(), b"fixture diagnostic\n");
    assert!(diagnostic.standard_error().fully_drained());
}

#[tokio::test]
async fn a_scripted_harness_that_writes_nowhere_emits_the_named_capture_diagnostic() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let session = fixture.session_id.to_string();
    let script = production_transcript_script(
        &[json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "terminal_reason": "completed",
            "result": "terminal convenience text",
            "session_id": session,
        })],
        false,
        0,
        &session,
    );
    std::fs::write(
        fixture.invocation.as_ref().unwrap().adapter().executable(),
        script,
    )
    .unwrap();

    let (fixture, outcome) = run_fixture(fixture).await;
    assert_eq!(
        outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
    assert!(!fixture.native_session.join("transcript.jsonl").exists());
    let observations = fixture.observations.snapshot();
    let quiescent = observations
        .iter()
        .position(|observation| {
            matches!(
                observation.observation(),
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::HarnessQuiescent,
                }
            )
        })
        .unwrap();
    let diagnostic = observations
        .iter()
        .position(|observation| {
            matches!(
                observation.observation(),
                AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Warning,
                    message,
                } if message
                    .strip_prefix(NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC)
                    .is_some_and(|detail| detail.starts_with(":"))
            )
        })
        .unwrap();
    assert!(diagnostic > quiescent);
}

#[tokio::test]
async fn production_failure_matrix_uses_only_existing_typed_outcomes() {
    enum Transcript {
        Malformed,
        Success,
        NativeFailure,
    }

    let cases = [
        (
            AgentValueMode::None,
            Transcript::Malformed,
            0,
            AgentFailureCause::HarnessProtocolFailed,
        ),
        (
            AgentValueMode::Response {
                output: Arc::from("response"),
            },
            Transcript::Success,
            0,
            AgentFailureCause::MissingResponse,
        ),
        (
            AgentValueMode::None,
            Transcript::NativeFailure,
            17,
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelError,
            },
        ),
        (
            AgentValueMode::None,
            Transcript::Success,
            17,
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            },
        ),
    ];
    for (value_mode, transcript, exit_status, cause) in cases {
        let fixture = ProcessFixture::new(value_mode, 1024);
        let session = fixture
            .invocation
            .as_ref()
            .unwrap()
            .diagnostic_session()
            .claude_code_native_session_id()
            .unwrap()
            .to_owned();
        let (events, malformed_tail) = match transcript {
            Transcript::Malformed => (Vec::new(), true),
            Transcript::Success => (
                vec![json!({
                    "type": "result",
                    "subtype": "success",
                    "is_error": false,
                    "terminal_reason": "completed",
                    "result": "terminal convenience text",
                    "session_id": session,
                })],
                false,
            ),
            Transcript::NativeFailure => (
                vec![json!({
                    "type": "result",
                    "subtype": "error_during_execution",
                    "is_error": true,
                    "terminal_reason": "error_during_execution",
                    "session_id": session,
                })],
                false,
            ),
        };
        let script =
            production_transcript_script(&events, malformed_tail, exit_status, session.as_str());
        std::fs::write(
            fixture.invocation.as_ref().unwrap().adapter().executable(),
            script,
        )
        .unwrap();
        let (_, outcome, started) = run_fixture_allowing_start_failure(fixture).await;
        assert!(started);
        assert_agent_failure(&outcome, cause);
    }

    let startup = ProcessFixture::new(AgentValueMode::None, 1024);
    std::fs::remove_file(startup.invocation.as_ref().unwrap().adapter().executable()).unwrap();
    let (_, startup_outcome, started) = run_fixture_allowing_start_failure(startup).await;
    assert!(!started);
    assert_eq!(
        startup_outcome,
        failed_agent_outcome(AgentFailureCause::HarnessStartFailed)
    );

    let validation = ResultProcessFixture::new(
        type_schema("number"),
        ResultExchangeFixture::standalone(json!({"result": 1})),
        None,
        ResultFixtureMode::Settle,
    );
    let (_, validation_outcome) =
        run_result_fixture(validation, PendingClock, FailingValidationWorker).await;
    assert_eq!(
        validation_outcome,
        failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[tokio::test]
async fn parser_rejection_is_surfaced_and_retained_beside_claude_invocation_metadata() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let session = fixture
        .invocation
        .as_ref()
        .unwrap()
        .diagnostic_session()
        .claude_code_native_session_id()
        .unwrap()
        .to_owned();
    let rejected = json!({
        "type": "stream_event",
        "event": {
            "type": "content_block_delta",
            "index": 7,
            "delta": {
                "type": "text_delta",
                "text": "SENTINEL_ASSISTANT_TEXT",
                "tool_input": "SENTINEL_TOOL_INPUT",
            },
        },
        "session_id": session,
        "parent_tool_use_id": "SENTINEL_PARENT_ID",
        "provider_payload": "SENTINEL_PROVIDER_PAYLOAD",
        "request_id": "SENTINEL_REQUEST_ID",
        "structured_output": "SENTINEL_STRUCTURED_OUTPUT",
        "tool_result": "SENTINEL_TOOL_RESULT",
    });
    std::fs::write(
        fixture.invocation.as_ref().unwrap().adapter().executable(),
        production_transcript_script(&[rejected], false, 0, session.as_str()),
    )
    .unwrap();

    let (fixture, outcome, started) = run_fixture_allowing_start_failure(fixture).await;
    assert!(started);
    assert_agent_failure(&outcome, AgentFailureCause::HarnessProtocolFailed);
    let AgentOutcome::Failed(failure) = &outcome else {
        panic!("fixture outcome was not failed");
    };
    let surfaced = serde_json::to_value(failure.protocol_rejection().unwrap()).unwrap();
    assert_eq!(surfaced["profile"], "ClaudeCodeStreamJsonV1");
    assert_eq!(
        surfaced["detail"]["reason"],
        "active_stream_parent_mismatch"
    );
    assert_eq!(surfaced["detail"]["outerEvent"], "stream_event");
    assert_eq!(surfaced["detail"]["streamEvent"], "content_block_delta");
    assert_eq!(surfaced["detail"]["contentIndex"], 7);
    assert_eq!(surfaced["detail"]["contentBlock"], "text");

    let retained_bytes =
        std::fs::read(fixture.diagnostic_directory.join("protocol-rejection.json")).unwrap();
    let retained: Value = serde_json::from_slice(&retained_bytes).unwrap();
    assert_eq!(retained, surfaced);
    assert!(retained_bytes.len() < 16 * 1024);
    let metadata: Value = serde_json::from_slice(
        &std::fs::read(fixture.diagnostic_directory.join("metadata.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        metadata["localRunId"],
        "00000000-0000-4000-8000-000000000001"
    );
    assert_eq!(metadata["attemptNumber"], 1);
    assert_eq!(metadata["stepId"], "agent-step");
    assert!(metadata["invocationId"].is_u64());
    assert_eq!(metadata["profile"], "ClaudeCodeStreamJsonV1");
    assert_eq!(metadata["claudeCodeVersion"], QUALIFICATION_VERSION);
    assert_eq!(
        metadata["nativeSession"],
        json!({
            "relativeDirectory": "session",
            "formatVersion": 1,
        })
    );
    assert!(metadata.get("nativeSessionPersistence").is_none());

    let serialized = surfaced.to_string();
    for sentinel in [
        "SENTINEL_ASSISTANT_TEXT",
        "SENTINEL_TOOL_INPUT",
        "SENTINEL_PARENT_ID",
        "SENTINEL_PROVIDER_PAYLOAD",
        "SENTINEL_REQUEST_ID",
        "SENTINEL_STRUCTURED_OUTPUT",
        "SENTINEL_TOOL_RESULT",
        session.as_str(),
    ] {
        assert!(!serialized.contains(sentinel));
    }
}

#[tokio::test]
async fn cancellation_before_launch_starts_no_process_or_diagnostic_capture() {
    let fixture = ProcessFixture::new(
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        1024,
    );
    let cancellation = fixture.invocation.as_ref().unwrap().cancellation().clone();
    assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
    let (fixture, outcome, started) = run_fixture_allowing_start_failure(fixture).await;

    assert_user_cancelled(outcome);
    assert!(!started);
    assert!(!fixture.arguments.exists());
    assert!(fixture.diagnostics.get("agent-step").is_none());
}

#[tokio::test]
async fn cancellation_during_native_work_drains_and_quiesces_before_reporting() {
    with_watchdog(async {
        let mut fixture = ProcessFixture::new(
            AgentValueMode::Response {
                output: Arc::from("response"),
            },
            1024,
        );
        std::fs::write(
            fixture.invocation.as_ref().unwrap().adapter().executable(),
            CANCELLATION_FAKE_CLAUDE,
        )
        .unwrap();
        let invocation = fixture.invocation.take().unwrap();
        let cancellation = invocation.cancellation().clone();
        let process_path = fixture.arguments.clone();
        let ready = fixture.ready.clone();
        let interrupted = fixture.interrupted.clone();
        let (task, start, outcome) = start_process_fixture(invocation, fixture.diagnostics.clone());

        start.receive().await.unwrap();
        assert_eq!(ready.receive().await, b"ready\n");
        let process = fixture_process(&process_path);
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        assert_eq!(interrupted.receive().await, b"interrupted\n");
        assert_user_cancelled(outcome.receive().await.unwrap());
        task.await.unwrap();
        assert!(process_group_is_quiescent(process));
        assert!(
            fixture
                .diagnostics
                .get("agent-step")
                .unwrap()
                .standard_error()
                .fully_drained()
        );
    })
    .await;
}

#[tokio::test]
async fn forced_cancellation_removes_a_stubborn_in_group_descendant() {
    with_watchdog(async {
        let mut fixture = ProcessFixture::new(AgentValueMode::None, 1024);
        std::fs::write(
            fixture.invocation.as_ref().unwrap().adapter().executable(),
            STUBBORN_DESCENDANT_FAKE_CLAUDE,
        )
        .unwrap();
        let invocation = fixture.invocation.take().unwrap();
        let cancellation = invocation.cancellation().clone();
        let process_control = invocation.process_control().clone();
        let leader_path = fixture.arguments.clone();
        let descendant_path = fixture.cwd.clone();
        let descendant_ready = fixture.descendant_ready.clone();
        let interrupted = fixture.interrupted.clone();
        let (task, start, outcome) = start_process_fixture(invocation, fixture.diagnostics.clone());

        start.receive().await.unwrap();
        assert_eq!(descendant_ready.receive().await, b"ready\n");
        let leader = fixture_process(&leader_path);
        let descendant = fixture_process(&descendant_path);
        assert_eq!(getpgid(Some(leader)).unwrap(), leader);
        assert_eq!(getpgid(Some(descendant)).unwrap(), leader);

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        process_control.interrupt();
        assert_eq!(interrupted.receive().await, b"interrupted\n");
        assert!(!process_group_is_quiescent(leader));
        assert_eq!(getpgid(Some(descendant)).unwrap(), leader);
        process_control.force();
        assert_user_cancelled(outcome.receive().await.unwrap());
        task.await.unwrap();
        assert!(process_group_is_quiescent(leader));
    })
    .await;
}

#[tokio::test]
async fn no_value_and_over_limit_runs_report_one_existing_terminal_outcome() {
    let no_value = ProcessFixture::new(AgentValueMode::None, 1024);
    let (_, outcome) = run_fixture(no_value).await;
    assert_eq!(
        outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );

    let over_limit = ProcessFixture::new(
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        3,
    );
    let (_, outcome) = run_fixture(over_limit).await;
    assert_eq!(
        outcome,
        failed_agent_outcome(AgentFailureCause::CapturedValueTooLarge)
    );
}
