use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use super::adapter::{ClaudeCodeStreamJsonV1Adapter, prepare_launch};
use super::test_support::{
    PendingClock, RecordingObservationSink, admitted_adapter, invocation_identity,
};
use super::*;
use crate::execution::workflow::admission::{CancellationSource, EnvironmentSnapshot};
use crate::execution::workflow::agent::{
    AgentInvocation, AgentInvocationLimits, AgentInvocationStaging, AgentProcessContext,
    AgentPrompt, AgentValueMode, PositiveDuration, agent_start_channel, agent_terminal_channel,
    invoke_agent_adapter,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::execution::workflow::claude_code::ClaudeCodeConfig;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::observation::NoopExecutionObserver;
use crate::execution::workflow::process_group::ProcessGuardRegistry;

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
previous=
for argument in "$@"; do
  case "$previous" in
    --append-system-prompt-file) prompt=$argument ;;
    --model) model=$argument ;;
  esac
  previous=$argument
done
cat "$prompt" > "$CLAUDE_FIXTURE_PROMPT"
printf '%s\n' "$PWD" > "$CLAUDE_FIXTURE_CWD"
{
  printf 'runner=%s\n' "$ONLY_RUNNER_VALUE"
  printf 'config=%s\n' "$CLAUDE_CONFIG_DIR"
  printf 'updates=%s\n' "$DISABLE_UPDATES"
  printf 'traffic=%s\n' "$CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"
  printf 'marketplace=%s\n' "$CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL"
  printf 'memory=%s\n' "$CLAUDE_CODE_DISABLE_AUTO_MEMORY"
  printf 'git=%s\n' "$CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS"
} > "$CLAUDE_FIXTURE_ENVIRONMENT"
printf 'fixture diagnostic\n' >&2
session=00000000-0000-4000-8000-000000000001
output=$(
printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.222"}\n' "$PWD" "$session" "$model"
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

type TestInvocation = AgentInvocation<
    ClaudeCodeConfig,
    ClaudeCodeStreamJsonV1ProtocolLimits,
    RecordingObservationSink,
>;

struct ProcessFixture {
    _temporary: tempfile::TempDir,
    invocation: Option<TestInvocation>,
    observations: RecordingObservationSink,
    diagnostics: StepDiagnosticLog,
    arguments: PathBuf,
    input: PathBuf,
    prompt: PathBuf,
    cwd: PathBuf,
    environment: PathBuf,
    expected_cwd: PathBuf,
}

impl ProcessFixture {
    fn new(value_mode: AgentValueMode, maximum_response_bytes: u64) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let cwd = execution_root.join("worktree");
        let staging = temporary.path().join("staging");
        let result_endpoint = staging.join("result-endpoint");
        let controls = temporary.path().join("controls");
        let config = temporary.path().join("runner-claude-config");
        for directory in [&cwd, &staging, &result_endpoint, &controls, &config] {
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
        let runner_environment = BTreeMap::from([
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
        ]);
        let admitted_root = AdmittedExecutionRoot::admit(&execution_root).unwrap();
        let working_directory = admitted_root
            .select_working_directory(Some("worktree"))
            .unwrap();
        let expected_cwd = working_directory.protocol_path().unwrap();
        let observations = RecordingObservationSink::default();
        let diagnostics = StepDiagnosticLog::default();
        let invocation = AgentInvocation::new(
            invocation_identity("run-claude-driver", "agent-step"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(
                working_directory,
                EnvironmentSnapshot::new(runner_environment),
            ),
            AgentInvocationStaging::new(result_endpoint),
            AgentDiagnosticSession::fixture(temporary.path().join("diagnostics/session")),
            AgentPrompt::new(
                Arc::from("exact system prompt\n"),
                Arc::from("exact message @literal\nsecond line"),
            ),
            Arc::from([]),
            value_mode,
            invocation_limits(maximum_response_bytes),
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        Self {
            _temporary: temporary,
            invocation: Some(invocation),
            observations,
            diagnostics,
            arguments,
            input,
            prompt,
            cwd: captured_cwd,
            environment,
            expected_cwd,
        }
    }
}

fn invocation_limits(
    maximum_response_bytes: u64,
) -> AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits> {
    AgentInvocationLimits::new(
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        NonZeroU64::new(maximum_response_bytes).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(512).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
    )
}

async fn run_fixture(mut fixture: ProcessFixture) -> (ProcessFixture, AgentOutcome) {
    let invocation = fixture.invocation.take().unwrap();
    let value_mode = invocation.value_mode().clone();
    let adapter = ClaudeCodeStreamJsonV1Adapter::new(
        fixture.diagnostics.clone(),
        NonZeroU64::new(1024).unwrap(),
        PendingClock,
        NoopExecutionObserver,
    );
    let (started, start) = agent_start_channel();
    let (terminal, outcome) = agent_terminal_channel(&value_mode);
    invoke_agent_adapter(&adapter, invocation, started, terminal).await;
    let outcome = outcome.receive().await.unwrap();
    assert_eq!(
        start.receive().await,
        Ok(()),
        "terminal outcome: {outcome:?}"
    );
    (fixture, outcome)
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
    assert_eq!(arguments.len(), 19);
    for rejected in [
        "--bare",
        "--continue",
        "--resume",
        "--fallback-model",
        "--replay-user-messages",
        "--worktree",
        "--session-id",
    ] {
        assert!(!arguments.contains(&OsStr::new(rejected)));
    }
    assert_eq!(
        arguments[..18],
        [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--forward-subagent-text",
            "--no-session-persistence",
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
            "runner=runner exact\nconfig={}\nupdates=1\ntraffic=1\nmarketplace=1\nmemory=1\ngit=1\n",
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
        AgentOutcome::Failed {
            cause: AgentFailureCause::CapturedValueTooLarge,
        }
    );
}
