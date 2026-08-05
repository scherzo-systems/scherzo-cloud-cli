use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::future::{Future, pending, ready};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use rustix::process::{Pid, WaitId, WaitIdOptions, getpgid, waitid};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{mpsc, oneshot, watch};

use super::adapter::{PiJsonV1Adapter, build_command, prepare_launch};
use super::*;
use crate::execution::workflow::admission::{
    CancellationReason, CancellationSource, EnvironmentSnapshot,
};
use crate::execution::workflow::agent::{
    AdmittedAgentAdapter, AgentCompatibilityProfile, AgentInputKind, AgentInvocation,
    AgentInvocationIdentity, AgentInvocationLimits, AgentInvocationStaging,
    AgentObservationEnvelope, AgentObservationSink, AgentOutcome, AgentProcessContext,
    AgentProcessControl, AgentPrompt, AgentStartReceiver, AgentTerminalReceiveError,
    AgentTerminalReceiver, AgentToolCallPhase, AgentValueMode, PositiveDuration,
    RetainedResultSchema, StagedAgentAttachment, WorkflowRunId, agent_start_channel,
    agent_terminal_channel, invoke_agent_adapter,
};
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::{StepDiagnostic, StepDiagnosticLog};
use crate::execution::workflow::execution_root::AdmittedExecutionRoot;
use crate::execution::workflow::observation::NoopExecutionObserver;
use crate::execution::workflow::pi::{PiConfig, Thinking};
use crate::execution::workflow::result_validation::{
    ResultValidationWorker, RunningResultValidation, ValidationWorkerDecision,
    ValidationWorkerRequest,
};
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};

const MAXIMUM_INPUT_BYTES: u64 = 64 * 1024;
const TEST_WATCHDOG: Duration = Duration::from_secs(10);
const RECORDED_CWD: &str = "/execution/worktree";
const RESPONSE_SUCCESS: &str = include_str!("fixtures/response-success.jsonl");
const NATIVE_RECOVERY: &str = include_str!("fixtures/native-recovery.jsonl");
const TERMINAL_TOOL_USE: &str = include_str!("fixtures/terminal-tool-use.jsonl");

#[derive(Clone)]
enum TestClock {
    Pending,
    Yielding,
    Controlled {
        now_seconds: Arc<AtomicU64>,
        registrations: mpsc::UnboundedSender<Duration>,
        release: watch::Receiver<bool>,
    },
}

impl CoordinatorClock for TestClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        match self {
            Self::Pending | Self::Yielding => Duration::ZERO,
            Self::Controlled { now_seconds, .. } => {
                Duration::from_secs(now_seconds.load(Ordering::SeqCst))
            }
        }
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        let (registrations, release) = match self {
            Self::Pending => return pending().await,
            Self::Yielding => return explicit_scheduling_point().await,
            Self::Controlled {
                registrations,
                release,
                ..
            } => (registrations, release),
        };
        let _ = registrations.send(deadline);
        let mut release = release.clone();
        while !*release.borrow_and_update() {
            if release.changed().await.is_err() {
                return;
            }
        }
        explicit_scheduling_point().await;
    }
}

async fn explicit_scheduling_point() {
    let (complete, completed) = oneshot::channel();
    tokio::spawn(async move {
        let _ = complete.send(());
    });
    let _ = completed.await;
}

const FAKE_PI: &str = r#"#!/bin/sh
set -eu

for argument in "$@"; do
  printf '%s\0' "$argument"
done > "$PI_FIXTURE_ARGUMENTS"
printf '%s\n' "$PWD" > "$PI_FIXTURE_CWD"
if IFS= read -r unexpected; then
  exit 70
fi
printf 'closed\n' > "$PI_FIXTURE_STDIN"
if [ "${PI_MODEL+x}" = x ]; then
  exit 71
fi
if [ "${ONLY_RUNNER_VALUE-}" != 'runner exact' ]; then
  exit 72
fi
pid=$$
printf '%s\n' "$pid" > "$PI_FIXTURE_PROCESS"
session_suffix=$(printf '%012d' "$pid")
printf '%s\n' "00000000-0000-4000-8000-$session_suffix" > "$PI_FIXTURE_SESSION"
printf 'fixture diagnostic\n' >&2
printf 'ready\n' > "$PI_FIXTURE_READY"
IFS= read -r released < "$PI_FIXTURE_HEADER_RELEASE"
printf 'header-ready\n' > "$PI_FIXTURE_HEADER_READY"
if [ "$PI_FIXTURE_MODE" = bad-header ]; then
  printf '%s\n' '{"type":"session","version":2}'
  exit 1
fi
printf '{"type":"session","version":3,"id":"00000000-0000-4000-8000-%s","timestamp":"2026-08-04T00:00:00Z","cwd":"%s"}\n' "$session_suffix" "$PWD"
IFS= read -r released < "$PI_FIXTURE_AGENT_RELEASE"
case "$PI_FIXTURE_MODE" in
  missing-agent-start)
    printf 'extension registration failed\n' >&2
    exit 1
    ;;
  invalid-agent-start)
    printf '%s\n' '{"type":"turn_start"}'
    exit 0
    ;;
  success)
    assistant='{"role":"assistant","content":[],"api":"test-api","provider":"test-provider","model":"test-model","usage":{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":2}'
    printf '%s\n' '{"type":"agent_start"}'
    printf '%s\n' '{"type":"turn_start"}'
    printf '{"type":"message_start","message":%s}\n' "$assistant"
    printf '{"type":"message_end","message":%s}\n' "$assistant"
    printf '{"type":"turn_end","message":%s,"toolResults":[]}\n' "$assistant"
    printf '{"type":"agent_end","messages":[%s],"willRetry":false}\n' "$assistant"
    printf '%s\n' '{"type":"agent_settled"}'
    ;;
  *)
    exit 73
    ;;
esac
"#;

const RESULT_BRIDGE_PI: &str = r#"#!/bin/sh
set -eu
name=$PI_FIXTURE_RESULT_TOOL_NAME
cwd=$PWD
usage='{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}}'
first=$(printf '{"role":"assistant","content":[{"type":"toolCall","id":"call-invalid","name":"%s","arguments":{"result":{"count":0}}}],"api":"test-api","provider":"test-provider","model":"test-model","usage":%s,"stopReason":"toolUse","timestamp":2}' "$name" "$usage")
first_result=$(printf '{"role":"toolResult","toolCallId":"call-invalid","toolName":"%s","content":[{"type":"text","text":"rejected"}],"details":{},"isError":true,"timestamp":3}' "$name")
second=$(printf '{"role":"assistant","content":[{"type":"toolCall","id":"call-valid","name":"%s","arguments":{"result":{"count":1}}}],"api":"test-api","provider":"test-provider","model":"test-model","usage":%s,"stopReason":"toolUse","timestamp":4}' "$name" "$usage")
second_result=$(printf '{"role":"toolResult","toolCallId":"call-valid","toolName":"%s","content":[{"type":"text","text":"Final workflow result accepted."}],"details":{},"isError":false,"timestamp":5}' "$name")
printf '%s\n' "$$" > "$PI_FIXTURE_PROCESS"
printf '{"type":"session","version":3,"id":"00000000-0000-4000-8000-000000000099","timestamp":"2026-08-04T00:00:00Z","cwd":"%s"}\n' "$cwd"
printf '%s\n' '{"type":"agent_start"}' '{"type":"turn_start"}'
printf '{"type":"message_start","message":%s}\n' "$first"
printf '{"type":"message_end","message":%s}\n' "$first"
printf '{"type":"tool_execution_start","toolCallId":"call-invalid","toolName":"%s","args":{"result":{"count":0}}}\n' "$name"
printf 'first-ready\n' > "$PI_FIXTURE_RESULT_FIRST_READY"
IFS= read -r released < "$PI_FIXTURE_RESULT_FIRST_RELEASE"
printf '{"type":"tool_execution_end","toolCallId":"call-invalid","toolName":"%s","result":{"content":[{"type":"text","text":"rejected"}],"details":{}},"isError":true}\n' "$name"
printf '{"type":"message_start","message":%s}\n' "$first_result"
printf '{"type":"message_end","message":%s}\n' "$first_result"
printf '{"type":"turn_end","message":%s,"toolResults":[%s]}\n' "$first" "$first_result"
printf '%s\n' '{"type":"turn_start"}'
printf '{"type":"message_start","message":%s}\n' "$second"
printf '{"type":"message_end","message":%s}\n' "$second"
printf '{"type":"tool_execution_start","toolCallId":"call-valid","toolName":"%s","args":{"result":{"count":1}}}\n' "$name"
printf 'second-ready\n' > "$PI_FIXTURE_RESULT_SECOND_READY"
IFS= read -r released < "$PI_FIXTURE_RESULT_SECOND_RELEASE"
printf '{"type":"tool_execution_end","toolCallId":"call-valid","toolName":"%s","result":{"content":[{"type":"text","text":"Final workflow result accepted."}],"details":{},"terminate":true},"isError":false}\n' "$name"
printf '{"type":"message_start","message":%s}\n' "$second_result"
printf '{"type":"message_end","message":%s}\n' "$second_result"
printf '{"type":"turn_end","message":%s,"toolResults":[%s]}\n' "$second" "$second_result"
printf '{"type":"agent_end","messages":[%s,%s,%s,%s],"willRetry":false}\n' "$first" "$first_result" "$second" "$second_result"
printf '%s\n' '{"type":"agent_settled"}'
case "$PI_FIXTURE_MODE" in
  result-bridge-settlement-success)
    printf 'settlement-ready\n' > "$PI_FIXTURE_RESULT_SETTLEMENT_READY"
    IFS= read -r released < "$PI_FIXTURE_RESULT_SETTLEMENT_RELEASE"
    ;;
  result-bridge-settlement-expiry)
    printf 'settlement-ready\n' > "$PI_FIXTURE_RESULT_SETTLEMENT_READY"
    while :; do :; done
    ;;
  result-bridge-settlement-eof-after-grace)
    "$PI_FIXTURE_DETACHED_HOLDER" \
      --exact execution::workflow::pi_json_v1::adapter_tests::detached_standard_output_holder_process \
      --ignored 3>&1 >/dev/null 2>&1 &
    ;;
  result-bridge-in-group-descendant)
    "$PI_FIXTURE_DETACHED_HOLDER" \
      --exact execution::workflow::pi_json_v1::adapter_tests::in_group_descendant_reaper_process \
      --ignored >/dev/null 2>&1 &
    ;;
esac
"#;

const PHASE_CANCELLATION_PI: &str = r#"#!/bin/sh
set -eu
trap '
  printf "interrupted\n" > "$PI_FIXTURE_STDIN"
  IFS= read -r released < "$PI_FIXTURE_AGENT_RELEASE"
  printf "%s\n" "{\"type\":\"late_native_event\"}"
  printf "late diagnostic\n" >&2
  exit 41
' INT
printf '%s\n' "$$" > "$PI_FIXTURE_PROCESS"
cat "$PI_FIXTURE_PHASE_TRANSCRIPT"
printf 'phase diagnostic\n' >&2
printf 'ready\n' > "$PI_FIXTURE_READY"
while :; do :; done
"#;

const STUBBORN_DESCENDANT_PI: &str = r#"#!/bin/sh
set -eu
trap 'printf "interrupted\n" > "$PI_FIXTURE_STDIN"; exit 0' INT
printf '%s\n' "$$" > "$PI_FIXTURE_PROCESS"
sh -c 'trap "" INT; printf "descendant-ready\n" > "$PI_FIXTURE_DESCENDANT_READY"; while :; do :; done' &
descendant=$!
printf '%s\n' "$descendant" > "$PI_FIXTURE_DESCENDANT"
printf 'phase diagnostic\n' >&2
printf 'ready\n' > "$PI_FIXTURE_READY"
while :; do :; done
"#;

#[derive(Clone)]
struct ObservationGate {
    reached: mpsc::UnboundedSender<()>,
    release: watch::Receiver<bool>,
}

#[derive(Clone)]
struct RecordingObservationSink {
    observations: mpsc::UnboundedSender<AgentObservationEnvelope>,
    gate: Arc<Mutex<Option<ObservationGate>>>,
}

impl AgentObservationSink for RecordingObservationSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        let _ = self.observations.send(observation);
        let gate = self.gate.lock().unwrap().clone();
        async move {
            let Some(mut gate) = gate else {
                return;
            };
            let _ = gate.reached.send(());
            while !*gate.release.borrow_and_update() {
                if gate.release.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

type TestInvocation = AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, RecordingObservationSink>;

struct ProcessFixture {
    _temporary: tempfile::TempDir,
    invocation: TestInvocation,
    observations: mpsc::UnboundedReceiver<AgentObservationEnvelope>,
    observation_gate: Arc<Mutex<Option<ObservationGate>>>,
    diagnostics: StepDiagnosticLog,
    arguments: PathBuf,
    cwd: PathBuf,
    standard_input: PathBuf,
    process: PathBuf,
    session: PathBuf,
    ready: PathBuf,
    phase_transcript: PathBuf,
    descendant: PathBuf,
    descendant_ready: PathBuf,
    header_release: PathBuf,
    header_ready: PathBuf,
    agent_release: PathBuf,
    result_endpoint: PathBuf,
    result_first_ready: PathBuf,
    result_first_release: PathBuf,
    result_second_ready: PathBuf,
    result_second_release: PathBuf,
    result_settlement_ready: PathBuf,
    result_settlement_release: PathBuf,
    result_tool_name: String,
    trust_state: PathBuf,
    expected_environment: BTreeMap<OsString, OsString>,
    expected_attachments: Vec<PathBuf>,
}

impl ProcessFixture {
    fn new(mode: &str, system_prompt: String, message: String) -> Self {
        Self::new_with_declared_cwd_and_value_mode(
            mode,
            system_prompt,
            message,
            "worktree",
            AgentValueMode::None,
        )
    }

    fn new_with_value_mode(
        mode: &str,
        system_prompt: String,
        message: String,
        value_mode: AgentValueMode,
    ) -> Self {
        Self::new_with_declared_cwd_and_value_mode(
            mode,
            system_prompt,
            message,
            "worktree",
            value_mode,
        )
    }

    fn new_with_declared_cwd(
        mode: &str,
        system_prompt: String,
        message: String,
        declared_cwd: &str,
    ) -> Self {
        Self::new_with_declared_cwd_and_value_mode(
            mode,
            system_prompt,
            message,
            declared_cwd,
            AgentValueMode::None,
        )
    }

    fn new_with_declared_cwd_and_value_mode(
        mode: &str,
        system_prompt: String,
        message: String,
        declared_cwd: &str,
        value_mode: AgentValueMode,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let cwd = execution_root.join("worktree");
        let staging = temporary.path().join("staging");
        let result_endpoint = staging.join("result-endpoint");
        let controls = temporary.path().join("controls");
        let agent_directory = temporary.path().join("agent");
        for directory in [
            &cwd,
            &staging,
            &result_endpoint,
            &controls,
            &agent_directory,
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::set_permissions(&result_endpoint, fs::Permissions::from_mode(0o700)).unwrap();
        let executable = temporary.path().join("pi-0.83.0-fake");
        fs::write(&executable, FAKE_PI).unwrap();
        let mut permissions = fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&executable, permissions).unwrap();

        let arguments = controls.join("arguments");
        let captured_cwd = controls.join("cwd");
        let standard_input = controls.join("stdin");
        let process = controls.join("process");
        let session = controls.join("session");
        let ready = controls.join("ready");
        let phase_transcript = controls.join("phase-transcript");
        let descendant = controls.join("descendant");
        let descendant_ready = controls.join("descendant-ready");
        let header_release = controls.join("header-release");
        let header_ready = controls.join("header-ready");
        let agent_release = controls.join("agent-release");
        let result_first_ready = controls.join("result-first-ready");
        let result_first_release = controls.join("result-first-release");
        let result_second_ready = controls.join("result-second-ready");
        let result_second_release = controls.join("result-second-release");
        let result_settlement_ready = controls.join("result-settlement-ready");
        let result_settlement_release = controls.join("result-settlement-release");
        fs::write(&phase_transcript, []).unwrap();
        for fifo in [
            &ready,
            &descendant_ready,
            &header_release,
            &header_ready,
            &agent_release,
            &result_first_ready,
            &result_first_release,
            &result_second_ready,
            &result_second_release,
            &result_settlement_ready,
            &result_settlement_release,
        ] {
            mkfifo(fifo, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        }

        let trust_state = agent_directory.join("trust.json");
        fs::write(&trust_state, b"saved trust sentinel\n").unwrap();
        let attachments = vec![staging.join("000000"), staging.join("000001")];
        fs::write(&attachments[0], b"first immutable attachment").unwrap();
        fs::write(&attachments[1], b"second immutable attachment").unwrap();
        for attachment in &attachments {
            let mut permissions = fs::metadata(attachment).unwrap().permissions();
            permissions.set_mode(0o400);
            fs::set_permissions(attachment, permissions).unwrap();
        }

        let path = std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin"));
        let detached_holder = std::env::current_exe().unwrap();
        let mut expected_environment = BTreeMap::from([
            (OsString::from("PATH"), path),
            (
                OsString::from("PI_CODING_AGENT_DIR"),
                agent_directory.as_os_str().to_owned(),
            ),
            (
                OsString::from("ONLY_RUNNER_VALUE"),
                OsString::from("runner exact"),
            ),
            (OsString::from("PI_FIXTURE_MODE"), OsString::from(mode)),
            (
                OsString::from("PI_FIXTURE_DETACHED_HOLDER"),
                detached_holder.into_os_string(),
            ),
            (
                OsString::from("PI_FIXTURE_ARGUMENTS"),
                arguments.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_CWD"),
                captured_cwd.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_STDIN"),
                standard_input.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_PROCESS"),
                process.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_SESSION"),
                session.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_READY"),
                ready.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_PHASE_TRANSCRIPT"),
                phase_transcript.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_DESCENDANT"),
                descendant.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_DESCENDANT_READY"),
                descendant_ready.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_HEADER_RELEASE"),
                header_release.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_HEADER_READY"),
                header_ready.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_AGENT_RELEASE"),
                agent_release.as_os_str().to_owned(),
            ),
        ]);
        let admitted_root = AdmittedExecutionRoot::admit(&execution_root).unwrap();
        let working_directory = admitted_root
            .select_working_directory(Some(declared_cwd))
            .unwrap();
        let identity = AgentInvocationIdentity::new(
            WorkflowRunId::from(Arc::from(format!("run-launch-fixture-{mode}"))),
            Arc::from("agent-step"),
            ActionId {
                transition_sequence: TransitionSequence::default(),
            },
        );
        let result_tool_name = super::result_bridge::result_tool_name(&identity).unwrap();
        expected_environment.extend([
            (
                OsString::from("PI_FIXTURE_RESULT_TOOL_NAME"),
                OsString::from(&result_tool_name),
            ),
            (
                OsString::from("PI_FIXTURE_RESULT_FIRST_READY"),
                result_first_ready.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_RESULT_FIRST_RELEASE"),
                result_first_release.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_RESULT_SECOND_READY"),
                result_second_ready.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_RESULT_SECOND_RELEASE"),
                result_second_release.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_RESULT_SETTLEMENT_READY"),
                result_settlement_ready.as_os_str().to_owned(),
            ),
            (
                OsString::from("PI_FIXTURE_RESULT_SETTLEMENT_RELEASE"),
                result_settlement_release.as_os_str().to_owned(),
            ),
        ]);
        let (observation_sender, observations) = mpsc::unbounded_channel();
        let observation_gate = Arc::new(Mutex::new(None));
        let invocation = AgentInvocation::new(
            identity,
            AdmittedAgentAdapter::new(
                AgentCompatibilityProfile::PiJsonV1,
                executable,
                Arc::from("0.83.0"),
                PiConfig {
                    model: "openai/gpt-5.6-sol".to_owned(),
                    thinking: Thinking::XHigh,
                },
            ),
            AgentProcessContext::new(
                working_directory,
                EnvironmentSnapshot::new(expected_environment.clone()),
            ),
            AgentInvocationStaging::new(result_endpoint.clone()),
            AgentPrompt::new(Arc::from(system_prompt), Arc::from(message)),
            Arc::from(
                attachments
                    .iter()
                    .cloned()
                    .map(|path| StagedAgentAttachment::new(path, Arc::from("text/plain"), None))
                    .collect::<Vec<_>>(),
            ),
            value_mode,
            invocation_limits(),
            CancellationSource::new(),
            crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
            RecordingObservationSink {
                observations: observation_sender,
                gate: Arc::clone(&observation_gate),
            },
        );
        Self {
            _temporary: temporary,
            invocation,
            observations,
            observation_gate,
            diagnostics: StepDiagnosticLog::default(),
            arguments,
            cwd: captured_cwd,
            standard_input,
            process,
            session,
            ready,
            phase_transcript,
            descendant,
            descendant_ready,
            header_release,
            header_ready,
            agent_release,
            result_endpoint,
            result_first_ready,
            result_first_release,
            result_second_ready,
            result_second_release,
            result_settlement_ready,
            result_settlement_release,
            result_tool_name,
            trust_state,
            expected_environment,
            expected_attachments: attachments,
        }
    }
}

fn count_result_mode() -> AgentValueMode {
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"count": {"type": "integer", "minimum": 1}},
        "required": ["count"],
        "additionalProperties": false
    });
    let bytes = Arc::<[u8]>::from(serde_json::to_vec(&document).unwrap());
    let schema = RetainedResultSchema::compile(bytes, Arc::new(document)).unwrap();
    AgentValueMode::Result {
        output: Arc::from("result"),
        schema,
    }
}

pub(super) fn invocation_limits() -> AgentInvocationLimits<PiJsonV1ProtocolLimits> {
    AgentInvocationLimits::new(
        NonZeroU64::new(MAXIMUM_INPUT_BYTES).unwrap(),
        NonZeroU64::new(MAXIMUM_INPUT_BYTES).unwrap(),
        NonZeroUsize::new(256).unwrap(),
        NonZeroU64::new(256 * 1024 * 1024).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroU64::new(8 * 1024).unwrap(),
        PositiveDuration::new(Duration::from_secs(5)).unwrap(),
        PositiveDuration::new(Duration::from_secs(30)).unwrap(),
        PiJsonV1ProtocolLimits::profile(),
    )
}

#[derive(Clone, Copy)]
pub(super) struct InlineValidationWorker;

#[derive(Clone, Copy)]
struct DeadlineValidationWorker;

pub(super) struct InlineValidation {
    decision: Option<Result<ValidationWorkerDecision, ()>>,
}

impl ResultValidationWorker for InlineValidationWorker {
    type Running = InlineValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        Ok(InlineValidation {
            decision: Some(request.evaluate()),
        })
    }
}

impl ResultValidationWorker for DeadlineValidationWorker {
    type Running = PendingResultValidation;

    fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        Ok(PendingResultValidation)
    }
}

impl RunningResultValidation for InlineValidation {
    fn wait(&mut self) -> impl Future<Output = Result<ValidationWorkerDecision, ()>> + Send {
        ready(self.decision.take().unwrap())
    }

    fn request_stop(&mut self) {}

    fn quiesce(self) -> impl Future<Output = ()> + Send {
        ready(())
    }
}

struct CapturedRun {
    arguments: Vec<Vec<u8>>,
    cwd: Vec<u8>,
    standard_input: Vec<u8>,
    process: Vec<u8>,
    session: Vec<u8>,
    trust_state: Vec<u8>,
    observations: Vec<AgentObservationEnvelope>,
    outcome: AgentOutcome,
    diagnostic: StepDiagnostic,
    expected_attachments: Vec<PathBuf>,
}

fn start_invocation(
    invocation: TestInvocation,
    diagnostics: StepDiagnosticLog,
) -> (
    tokio::task::JoinHandle<()>,
    AgentStartReceiver,
    AgentTerminalReceiver,
) {
    start_invocation_with_clock(invocation, diagnostics, TestClock::Yielding)
}

fn start_invocation_with_clock<Clock>(
    invocation: TestInvocation,
    diagnostics: StepDiagnosticLog,
    clock: Clock,
) -> (
    tokio::task::JoinHandle<()>,
    AgentStartReceiver,
    AgentTerminalReceiver,
)
where
    Clock: CoordinatorClock,
{
    let value_mode = invocation.value_mode().clone();
    let (started_callback, started) = agent_start_channel();
    let (terminal_callback, terminal) = agent_terminal_channel(&value_mode);
    let adapter = PiJsonV1Adapter::<Clock, _>::new(
        diagnostics,
        NonZeroU64::new(1024).unwrap(),
        clock,
        NoopExecutionObserver,
    )
    .unwrap();
    let task = tokio::spawn(async move {
        invoke_agent_adapter(&adapter, invocation, started_callback, terminal_callback).await;
    });
    (task, started, terminal)
}

fn start_invocation_with_clock_and_worker<Clock, Worker>(
    invocation: TestInvocation,
    diagnostics: StepDiagnosticLog,
    clock: Clock,
    worker: Worker,
) -> (
    tokio::task::JoinHandle<()>,
    AgentStartReceiver,
    AgentTerminalReceiver,
)
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    let value_mode = invocation.value_mode().clone();
    let (started_callback, started) = agent_start_channel();
    let (terminal_callback, terminal) = agent_terminal_channel(&value_mode);
    let adapter = PiJsonV1Adapter::with_validation_worker(
        diagnostics,
        NonZeroU64::new(1024).unwrap(),
        clock,
        NoopExecutionObserver,
        worker,
    );
    let task = tokio::spawn(async move {
        invoke_agent_adapter(&adapter, invocation, started_callback, terminal_callback).await;
    });
    (task, started, terminal)
}

async fn admit_test_cancellation(
    cancellation: &CancellationSource,
    process_control: AgentProcessControl,
    mut clock: TestClock,
    interrupted: PathBuf,
    registered_deadlines: &mut mpsc::UnboundedReceiver<Duration>,
) -> (Duration, tokio::task::JoinHandle<()>) {
    assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
    let deadline = clock.now() + Duration::from_secs(5);
    process_control.interrupt();
    let task = tokio::spawn(async move {
        clock.wait_until(deadline).await;
        process_control.force();
    });
    read_signal(interrupted).await;
    assert_eq!(registered_deadlines.recv().await, Some(deadline));
    (deadline, task)
}

async fn assert_user_cancellation(
    task: tokio::task::JoinHandle<()>,
    terminal: AgentTerminalReceiver,
) {
    assert_eq!(
        terminal.receive().await.unwrap(),
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest,
        }
    );
    task.await.unwrap();
}

async fn run_success(mut fixture: ProcessFixture) -> CapturedRun {
    let plan = prepare_launch(&fixture.invocation).unwrap();
    let command = build_command(&fixture.invocation, &plan).unwrap();
    assert_eq!(
        command.as_std().get_program(),
        fixture.invocation.adapter().executable()
    );
    assert_eq!(
        command.as_std().get_args().collect::<Vec<_>>(),
        plan.arguments()
            .iter()
            .map(OsString::as_os_str)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        command
            .as_std()
            .get_envs()
            .map(|(name, value)| (name.to_owned(), value.unwrap().to_owned()))
            .collect::<BTreeMap<_, _>>(),
        fixture.expected_environment
    );
    assert_eq!(command.as_std().get_current_dir(), None);
    drop(command);

    let (task, started, terminal) =
        start_invocation(fixture.invocation, fixture.diagnostics.clone());

    read_signal(fixture.ready.clone()).await;
    let process = fs::read(&fixture.process).unwrap();
    assert_process_group_leader(&process);
    assert!(fixture.observations.try_recv().is_err());
    write_signal(fixture.header_release.clone()).await;
    read_signal(fixture.header_ready.clone()).await;
    let header = fixture.observations.recv().await.unwrap();
    assert_eq!(
        header.observation(),
        &AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::SessionEstablished,
        }
    );
    assert!(fixture.observations.try_recv().is_err());
    write_signal(fixture.agent_release.clone()).await;

    started.receive().await.unwrap();
    let outcome = terminal.receive().await.unwrap();
    task.await.unwrap();
    let mut observations = vec![header];
    while let Ok(observation) = fixture.observations.try_recv() {
        observations.push(observation);
    }
    CapturedRun {
        arguments: nul_values(&fs::read(&fixture.arguments).unwrap()),
        cwd: fs::read(&fixture.cwd).unwrap(),
        standard_input: fs::read(&fixture.standard_input).unwrap(),
        process,
        session: fs::read(&fixture.session).unwrap(),
        trust_state: fs::read(&fixture.trust_state).unwrap(),
        observations,
        outcome,
        diagnostic: fixture.diagnostics.get("agent-step").unwrap(),
        expected_attachments: fixture.expected_attachments,
    }
}

async fn run_start_failure(
    mut fixture: ProcessFixture,
    valid_header: bool,
    release_agent: bool,
) -> (
    AgentOutcome,
    Vec<AgentObservationEnvelope>,
    StepDiagnostic,
    bool,
) {
    let (task, started, terminal) =
        start_invocation(fixture.invocation, fixture.diagnostics.clone());
    read_signal(fixture.ready.clone()).await;
    write_signal(fixture.header_release.clone()).await;
    read_signal(fixture.header_ready.clone()).await;
    if valid_header {
        let header = fixture.observations.recv().await.unwrap();
        assert_eq!(
            header.observation(),
            &AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::SessionEstablished,
            }
        );
    }
    if release_agent {
        write_signal(fixture.agent_release.clone()).await;
    }
    let outcome = terminal.receive().await.unwrap();
    task.await.unwrap();
    let lifecycle_started = started.receive().await.is_ok();
    let mut observations = Vec::new();
    while let Ok(observation) = fixture.observations.try_recv() {
        observations.push(observation);
    }
    (
        outcome,
        observations,
        fixture.diagnostics.get("agent-step").unwrap(),
        lifecycle_started,
    )
}

async fn validation_socket_exchange(socket_path: &Path, request: Value) -> Value {
    let response = validation_socket_raw_exchange(socket_path, request).await;
    let length = u32::from_be_bytes(response[..4].try_into().unwrap());
    assert_eq!(usize::try_from(length).unwrap(), response.len() - 4);
    serde_json::from_slice(&response[4..]).unwrap()
}

async fn validation_socket_raw_exchange(socket_address: &Path, request: Value) -> Vec<u8> {
    let mut stream = tokio::net::UnixStream::connect(socket_address)
        .await
        .unwrap();
    let payload = serde_json::to_vec(&request).unwrap();
    stream
        .write_all(&u32::try_from(payload.len()).unwrap().to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&payload).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    response
}

struct RunningResultFixture {
    _temporary: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
    terminal: tokio::task::JoinHandle<Result<AgentOutcome, AgentTerminalReceiveError>>,
    socket_path: PathBuf,
    socket_address: PathBuf,
    extension_path: PathBuf,
    first_release: PathBuf,
    second_ready: PathBuf,
    second_release: PathBuf,
    settlement_ready: PathBuf,
    settlement_release: PathBuf,
    process: PathBuf,
    descendant: PathBuf,
    descendant_ready: PathBuf,
    tool_name: String,
    observations: mpsc::UnboundedReceiver<AgentObservationEnvelope>,
    cancellation: CancellationSource,
}

impl RunningResultFixture {
    fn first_request(&self) -> Value {
        json!({
            "kind": "ValidatePiResultV1",
            "toolCallId": "call-invalid",
            "toolName": &self.tool_name,
            "arguments": {"result": {"count": 0}}
        })
    }

    fn corrected_request(&self) -> Value {
        json!({
            "kind": "ValidatePiResultV1",
            "toolCallId": "call-valid",
            "toolName": &self.tool_name,
            "arguments": {"result": {"count": 1}}
        })
    }

    async fn finish(&mut self) -> AgentOutcome {
        let outcome = (&mut self.terminal).await.unwrap().unwrap();
        (&mut self.task).await.unwrap();
        outcome
    }
}

fn result_process_fixture(mode: &str) -> ProcessFixture {
    ProcessFixture::new_with_value_mode(
        mode,
        "system".to_owned(),
        "message".to_owned(),
        count_result_mode(),
    )
}

fn controlled_clock() -> (
    TestClock,
    Arc<AtomicU64>,
    watch::Sender<bool>,
    mpsc::UnboundedReceiver<Duration>,
) {
    let now_seconds = Arc::new(AtomicU64::new(100));
    let (deadline_release, release_deadline) = watch::channel(false);
    let (registrations, registered_deadlines) = mpsc::unbounded_channel();
    (
        TestClock::Controlled {
            now_seconds: Arc::clone(&now_seconds),
            registrations,
            release: release_deadline,
        },
        now_seconds,
        deadline_release,
        registered_deadlines,
    )
}

async fn launch_deadline_result_fixture(
    mode: &str,
) -> (
    RunningResultFixture,
    watch::Sender<bool>,
    mpsc::UnboundedReceiver<Duration>,
) {
    let fixture = result_process_fixture(mode);
    let (clock, _now_seconds, deadline_release, registered_deadlines) = controlled_clock();
    (
        launch_result_fixture(fixture, clock, DeadlineValidationWorker).await,
        deadline_release,
        registered_deadlines,
    )
}

async fn launch_controlled_settlement_fixture(
    mode: &str,
) -> (
    RunningResultFixture,
    Arc<AtomicU64>,
    watch::Sender<bool>,
    mpsc::UnboundedReceiver<Duration>,
) {
    let fixture = result_process_fixture(mode);
    let (clock, now_seconds, deadline_release, registered_deadlines) = controlled_clock();
    (
        launch_result_fixture(fixture, clock, InlineValidationWorker).await,
        now_seconds,
        deadline_release,
        registered_deadlines,
    )
}

async fn wait_for_registered_deadline(
    registrations: &mut mpsc::UnboundedReceiver<Duration>,
    expected: Duration,
) {
    while let Some(deadline) = registrations.recv().await {
        if deadline == expected {
            return;
        }
        assert_eq!(deadline, Duration::from_secs(105));
    }
    panic!("the controlled clock closed before registering {expected:?}");
}

async fn validate_corrected_result(running: &RunningResultFixture) {
    assert_eq!(
        validation_socket_exchange(&running.socket_address, running.corrected_request()).await,
        json!({"kind": "Valid"})
    );
}

async fn advance_result_fixture_to_settlement(
    running: &mut RunningResultFixture,
    registered_deadlines: &mut mpsc::UnboundedReceiver<Duration>,
) {
    assert_eq!(
        validation_socket_exchange(&running.socket_address, running.first_request()).await["kind"],
        "Rejected"
    );
    write_signal(running.first_release.clone()).await;
    read_signal(running.second_ready.clone()).await;
    validate_corrected_result(running).await;
    assert!(
        !running.task.is_finished(),
        "Valid is provisional until native tool success"
    );
    write_signal(running.second_release.clone()).await;
    read_signal(running.settlement_ready.clone()).await;
    wait_for_registered_deadline(registered_deadlines, Duration::from_secs(130)).await;
}

async fn launch_result_fixture<Clock, Worker>(
    fixture: ProcessFixture,
    clock: Clock,
    worker: Worker,
) -> RunningResultFixture
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    fs::write(fixture.invocation.adapter().executable(), RESULT_BRIDGE_PI).unwrap();
    let cancellation = fixture.invocation.cancellation().clone();
    let socket_path = fixture.result_endpoint.join("result-validation.sock");
    let extension_path = fixture
        .result_endpoint
        .join("pi-json-v1-result-extension.ts");
    let tool_name = fixture.result_tool_name;
    let socket_address = validation_socket_address(&tool_name);
    let alias_directory = socket_address.parent().unwrap().parent().unwrap();
    let _ = fs::remove_file(alias_directory.join("e"));
    let _ = fs::remove_dir(alias_directory);
    let (task, started, terminal) = start_invocation_with_clock_and_worker(
        fixture.invocation,
        fixture.diagnostics,
        clock,
        worker,
    );
    let mut terminal = tokio::spawn(async move { terminal.receive().await });
    tokio::select! {
        () = read_signal(fixture.result_first_ready) => {}
        outcome = &mut terminal => {
            panic!("result fixture stopped before its first request: {outcome:?}");
        }
    }
    started.receive().await.unwrap();
    RunningResultFixture {
        _temporary: fixture._temporary,
        task,
        terminal,
        socket_path,
        socket_address,
        extension_path,
        first_release: fixture.result_first_release,
        second_ready: fixture.result_second_ready,
        second_release: fixture.result_second_release,
        settlement_ready: fixture.result_settlement_ready,
        settlement_release: fixture.result_settlement_release,
        process: fixture.process,
        descendant: fixture.descendant,
        descendant_ready: fixture.descendant_ready,
        tool_name,
        observations: fixture.observations,
        cancellation,
    }
}

fn validation_socket_address(tool_name: &str) -> PathBuf {
    let identity = tool_name.strip_prefix("scherzo_result_").unwrap();
    Path::new("/tmp")
        .join(format!(".szp-{identity}-{}", std::process::id()))
        .join("e")
        .join("result-validation.sock")
}

async fn read_signal(path: PathBuf) {
    let bytes = tokio::task::spawn_blocking(move || fs::read(path))
        .await
        .unwrap()
        .unwrap();
    assert!(!bytes.is_empty());
}

async fn write_signal(path: PathBuf) {
    tokio::task::spawn_blocking(move || fs::write(path, b"release\n"))
        .await
        .unwrap()
        .unwrap();
}

fn process_id(bytes: &[u8]) -> Pid {
    let process_id = std::str::from_utf8(bytes)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    Pid::from_raw(process_id).unwrap()
}

fn assert_process_group_leader(bytes: &[u8]) {
    let process = process_id(bytes);
    assert_eq!(getpgid(Some(process)).unwrap(), process);
}

#[derive(Clone, Copy, Debug)]
enum CancellationPhase {
    Model,
    Tool,
    Retry,
    Settlement,
}

impl CancellationPhase {
    fn transcript(self, cwd: &str) -> String {
        match self {
            Self::Model => format!(
                concat!(
                    "{{\"type\":\"session\",\"version\":3,",
                    "\"id\":\"00000000-0000-4000-8000-000000000011\",",
                    "\"timestamp\":\"2026-08-04T00:00:00Z\",\"cwd\":{cwd:?}}}\n",
                    "{{\"type\":\"agent_start\"}}\n",
                    "{{\"type\":\"turn_start\"}}\n",
                    "{{\"type\":\"message_start\",\"message\":{{",
                    "\"role\":\"assistant\",\"content\":[],\"api\":\"test-api\",",
                    "\"provider\":\"test-provider\",\"model\":\"test-model\",",
                    "\"usage\":{{\"input\":0,\"output\":0,\"cacheRead\":0,",
                    "\"cacheWrite\":0,\"totalTokens\":0,\"cost\":{{\"input\":0,",
                    "\"output\":0,\"cacheRead\":0,\"cacheWrite\":0,\"total\":0}}}},",
                    "\"stopReason\":\"stop\",\"timestamp\":2}}}}\n"
                ),
                cwd = cwd,
            ),
            Self::Tool => transcript_through(TERMINAL_TOOL_USE, cwd, "tool_execution_start"),
            Self::Retry => transcript_through(NATIVE_RECOVERY, cwd, "auto_retry_start"),
            Self::Settlement => RESPONSE_SUCCESS.replace(RECORDED_CWD, cwd),
        }
    }

    fn reached(self, observation: &AgentObservation) -> bool {
        match self {
            Self::Model => matches!(
                observation,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::MessageStarted,
                }
            ),
            Self::Tool => matches!(
                observation,
                AgentObservation::ToolCall {
                    phase: AgentToolCallPhase::Started,
                    ..
                }
            ),
            Self::Retry => matches!(
                observation,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::RetryStarted,
                }
            ),
            Self::Settlement => matches!(
                observation,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::HarnessQuiescent,
                }
            ),
        }
    }
}

fn transcript_through(source: &str, cwd: &str, final_event: &str) -> String {
    let expected = format!("\"type\":\"{final_event}\"");
    let mut transcript = String::new();
    for line in source.lines() {
        transcript.push_str(&line.replace(RECORDED_CWD, cwd));
        transcript.push('\n');
        if line.contains(&expected) {
            return transcript;
        }
    }
    panic!("recorded transcript has no {final_event} event");
}

async fn wait_for_phase(
    phase: CancellationPhase,
    observations: &mut mpsc::UnboundedReceiver<AgentObservationEnvelope>,
) {
    while let Some(observation) = observations.recv().await {
        if phase.reached(observation.observation()) {
            return;
        }
    }
    panic!("PiJsonV1 stopped before reaching the {phase:?} cancellation phase");
}

fn nul_values(bytes: &[u8]) -> Vec<Vec<u8>> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|value| !value.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

#[tokio::test]
async fn launch_uses_exact_direct_process_contract_and_delays_started_until_agent_start() {
    with_watchdog(async {
        let message = "@/caller/path\n$HOME; * remains literal";
        let first = run_success(ProcessFixture::new(
            "success",
            "system @ exact\n".to_owned(),
            message.to_owned(),
        ))
        .await;
        let second = run_success(ProcessFixture::new(
            "success",
            "system @ exact\n".to_owned(),
            message.to_owned(),
        ))
        .await;

        for run in [&first, &second] {
            let expected = [
                b"--mode".as_slice(),
                b"json".as_slice(),
                b"--approve".as_slice(),
                b"--no-session".as_slice(),
                b"--model".as_slice(),
                b"openai/gpt-5.6-sol".as_slice(),
                b"--thinking".as_slice(),
                b"xhigh".as_slice(),
                b"--append-system-prompt".as_slice(),
                b"system @ exact\n".as_slice(),
            ];
            assert_eq!(&run.arguments[..expected.len()], expected);
            assert_eq!(
                run.arguments[expected.len()],
                format!("@{}", run.expected_attachments[0].display()).as_bytes()
            );
            assert_eq!(
                run.arguments[expected.len() + 1],
                format!("@{}", run.expected_attachments[1].display()).as_bytes()
            );
            assert_eq!(
                run.arguments.last().unwrap(),
                format!("\n{message}").as_bytes()
            );
            assert_eq!(run.standard_input, b"closed\n");
            assert_eq!(run.trust_state, b"saved trust sentinel\n");
            assert_eq!(
                run.outcome,
                AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
            );
            assert_eq!(run.diagnostic.standard_output().bytes(), b"");
            assert!(run.diagnostic.standard_output().fully_drained());
            assert_eq!(
                run.diagnostic.standard_error().bytes(),
                b"fixture diagnostic\n"
            );
            assert!(run.diagnostic.standard_error().fully_drained());
            assert_eq!(
                run.observations
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
        }
        assert_ne!(first.process, second.process);
        assert_ne!(first.session, second.session);
        assert!(first.cwd.ends_with(b"/execution/worktree\n"));
        assert!(second.cwd.ends_with(b"/execution/worktree\n"));
    })
    .await;
}

#[tokio::test]
async fn result_bridge_rejects_then_accepts_one_exactly_correlated_candidate() {
    with_watchdog(async {
        let fixture = ProcessFixture::new_with_value_mode(
            "result-bridge",
            "system".to_owned(),
            "message".to_owned(),
            count_result_mode(),
        );
        let mut running =
            launch_result_fixture(fixture, TestClock::Pending, InlineValidationWorker).await;
        assert!(running.socket_path.exists());
        assert!(running.extension_path.exists());
        let rejected = validation_socket_exchange(
            &running.socket_address,
            json!({
                "kind": "ValidatePiResultV1",
                "toolCallId": "call-invalid",
                "toolName": &running.tool_name,
                "arguments": {"result": {"count": 0}}
            }),
        )
        .await;
        assert_eq!(rejected["kind"], "Rejected");
        assert!(rejected["feedback"].as_str().is_some_and(|feedback| {
            feedback.starts_with("Result rejected by the workflow schema:\n")
                && feedback.len() <= 8 * 1024
        }));
        write_signal(running.first_release.clone()).await;

        read_signal(running.second_ready.clone()).await;
        validate_corrected_result(&running).await;
        assert!(running.socket_path.exists());
        assert!(running.extension_path.exists());
        write_signal(running.second_release.clone()).await;

        let outcome = running.finish().await;
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("corrected result must complete");
        };
        assert_eq!(result.value(), &json!({"count": 1}));
        assert!(!running.socket_path.exists());
        assert!(!running.extension_path.exists());
        let mut rejected_observed = false;
        while let Ok(observation) = running.observations.try_recv() {
            if matches!(
                observation.observation(),
                AgentObservation::ValueRejected {
                    kind: AgentValueKind::Result,
                    ..
                }
            ) {
                rejected_observed = true;
            }
        }
        assert!(rejected_observed);
    })
    .await;
}

#[tokio::test]
async fn accepted_result_exits_and_quiesces_before_the_injected_settlement_deadline() {
    with_watchdog(async {
        let (mut running, _now_seconds, _deadline_release, mut registered_deadlines) =
            launch_controlled_settlement_fixture("result-bridge-settlement-success").await;
        advance_result_fixture_to_settlement(&mut running, &mut registered_deadlines).await;
        assert!(
            !running.task.is_finished(),
            "agent_end and agent_settled are not process quiescence"
        );
        write_signal(running.settlement_release.clone()).await;

        let outcome = running.finish().await;
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("cooperative settlement must commit the accepted result");
        };
        assert_eq!(result.value(), &json!({"count": 1}));
    })
    .await;
}

#[tokio::test]
async fn settlement_expiry_terminates_the_process_group_and_discards_the_candidate() {
    with_watchdog(async {
        let (mut running, now_seconds, deadline_release, mut registered_deadlines) =
            launch_controlled_settlement_fixture("result-bridge-settlement-expiry").await;
        advance_result_fixture_to_settlement(&mut running, &mut registered_deadlines).await;
        let process = process_id(&fs::read(&running.process).unwrap());
        assert_eq!(getpgid(Some(process)).unwrap(), process);

        now_seconds.store(130, Ordering::SeqCst);
        deadline_release.send_replace(true);
        assert_eq!(
            running.finish().await,
            AgentOutcome::Failed {
                cause: AgentFailureCause::ResultSettlementFailed
            }
        );
        assert!(getpgid(Some(process)).is_err());
    })
    .await;
}

#[tokio::test]
async fn adapter_waits_for_a_terminated_process_group_to_be_observed_absent() {
    with_watchdog(async {
        let (mut running, now_seconds, deadline_release, mut registered_deadlines) =
            launch_controlled_settlement_fixture("result-bridge-in-group-descendant").await;
        advance_result_fixture_to_settlement(&mut running, &mut registered_deadlines).await;
        let process = process_id(&fs::read(&running.process).unwrap());
        let descendant = process_id(&fs::read(&running.descendant).unwrap());
        assert_eq!(getpgid(Some(descendant)).unwrap(), process);

        now_seconds.store(130, Ordering::SeqCst);
        deadline_release.send_replace(true);
        read_signal(running.descendant_ready.clone()).await;
        assert_eq!(
            getpgid(Some(descendant)).unwrap(),
            process,
            "the unreaped descendant must keep the process group observable"
        );
        assert!(
            !running.terminal.is_finished(),
            "the adapter must not report completion while the process group still exists"
        );

        write_signal(running.settlement_release.clone()).await;
        assert_eq!(
            running.finish().await,
            AgentOutcome::Failed {
                cause: AgentFailureCause::ResultSettlementFailed
            }
        );
        assert!(getpgid(Some(descendant)).is_err());
    })
    .await;
}

#[tokio::test]
async fn stdout_eof_after_settlement_grace_discards_the_candidate() {
    with_watchdog(async {
        let (mut running, now_seconds, deadline_release, mut registered_deadlines) =
            launch_controlled_settlement_fixture("result-bridge-settlement-eof-after-grace").await;
        advance_result_fixture_to_settlement(&mut running, &mut registered_deadlines).await;

        now_seconds.store(130, Ordering::SeqCst);
        deadline_release.send_replace(true);
        let outcome = (&mut running.terminal).await.unwrap().unwrap();
        write_signal(running.settlement_release.clone()).await;
        (&mut running.task).await.unwrap();

        assert_eq!(
            outcome,
            AgentOutcome::Failed {
                cause: AgentFailureCause::ResultSettlementFailed
            }
        );
    })
    .await;
}

#[tokio::test]
async fn wrong_bridge_identity_is_fatal_without_authoritative_acceptance() {
    with_watchdog(async {
        let fixture = ProcessFixture::new_with_value_mode(
            "result-bridge-wrong-identity",
            "system".to_owned(),
            "message".to_owned(),
            count_result_mode(),
        );
        let mut running =
            launch_result_fixture(fixture, TestClock::Pending, InlineValidationWorker).await;
        let response = validation_socket_exchange(
            &running.socket_address,
            json!({
                "kind": "ValidatePiResultV1",
                "toolCallId": "call-invalid",
                "toolName": "scherzo_result_wrong",
                "arguments": {"result": {"count": 1}}
            }),
        )
        .await;
        assert_eq!(response["kind"], "Fatal");

        assert_eq!(
            running.finish().await,
            AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessProtocolFailed
            }
        );
        assert!(!running.socket_path.exists());
        assert!(!running.extension_path.exists());
    })
    .await;
}

#[tokio::test]
async fn validation_exhaustion_returns_fatal_before_the_typed_failure() {
    with_watchdog(async {
        let (mut running, deadline_release, mut registered_deadlines) =
            launch_deadline_result_fixture("result-bridge-deadline").await;
        let socket_address = running.socket_address.clone();
        let request = running.first_request();
        let response =
            tokio::spawn(async move { validation_socket_exchange(&socket_address, request).await });
        for _ in 0..3 {
            assert_eq!(
                registered_deadlines.recv().await,
                Some(Duration::from_secs(105))
            );
        }
        deadline_release.send_replace(true);
        assert_eq!(response.await.unwrap()["kind"], "Fatal");
        assert_eq!(
            running.finish().await,
            AgentOutcome::Failed {
                cause: AgentFailureCause::ResultValidationLimitExceeded {
                    deadline: PositiveDuration::new(Duration::from_secs(5)).unwrap()
                }
            }
        );
    })
    .await;
}

#[tokio::test]
async fn cancellation_preempts_socket_validation_without_a_fatal_reply() {
    with_watchdog(async {
        let (mut running, _deadline_release, mut registered_deadlines) =
            launch_deadline_result_fixture("result-bridge-cancelled").await;
        let socket_address = running.socket_address.clone();
        let request = running.first_request();
        let response =
            tokio::spawn(
                async move { validation_socket_raw_exchange(&socket_address, request).await },
            );
        for _ in 0..3 {
            assert_eq!(
                registered_deadlines.recv().await,
                Some(Duration::from_secs(105))
            );
        }
        assert!(
            running
                .cancellation
                .request_cancellation(CancellationReason::UserRequest)
        );
        assert_eq!(
            running.finish().await,
            AgentOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        assert!(response.await.unwrap().is_empty());
    })
    .await;
}

#[test]
fn positional_transport_escape_preserves_every_adversarial_message() {
    for message in [
        "-option-shaped",
        "--- separator-shaped",
        "-- end-looking",
        "@/absolute-looking/path",
        "@imported.txt\nembedded @other.txt",
    ] {
        let fixture = ProcessFixture::new("success", "system".to_owned(), message.to_owned());
        let plan = prepare_launch(&fixture.invocation).unwrap();
        assert_eq!(
            plan.arguments().last(),
            Some(&OsString::from(format!("\n{message}")))
        );
    }
}

#[tokio::test]
async fn exact_input_limits_launch_unchanged_and_one_excess_byte_never_launches() {
    with_watchdog(async {
        let system_prompt = "s".repeat(usize::try_from(MAXIMUM_INPUT_BYTES).unwrap());
        let message = "m".repeat(usize::try_from(MAXIMUM_INPUT_BYTES).unwrap());
        let exact = run_success(ProcessFixture::new(
            "success",
            system_prompt.clone(),
            message.clone(),
        ))
        .await;
        assert_eq!(exact.arguments[9], system_prompt.as_bytes());
        assert_eq!(exact.arguments.last().unwrap().len(), message.len() + 1);
        assert_eq!(exact.arguments.last().unwrap()[0], b'\n');
        assert_eq!(&exact.arguments.last().unwrap()[1..], message.as_bytes());

        for (system_prompt, message, input) in [
            (
                "s".repeat(usize::try_from(MAXIMUM_INPUT_BYTES + 1).unwrap()),
                "message".to_owned(),
                AgentInputKind::SystemPrompt,
            ),
            (
                "system".to_owned(),
                "m".repeat(usize::try_from(MAXIMUM_INPUT_BYTES + 1).unwrap()),
                AgentInputKind::Message,
            ),
        ] {
            let fixture = ProcessFixture::new("success", system_prompt, message);
            let capture = fixture.arguments.clone();
            let value_mode = fixture.invocation.value_mode().clone();
            let (started_callback, started) = agent_start_channel();
            let (terminal_callback, terminal) = agent_terminal_channel(&value_mode);
            let adapter = PiJsonV1Adapter::<TestClock, _>::new(
                fixture.diagnostics,
                NonZeroU64::new(1024).unwrap(),
                TestClock::Pending,
                NoopExecutionObserver,
            )
            .unwrap();
            invoke_agent_adapter(
                &adapter,
                fixture.invocation,
                started_callback,
                terminal_callback,
            )
            .await;
            assert_eq!(
                terminal.receive().await.unwrap(),
                AgentOutcome::Failed {
                    cause: AgentFailureCause::HarnessInputTooLarge {
                        input,
                        admitted_bytes: NonZeroU64::new(MAXIMUM_INPUT_BYTES).unwrap(),
                        observed_bytes: MAXIMUM_INPUT_BYTES + 1,
                    }
                }
            );
            assert!(started.receive().await.is_err());
            assert!(!capture.exists());
        }
    })
    .await;
}

#[tokio::test]
async fn every_pre_agent_start_process_failure_is_a_start_failure_without_started() {
    with_watchdog(async {
        for (mode, valid_header, release_agent) in [
            ("bad-header", false, false),
            ("missing-agent-start", true, true),
            ("invalid-agent-start", true, true),
        ] {
            let (outcome, observations, diagnostic, lifecycle_started) = run_start_failure(
                ProcessFixture::new(mode, "system".to_owned(), "message".to_owned()),
                valid_header,
                release_agent,
            )
            .await;
            assert_eq!(
                outcome,
                AgentOutcome::Failed {
                    cause: AgentFailureCause::HarnessStartFailed,
                }
            );
            assert!(!lifecycle_started);
            assert!(!observations.iter().any(|observation| matches!(
                observation.observation(),
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::HarnessStarted
                }
            )));
            assert!(diagnostic.standard_error().fully_drained());
        }

        let fixture = ProcessFixture::new("success", "system".to_owned(), "message".to_owned());
        fs::remove_file(fixture.invocation.adapter().executable()).unwrap();
        let value_mode = fixture.invocation.value_mode().clone();
        let (started_callback, started) = agent_start_channel();
        let (terminal_callback, terminal) = agent_terminal_channel(&value_mode);
        let adapter = PiJsonV1Adapter::<TestClock, _>::new(
            fixture.diagnostics,
            NonZeroU64::new(1024).unwrap(),
            TestClock::Pending,
            NoopExecutionObserver,
        )
        .unwrap();
        invoke_agent_adapter(
            &adapter,
            fixture.invocation,
            started_callback,
            terminal_callback,
        )
        .await;
        assert_eq!(
            terminal.receive().await.unwrap(),
            AgentOutcome::Failed {
                cause: AgentFailureCause::HarnessStartFailed,
            }
        );
        assert!(started.receive().await.is_err());
    })
    .await;
}

#[tokio::test]
async fn cancellation_before_start_never_launches_pi() {
    with_watchdog(async {
        let fixture = ProcessFixture::new("success", "system".to_owned(), "message".to_owned());
        let cancellation = fixture.invocation.cancellation().clone();
        let process = fixture.process.clone();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));

        let (task, started, terminal) =
            start_invocation(fixture.invocation, fixture.diagnostics.clone());
        assert_user_cancellation(task, terminal).await;
        assert!(started.receive().await.is_err());
        assert!(!process.exists());
        assert!(fixture.diagnostics.get("agent-step").is_none());
    })
    .await;
}

#[tokio::test]
async fn cancellation_wins_during_every_native_phase_and_drains_late_events() {
    with_watchdog(async {
        for phase in [
            CancellationPhase::Model,
            CancellationPhase::Tool,
            CancellationPhase::Retry,
            CancellationPhase::Settlement,
        ] {
            let mut fixture = ProcessFixture::new_with_value_mode(
                "success",
                "system".to_owned(),
                "message".to_owned(),
                AgentValueMode::Response {
                    output: Arc::from("response"),
                },
            );
            fs::write(
                fixture.invocation.adapter().executable(),
                PHASE_CANCELLATION_PI,
            )
            .unwrap();
            let cwd = fixture.invocation.process().cwd().to_str().unwrap();
            fs::write(&fixture.phase_transcript, phase.transcript(cwd)).unwrap();
            mkfifo(&fixture.standard_input, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
            let cancellation = fixture.invocation.cancellation().clone();
            let diagnostics = fixture.diagnostics.clone();
            let interrupted = fixture.standard_input.clone();
            let release = fixture.agent_release.clone();
            let (task, started, terminal) =
                start_invocation(fixture.invocation, diagnostics.clone());

            read_signal(fixture.ready).await;
            wait_for_phase(phase, &mut fixture.observations).await;
            started.receive().await.unwrap();
            assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
            read_signal(interrupted).await;
            assert!(
                !task.is_finished(),
                "the {phase:?} invocation must not report before its process releases"
            );
            write_signal(release).await;
            assert_eq!(
                terminal.receive().await.unwrap(),
                AgentOutcome::Cancelled {
                    reason: CancellationReason::UserRequest,
                },
                "late EOF and nonzero exit cannot replace {phase:?} cancellation"
            );
            task.await.unwrap();

            while let Ok(observation) = fixture.observations.try_recv() {
                assert!(
                    !matches!(
                        observation.observation(),
                        AgentObservation::UnrecognizedHarnessEvent { event }
                            if event["type"] == "late_native_event"
                    ),
                    "the {phase:?} parser accepted an event after cancellation"
                );
            }
            let diagnostic = diagnostics.get("agent-step").unwrap();
            assert_eq!(
                diagnostic.standard_error().bytes(),
                b"phase diagnostic\nlate diagnostic\n"
            );
            assert!(diagnostic.standard_output().fully_drained());
            assert!(diagnostic.standard_error().fully_drained());
        }
    })
    .await;
}

#[tokio::test]
async fn cancellation_keeps_the_admitted_deadline_when_observation_delivery_is_blocked() {
    with_watchdog(async {
        let fixture = ProcessFixture::new("success", "system".to_owned(), "message".to_owned());
        fs::write(
            fixture.invocation.adapter().executable(),
            PHASE_CANCELLATION_PI,
        )
        .unwrap();
        let cwd = fixture.invocation.process().cwd().to_str().unwrap();
        fs::write(
            &fixture.phase_transcript,
            CancellationPhase::Model.transcript(cwd),
        )
        .unwrap();
        mkfifo(&fixture.standard_input, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();

        let (gate_reached, mut reached) = mpsc::unbounded_channel();
        let (gate_release, release_gate) = watch::channel(false);
        *fixture.observation_gate.lock().unwrap() = Some(ObservationGate {
            reached: gate_reached,
            release: release_gate,
        });

        let cancellation = fixture.invocation.cancellation().clone();
        let process_control = fixture.invocation.process_control().clone();
        let interrupted = fixture.standard_input.clone();
        let process_release = fixture.agent_release.clone();
        let now_seconds = Arc::new(AtomicU64::new(42));
        let (deadline_release, release_deadline) = watch::channel(false);
        let (registrations, mut registered_deadlines) = mpsc::unbounded_channel();
        let deadline_clock = TestClock::Controlled {
            now_seconds: Arc::clone(&now_seconds),
            registrations,
            release: release_deadline,
        };
        let (task, _started, terminal) = start_invocation(fixture.invocation, fixture.diagnostics);

        read_signal(fixture.ready).await;
        reached.recv().await.unwrap();
        let (admitted_deadline, deadline_task) = admit_test_cancellation(
            &cancellation,
            process_control,
            deadline_clock,
            interrupted,
            &mut registered_deadlines,
        )
        .await;

        // Observation delivery remains blocked here. Process interruption and deadline
        // registration are owned by cancellation supervision rather than parser timing.
        now_seconds.store(100, Ordering::SeqCst);
        gate_release.send(true).unwrap();
        write_signal(process_release).await;
        assert_user_cancellation(task, terminal).await;
        drop(deadline_release);
        deadline_task.await.unwrap();

        assert_eq!(admitted_deadline, Duration::from_secs(47));
    })
    .await;
}

#[tokio::test]
async fn stubborn_descendant_is_forced_at_the_injected_deadline_before_terminal_report() {
    with_watchdog(async {
        let fixture = ProcessFixture::new("success", "system".to_owned(), "message".to_owned());
        fs::write(
            fixture.invocation.adapter().executable(),
            STUBBORN_DESCENDANT_PI,
        )
        .unwrap();
        mkfifo(&fixture.standard_input, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let cancellation = fixture.invocation.cancellation().clone();
        let process_control = fixture.invocation.process_control().clone();
        let diagnostics = fixture.diagnostics.clone();
        let interrupted = fixture.standard_input.clone();
        let (deadline_release, release_deadline) = watch::channel(false);
        let (registrations, mut registered_deadlines) = mpsc::unbounded_channel();
        let deadline_clock = TestClock::Controlled {
            now_seconds: Arc::new(AtomicU64::new(42)),
            registrations,
            release: release_deadline,
        };
        let (task, _started, terminal) = start_invocation(fixture.invocation, diagnostics.clone());

        read_signal(fixture.descendant_ready).await;
        read_signal(fixture.ready).await;
        let process = process_id(&fs::read(fixture.process).unwrap());
        let descendant = process_id(&fs::read(fixture.descendant).unwrap());
        assert_eq!(getpgid(Some(process)).unwrap(), process);
        assert_eq!(getpgid(Some(descendant)).unwrap(), process);

        let (admitted_deadline, deadline_task) = admit_test_cancellation(
            &cancellation,
            process_control,
            deadline_clock,
            interrupted,
            &mut registered_deadlines,
        )
        .await;
        assert_eq!(
            admitted_deadline,
            Duration::from_secs(47),
            "escalation must use the admitted five-second cancellation grace"
        );
        assert!(!task.is_finished());
        assert_eq!(getpgid(Some(descendant)).unwrap(), process);

        deadline_release.send(true).unwrap();
        deadline_task.await.unwrap();
        assert_user_cancellation(task, terminal).await;
        let diagnostic = diagnostics.get("agent-step").unwrap();
        assert_eq!(diagnostic.standard_error().bytes(), b"phase diagnostic\n");
        assert!(diagnostic.standard_output().fully_drained());
        assert!(diagnostic.standard_error().fully_drained());
    })
    .await;
}

#[tokio::test]
async fn valid_lexical_cwd_uses_the_path_reported_by_the_launched_process() {
    with_watchdog(async {
        let fixture = ProcessFixture::new_with_declared_cwd(
            "success",
            "system".to_owned(),
            "message".to_owned(),
            "worktree/.",
        );

        let (outcome, _, _, lifecycle_started) = run_start_failure(fixture, false, true).await;
        assert!(lifecycle_started);
        assert_eq!(
            outcome,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
            "a valid runtime-relative cwd must not make its own Pi session header fail"
        );
    })
    .await;
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is allowed only as an anti-hang watchdog, not a behavior assertion"
)]
async fn with_watchdog<Output>(future: impl Future<Output = Output>) -> Output {
    match tokio::time::timeout(TEST_WATCHDOG, future).await {
        Ok(output) => output,
        Err(_) => panic!("PiJsonV1 process fixture watchdog expired"),
    }
}

#[test]
#[ignore = "launched as a detached stdout holder by the settlement regression"]
fn detached_standard_output_holder_process() {
    rustix::process::setsid().unwrap();
    let ready = std::env::var_os("PI_FIXTURE_RESULT_SETTLEMENT_READY").unwrap();
    fs::write(ready, b"settlement-ready\n").unwrap();
    let release = std::env::var_os("PI_FIXTURE_RESULT_SETTLEMENT_RELEASE").unwrap();
    assert!(!fs::read(release).unwrap().is_empty());
}

#[test]
#[ignore = "launched as an out-of-group reaper by the process-group regression"]
fn in_group_descendant_reaper_process() {
    let mut descendant = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "execution::workflow::pi_json_v1::adapter_tests::in_group_descendant_process",
            "--ignored",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let descendant_pid =
        Pid::from_raw(i32::try_from(descendant.id()).unwrap()).expect("child PID must be positive");
    rustix::process::setsid().unwrap();
    let descendant_path = std::env::var_os("PI_FIXTURE_DESCENDANT").unwrap();
    fs::write(descendant_path, format!("{}\n", descendant.id())).unwrap();
    let settlement_ready = std::env::var_os("PI_FIXTURE_RESULT_SETTLEMENT_READY").unwrap();
    fs::write(settlement_ready, b"settlement-ready\n").unwrap();

    waitid(
        WaitId::Pid(descendant_pid),
        WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
    )
    .unwrap();
    let descendant_ready = std::env::var_os("PI_FIXTURE_DESCENDANT_READY").unwrap();
    fs::write(descendant_ready, b"descendant-zombie\n").unwrap();
    let release = std::env::var_os("PI_FIXTURE_RESULT_SETTLEMENT_RELEASE").unwrap();
    assert!(!fs::read(release).unwrap().is_empty());
    assert!(!descendant.wait().unwrap().success());
}

#[test]
#[ignore = "launched as the in-group descendant by the process-group regression"]
fn in_group_descendant_process() {
    loop {
        std::thread::park();
    }
}

#[test]
fn fake_fixture_uses_no_ambient_pi() {
    let fixture = ProcessFixture::new("success", "system".to_owned(), "message".to_owned());
    assert_ne!(fixture.invocation.adapter().executable(), Path::new("pi"));
    assert!(fixture.invocation.adapter().executable().is_absolute());
    assert_eq!(
        fixture.invocation.adapter().executable(),
        fixture._temporary.path().join("pi-0.83.0-fake")
    );
    assert_eq!(
        fixture
            .invocation
            .process()
            .environment()
            .variable(OsStr::new("PI_MODEL")),
        None
    );
}
