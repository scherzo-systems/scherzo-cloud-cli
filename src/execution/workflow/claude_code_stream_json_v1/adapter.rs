use std::ffi::OsString;
use std::io::{self, Write as _};
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::CommandExt as _;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;

use rustix::process::Pid;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::process::{Child, ChildStdout, Command};

use super::{
    ClaudeCodeStreamJsonV1Parser, ClaudeCodeStreamJsonV1ProtocolLimits,
    FIXED_INVOCATION_ENVIRONMENT, initial_user_text_frame, normal_mode_arguments,
};
use crate::execution::claude_code::CLAUDE_CODE_STREAM_JSON_V1_VERSION;
use crate::execution::workflow::agent::{
    AgentAdapter, AgentCompatibilityProfile, AgentFailureCause, AgentInputKind, AgentInvocation,
    AgentLifecycleMilestone, AgentObservation, AgentObservationSink, AgentOutcome,
    AgentStartCallback, AgentTerminalCallback, AgentValueKind, check_agent_input_bound,
    failed_agent_outcome,
};
use crate::execution::workflow::child_guard::force_stop_direct_child;
use crate::execution::workflow::claude_code::ClaudeCodeConfig;
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::observation::ExecutionObserver;
use crate::execution::workflow::process_group::terminate_process_group;

const SYSTEM_PROMPT_FILE_PREFIX: &str = "claude-code-system-prompt-";
const READ_BUFFER_BYTES: usize = 8 * 1024;

#[derive(Clone)]
pub(crate) struct ClaudeCodeStreamJsonV1Adapter<Clock, Observer> {
    diagnostics: StepDiagnosticLog,
    maximum_diagnostic_stream_bytes: NonZeroU64,
    clock: Clock,
    observer: Observer,
}

impl<Clock, Observer> ClaudeCodeStreamJsonV1Adapter<Clock, Observer> {
    pub(crate) fn new(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
    ) -> Self {
        Self {
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
        }
    }
}

impl<Clock, Observer, Sink> AgentAdapter<Sink> for ClaudeCodeStreamJsonV1Adapter<Clock, Observer>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Sink: AgentObservationSink,
{
    type NativeConfiguration = ClaudeCodeConfig;
    type ProtocolLimits = ClaudeCodeStreamJsonV1ProtocolLimits;

    async fn invoke(
        &self,
        invocation: AgentInvocation<Self::NativeConfiguration, Self::ProtocolLimits, Sink>,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) {
        let cancellation = invocation.cancellation().clone();
        let outcome = self.invoke_inner(invocation, &started).await;
        let outcome = cancellation
            .cancellation_reason()
            .map_or(outcome, |reason| AgentOutcome::Cancelled { reason });
        let _ = terminal.report(outcome);
    }
}

impl<Clock, Observer> ClaudeCodeStreamJsonV1Adapter<Clock, Observer>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
{
    async fn invoke_inner<Sink>(
        &self,
        mut invocation: AgentInvocation<
            ClaudeCodeConfig,
            ClaudeCodeStreamJsonV1ProtocolLimits,
            Sink,
        >,
        started: &AgentStartCallback,
    ) -> AgentOutcome
    where
        Sink: AgentObservationSink,
    {
        // Claude and Pi intentionally own separate native startup transitions; sharing
        // this gate would couple init acknowledgement to Pi's agent-start protocol.
        // jscpd:ignore-start
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }
        let plan = match prepare_launch(&invocation) {
            Ok(plan) => plan,
            Err(cause) => return failed_agent_outcome(cause),
        };
        let Some(process_directives) = invocation.take_process_directives() else {
            return failed_agent_outcome(AgentFailureCause::HarnessStartFailed);
        };
        drop(process_directives);
        // jscpd:ignore-end

        // Diagnostic drain is tied to this profile's process launch and parser lifetime,
        // rather than Pi's result bridge and settlement supervisor.
        // jscpd:ignore-start
        let (process, standard_error) = match launch_process(&invocation, &plan).await {
            Ok(process) => process,
            Err(cause) => return failed_agent_outcome(cause),
        };
        let diagnostic = self.diagnostics.start_standard_error_capture(
            invocation.identity().step().to_owned(),
            invocation.identity().invocation(),
            self.maximum_diagnostic_stream_bytes,
            standard_error,
            self.observer.clone(),
        );
        // jscpd:ignore-end
        let parser = ClaudeCodeStreamJsonV1Parser::profile(
            Arc::clone(&plan.expected_cwd),
            Arc::from(invocation.adapter().native_configuration().model.as_str()),
            invocation.value_mode().kind(),
            invocation.limits().maximum_response_bytes(),
        );
        let outcome = drive_process(&invocation, started, process, parser, &plan.input).await;
        diagnostic.finish().await;
        outcome
    }
}

pub(super) struct ClaudeCodeStreamJsonV1LaunchPlan {
    arguments: Vec<OsString>,
    expected_cwd: Arc<str>,
    input: Vec<u8>,
    _system_prompt_file: tempfile::NamedTempFile,
}

impl ClaudeCodeStreamJsonV1LaunchPlan {
    #[cfg(test)]
    pub(super) fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    #[cfg(test)]
    pub(super) fn input(&self) -> &[u8] {
        &self.input
    }

    #[cfg(test)]
    pub(super) fn system_prompt_file(&self) -> &std::path::Path {
        self._system_prompt_file.path()
    }
}

pub(super) fn prepare_launch<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
) -> Result<ClaudeCodeStreamJsonV1LaunchPlan, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    check_agent_input_bound(
        invocation.prompt().system_prompt(),
        invocation.limits().maximum_system_prompt_bytes(),
        AgentInputKind::SystemPrompt,
    )?;
    check_agent_input_bound(
        invocation.prompt().message(),
        invocation.limits().maximum_message_bytes(),
        AgentInputKind::Message,
    )?;
    if invocation.adapter().profile() != AgentCompatibilityProfile::ClaudeCodeStreamJsonV1
        || invocation.adapter().version() != CLAUDE_CODE_STREAM_JSON_V1_VERSION
        || !invocation.adapter().executable().is_absolute()
        || invocation
            .diagnostic_session()
            .verify_path_binding()
            .is_err()
        || !invocation.attachments().is_empty()
        || invocation.value_mode().kind() == AgentValueKind::Result
    {
        return Err(AgentFailureCause::HarnessStartFailed);
    }

    // Claude correlates this path in system/init, independently from Pi's session header.
    // jscpd:ignore-start
    let expected_cwd = invocation
        .process()
        .protocol_cwd()
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let expected_cwd = expected_cwd
        .to_str()
        .map(Arc::from)
        .ok_or(AgentFailureCause::HarnessStartFailed)?;
    // jscpd:ignore-end
    let mut system_prompt_file = tempfile::Builder::new()
        .prefix(SYSTEM_PROMPT_FILE_PREFIX)
        .tempfile_in(invocation.staging().result_endpoint_directory())
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    system_prompt_file
        .write_all(invocation.prompt().system_prompt().as_bytes())
        .and_then(|()| system_prompt_file.flush())
        .and_then(|()| {
            system_prompt_file
                .as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o400))
        })
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;

    let configuration = invocation.adapter().native_configuration();
    let arguments = normal_mode_arguments(
        &configuration.model,
        configuration.effort.as_str(),
        system_prompt_file.path(),
    );
    let input = initial_user_text_frame(invocation.prompt().message())
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    Ok(ClaudeCodeStreamJsonV1LaunchPlan {
        arguments,
        expected_cwd,
        input,
        _system_prompt_file: system_prompt_file,
    })
}

pub(super) fn build_command<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    plan: &ClaudeCodeStreamJsonV1LaunchPlan,
) -> Result<Command, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    invocation
        .diagnostic_session()
        .verify_path_binding()
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let mut command = Command::new(invocation.adapter().executable());
    command
        .args(&plan.arguments)
        .env_clear()
        .envs(invocation.process().environment().variables())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in FIXED_INVOCATION_ENVIRONMENT {
        command.env(name, value);
    }
    command.as_std_mut().process_group(0);
    invocation
        .process()
        .bind_command(command.as_std_mut())
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    Ok(command)
}

struct LaunchedClaudeCodeProcess {
    child: Child,
    process_group: Pid,
    standard_input: tokio::process::ChildStdin,
    standard_output: ChildStdout,
}

async fn launch_process<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    plan: &ClaudeCodeStreamJsonV1LaunchPlan,
) -> Result<(LaunchedClaudeCodeProcess, tokio::process::ChildStderr), AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    // Claude needs piped stream-JSON input while Pi uses a null stdin and a different
    // guarded launch lifecycle, so keeping this launch extraction local is clearer.
    // jscpd:ignore-start
    let mut command = build_command(invocation, plan)?;
    let mut child = command
        .spawn()
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let Some(process_group) = child
        .id()
        .and_then(|process_id| i32::try_from(process_id).ok())
        .and_then(Pid::from_raw)
    else {
        stop_child(&mut child, None).await;
        return Err(AgentFailureCause::HarnessStartFailed);
    };
    let (Some(standard_input), Some(standard_output), Some(standard_error)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        stop_child(&mut child, Some(process_group)).await;
        return Err(AgentFailureCause::HarnessStartFailed);
    };
    // jscpd:ignore-end
    Ok((
        LaunchedClaudeCodeProcess {
            child,
            process_group,
            standard_input,
            standard_output,
        },
        standard_error,
    ))
}

async fn drive_process<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    started: &AgentStartCallback,
    process: LaunchedClaudeCodeProcess,
    mut parser: ClaudeCodeStreamJsonV1Parser,
    input: &[u8],
) -> AgentOutcome
where
    Sink: AgentObservationSink,
{
    let LaunchedClaudeCodeProcess {
        mut child,
        process_group,
        mut standard_input,
        mut standard_output,
    } = process;
    if standard_input.write_all(input).await.is_err() || standard_input.shutdown().await.is_err() {
        stop_child(&mut child, Some(process_group)).await;
        return failed_agent_outcome(AgentFailureCause::HarnessStartFailed);
    }
    drop(standard_input);

    // The loop shape is intentionally local: Claude's result ends an exchange while Pi's
    // native terminal and result-bridge settlement boundaries are different.
    // jscpd:ignore-start
    let mut buffer = [0_u8; READ_BUFFER_BYTES];
    let mut standard_output_closed = false;
    let mut process_completion: Option<ExitStatus> = None;
    let mut parser_enabled = true;
    let mut failure = None;
    while !standard_output_closed || process_completion.is_none() {
        tokio::select! {
            read = standard_output.read(&mut buffer), if !standard_output_closed => {
                match read {
                    Ok(0) => standard_output_closed = true,
                    // jscpd:ignore-end
                    // Claude reports start from system/init and emits observations only
                    // after one complete native frame validates.
                    // jscpd:ignore-start
                    Ok(read) if parser_enabled => {
                        let mut observations = Vec::new();
                        let parsed = parser.push_stdout(&buffer[..read], |observation| {
                            observations.push(observation);
                        });
                        for observation in observations {
                            let reports_start = matches!(
                                &observation,
                                AgentObservation::Lifecycle {
                                    milestone: AgentLifecycleMilestone::HarnessStarted,
                                }
                            );
                            if reports_start && started.report().is_err() {
                                failure = Some(AgentFailureCause::HarnessProtocolFailed);
                                parser_enabled = false;
                                terminate_process_group(process_group);
                                let _ = child.start_kill();
                                break;
                            }
                            if invocation.observations().emit(observation).await.is_err() {
                                failure = Some(AgentFailureCause::HarnessProtocolFailed);
                                parser_enabled = false;
                                terminate_process_group(process_group);
                                let _ = child.start_kill();
                                break;
                            }
                        }
                        if parser_enabled
                            && let Err(cause) = parsed
                        {
                            failure = Some(cause);
                            parser_enabled = false;
                            terminate_process_group(process_group);
                            let _ = child.start_kill();
                        }
                    }
                    // jscpd:ignore-end
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        standard_output_closed = true;
                        if parser_enabled {
                            failure = Some(parser.protocol_failure());
                            parser_enabled = false;
                            terminate_process_group(process_group);
                            let _ = child.start_kill();
                        }
                    }
                }
            }
            waited = child.wait(), if process_completion.is_none() => {
                match waited {
                    Ok(status) => process_completion = Some(status),
                    Err(_) => {
                        failure.get_or_insert(AgentFailureCause::HarnessProtocolFailed);
                        terminate_process_group(process_group);
                        let _ = child.start_kill();
                        process_completion = child.wait().await.ok();
                    }
                }
            }
        }
    }

    if let Some(cause) = failure {
        return failed_agent_outcome(cause);
    }
    let Some(status) = process_completion else {
        return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
    };
    parser.finish(status.success())
}

async fn stop_child(child: &mut Child, process_group: Option<Pid>) {
    if let Some(process_group) = process_group {
        terminate_process_group(process_group);
    }
    let _ = force_stop_direct_child(child).await;
}
