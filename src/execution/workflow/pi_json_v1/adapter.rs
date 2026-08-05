use std::ffi::OsString;
use std::fs;
use std::future::{Future, pending};
use std::io;
use std::num::NonZeroU64;
use std::ops::Add as _;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;

use rustix::process::Pid;
use tokio::io::AsyncReadExt as _;
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};

use super::result_bridge::{
    IncomingResultRequest, PreparedResultBridge, ResultSocketEvent, ValidatePiResultV1Response,
};
use super::{
    AcceptedPiJsonV1Result, PiJsonV1Parser, PiJsonV1ProcessCompletion, PiJsonV1ProtocolLimits,
};
use crate::execution::pi::{PiCompatibilityProfile, compatibility_profile_for_version};
use crate::execution::workflow::admission::{CancellationReason, CancellationSource};
use crate::execution::workflow::agent::{
    AgentAdapter, AgentCompatibilityProfile, AgentFailureCause, AgentInputKind, AgentInvocation,
    AgentLifecycleMilestone, AgentObservation, AgentObservationSink, AgentOutcome,
    AgentProcessDirective, AgentStartCallback, AgentTerminalCallback, AgentValueKind,
    AgentValueMode, PositiveDuration,
};
use crate::execution::workflow::child_guard::{StoppedChildGuard, force_stop_direct_child};
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::observation::{ExecutionObserver, NoopExecutionObserver};
use crate::execution::workflow::pi::PiConfig;
use crate::execution::workflow::process_group::{
    ProcessGuardRegistration, interrupt_process_group, process_group_is_quiescent,
    reap_process_group_children, terminate_authenticated_process_group, terminate_process_group,
};
use crate::execution::workflow::result_validation::{
    AuthoritativeResultValidator, ProcessResultValidationWorker, ResultValidationDecision,
    ResultValidationOutcome, ResultValidationWorker,
};

pub(crate) struct PiJsonV1Adapter<
    Clock,
    Observer = NoopExecutionObserver,
    Worker = ProcessResultValidationWorker,
> {
    diagnostics: StepDiagnosticLog,
    maximum_diagnostic_stream_bytes: NonZeroU64,
    clock: Clock,
    observer: Observer,
    validation_worker: Worker,
}

impl<Clock, Observer> PiJsonV1Adapter<Clock, Observer, ProcessResultValidationWorker> {
    pub(crate) fn new(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
    ) -> io::Result<Self> {
        Ok(Self {
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            validation_worker: ProcessResultValidationWorker::for_current_executable()?,
        })
    }
}

impl<Clock, Observer, Worker> PiJsonV1Adapter<Clock, Observer, Worker> {
    #[cfg(test)]
    pub(super) fn with_validation_worker(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
        validation_worker: Worker,
    ) -> Self {
        Self {
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            validation_worker,
        }
    }
}

impl<Clock, Observer, Worker> Clone for PiJsonV1Adapter<Clock, Observer, Worker>
where
    Clock: Clone,
    Observer: Clone,
    Worker: Clone,
{
    fn clone(&self) -> Self {
        Self {
            diagnostics: self.diagnostics.clone(),
            maximum_diagnostic_stream_bytes: self.maximum_diagnostic_stream_bytes,
            clock: self.clock.clone(),
            observer: self.observer.clone(),
            validation_worker: self.validation_worker.clone(),
        }
    }
}

// The trait boilerplate matches the scripted adapter, but each adapter owns a
// distinct lifecycle and keeping their invocation logic separate avoids coupling them.
// jscpd:ignore-start
impl<Clock, Observer, Worker, Sink> AgentAdapter<Sink> for PiJsonV1Adapter<Clock, Observer, Worker>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type NativeConfiguration = PiConfig;
    type ProtocolLimits = PiJsonV1ProtocolLimits;

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
// jscpd:ignore-end

impl<Clock, Observer, Worker> PiJsonV1Adapter<Clock, Observer, Worker>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
{
    async fn invoke_inner<Sink>(
        &self,
        mut invocation: AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
        started: &AgentStartCallback,
    ) -> AgentOutcome
    where
        Sink: AgentObservationSink,
    {
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }

        let mut plan = match prepare_launch(&invocation) {
            Ok(plan) => plan,
            Err(cause) => return failed(cause),
        };
        let mut result_bridge = match self.prepare_result_bridge(&invocation) {
            Ok(result_bridge) => result_bridge,
            Err(cause) => return failed(cause),
        };
        if let Some(result_bridge) = result_bridge.as_ref() {
            plan.add_result_extension(result_bridge.bridge.extension_path());
        }
        let Some(process_directives) = invocation.take_process_directives() else {
            let _ = shutdown_result_bridge(result_bridge).await;
            return failed(AgentFailureCause::HarnessStartFailed);
        };
        let (process, standard_error) = match launch_process(&invocation, &plan).await {
            Ok(launched) => launched,
            Err(cause) => {
                let _ = shutdown_result_bridge(result_bridge).await;
                return failed(cause);
            }
        };
        let diagnostic = self.diagnostics.start_standard_error_capture(
            invocation.identity().step().to_owned(),
            invocation.identity().invocation(),
            self.maximum_diagnostic_stream_bytes,
            standard_error,
            self.observer.clone(),
        );
        let expected_result_tool_name = result_bridge
            .as_ref()
            .map(|result_bridge| Arc::clone(result_bridge.bridge.tool_name()));
        let parser = PiJsonV1Parser::new(
            Arc::clone(&plan.expected_cwd),
            invocation.value_mode().kind(),
            invocation.limits().maximum_response_bytes(),
            *invocation.limits().adapter_protocol(),
            expected_result_tool_name,
        );
        let outcome = drive_process(
            &invocation,
            started,
            process,
            parser,
            process_directives,
            &mut result_bridge,
            ResultSettlementConfiguration {
                clock: self.clock.clone(),
                grace: invocation.limits().result_settlement_grace(),
            },
        )
        .await;
        let bridge_shutdown = shutdown_result_bridge(result_bridge).await;
        if matches!(
            &outcome,
            AgentOutcome::Failed {
                cause: AgentFailureCause::ResultSettlementFailed,
            }
        ) {
            diagnostic.abort();
        }
        diagnostic.finish().await;
        if bridge_shutdown.is_err() && matches!(outcome, AgentOutcome::Completed(_)) {
            failed(AgentFailureCause::HarnessProtocolFailed)
        } else {
            outcome
        }
    }

    fn prepare_result_bridge<Sink>(
        &self,
        invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    ) -> Result<Option<ActiveResultBridge<Clock, Worker>>, AgentFailureCause>
    where
        Sink: AgentObservationSink,
    {
        let AgentValueMode::Result { schema, .. } = invocation.value_mode() else {
            return Ok(None);
        };
        let bridge = PreparedResultBridge::prepare(
            invocation.identity(),
            invocation.staging().result_endpoint_directory(),
            schema,
            *invocation.limits().adapter_protocol(),
            invocation.limits().result_validation_deadline(),
            self.clock.clone(),
        )
        .map_err(|()| AgentFailureCause::HarnessStartFailed)?;
        let validator = AuthoritativeResultValidator::new(
            schema.clone(),
            invocation.limits().maximum_result_bytes(),
            invocation
                .limits()
                .maximum_result_rejection_feedback_bytes(),
            invocation.limits().result_validation_deadline(),
            self.clock.clone(),
            self.validation_worker.clone(),
        );
        Ok(Some(ActiveResultBridge { bridge, validator }))
    }
}

struct ActiveResultBridge<Clock, Worker> {
    bridge: PreparedResultBridge,
    validator: AuthoritativeResultValidator<Clock, Worker>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct PiJsonV1LaunchPlan {
    arguments: Vec<OsString>,
    expected_cwd: Arc<str>,
}

impl PiJsonV1LaunchPlan {
    fn add_result_extension(&mut self, extension_path: &std::path::Path) {
        const EXTENSION_ARGUMENT_INDEX: usize = 10;
        self.arguments.splice(
            EXTENSION_ARGUMENT_INDEX..EXTENSION_ARGUMENT_INDEX,
            [
                OsString::from("--extension"),
                extension_path.as_os_str().to_owned(),
            ],
        );
    }

    #[cfg(test)]
    pub(super) fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
}

pub(super) fn prepare_launch<Sink>(
    invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
) -> Result<PiJsonV1LaunchPlan, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    check_input_bound(
        invocation.prompt().message(),
        invocation.limits().maximum_message_bytes(),
        AgentInputKind::Message,
    )?;

    if invocation.adapter().profile() != AgentCompatibilityProfile::PiJsonV1
        || compatibility_profile_for_version(invocation.adapter().version())
            != Some(PiCompatibilityProfile::PiJsonV1)
        || !invocation.adapter().executable().is_absolute()
    {
        return Err(AgentFailureCause::HarnessStartFailed);
    }
    let expected_cwd = invocation
        .process()
        .protocol_cwd()
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let system_prompt = combined_system_prompt(
        &expected_cwd,
        invocation.prompt().system_prompt(),
        invocation.limits().maximum_system_prompt_bytes(),
    )?;
    let expected_cwd = expected_cwd
        .to_str()
        .map(Arc::from)
        .ok_or(AgentFailureCause::HarnessStartFailed)?;
    let config = invocation.adapter().native_configuration();
    let mut arguments = Vec::with_capacity(13_usize.saturating_add(invocation.attachments().len()));
    arguments.extend([
        OsString::from("--mode"),
        OsString::from("json"),
        OsString::from("--approve"),
        OsString::from("--no-session"),
        OsString::from("--model"),
        OsString::from(&config.model),
        OsString::from("--thinking"),
        OsString::from(config.thinking.as_str()),
        OsString::from("--append-system-prompt"),
        OsString::from(system_prompt),
    ]);
    for attachment in invocation.attachments() {
        let mut argument = OsString::from("@");
        argument.push(attachment.path());
        arguments.push(argument);
    }
    let mut message = OsString::from("\n");
    message.push(invocation.prompt().message());
    arguments.push(message);

    Ok(PiJsonV1LaunchPlan {
        arguments,
        expected_cwd,
    })
}

pub(super) fn build_command<Sink>(
    invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    plan: &PiJsonV1LaunchPlan,
) -> Result<Command, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    let mut command = Command::new(invocation.adapter().executable());
    command
        .args(&plan.arguments)
        .env_clear()
        .envs(invocation.process().environment().variables())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.as_std_mut().process_group(0);
    invocation
        .process()
        .bind_command(command.as_std_mut())
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    Ok(command)
}

async fn launch_process<Sink>(
    invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    plan: &PiJsonV1LaunchPlan,
) -> Result<(LaunchedPiProcess, ChildStderr), AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    if !invocation.process_guards().is_durable() {
        return launch_direct_process(invocation, plan).await;
    }

    let environment = invocation
        .process()
        .environment()
        .variables()
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    let mut child = StoppedChildGuard::spawn(
        invocation.adapter().executable(),
        &plan.arguments,
        &environment,
        |command| {
            invocation
                .process()
                .bind_command(command)
                .map_err(|_| io::Error::other("agent working directory is unavailable"))
        },
    )
    .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let process_group = child.identity().process_group();
    let (Some(standard_output), Some(standard_error)) = (child.take_stdout(), child.take_stderr())
    else {
        let _ = child.force_stop().await;
        return Err(AgentFailureCause::HarnessStartFailed);
    };
    let mut registration = match invocation.process_guards().register(
        invocation.identity().step(),
        invocation.identity().invocation().transition_sequence.get(),
        child.identity(),
    ) {
        Ok(registration) => registration,
        Err(()) => {
            let _ = child.force_stop().await;
            return Err(AgentFailureCause::HarnessStartFailed);
        }
    };
    if child.continue_execution().is_err() || registration.mark_released().is_err() {
        let _ = child.force_stop().await;
        let _ = registration.mark_quiesced();
        return Err(AgentFailureCause::HarnessStartFailed);
    }

    Ok((
        LaunchedPiProcess {
            child: PiChild::Guarded {
                child,
                registration,
            },
            process_group,
            standard_output,
        },
        standard_error,
    ))
}

async fn launch_direct_process<Sink>(
    invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    plan: &PiJsonV1LaunchPlan,
) -> Result<(LaunchedPiProcess, ChildStderr), AgentFailureCause>
where
    Sink: AgentObservationSink,
{
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
    let (Some(standard_output), Some(standard_error)) = (child.stdout.take(), child.stderr.take())
    else {
        stop_child(&mut child, Some(process_group)).await;
        return Err(AgentFailureCause::HarnessStartFailed);
    };
    Ok((
        LaunchedPiProcess {
            child: PiChild::Direct(child),
            process_group,
            standard_output,
        },
        standard_error,
    ))
}

// Pi 0.83 treats the CLI append value as a replacement for the trusted
// project's APPEND_SYSTEM.md, so preserve both inputs in the one native flag.
fn combined_system_prompt(
    working_directory: &Path,
    workflow_prompt: &str,
    maximum_bytes: NonZeroU64,
) -> Result<String, AgentFailureCause> {
    let project_prompt = match fs::read_to_string(working_directory.join(".pi/APPEND_SYSTEM.md")) {
        Ok(prompt) => Some(prompt),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(_) => return Err(AgentFailureCause::HarnessStartFailed),
    };
    let combined = project_prompt.map_or_else(
        || workflow_prompt.to_owned(),
        |project_prompt| format!("{project_prompt}\n\n{workflow_prompt}"),
    );
    check_input_bound(&combined, maximum_bytes, AgentInputKind::SystemPrompt)?;
    Ok(combined)
}

fn check_input_bound(
    value: &str,
    admitted_bytes: NonZeroU64,
    input: AgentInputKind,
) -> Result<(), AgentFailureCause> {
    let observed_bytes = u64::try_from(value.len()).unwrap_or(u64::MAX);
    if observed_bytes > admitted_bytes.get() {
        return Err(AgentFailureCause::HarnessInputTooLarge {
            input,
            admitted_bytes,
            observed_bytes,
        });
    }
    Ok(())
}

struct LaunchedPiProcess {
    child: PiChild,
    process_group: Pid,
    standard_output: ChildStdout,
}

enum PiChild {
    Guarded {
        child: StoppedChildGuard,
        registration: ProcessGuardRegistration,
    },
    Direct(Child),
}

impl PiChild {
    fn force_process_group(&self, process_group: Pid) {
        match self {
            Self::Guarded { child, .. } => {
                let _ = terminate_authenticated_process_group(child.identity());
            }
            Self::Direct(_) => terminate_process_group(process_group),
        }
    }

    async fn wait(&mut self) -> Result<ExitStatus, ()> {
        let status = match self {
            Self::Guarded { child, .. } => child.wait().await.map_err(|_| ()),
            Self::Direct(child) => child.wait().await.map_err(|_| ()),
        }?;
        self.mark_quiesced()?;
        Ok(status)
    }

    async fn force_stop(&mut self, process_group: Pid) -> Result<(), ()> {
        self.force_process_group(process_group);
        match self {
            Self::Guarded { child, .. } => child.force_stop().await.map_err(|_| ())?,
            Self::Direct(child) => force_stop_direct_child(child).await?,
        }
        self.mark_quiesced()
    }

    fn mark_quiesced(&mut self) -> Result<(), ()> {
        match self {
            Self::Guarded { registration, .. } => registration.mark_quiesced(),
            Self::Direct(_) => Ok(()),
        }
    }
}

const PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(10);

struct ResultSettlementConfiguration<Clock> {
    clock: Clock,
    grace: PositiveDuration,
}

async fn drive_process<Clock, Worker, Sink>(
    invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    started: &AgentStartCallback,
    process: LaunchedPiProcess,
    mut parser: PiJsonV1Parser,
    process_directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    result_bridge: &mut Option<ActiveResultBridge<Clock, Worker>>,
    settlement: ResultSettlementConfiguration<Clock>,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    let LaunchedPiProcess {
        mut child,
        process_group,
        mut standard_output,
    } = process;
    let cancellation_source = invocation.cancellation().clone();
    let cancellation = cancellation_source.wait_for_cancellation();
    tokio::pin!(cancellation);
    let (stop_supervisor, supervisor_shutdown) = oneshot::channel();
    let (begin_settlement, settlement_starts) = mpsc::unbounded_channel();
    let (settlement_outcomes, mut settlement_outcome) = mpsc::unbounded_channel();
    let mut process_group_probe_clock = settlement.clock.clone();
    let process_supervisor = tokio::spawn(supervise_process_group(
        process_group,
        cancellation_source.clone(),
        process_directives,
        supervisor_shutdown,
        settlement,
        settlement_starts,
        settlement_outcomes,
    ));
    let mut buffer = [0_u8; 8 * 1024];
    let mut standard_output_closed = false;
    let mut process_completion = None;
    let mut process_group_quiescent = false;
    let mut process_group_termination_requested = false;
    let mut settlement_admitted = false;
    let mut settlement_active = false;
    let mut parser_enabled = true;
    let mut failure = None;
    let mut cancelled = None;
    let mut wait_failed = false;

    while !standard_output_closed
        || (process_completion.is_none() && !wait_failed)
        || !process_group_quiescent
    {
        tokio::select! {
            biased;
            reason = &mut cancellation, if cancelled.is_none() => {
                cancelled = Some(reason);
                parser_enabled = false;
            }
            read = standard_output.read(&mut buffer), if !standard_output_closed => {
                match read {
                    Ok(0) => standard_output_closed = true,
                    Ok(read) if parser_enabled => {
                        let mut observations = Vec::new();
                        let parsed = parser.push_stdout(&buffer[..read], |observation| {
                            observations.push(observation);
                        });
                        if parsed.is_ok()
                            && parser.accepted_result_ready_for_settlement()
                            && !settlement_admitted
                        {
                            if begin_settlement.send(()).is_err() {
                                failure = Some(AgentFailureCause::HarnessProtocolFailed);
                                parser_enabled = false;
                                child.force_process_group(process_group);
                            } else {
                                settlement_admitted = true;
                                settlement_active = true;
                            }
                        }
                        if parser_enabled {
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
                                    child.force_process_group(process_group);
                                    break;
                                }
                                let emitted = invocation.observations().emit(observation).await;
                                if let Some(reason) = cancellation_source.cancellation_reason() {
                                    cancelled = Some(reason);
                                    parser_enabled = false;
                                    break;
                                }
                                if emitted.is_err() {
                                    failure = Some(AgentFailureCause::HarnessProtocolFailed);
                                    parser_enabled = false;
                                    child.force_process_group(process_group);
                                    break;
                                }
                            }
                        }
                        if parser_enabled
                            && let Err(cause) = parsed
                        {
                            failure = Some(cause);
                            parser_enabled = false;
                            child.force_process_group(process_group);
                        }
                    }
                    Ok(_) => {}
                    Err(_) => {
                        standard_output_closed = true;
                        if cancelled.is_none() {
                            if let Some(reason) = cancellation_source.cancellation_reason() {
                                cancelled = Some(reason);
                            } else {
                                failure = Some(parser.failure_for_current_phase());
                                child.force_process_group(process_group);
                            }
                            parser_enabled = false;
                        }
                    }
                }
            }
            event = receive_result_event(result_bridge), if parser_enabled => {
                match handle_result_event(
                    event,
                    result_bridge,
                    &mut parser,
                    &cancellation_source,
                ).await {
                    ResultEventProgress::Continue { observation } => {
                        if let Some(observation) = observation
                        {
                            let emitted = invocation.observations().emit(observation).await;
                            if let Some(reason) = cancellation_source.cancellation_reason() {
                                cancelled = Some(reason);
                                parser_enabled = false;
                            } else if emitted.is_err() {
                                failure = Some(AgentFailureCause::HarnessProtocolFailed);
                                parser_enabled = false;
                                child.force_process_group(process_group);
                            }
                        }
                    }
                    ResultEventProgress::Failed(cause) => {
                        failure = Some(cause);
                        parser_enabled = false;
                        child.force_process_group(process_group);
                    }
                    ResultEventProgress::Cancelled(reason) => {
                        cancelled = Some(reason);
                        parser_enabled = false;
                    }
                }
            }
            outcome = settlement_outcome.recv(), if parser_enabled && settlement_active => {
                settlement_active = false;
                match outcome {
                    Some(SettlementDeadlineOutcome::Expired) | None => {
                        standard_output_closed = true;
                        failure = Some(AgentFailureCause::ResultSettlementFailed);
                        parser_enabled = false;
                        child.force_process_group(process_group);
                    }
                }
            }
            waited = child.wait(), if process_completion.is_none() && !wait_failed => {
                match waited {
                    Ok(status) => process_completion = Some(status),
                    Err(_) => {
                        wait_failed = true;
                        parser_enabled = false;
                        if cancelled.is_none() {
                            failure = Some(parser.failure_for_current_phase());
                        }
                        child.force_process_group(process_group);
                    }
                }
            }
            () = wait_for_process_group_probe(&mut process_group_probe_clock),
                if process_group_termination_requested => {}
        }

        if standard_output_closed
            && (process_completion.is_some() || wait_failed)
            && !process_group_quiescent
        {
            reap_process_group_children(process_group);
            if process_group_is_quiescent(process_group) {
                process_group_quiescent = true;
            } else if (!settlement_active || !parser_enabled)
                && !process_group_termination_requested
            {
                child.force_process_group(process_group);
                process_group_termination_requested = true;
            }
        }
    }

    if cancelled.is_none() {
        cancelled = cancellation_source.cancellation_reason();
    }
    if !process_group_quiescent {
        child.force_process_group(process_group);
    }
    if process_completion.is_none() {
        process_completion = child.wait().await.ok();
        if process_completion.is_none() {
            let _ = child.force_stop(process_group).await;
        }
    }
    let _ = stop_supervisor.send(());
    let _ = process_supervisor.await;

    if let Some(reason) = cancelled {
        return parser.finish(PiJsonV1ProcessCompletion::cancelled(
            process_completion.is_some_and(|status| status.success()),
            reason,
        ));
    }
    if let Some(cause) = failure {
        return failed(cause);
    }
    if wait_failed {
        return failed(parser.failure_for_current_phase());
    }
    let Some(status) = process_completion else {
        return failed(parser.failure_for_current_phase());
    };
    parser.finish(PiJsonV1ProcessCompletion::exited(status.success()))
}

async fn wait_for_process_group_probe<Clock: CoordinatorClock>(clock: &mut Clock) {
    let deadline = clock.now().add(PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL);
    clock.clone().wait_until(deadline).await;
}

enum ResultEventProgress {
    Continue {
        observation: Option<AgentObservation>,
    },
    Failed(AgentFailureCause),
    Cancelled(CancellationReason),
}

async fn receive_result_event<Clock, Worker>(
    result_bridge: &mut Option<ActiveResultBridge<Clock, Worker>>,
) -> ResultSocketEvent {
    match result_bridge {
        Some(result_bridge) => result_bridge.bridge.receive().await,
        None => pending().await,
    }
}

async fn handle_result_event<Clock, Worker>(
    event: ResultSocketEvent,
    result_bridge: &mut Option<ActiveResultBridge<Clock, Worker>>,
    parser: &mut PiJsonV1Parser,
    cancellation: &CancellationSource,
) -> ResultEventProgress
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    let ResultSocketEvent::Request(incoming) = event else {
        return ResultEventProgress::Failed(parser.failure_for_current_phase());
    };
    let Some(result_bridge) = result_bridge.as_mut() else {
        return ResultEventProgress::Failed(parser.failure_for_current_phase());
    };

    let request = incoming.request();
    let Some(candidate) = request.candidate().cloned() else {
        return fail_correlated_request(incoming, parser, cancellation).await;
    };
    if request.tool_name() != result_bridge.bridge.tool_name().as_ref()
        || parser
            .correlate_result_request(
                request.tool_name(),
                request.tool_call_id(),
                request.arguments(),
            )
            .is_err()
    {
        return fail_correlated_request(incoming, parser, cancellation).await;
    }

    let call_id = Arc::<str>::from(request.tool_call_id());
    let tool_name = Arc::<str>::from(request.tool_name());
    let arguments = Arc::new(request.arguments().clone());
    match result_bridge
        .validator
        .validate(Arc::new(candidate), cancellation)
        .await
    {
        ResultValidationOutcome::Cancelled { reason } => ResultEventProgress::Cancelled(reason),
        ResultValidationOutcome::Decided(ResultValidationDecision::Rejected { feedback }) => {
            match respond_with_cancellation(
                incoming,
                ValidatePiResultV1Response::rejected(&feedback),
                cancellation,
            )
            .await
            {
                ResponseProgress::Delivered => ResultEventProgress::Continue {
                    observation: Some(AgentObservation::ValueRejected {
                        kind: AgentValueKind::Result,
                        feedback,
                    }),
                },
                ResponseProgress::Failed => {
                    ResultEventProgress::Failed(parser.failure_for_current_phase())
                }
                ResponseProgress::Cancelled(reason) => ResultEventProgress::Cancelled(reason),
            }
        }
        ResultValidationOutcome::Decided(ResultValidationDecision::Fatal(fatal)) => {
            let cause = AgentFailureCause::from(fatal);
            match respond_with_cancellation(
                incoming,
                ValidatePiResultV1Response::fatal("Result validation could not continue."),
                cancellation,
            )
            .await
            {
                ResponseProgress::Delivered | ResponseProgress::Failed => {
                    ResultEventProgress::Failed(cause)
                }
                ResponseProgress::Cancelled(reason) => ResultEventProgress::Cancelled(reason),
            }
        }
        ResultValidationOutcome::Decided(ResultValidationDecision::Valid(result)) => {
            if let Err(cause) = parser.accept_result(AcceptedPiJsonV1Result::new(
                call_id, tool_name, arguments, result,
            )) {
                return fail_request_with_cause(incoming, cause, cancellation).await;
            }
            match respond_with_cancellation(
                incoming,
                ValidatePiResultV1Response::valid(),
                cancellation,
            )
            .await
            {
                ResponseProgress::Delivered => ResultEventProgress::Continue { observation: None },
                ResponseProgress::Failed => {
                    ResultEventProgress::Failed(parser.failure_for_current_phase())
                }
                ResponseProgress::Cancelled(reason) => ResultEventProgress::Cancelled(reason),
            }
        }
    }
}

async fn fail_correlated_request(
    incoming: IncomingResultRequest,
    parser: &PiJsonV1Parser,
    cancellation: &CancellationSource,
) -> ResultEventProgress {
    fail_request_with_cause(incoming, parser.failure_for_current_phase(), cancellation).await
}

async fn fail_request_with_cause(
    incoming: IncomingResultRequest,
    cause: AgentFailureCause,
    cancellation: &CancellationSource,
) -> ResultEventProgress {
    match respond_with_cancellation(
        incoming,
        ValidatePiResultV1Response::fatal("Result validation channel correlation failed."),
        cancellation,
    )
    .await
    {
        ResponseProgress::Delivered | ResponseProgress::Failed => {
            ResultEventProgress::Failed(cause)
        }
        ResponseProgress::Cancelled(reason) => ResultEventProgress::Cancelled(reason),
    }
}

enum ResponseProgress {
    Delivered,
    Failed,
    Cancelled(CancellationReason),
}

async fn respond_with_cancellation(
    incoming: IncomingResultRequest,
    response: ValidatePiResultV1Response,
    cancellation: &CancellationSource,
) -> ResponseProgress {
    let response = incoming.respond(response);
    tokio::pin!(response);
    let accepted_cancellation = cancellation.wait_for_cancellation();
    tokio::pin!(accepted_cancellation);
    tokio::select! {
        biased;
        reason = &mut accepted_cancellation => ResponseProgress::Cancelled(reason),
        delivered = &mut response => {
            if delivered.is_ok() {
                ResponseProgress::Delivered
            } else {
                ResponseProgress::Failed
            }
        }
    }
}

async fn shutdown_result_bridge<Clock, Worker>(
    result_bridge: Option<ActiveResultBridge<Clock, Worker>>,
) -> Result<(), ()> {
    match result_bridge {
        Some(result_bridge) => result_bridge.bridge.shutdown().await,
        None => Ok(()),
    }
}

enum SettlementDeadlineOutcome {
    Expired,
}

type SettlementDeadlineWait = Pin<Box<dyn Future<Output = ()> + Send>>;

async fn wait_for_settlement_deadline(wait: &mut Option<SettlementDeadlineWait>) {
    match wait {
        Some(wait) => wait.await,
        None => pending().await,
    }
}

async fn supervise_process_group<Clock: CoordinatorClock>(
    process_group: Pid,
    cancellation: CancellationSource,
    mut directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    mut shutdown: oneshot::Receiver<()>,
    mut settlement: ResultSettlementConfiguration<Clock>,
    mut settlement_starts: mpsc::UnboundedReceiver<()>,
    settlement_outcomes: mpsc::UnboundedSender<SettlementDeadlineOutcome>,
) {
    let accepted_cancellation = cancellation.wait_for_cancellation();
    tokio::pin!(accepted_cancellation);
    let mut interrupted = false;
    let mut cancellation_observed = false;
    let mut directives_open = true;
    let mut settlement_starts_open = true;
    let mut settlement_deadline: Option<SettlementDeadlineWait> = None;
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return,
            _ = &mut accepted_cancellation, if !cancellation_observed => {
                cancellation_observed = true;
                settlement_deadline = None;
                if !interrupted {
                    interrupt_process_group(process_group);
                    interrupted = true;
                }
            }
            directive = directives.recv(), if directives_open => {
                match directive {
                    Some(AgentProcessDirective::Interrupt) if !interrupted => {
                        interrupt_process_group(process_group);
                        interrupted = true;
                    }
                    Some(AgentProcessDirective::Interrupt) => {}
                    Some(AgentProcessDirective::Force) => {
                        terminate_process_group(process_group);
                        return;
                    }
                    None => directives_open = false,
                }
            }
            start = settlement_starts.recv(), if settlement_starts_open => {
                match start {
                    Some(()) if settlement_deadline.is_none() && !cancellation_observed => {
                        let deadline = settlement.clock.now().add(settlement.grace.get());
                        let clock = settlement.clock.clone();
                        settlement_deadline = Some(Box::pin(async move {
                            clock.wait_until(deadline).await;
                        }));
                    }
                    Some(()) => {
                        terminate_process_group(process_group);
                        let _ = settlement_outcomes.send(SettlementDeadlineOutcome::Expired);
                        return;
                    }
                    None => settlement_starts_open = false,
                }
            }
            () = wait_for_settlement_deadline(&mut settlement_deadline),
                if settlement_deadline.is_some() =>
            {
                terminate_process_group(process_group);
                let _ = settlement_outcomes.send(SettlementDeadlineOutcome::Expired);
                return;
            }
        }
    }
}

impl PiJsonV1Parser {
    fn failure_for_current_phase(&self) -> AgentFailureCause {
        self.protocol_failure()
    }
}

async fn stop_child(child: &mut Child, process_group: Option<Pid>) {
    if let Some(process_group) = process_group {
        terminate_process_group(process_group);
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
}

fn failed(cause: AgentFailureCause) -> AgentOutcome {
    AgentOutcome::Failed { cause }
}
