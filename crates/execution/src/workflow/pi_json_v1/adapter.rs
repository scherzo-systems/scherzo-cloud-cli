use std::ffi::OsString;
use std::fs;
use std::future::pending;
use std::io;
use std::num::NonZeroU64;
#[cfg(test)]
use std::os::unix::process::CommandExt as _;
use std::path::Path;
#[cfg(test)]
use std::process::Stdio;
use std::sync::Arc;

use tokio::process::ChildStderr;
#[cfg(test)]
use tokio::process::Command;
use tokio::sync::mpsc;

use super::input_transport::PreparedInputTransport;
use super::result_bridge::{
    IncomingResultRequest, PreparedResultBridge, ResultSocketEvent, ValidatePiResultV1Response,
};
use super::{
    AcceptedPiJsonV1Result, PiJsonV1Parser, PiJsonV1ProcessCompletion, PiJsonV1ProtocolLimits,
};
use crate::pi::{PiCompatibilityProfile, compatibility_profile_for_version};
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{
    AgentCompatibilityProfile, AgentFailure, AgentFailureCause, AgentInputKind, AgentInvocation,
    AgentLifecycleMilestone, AgentObservation, AgentObservationSink, AgentOutcome,
    AgentProcessDirective, AgentStartCallback, AgentValueKind, AgentValueMode,
    MAXIMUM_INLINE_AGENT_INPUT_BYTES, PositiveDuration, check_agent_input_bound,
    failed_agent_outcome, finish_agent_diagnostic_capture, run_cancellable_blocking_launch,
};
use crate::workflow::agent_process_driver::{self, GuardedProcess, Launch, Settlement};
use crate::workflow::child_guard::ChildGuardCancellation;
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::observation::{ExecutionObserver, NoopExecutionObserver};
use crate::workflow::pi::PiConfig;
use crate::workflow::result_validation::{
    AuthoritativeResultValidator, ProcessResultValidationWorker, ResultValidationDecision,
    ResultValidationOutcome, ResultValidationWorker,
};

#[derive(Clone, Default)]
pub(crate) struct PiProfile;
pub(crate) type PiJsonV1Adapter<
    Clock,
    Observer = NoopExecutionObserver,
    Worker = ProcessResultValidationWorker,
> = agent_process_driver::AdapterCore<Clock, Observer, Worker, PiProfile>;

impl<Clock, Observer, Worker, Sink>
    agent_process_driver::NativeProcessProfile<Clock, Observer, Worker, Sink> for PiProfile
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type Configuration = PiConfig;
    type Limits = PiJsonV1ProtocolLimits;
    const NAME: &'static str = "pi_json_v1";

    async fn invoke(
        adapter: &agent_process_driver::AdapterCore<Clock, Observer, Worker, Self>,
        invocation: AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
        started: &AgentStartCallback,
    ) -> AgentOutcome {
        adapter.invoke_inner(invocation, started).await
    }
}

impl<Clock, Observer, Worker> agent_process_driver::AdapterCore<Clock, Observer, Worker, PiProfile>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
{
    async fn invoke_inner<Sink>(
        &self,
        invocation: AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
        started: &AgentStartCallback,
    ) -> AgentOutcome
    where
        Sink: AgentObservationSink,
    {
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }

        let adapter = self.clone();
        let (invocation, (plan, mut result_bridge)) = match self
            .prepare_invocation(
                invocation,
                move |invocation| {
                    prepare_launch(invocation).and_then(|mut plan| {
                        let result_bridge = adapter.prepare_result_bridge(invocation)?;
                        if let Some(result_bridge) = result_bridge.as_ref() {
                            plan.add_result_extension(result_bridge.bridge.extension_path());
                        }
                        Ok((plan, result_bridge))
                    })
                },
                AgentFailureCause::start_failure("launch preparation", "unavailable"),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        // Even without a durable guard store, the stopped-child handshake ensures the
        // process is contained before it can run. Registration is a no-op in that case.
        let cancellation_source = invocation.cancellation().clone();
        let diagnostics = self.diagnostics.clone();
        let maximum_stream_bytes = self.maximum_diagnostic_stream_bytes;
        let launch = match run_cancellable_blocking_launch(
            &cancellation_source,
            move |launch_cancellation| {
                let spawn_diagnostics = ProcessSpawnDiagnosticCapture {
                    log: &diagnostics,
                    maximum_stream_bytes,
                };
                let launched = launch_guarded_process(
                    &invocation,
                    &plan,
                    spawn_diagnostics,
                    &launch_cancellation,
                );
                (invocation, plan, launched)
            },
        )
        .await
        {
            Ok((launch, cancellation_reason)) => (launch, cancellation_reason),
            Err(_) => {
                let _ = shutdown_result_bridge(result_bridge).await;
                return failed(AgentFailureCause::start_failure(
                    "launch preparation",
                    "unavailable",
                ));
            }
        };
        let ((mut invocation, plan, launched), cancellation_reason) = launch;
        if let Some(reason) = cancellation_reason {
            if let Ok((mut process, _)) = launched {
                let _ = process.child.force_stop(process.process_group).await;
            }
            let _ = shutdown_result_bridge(result_bridge).await;
            return AgentOutcome::Cancelled { reason };
        }
        let (process, standard_error) = match launched {
            Ok(launched) => launched,
            Err(cause) => {
                let _ = shutdown_result_bridge(result_bridge).await;
                self.diagnostics.record_agent_start_failure(
                    invocation.identity(),
                    self.maximum_diagnostic_stream_bytes,
                    &cause,
                );
                return failed(cause);
            }
        };
        let mut process = process;
        let Some(process_directives) = agent_process_driver::take_process_directives(
            &mut invocation,
            &mut process.child,
            process.process_group,
        )
        .await
        else {
            let _ = shutdown_result_bridge(result_bridge).await;
            return failed(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ));
        };
        let diagnostic = self.start_diagnostic(&invocation, standard_error);
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
        finish_agent_diagnostic_capture(invocation.diagnostic_session(), diagnostic, &outcome)
            .await;
        if let Err(error) = bridge_shutdown {
            self.diagnostics.record_adapter_error(
                invocation.identity().step().to_owned(),
                invocation.identity().invocation(),
                self.maximum_diagnostic_stream_bytes,
                "bridge shutdown",
                &error,
            );
        }
        outcome
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
        .map_err(|error| AgentFailureCause::start_failure("result bridge", error))?;
        let Some(validator) = self.result_validator(invocation) else {
            return Err(AgentFailureCause::start_failure(
                "result bridge",
                "result validator unavailable",
            ));
        };
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
    result_extension_argument_index: usize,
    expected_cwd: Arc<str>,
}

impl PiJsonV1LaunchPlan {
    fn add_result_extension(&mut self, extension_path: &std::path::Path) {
        self.arguments.splice(
            self.result_extension_argument_index..self.result_extension_argument_index,
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
    check_agent_input_bound(
        invocation.prompt().message(),
        invocation.limits().maximum_message_bytes(),
        AgentInputKind::Message,
    )?;

    agent_process_driver::require_native_profile(
        invocation,
        AgentCompatibilityProfile::PiJsonV1,
        compatibility_profile_for_version(invocation.adapter().version())
            == Some(PiCompatibilityProfile::PiJsonV1),
        || AgentFailureCause::start_failure("launch preparation", "unavailable"),
    )?;
    agent_process_driver::verify_session_binding(
        invocation
            .diagnostic_session()
            .verify_pi_native_session_path_binding(),
        "pi diagnostic session binding",
    )?;
    let expected_cwd = invocation.process().protocol_cwd().map_err(|error| {
        AgentFailureCause::start_failure("working directory", format!("{error:?}"))
    })?;
    let system_prompt = combined_system_prompt(
        &expected_cwd,
        invocation.prompt().system_prompt(),
        invocation.limits().maximum_system_prompt_bytes(),
    )?;
    let expected_cwd =
        expected_cwd
            .to_str()
            .map(Arc::from)
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
    let staged_system_prompt =
        (system_prompt.len() > MAXIMUM_INLINE_AGENT_INPUT_BYTES).then_some(system_prompt.as_str());
    let staged_message = if invocation.prompt().message().len() > MAXIMUM_INLINE_AGENT_INPUT_BYTES {
        Some(
            invocation
                .staging()
                .message_file()
                .ok_or(AgentFailureCause::start_failure(
                    "launch preparation",
                    "unavailable",
                ))?,
        )
    } else {
        None
    };
    let input_transport = PreparedInputTransport::prepare(
        invocation.identity(),
        invocation.staging().result_endpoint_directory(),
        staged_system_prompt,
        staged_message,
    )
    .map_err(|error| AgentFailureCause::start_failure("input transport", error))?;

    let config = invocation.adapter().native_configuration();
    let mut arguments = Vec::with_capacity(15_usize.saturating_add(invocation.attachments().len()));
    arguments.extend([
        OsString::from("--mode"),
        OsString::from("json"),
        OsString::from("--approve"),
        OsString::from("--session-dir"),
        invocation
            .diagnostic_session()
            .pi_native_session_directory()
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?
            .as_os_str()
            .to_owned(),
        OsString::from("--model"),
        OsString::from(&config.model),
        OsString::from("--thinking"),
        OsString::from(config.thinking.as_str()),
        OsString::from("--append-system-prompt"),
        input_transport
            .system_prompt_marker()
            .map_or_else(|| OsString::from(system_prompt), OsString::from),
    ]);
    arguments.extend([
        OsString::from("--extension"),
        input_transport.extension_path().as_os_str().to_owned(),
    ]);
    let result_extension_argument_index = arguments.len();
    for attachment in invocation.attachments() {
        let mut argument = OsString::from("@");
        argument.push(attachment.path());
        arguments.push(argument);
    }
    if let Some(message_marker) = input_transport.message_marker() {
        arguments.push(OsString::from(message_marker));
    } else {
        let mut message = OsString::from("\n");
        message.push(invocation.prompt().message());
        arguments.push(message);
    }

    Ok(PiJsonV1LaunchPlan {
        arguments,
        result_extension_argument_index,
        expected_cwd,
    })
}

#[cfg(test)]
pub(super) fn build_command<Sink>(
    invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    plan: &PiJsonV1LaunchPlan,
) -> Result<Command, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    agent_process_driver::verify_session_binding(
        invocation
            .diagnostic_session()
            .verify_pi_native_session_path_binding(),
        "pi diagnostic session binding",
    )?;
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
        .map_err(|error| {
            AgentFailureCause::start_failure("command binding", format!("{error:?}"))
        })?;
    Ok(command)
}

fn launch_guarded_process<Sink>(
    invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    plan: &PiJsonV1LaunchPlan,
    spawn_diagnostics: ProcessSpawnDiagnosticCapture<'_>,
    cancellation: &ChildGuardCancellation,
) -> Result<(LaunchedPiProcess, ChildStderr), AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    let environment = invocation
        .process()
        .environment()
        .variables()
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    let GuardedProcess {
        child,
        process_group,
        standard_output,
        standard_error,
        ..
    } = Launch::for_invocation(
        invocation,
        &plan.arguments,
        &environment,
        cancellation,
        false,
    )
    .spawn(
        |command| agent_process_driver::bind_agent_command(invocation, command),
        || {
            invocation
                .diagnostic_session()
                .verify_pi_native_session_path_binding()
                .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))
        },
        |error| spawn_diagnostics.capture(invocation, &error),
        |error| spawn_diagnostics.capture(invocation, &error),
        || AgentFailureCause::start_failure("launch preparation", "unavailable"),
    )?;
    Ok((
        LaunchedPiProcess {
            child,
            process_group,
            standard_output,
        },
        standard_error,
    ))
}

#[derive(Clone, Copy)]
struct ProcessSpawnDiagnosticCapture<'a> {
    log: &'a StepDiagnosticLog,
    maximum_stream_bytes: NonZeroU64,
}

impl ProcessSpawnDiagnosticCapture<'_> {
    fn capture<Sink>(
        self,
        invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
        error: &io::Error,
    ) -> AgentFailureCause
    where
        Sink: AgentObservationSink,
    {
        let _ = self.log.record_process_spawn_failure(
            invocation.identity().step().to_owned(),
            invocation.identity().invocation(),
            self.maximum_stream_bytes,
            error,
        );
        AgentFailureCause::start_failure("process spawn", error)
    }
}

// Pi 0.84 treats the CLI append value as a replacement for the trusted
// project's APPEND_SYSTEM.md, so preserve both inputs in the one native flag.
fn combined_system_prompt(
    working_directory: &Path,
    workflow_prompt: &str,
    maximum_bytes: NonZeroU64,
) -> Result<String, AgentFailureCause> {
    let project_prompt = match fs::read_to_string(working_directory.join(".pi/APPEND_SYSTEM.md")) {
        Ok(prompt) => Some(prompt),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(AgentFailureCause::start_failure(
                "project prompt read",
                error,
            ));
        }
    };
    let combined = project_prompt.map_or_else(
        || workflow_prompt.to_owned(),
        |project_prompt| format!("{project_prompt}\n\n{workflow_prompt}"),
    );
    check_agent_input_bound(&combined, maximum_bytes, AgentInputKind::SystemPrompt)?;
    Ok(combined)
}

type LaunchedPiProcess = agent_process_driver::ProcessOutput;

#[cfg(test)]
pub(super) use agent_process_driver::PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL;

struct ResultSettlementConfiguration<Clock> {
    clock: Clock,
    grace: PositiveDuration,
}

enum PiExtra {
    Result(ResultSocketEvent),
    SettlementExpired,
}

struct PiProtocol<'a, Clock, Worker, Sink> {
    invocation: &'a AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    started: &'a AgentStartCallback,
    parser: PiJsonV1Parser,
    result_bridge: &'a mut Option<ActiveResultBridge<Clock, Worker>>,
    begin_settlement: mpsc::UnboundedSender<()>,
    settlement_outcome: mpsc::UnboundedReceiver<()>,
    settlement_admitted: bool,
    settlement_active: bool,
    pending_result_event: Option<ResultSocketEvent>,
    start_reported: bool,
    failure: Option<AgentFailure>,
}

impl<Clock, Worker, Sink> agent_process_driver::Protocol<Clock>
    for PiProtocol<'_, Clock, Worker, Sink>
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type Extra = PiExtra;

    fn extra_enabled(&self, state: &agent_process_driver::State<Clock>) -> bool {
        state.parser_enabled
            && (self.settlement_active
                || (self.pending_result_event.is_none() && self.result_bridge.is_some()))
    }

    async fn extra(&mut self) -> PiExtra {
        tokio::select! {
            biased;
            event = receive_result_event(self.result_bridge), if self.pending_result_event.is_none() => PiExtra::Result(event),
            _ = self.settlement_outcome.recv(), if self.settlement_active => PiExtra::SettlementExpired,
        }
    }

    async fn on_extra(&mut self, event: PiExtra, state: &mut agent_process_driver::State<Clock>) {
        if !state.parser_enabled {
            return;
        }
        match event {
            PiExtra::Result(event) => self.pending_result_event = Some(event),
            PiExtra::SettlementExpired => {
                self.settlement_active = false;
                state.output_closed = true;
                self.failure = Some(AgentFailureCause::ResultSettlementFailed.into());
                state.parser_enabled = false;
                state.force_group();
            }
        }
    }

    async fn on_stdout(&mut self, bytes: &[u8], state: &mut agent_process_driver::State<Clock>) {
        let (parsed, observations) = agent_process_driver::collect_stdout_observations(|emit| {
            self.parser.push_stdout(bytes, emit)
        });
        if parsed.is_ok()
            && self.parser.accepted_result_ready_for_settlement()
            && !self.settlement_admitted
        {
            if self.begin_settlement.send(()).is_err() {
                self.failure = Some(AgentFailureCause::HarnessProtocolFailed.into());
                state.parser_enabled = false;
                state.force_group();
            } else {
                self.settlement_admitted = true;
                self.settlement_active = true;
            }
        }
        if state.parser_enabled {
            for observation in observations {
                let reports_start = matches!(
                    &observation,
                    AgentObservation::Lifecycle {
                        milestone: AgentLifecycleMilestone::HarnessStarted,
                    }
                );
                // Native retries can report start repeatedly; the workflow callback is one-shot.
                if reports_start && !self.start_reported {
                    if self.started.report().is_err() {
                        self.failure = Some(AgentFailureCause::HarnessProtocolFailed.into());
                        state.parser_enabled = false;
                        state.force_group();
                        break;
                    }
                    self.start_reported = true;
                }
                let emitted = self.invocation.observations().emit(observation).await;
                if let Some(reason) = state.cancellation.cancellation_reason() {
                    state.cancelled = Some(reason);
                    state.parser_enabled = false;
                    break;
                }
                if emitted.is_err() {
                    self.failure = Some(AgentFailureCause::HarnessProtocolFailed.into());
                    state.parser_enabled = false;
                    state.force_group();
                    break;
                }
            }
        }
        if state.parser_enabled && parsed.is_err() {
            self.failure = Some(self.parser.agent_failure_for_current_phase());
            state.parser_enabled = false;
            state.force_group();
        }
    }

    fn classify_read_failure(&mut self) {
        self.failure = Some(self.parser.agent_failure_for_current_phase());
    }
    fn cancellation_precedes_read_failure(&self) -> bool {
        true
    }
    fn read_failure_enabled(&self, state: &agent_process_driver::State<Clock>) -> bool {
        state.cancelled.is_none()
    }

    async fn on_wait_error(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if state.cancelled.is_none() {
            self.failure = Some(self.parser.agent_failure_for_current_phase());
        }
        state.force_group();
    }

    async fn after_event(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if state.parser_enabled
            && let Some(event) = self.pending_result_event.take()
        {
            match handle_result_event(
                event,
                self.result_bridge,
                &mut self.parser,
                &state.cancellation,
            )
            .await
            {
                ResultEventProgress::Continue { observation } => {
                    if let Some(observation) = observation {
                        let emitted = self.invocation.observations().emit(observation).await;
                        if let Some(reason) = state.cancellation.cancellation_reason() {
                            state.cancelled = Some(reason);
                            state.parser_enabled = false;
                        } else if emitted.is_err() {
                            self.failure = Some(AgentFailureCause::HarnessProtocolFailed.into());
                            state.parser_enabled = false;
                            state.force_group();
                        }
                    }
                }
                ResultEventProgress::Pending(incoming) => {
                    self.pending_result_event = Some(ResultSocketEvent::Request(incoming));
                }
                ResultEventProgress::Failed(failure) => {
                    self.failure = Some(failure);
                    state.parser_enabled = false;
                    state.force_group();
                }
                ResultEventProgress::Cancelled(reason) => {
                    state.cancelled = Some(reason);
                    state.parser_enabled = false;
                }
            }
        }
    }

    fn needs_group(&self, state: &agent_process_driver::State<Clock>) -> bool {
        !state.group_quiescent
    }

    fn probe_group(&self, state: &agent_process_driver::State<Clock>) -> bool {
        state.termination_requested
    }

    fn on_group_live(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if (!self.settlement_active || !state.parser_enabled) && !state.termination_requested {
            state.force_group();
        }
    }

    async fn finish(
        self,
        state: agent_process_driver::State<Clock>,
        _supervisor_quiesced: bool,
    ) -> AgentOutcome {
        if let Some(reason) = state.cancelled {
            return self.parser.finish(PiJsonV1ProcessCompletion::cancelled(
                state.completion.is_some_and(|status| status.success()),
                reason,
            ));
        }
        if let Some(failure) = self.failure {
            return AgentOutcome::Failed(failure);
        }
        if state.wait_failed {
            return AgentOutcome::Failed(self.parser.agent_failure_for_current_phase());
        }
        let Some(status) = state.completion else {
            return AgentOutcome::Failed(self.parser.agent_failure_for_current_phase());
        };
        self.parser
            .finish(PiJsonV1ProcessCompletion::exited(status.success()))
    }
}

async fn drive_process<Clock, Worker, Sink>(
    invocation: &AgentInvocation<PiConfig, PiJsonV1ProtocolLimits, Sink>,
    started: &AgentStartCallback,
    process: LaunchedPiProcess,
    parser: PiJsonV1Parser,
    process_directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    result_bridge: &mut Option<ActiveResultBridge<Clock, Worker>>,
    settlement: ResultSettlementConfiguration<Clock>,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    let (begin_settlement, settlement_starts) = mpsc::unbounded_channel();
    let (expired, settlement_outcome) = mpsc::unbounded_channel();
    let protocol = PiProtocol {
        invocation,
        started,
        parser,
        result_bridge,
        begin_settlement,
        settlement_outcome,
        settlement_admitted: false,
        settlement_active: false,
        pending_result_event: None,
        start_reported: false,
        failure: None,
    };
    agent_process_driver::drive_signalled(
        process,
        invocation.cancellation().clone(),
        settlement.clock.clone(),
        protocol,
        process_directives,
        Some(Settlement {
            clock: settlement.clock,
            grace: settlement.grace,
            starts: settlement_starts,
            expired,
        }),
    )
    .await
}

enum ResultEventProgress {
    Continue {
        observation: Option<AgentObservation>,
    },
    Pending(IncomingResultRequest),
    Failed(AgentFailure),
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
        return ResultEventProgress::Failed(parser.agent_failure_for_current_phase());
    };
    let Some(result_bridge) = result_bridge.as_mut() else {
        return ResultEventProgress::Failed(parser.agent_failure_for_current_phase());
    };

    let request = incoming.request();
    let Some(candidate) = request.candidate().cloned() else {
        return fail_correlated_request(incoming, parser, cancellation).await;
    };
    if request.tool_name() != result_bridge.bridge.tool_name().as_ref() {
        return fail_correlated_request(incoming, parser, cancellation).await;
    }
    match parser.try_correlate_result_request(
        request.tool_name(),
        request.tool_call_id(),
        request.arguments(),
    ) {
        Ok(true) => {}
        Ok(false) => return ResultEventProgress::Pending(incoming),
        Err(_) => return fail_correlated_request(incoming, parser, cancellation).await,
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
                    ResultEventProgress::Failed(parser.agent_failure_for_current_phase())
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
                    ResultEventProgress::Failed(cause.into())
                }
                ResponseProgress::Cancelled(reason) => ResultEventProgress::Cancelled(reason),
            }
        }
        ResultValidationOutcome::Decided(ResultValidationDecision::Valid(result)) => {
            if parser
                .accept_result(AcceptedPiJsonV1Result::new(
                    call_id, tool_name, arguments, result,
                ))
                .is_err()
            {
                return fail_request_with_failure(
                    incoming,
                    parser.agent_failure_for_current_phase(),
                    cancellation,
                )
                .await;
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
                    ResultEventProgress::Failed(parser.agent_failure_for_current_phase())
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
    fail_request_with_failure(
        incoming,
        parser.agent_failure_for_current_phase(),
        cancellation,
    )
    .await
}

async fn fail_request_with_failure(
    incoming: IncomingResultRequest,
    failure: AgentFailure,
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
            ResultEventProgress::Failed(failure)
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
) -> io::Result<()> {
    match result_bridge {
        Some(result_bridge) => result_bridge.bridge.shutdown().await,
        None => Ok(()),
    }
}

impl PiJsonV1Parser {
    fn agent_failure_for_current_phase(&self) -> AgentFailure {
        self.failure
            .clone()
            .unwrap_or_else(|| AgentFailure::new(self.protocol_failure()))
    }
}

fn failed(cause: AgentFailureCause) -> AgentOutcome {
    failed_agent_outcome(cause)
}
