use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::future::{Future, pending};
use std::io;
use std::num::NonZeroU64;
use std::ops::Add as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use rustix::process::Pid;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::process::ChildStdout;
use tokio::sync::{mpsc, oneshot};

use super::input::initial_turn_input;
use super::{CodexAppServerV1Parser, CodexAppServerV1ProtocolLimits, ParserProgress};
use crate::execution::codex::{CodexCompatibilityProfile, compatibility_profile_for_version};
use crate::execution::workflow::admission::CancellationSource;
use crate::execution::workflow::agent::{
    AgentAdapter, AgentCompatibilityProfile, AgentFailure, AgentFailureCause,
    AgentHarnessSetupStage, AgentInputKind, AgentInvocation, AgentLifecycleMilestone,
    AgentObservation, AgentObservationSink, AgentOutcome, AgentProcessDirective,
    AgentStartCallback, AgentTerminalCallback, AgentValueMode, PositiveDuration,
    check_agent_input_bound, failed_agent_outcome, finish_agent_diagnostic_capture,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::execution::workflow::child_guard::StoppedChildGuard;
use crate::execution::workflow::codex::CodexConfig;
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::observation::ExecutionObserver;
use crate::execution::workflow::process_group::{
    ProcessGuardRegistration, process_group_is_quiescent, terminate_authenticated_process_group,
    terminate_process_group,
};
use crate::execution::workflow::result_validation::{
    AuthoritativeResultValidator, ProcessResultValidationWorker, ResultValidationDecision,
    ResultValidationOutcome, ResultValidationWorker,
};

const READ_BUFFER_BYTES: usize = 8 * 1024;
const PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(10);

pub(crate) struct CodexAppServerV1Adapter<Clock, Observer, Worker = ProcessResultValidationWorker> {
    diagnostics: StepDiagnosticLog,
    maximum_diagnostic_stream_bytes: NonZeroU64,
    clock: Clock,
    observer: Observer,
    validation_worker: Worker,
    #[cfg(test)]
    synthetic_model_provider: Option<Arc<str>>,
}

impl<Clock, Observer> CodexAppServerV1Adapter<Clock, Observer, ProcessResultValidationWorker> {
    // Codex retains its profile-only fixture provider alongside the shared adapter fields;
    // a shared constructor would expose synthetic provider selection to other harnesses.
    // jscpd:ignore-start
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
            #[cfg(test)]
            synthetic_model_provider: None,
        })
    }
    // jscpd:ignore-end
}

// The injected worker/provider constructor remains local to Codex transcript fixtures;
// sharing another profile's test constructor would couple native fixture controls.
// jscpd:ignore-start
impl<Clock, Observer, Worker> CodexAppServerV1Adapter<Clock, Observer, Worker> {
    #[cfg(test)]
    pub(super) fn with_validation_worker(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
        validation_worker: Worker,
        synthetic_model_provider: Option<Arc<str>>,
    ) -> Self {
        Self {
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            validation_worker,
            synthetic_model_provider,
        }
    }

    fn selected_model_provider(&self) -> Option<Arc<str>> {
        #[cfg(test)]
        {
            self.synthetic_model_provider.clone()
        }
        #[cfg(not(test))]
        {
            None
        }
    }
}
// jscpd:ignore-end

// Clone keeps Codex's test-only provider state private instead of adding that concern to
// the shared adapter contract.
// jscpd:ignore-start
impl<Clock, Observer, Worker> Clone for CodexAppServerV1Adapter<Clock, Observer, Worker>
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
            #[cfg(test)]
            synthetic_model_provider: self.synthetic_model_provider.clone(),
        }
    }
}
// jscpd:ignore-end

impl<Clock, Observer, Worker, Sink> AgentAdapter<Sink>
    for CodexAppServerV1Adapter<Clock, Observer, Worker>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type NativeConfiguration = CodexConfig;
    type ProtocolLimits = CodexAppServerV1ProtocolLimits;

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

// Codex setup and result-validator preparation stay with its stdio lifecycle; a shared
// harness driver would erase profile-specific setup and correction transitions.
// jscpd:ignore-start
impl<Clock, Observer, Worker> CodexAppServerV1Adapter<Clock, Observer, Worker>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
{
    async fn invoke_inner<Sink>(
        &self,
        invocation: AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
        started: &AgentStartCallback,
    ) -> AgentOutcome
    where
        Sink: AgentObservationSink,
    {
        // Setup ordering is part of Codex start authority; keep it local even though the
        // cancellation checkpoints resemble the independent Pi adapter.
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }
        let (mut invocation, mut plan) = match tokio::task::spawn_blocking(move || {
            let plan = prepare_launch(&invocation);
            (invocation, plan)
        })
        .await
        {
            Ok((invocation, Ok(plan))) => (invocation, plan),
            Ok((_, Err(cause))) => return failed_agent_outcome(cause),
            Err(_) => {
                return setup_failed(AgentHarnessSetupStage::ExecutableLaunch);
            }
        };
        let result_validator = match invocation.value_mode() {
            AgentValueMode::Result { schema, .. } => Some(AuthoritativeResultValidator::new(
                schema.clone(),
                invocation.limits().maximum_result_bytes(),
                invocation
                    .limits()
                    .maximum_result_rejection_feedback_bytes(),
                invocation.limits().result_validation_deadline(),
                self.clock.clone(),
                self.validation_worker.clone(),
            )),
            AgentValueMode::None | AgentValueMode::Response { .. } => None,
        };
        let Some(process_directives) = invocation.take_process_directives() else {
            return setup_failed(AgentHarnessSetupStage::ExecutableLaunch);
        };
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }
        let (process, standard_error) = match launch_process(&invocation, &plan).await {
            Ok(process) => process,
            Err(cause) => return failed_agent_outcome(cause),
        };
        // jscpd:ignore-end
        let diagnostic = self.diagnostics.start_standard_error_capture(
            invocation.identity().step().to_owned(),
            invocation.identity().invocation(),
            self.maximum_diagnostic_stream_bytes,
            standard_error,
            self.observer.clone(),
        );
        let configuration = invocation.adapter().native_configuration();
        let expected_cwd = Arc::clone(&plan.expected_cwd);
        let codex_home = Arc::clone(&plan.codex_home);
        let sqlite_home = Arc::clone(&plan.sqlite_home);
        let initial_input = std::mem::take(&mut plan.initial_input);
        let parser = match CodexAppServerV1Parser::profile(
            expected_cwd,
            codex_home,
            sqlite_home,
            Arc::from(invocation.adapter().version()),
            Arc::from(configuration.model.as_str()),
            Arc::from(configuration.effort.as_str()),
            Arc::from(invocation.prompt().system_prompt()),
            initial_input,
            self.selected_model_provider(),
            invocation.value_mode().kind(),
            invocation.limits().maximum_response_bytes(),
            *invocation.limits().adapter_protocol(),
        ) {
            Ok(parser) => parser,
            Err(cause) => {
                let mut process = process;
                let _ = process.child.force_stop(process.process_group).await;
                diagnostic.abort();
                diagnostic.finish().await;
                return failed_agent_outcome(cause);
            }
        };
        let outcome = drive_process(
            &invocation,
            started,
            process,
            parser,
            process_directives,
            result_validator,
            ResultSettlementConfiguration {
                clock: self.clock.clone(),
                grace: invocation.limits().result_settlement_grace(),
            },
        )
        .await;
        finish_agent_diagnostic_capture(invocation.diagnostic_session(), diagnostic, &outcome)
            .await;
        outcome
    }
}

#[derive(Debug)]
pub(super) struct CodexAppServerV1LaunchPlan {
    arguments: Vec<OsString>,
    expected_cwd: Arc<str>,
    codex_home: Arc<str>,
    sqlite_home: Arc<str>,
    initial_input: Vec<serde_json::Value>,
    _sqlite_state: tempfile::TempDir,
}

impl CodexAppServerV1LaunchPlan {
    #[cfg(test)]
    pub(super) fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    #[cfg(test)]
    pub(super) fn initial_input(&self) -> &[serde_json::Value] {
        &self.initial_input
    }

    #[cfg(test)]
    pub(super) fn sqlite_home(&self) -> &Path {
        self._sqlite_state.path()
    }
}

pub(super) fn prepare_launch<Sink>(
    invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
) -> Result<CodexAppServerV1LaunchPlan, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    // Each native profile owns which inputs are admitted before launch and how a failure
    // is attributed; only the byte-bound primitive is shared.
    // jscpd:ignore-start
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
    // jscpd:ignore-end
    if invocation.adapter().profile() != AgentCompatibilityProfile::CodexAppServerV1
        || compatibility_profile_for_version(invocation.adapter().version())
            != Some(CodexCompatibilityProfile::CodexAppServerV1)
        || !invocation.adapter().executable().is_absolute()
        || invocation
            .diagnostic_session()
            .verify_path_binding()
            .is_err()
    {
        return Err(AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::ExecutableLaunch,
        });
    }
    let expected_cwd =
        invocation
            .process()
            .protocol_cwd()
            .map_err(|_| AgentFailureCause::HarnessSetupFailed {
                stage: AgentHarnessSetupStage::ExecutableLaunch,
            })?;
    let expected_cwd: Arc<str> =
        expected_cwd
            .to_str()
            .map(Arc::from)
            .ok_or(AgentFailureCause::HarnessSetupFailed {
                stage: AgentHarnessSetupStage::ExecutableLaunch,
            })?;
    let codex_home = codex_home_from_environment(invocation.process().environment().variables())?;
    let sqlite_state = prepare_sqlite_state(
        invocation.staging().result_endpoint_directory(),
        Path::new(codex_home.as_ref()),
    )?;
    let sqlite_home: Arc<str> = sqlite_state
        .path()
        .to_str()
        .map(Arc::from)
        .ok_or_else(|| setup_failure(AgentHarnessSetupStage::ExecutableLaunch))?;
    let quoted_cwd = serde_json::to_string(expected_cwd.as_ref()).map_err(|_| {
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::ExecutableLaunch,
        }
    })?;
    let quoted_sqlite_home = serde_json::to_string(sqlite_home.as_ref())
        .map_err(|_| setup_failure(AgentHarnessSetupStage::ExecutableLaunch))?;
    let project_trust = format!("projects={{{quoted_cwd}={{trust_level=\"trusted\"}}}}");
    let sqlite_override = format!("sqlite_home={quoted_sqlite_home}");
    let arguments = [
        OsString::from("--dangerously-bypass-hook-trust"),
        OsString::from("-c"),
        OsString::from(project_trust),
        OsString::from("-c"),
        OsString::from(sqlite_override),
        OsString::from("app-server"),
        OsString::from("--strict-config"),
        OsString::from("--listen"),
        OsString::from("stdio://"),
    ]
    .into();
    let initial_input = initial_turn_input(invocation)?;
    Ok(CodexAppServerV1LaunchPlan {
        arguments,
        expected_cwd,
        codex_home,
        sqlite_home,
        initial_input,
        _sqlite_state: sqlite_state,
    })
}

fn codex_home_from_environment(
    environment: &BTreeMap<OsString, OsString>,
) -> Result<Arc<str>, AgentFailureCause> {
    let path = environment
        .get(OsStr::new("CODEX_HOME"))
        .map(PathBuf::from)
        .or_else(|| {
            environment
                .get(OsStr::new("HOME"))
                .map(PathBuf::from)
                .map(|home| home.join(".codex"))
        })
        .filter(|path| path.is_absolute())
        .ok_or_else(|| setup_failure(AgentHarnessSetupStage::ExecutableLaunch))?;
    path.to_str()
        .map(Arc::from)
        .ok_or_else(|| setup_failure(AgentHarnessSetupStage::ExecutableLaunch))
}

fn prepare_sqlite_state(
    staging: &Path,
    codex_home: &Path,
) -> Result<tempfile::TempDir, AgentFailureCause> {
    let canonical_staging = std::fs::canonicalize(staging)
        .map_err(|_| setup_failure(AgentHarnessSetupStage::ExecutableLaunch))?;
    let canonical_codex_home = std::fs::canonicalize(codex_home)
        .map_err(|_| setup_failure(AgentHarnessSetupStage::ExecutableLaunch))?;
    if canonical_staging != staging
        || !canonical_staging.is_absolute()
        || canonical_staging.starts_with(canonical_codex_home)
    {
        return Err(setup_failure(AgentHarnessSetupStage::ExecutableLaunch));
    }
    let sqlite_state = tempfile::Builder::new()
        .prefix("codex-sqlite-")
        .tempdir_in(canonical_staging)
        .map_err(|_| setup_failure(AgentHarnessSetupStage::ExecutableLaunch))?;
    std::fs::set_permissions(sqlite_state.path(), std::fs::Permissions::from_mode(0o700))
        .map_err(|_| setup_failure(AgentHarnessSetupStage::ExecutableLaunch))?;
    Ok(sqlite_state)
}

struct LaunchedCodexProcess {
    child: CodexChild,
    process_group: Pid,
    standard_input: UnixStream,
    standard_output: ChildStdout,
}

struct CodexChild {
    child: StoppedChildGuard,
    registration: ProcessGuardRegistration,
}

impl CodexChild {
    fn force_process_group(&self) {
        let _ = terminate_authenticated_process_group(self.child.identity());
    }

    async fn wait(&mut self) -> Result<ExitStatus, ()> {
        let status = self.child.wait().await.map_err(|_| ())?;
        self.registration.mark_quiesced()?;
        Ok(status)
    }

    async fn force_stop(&mut self, process_group: Pid) -> Result<(), ()> {
        self.force_process_group();
        self.child.force_stop().await.map_err(|_| ())?;
        self.registration.mark_quiesced()?;
        if process_group_is_quiescent(process_group) {
            Ok(())
        } else {
            Err(())
        }
    }
}

async fn launch_process<Sink>(
    invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
    plan: &CodexAppServerV1LaunchPlan,
) -> Result<(LaunchedCodexProcess, tokio::process::ChildStderr), AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    // The guarded launch sequence stays beside Codex's stdio topology, diagnostic session,
    // and setup-stage attribution instead of creating a cross-harness launch abstraction.
    // jscpd:ignore-start
    let environment = invocation_environment(invocation);
    let (mut child, standard_input) = StoppedChildGuard::spawn_with_stdin(
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
    .map_err(|_| AgentFailureCause::HarnessSetupFailed {
        stage: AgentHarnessSetupStage::ExecutableLaunch,
    })?;
    let process_group = child.identity().process_group();
    let (Some(standard_output), Some(standard_error)) = (child.take_stdout(), child.take_stderr())
    else {
        let _ = child.force_stop().await;
        return Err(AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::ExecutableLaunch,
        });
    };
    let mut registration = match invocation.process_guards().register(
        invocation.identity().step(),
        invocation.identity().invocation().transition_sequence.get(),
        child.identity(),
    ) {
        Ok(registration) => registration,
        Err(()) => {
            let _ = child.force_stop().await;
            return Err(AgentFailureCause::HarnessSetupFailed {
                stage: AgentHarnessSetupStage::ExecutableLaunch,
            });
        }
    };
    if release_guarded_codex(invocation.diagnostic_session(), || {
        child.continue_execution().map_err(|_| ())?;
        registration.mark_released()
    })
    .is_err()
    {
        let _ = child.force_stop().await;
        let _ = registration.mark_quiesced();
        return Err(AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::ExecutableLaunch,
        });
    }
    // jscpd:ignore-end
    Ok((
        LaunchedCodexProcess {
            child: CodexChild {
                child,
                registration,
            },
            process_group,
            standard_input,
            standard_output,
        },
        standard_error,
    ))
}

fn release_guarded_codex(
    diagnostic_session: &AgentDiagnosticSession,
    release: impl FnOnce() -> Result<(), ()>,
) -> Result<(), AgentFailureCause> {
    diagnostic_session.verify_path_binding().map_err(|_| {
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::ExecutableLaunch,
        }
    })?;
    release().map_err(|()| AgentFailureCause::HarnessSetupFailed {
        stage: AgentHarnessSetupStage::ExecutableLaunch,
    })
}

fn invocation_environment<Sink>(
    invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
) -> Vec<(OsString, OsString)>
where
    Sink: AgentObservationSink,
{
    invocation
        .process()
        .environment()
        .variables()
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect()
}

struct ResultSettlementConfiguration<Clock> {
    clock: Clock,
    grace: PositiveDuration,
}

type ResultSettlementWait = Pin<Box<dyn Future<Output = ()> + Send>>;

async fn drive_process<Clock, Worker, Sink>(
    invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
    started: &AgentStartCallback,
    process: LaunchedCodexProcess,
    mut parser: CodexAppServerV1Parser,
    process_directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    mut result_validator: Option<AuthoritativeResultValidator<Clock, Worker>>,
    settlement: ResultSettlementConfiguration<Clock>,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    let LaunchedCodexProcess {
        mut child,
        process_group,
        standard_input,
        mut standard_output,
    } = process;
    let cancellation = invocation.cancellation().clone();
    let (cooperative_interrupt, mut cooperative_interrupts) = mpsc::unbounded_channel();
    let (stop_supervisor, supervisor_shutdown) = oneshot::channel();
    let supervisor = tokio::spawn(supervise_process_group(
        process_group,
        cancellation.clone(),
        process_directives,
        cooperative_interrupt,
        supervisor_shutdown,
    ));
    let mut standard_input = Some(standard_input);
    let mut standard_output_closed = false;
    let mut process_completion = None;
    let mut wait_failed = false;
    let mut parser_enabled = true;
    let mut start_reported = false;
    let mut failure = None;
    let mut cancelled = None;
    let mut cooperative_interrupt_started = false;
    let mut cooperative_interrupts_open = true;
    let mut cleanup_deadline_armed = false;
    let mut cleanup_deadline: Pin<Box<dyn Future<Output = ()> + Send>> =
        Box::pin(std::future::pending());
    let write_grace = settlement.grace.get();
    let mut result_settlement_wait: Option<ResultSettlementWait> = None;
    let mut clock = settlement.clock.clone();
    let mut process_group_probe_clock = settlement.clock.clone();

    if let Err(cause) =
        write_pending_frames(&mut standard_input, &mut parser, &mut clock, write_grace).await
    {
        failure = Some(cause);
        parser_enabled = false;
        standard_input.take();
        child.force_process_group();
    }

    let accepted_cancellation = cancellation.wait_for_cancellation();
    tokio::pin!(accepted_cancellation);
    let mut buffer = [0_u8; READ_BUFFER_BYTES];
    while !standard_output_closed
        || (process_completion.is_none() && !wait_failed)
        || (result_settlement_wait.is_some() && !process_group_is_quiescent(process_group))
    {
        tokio::select! {
            biased;
            // Codex's cancellation branch writes the native interruption before shared
            // deadline escalation can force the guarded process group.
            // jscpd:ignore-start
            reason = &mut accepted_cancellation, if cancelled.is_none() => {
                cancelled = Some(reason);
                if !cooperative_interrupt_started {
                    cooperative_interrupt_started = true;
                    if begin_cooperative_interrupt(
                        &mut standard_input,
                        &mut parser,
                        &mut clock,
                        write_grace,
                    )
                    .await
                    .is_err()
                    {
                        parser_enabled = false;
                        let _ = close_standard_input(&mut standard_input).await;
                    }
                }
            }
            requested = cooperative_interrupts.recv(),
                if cooperative_interrupts_open && !cooperative_interrupt_started =>
            {
                match requested {
                    Some(()) => {
                        cooperative_interrupt_started = true;
                        if let Some(reason) = cancellation.cancellation_reason() {
                            cancelled = Some(reason);
                        }
                        if begin_cooperative_interrupt(
                            &mut standard_input,
                            &mut parser,
                            &mut clock,
                            write_grace,
                        )
                        .await
                        .is_err()
                        {
                            parser_enabled = false;
                            let _ = close_standard_input(&mut standard_input).await;
                        }
                    }
                    None => cooperative_interrupts_open = false,
                }
            }
            () = &mut cleanup_deadline, if cleanup_deadline_armed => {
                cleanup_deadline_armed = false;
                child.force_process_group();
            }
            read = standard_output.read(&mut buffer), if !standard_output_closed => {
                match read {
                    Ok(0) => standard_output_closed = true,
                    Ok(read) if parser_enabled => {
                        let mut observations = Vec::new();
                        let parsed = parser.push_stdout(&buffer[..read], |observation| {
                            observations.push(observation);
                        });
                        // jscpd:ignore-end
                        match parsed {
                            Ok(progress) => {
                                if cancelled.is_none()
                                    && let Some(reason) = cancellation.cancellation_reason()
                                {
                                    cancelled = Some(reason);
                                    if !cooperative_interrupt_started {
                                        cooperative_interrupt_started = true;
                                        if begin_cooperative_interrupt(
                                            &mut standard_input,
                                            &mut parser,
                                            &mut clock,
                                            write_grace,
                                        )
                                        .await
                                        .is_err()
                                        {
                                            parser_enabled = false;
                                            let _ = close_standard_input(&mut standard_input).await;
                                        }
                                    }
                                }
                                if cancelled.is_none() && progress.start_acknowledged {
                                    if start_reported || started.report().is_err() {
                                        failure = Some(AgentFailureCause::HarnessSetupFailed {
                                            stage: AgentHarnessSetupStage::StartAcknowledgement,
                                        });
                                        parser_enabled = false;
                                        child.force_process_group();
                                    } else {
                                        start_reported = true;
                                    }
                                }
                                if parser_enabled
                                    && cancelled.is_none()
                                    && emit_observations(invocation, observations).await.is_err()
                                {
                                    failure = Some(parser.failure_for_current_phase());
                                    parser_enabled = false;
                                    child.force_process_group();
                                }
                                if parser_enabled
                                    && let Err(cause) = write_pending_frames(
                                        &mut standard_input,
                                        &mut parser,
                                        &mut clock,
                                        write_grace,
                                    ).await
                                {
                                    if cancelled.is_none() {
                                        failure = Some(cause);
                                        child.force_process_group();
                                    }
                                    parser_enabled = false;
                                }
                                if progress.close_standard_input
                                    && close_standard_input(&mut standard_input).await.is_err()
                                {
                                    if cancelled.is_none() {
                                        failure = Some(AgentFailureCause::HarnessProtocolFailed);
                                        child.force_process_group();
                                    }
                                    parser_enabled = false;
                                }
                            }
                            Err(mut cause) => {
                                if cancelled.is_none()
                                    && !start_reported
                                    && parser.start_acknowledged()
                                {
                                    if started.report().is_err() {
                                        cause = AgentFailureCause::HarnessSetupFailed {
                                            stage: AgentHarnessSetupStage::StartAcknowledgement,
                                        };
                                    } else {
                                        start_reported = true;
                                    }
                                }
                                if cancelled.is_none()
                                    && emit_observations(invocation, observations).await.is_err()
                                {
                                    cause = parser.failure_for_current_phase();
                                }
                                parser.prevent_value_commit();
                                let _ = parser.request_turn_interrupt();
                                let _ = write_pending_frames(
                                    &mut standard_input,
                                    &mut parser,
                                    &mut clock,
                                    write_grace,
                                )
                                .await;
                                let _ = close_standard_input(&mut standard_input).await;
                                parser_enabled = false;
                                if cancelled.is_none() {
                                    failure = Some(cause);
                                    let deadline = clock.now()
                                        + invocation.limits().result_settlement_grace().get();
                                    let deadline_clock = clock.clone();
                                    cleanup_deadline = Box::pin(async move {
                                        deadline_clock.wait_until(deadline).await;
                                    });
                                    cleanup_deadline_armed = true;
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        standard_output_closed = true;
                        if parser_enabled {
                            failure = Some(parser.failure_for_current_phase());
                            parser_enabled = false;
                            child.force_process_group();
                        }
                    }
                }
            }
            waited = child.wait(), if process_completion.is_none() && !wait_failed => {
                match waited {
                    Ok(status) => process_completion = Some(status),
                    Err(()) => {
                        wait_failed = true;
                        if cancelled.is_none() {
                            failure.get_or_insert_with(|| parser.failure_for_current_phase());
                        }
                        parser_enabled = false;
                        child.force_process_group();
                    }
                }
            }
            () = wait_for_result_settlement(&mut result_settlement_wait),
                if result_settlement_wait.is_some() =>
            {
                result_settlement_wait = None;
                failure = Some(AgentFailureCause::ResultSettlementFailed);
                parser_enabled = false;
                standard_input.take();
                child.force_process_group();
            }
            () = wait_for_process_group_probe(&mut process_group_probe_clock),
                if result_settlement_wait.is_some()
                    && standard_output_closed
                    && process_completion.is_some() => {}
        }

        if parser_enabled && let Some(candidate) = parser.take_result_candidate() {
            let Some(validator) = result_validator.as_mut() else {
                failure = Some(AgentFailureCause::HarnessProtocolFailed);
                parser_enabled = false;
                standard_input.take();
                child.force_process_group();
                continue;
            };
            let progress = match validator.validate(candidate, &cancellation).await {
                ResultValidationOutcome::Cancelled { reason } => {
                    cancelled = Some(reason);
                    parser_enabled = false;
                    standard_input.take();
                    None
                }
                ResultValidationOutcome::Decided(ResultValidationDecision::Fatal(fatal)) => {
                    failure = Some(AgentFailureCause::from(fatal));
                    parser_enabled = false;
                    standard_input.take();
                    child.force_process_group();
                    None
                }
                ResultValidationOutcome::Decided(ResultValidationDecision::Rejected {
                    feedback,
                }) => {
                    let progress = parser.reject_result(Arc::clone(&feedback));
                    let mut observations = vec![AgentObservation::ValueRejected {
                        kind: crate::execution::workflow::agent::AgentValueKind::Result,
                        feedback,
                    }];
                    observations.extend(parser.take_observations());
                    Some((progress, observations, false))
                }
                ResultValidationOutcome::Decided(ResultValidationDecision::Valid(result)) => {
                    let progress = parser.accept_result(result);
                    Some((progress, parser.take_observations(), true))
                }
            };
            if let Some((progress, observations, accepted)) = progress {
                if emit_observations(invocation, observations).await.is_err() {
                    failure = Some(parser.failure_for_current_phase());
                    parser_enabled = false;
                    standard_input.take();
                    child.force_process_group();
                } else {
                    if accepted {
                        let mut settlement_clock = settlement.clock.clone();
                        let deadline = settlement_clock.now().add(settlement.grace.get());
                        result_settlement_wait = Some(Box::pin(async move {
                            settlement_clock.wait_until(deadline).await;
                        }));
                    }
                    if let Err(cause) = apply_result_progress(
                        progress,
                        &mut standard_input,
                        &mut parser,
                        &mut clock,
                        write_grace,
                    )
                    .await
                    {
                        failure = Some(cause);
                        parser_enabled = false;
                        standard_input.take();
                        child.force_process_group();
                    }
                }
            }
        }

        if result_settlement_wait.is_some()
            && standard_output_closed
            && process_completion.is_some()
            && process_group_is_quiescent(process_group)
        {
            result_settlement_wait = None;
        }
    }

    // Codex must settle its guarded child before interpreting App Server terminal state;
    // this remains local because other profiles have different validation workers.
    // jscpd:ignore-start
    if process_completion.is_none() {
        process_completion = child.wait().await.ok();
        if process_completion.is_none() {
            let _ = child.force_stop(process_group).await;
        }
    }
    let _ = stop_supervisor.send(());
    let supervisor_quiesced = supervisor.await.is_ok();
    // jscpd:ignore-end
    if cancelled.is_none() {
        cancelled = cancellation.cancellation_reason();
    }
    let protocol_rejection = parser.protocol_rejection();
    let outcome = if let Some(reason) = cancelled {
        AgentOutcome::Cancelled { reason }
    } else if let Some(cause) = failure {
        failed_agent_outcome(cause)
    } else if wait_failed
        || !supervisor_quiesced
        || !process_group_is_quiescent(process_group)
        || !standard_output_closed
        || emit_observations(
            invocation,
            vec![AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessQuiescent,
            }],
        )
        .await
        .is_err()
    {
        failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed)
    } else if let Some(status) = process_completion {
        parser.finish(status.success())
    } else {
        failed_agent_outcome(parser.failure_for_current_phase())
    };
    match outcome {
        AgentOutcome::Failed(failure)
            if matches!(
                failure.cause(),
                AgentFailureCause::HarnessSetupFailed { .. }
                    | AgentFailureCause::HarnessProtocolFailed
            ) =>
        {
            AgentOutcome::Failed(AgentFailure::with_protocol_rejection(
                failure.cause().clone(),
                protocol_rejection.clone(),
            ))
        }
        outcome => outcome,
    }
}

async fn begin_cooperative_interrupt<Clock: CoordinatorClock>(
    standard_input: &mut Option<UnixStream>,
    parser: &mut CodexAppServerV1Parser,
    clock: &mut Clock,
    write_grace: Duration,
) -> Result<(), AgentFailureCause> {
    let active_turn = parser.request_turn_interrupt()?;
    write_pending_frames(standard_input, parser, clock, write_grace).await?;
    if !active_turn {
        close_standard_input(standard_input)
            .await
            .map_err(|()| parser.failure_for_current_phase())?;
    }
    Ok(())
}

// Codex applies correction frames and accepted-result stdin closure inside its JSON-RPC
// driver; sharing Claude's exchange progress helper would couple distinct protocols.
// jscpd:ignore-start
async fn apply_result_progress<Clock: CoordinatorClock>(
    progress: Result<ParserProgress, AgentFailureCause>,
    standard_input: &mut Option<UnixStream>,
    parser: &mut CodexAppServerV1Parser,
    clock: &mut Clock,
    write_grace: Duration,
) -> Result<(), AgentFailureCause> {
    let progress = progress?;
    write_pending_frames(standard_input, parser, clock, write_grace).await?;
    if progress.close_standard_input {
        close_standard_input(standard_input)
            .await
            .map_err(|()| AgentFailureCause::HarnessProtocolFailed)?;
    }
    Ok(())
}
// jscpd:ignore-end

// Codex owns when its accepted-turn settlement timer and group probes are active;
// sharing another profile's waits would couple native terminal state machines.
// jscpd:ignore-start
async fn wait_for_result_settlement(wait: &mut Option<ResultSettlementWait>) {
    match wait {
        Some(wait) => wait.await,
        None => pending().await,
    }
}

async fn wait_for_process_group_probe<Clock: CoordinatorClock>(clock: &mut Clock) {
    let deadline = clock.now().add(PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL);
    clock.clone().wait_until(deadline).await;
}
// jscpd:ignore-end

async fn write_pending_frames<Clock: CoordinatorClock>(
    standard_input: &mut Option<UnixStream>,
    parser: &mut CodexAppServerV1Parser,
    clock: &mut Clock,
    write_grace: Duration,
) -> Result<(), AgentFailureCause> {
    let failure = parser.failure_for_current_phase();
    let deadline = clock.now() + write_grace;
    let deadline_clock = clock.clone();
    let write = async {
        while let Some(frame) = parser.take_outbound() {
            let Some(input) = standard_input.as_mut() else {
                return Err(());
            };
            input.write_all(&frame).await.map_err(|_| ())?;
        }
        Ok(())
    };
    tokio::select! {
        biased;
        result = write => result.map_err(|()| failure.clone()),
        () = deadline_clock.wait_until(deadline) => Err(failure),
    }
}

// Codex closes this stream only after its native terminal frame; keeping the helper local
// prevents another streaming profile from acquiring that protocol transition.
// jscpd:ignore-start
async fn close_standard_input(standard_input: &mut Option<UnixStream>) -> Result<(), ()> {
    let Some(mut input) = standard_input.take() else {
        return Ok(());
    };
    match input.shutdown().await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
        Err(_) => Err(()),
    }
}
// jscpd:ignore-end

async fn emit_observations<Sink>(
    invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
    observations: Vec<AgentObservation>,
) -> Result<(), ()>
where
    Sink: AgentObservationSink,
{
    for observation in observations {
        invocation
            .observations()
            .emit(observation)
            .await
            .map_err(|_| ())?;
    }
    Ok(())
}

async fn supervise_process_group(
    process_group: Pid,
    cancellation: CancellationSource,
    mut directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    cooperative_interrupt: mpsc::UnboundedSender<()>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let accepted_cancellation = cancellation.wait_for_cancellation();
    tokio::pin!(accepted_cancellation);
    let mut cancellation_observed = false;
    let mut interrupt_requested = false;
    let mut directives_open = true;
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return,
            // The supervisor keeps forced containment independent from blocked stdio, but
            // routes every graceful request back through App Server's native interrupt.
            // jscpd:ignore-start
            _ = &mut accepted_cancellation, if !cancellation_observed => {
                cancellation_observed = true;
                if !interrupt_requested {
                    let _ = cooperative_interrupt.send(());
                    interrupt_requested = true;
                }
            }
            directive = directives.recv(), if directives_open => {
                match directive {
                    Some(AgentProcessDirective::Interrupt) if !interrupt_requested => {
                        let _ = cooperative_interrupt.send(());
                        interrupt_requested = true;
                    }
                    Some(AgentProcessDirective::Interrupt) => {}
                    Some(AgentProcessDirective::Force) => {
                        terminate_process_group(process_group);
                        return;
                    }
                    None => directives_open = false,
                }
            }
            // jscpd:ignore-end
        }
    }
}

fn setup_failure(stage: AgentHarnessSetupStage) -> AgentFailureCause {
    AgentFailureCause::HarnessSetupFailed { stage }
}

fn setup_failed(stage: AgentHarnessSetupStage) -> AgentOutcome {
    failed_agent_outcome(setup_failure(stage))
}
