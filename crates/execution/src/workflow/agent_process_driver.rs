//! Common guarded process lifecycle for the native streaming adapters.
mod process_loop;
pub(super) use process_loop::{ProcessOutput, Protocol, State, Supervisor, drive, drive_signalled};
#[cfg(test)]
pub(crate) mod test_support;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::File;
use std::future::{Future, pending};
use std::io;
use std::io::Read as _;
use std::num::NonZeroU64;
use std::ops::Add as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::pin::Pin;
use std::process::ExitStatus;
use std::time::Duration;

use rustix::process::Pid;
use tokio::io::AsyncReadExt as _;
use tokio::net::UnixStream;
use tokio::process::{ChildStderr, ChildStdout};
use tokio::sync::{mpsc, oneshot};

use super::admission::CancellationSource;
use super::agent::{
    AgentAdapter, AgentCompatibilityProfile, AgentFailureCause, AgentInputKind, AgentInvocation,
    AgentInvocationIdentity, AgentObservation, AgentObservationSink, AgentOutcome,
    AgentProcessDirective, AgentStartCallback, AgentTerminalCallback, AgentValueMode,
    PositiveDuration, StagedAgentAttachment, check_agent_input_bound, failed_agent_outcome,
    run_cancellable_blocking_launch,
};
use super::child_guard::{ChildGuardCancellation, StoppedChildGuard};
use super::coordinator::CoordinatorClock;
use super::diagnostic::{PendingStepDiagnostic, StepDiagnosticLog};
use super::observation::ExecutionObserver;
use super::process_group::{
    ProcessGuardRegistration, ProcessGuardRegistry, interrupt_process_group,
    mark_process_guard_quiesced, process_group_is_quiescent, reap_process_group_children,
    terminate_authenticated_process_group, terminate_process_group,
};
use super::result_validation::{
    AuthoritativeResultValidator, ProcessResultValidationWorker, ResultValidationWorker,
};
use tracing::Instrument as _;

pub(super) async fn report_invocation(
    profile: &'static str,
    identity: AgentInvocationIdentity,
    cancellation: CancellationSource,
    terminal: AgentTerminalCallback,
    work: impl Future<Output = AgentOutcome>,
) {
    let span = tracing::info_span!("agent_invocation", profile,
        step = %identity.step(),
        sequence = identity.invocation().transition_sequence.get());
    let outcome = work.instrument(span).await;
    let outcome = cancellation
        .cancellation_reason()
        .map_or(outcome, |reason| AgentOutcome::Cancelled { reason });
    let _ = terminal.report(outcome);
}

pub(crate) trait NativeProcessProfile<Clock, Observer, Worker, Sink>:
    Clone + Send + Sync + 'static
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type Configuration: Send + Sync + 'static;
    type Limits: Send + Sync + 'static;
    const NAME: &'static str;
    fn invoke(
        adapter: &AdapterCore<Clock, Observer, Worker, Self>,
        invocation: AgentInvocation<Self::Configuration, Self::Limits, Sink>,
        started: &AgentStartCallback,
    ) -> impl Future<Output = AgentOutcome> + Send
    where
        Self: Sized;
}

impl<Clock, Observer, Worker, Sink, Profile> AgentAdapter<Sink>
    for AdapterCore<Clock, Observer, Worker, Profile>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
    Profile: NativeProcessProfile<Clock, Observer, Worker, Sink>,
{
    type NativeConfiguration = Profile::Configuration;
    type ProtocolLimits = Profile::Limits;

    async fn invoke(
        &self,
        invocation: AgentInvocation<Self::NativeConfiguration, Self::ProtocolLimits, Sink>,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) {
        report_invocation(
            Profile::NAME,
            invocation.identity().clone(),
            invocation.cancellation().clone(),
            terminal,
            Profile::invoke(self, invocation, &started),
        )
        .await;
    }
}

pub(crate) struct AdapterCore<Clock, Observer, Worker, Profile = ()> {
    pub(super) profile: Profile,
    pub(super) diagnostics: StepDiagnosticLog,
    pub(super) maximum_diagnostic_stream_bytes: NonZeroU64,
    pub(super) clock: Clock,
    pub(super) observer: Observer,
    pub(super) validation_worker: Worker,
}

impl<Clock: Clone, Observer: Clone, Worker: Clone, Profile: Clone> Clone
    for AdapterCore<Clock, Observer, Worker, Profile>
{
    fn clone(&self) -> Self {
        Self {
            profile: self.profile.clone(),
            diagnostics: self.diagnostics.clone(),
            maximum_diagnostic_stream_bytes: self.maximum_diagnostic_stream_bytes,
            clock: self.clock.clone(),
            observer: self.observer.clone(),
            validation_worker: self.validation_worker.clone(),
        }
    }
}

impl<Clock, Observer, Profile: Default>
    AdapterCore<Clock, Observer, ProcessResultValidationWorker, Profile>
{
    pub(super) fn new_default(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
    ) -> io::Result<Self> {
        Ok(Self::with_worker(
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            ProcessResultValidationWorker::for_current_executable()?,
        ))
    }
}

impl<Clock, Observer, Worker, Profile> AdapterCore<Clock, Observer, Worker, Profile> {
    pub(super) fn with_profile(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
        validation_worker: Worker,
        profile: Profile,
    ) -> Self {
        Self {
            profile,
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            validation_worker,
        }
    }

    pub(super) async fn launch_stdio<Config, Limits, Sink, Plan>(
        &self,
        invocation: AgentInvocation<Config, Limits, Sink>,
        plan: Plan,
        join_failure: AgentFailureCause,
    ) -> Result<
        (
            AgentInvocation<Config, Limits, Sink>,
            Plan,
            StdioProcess,
            ChildStderr,
            mpsc::UnboundedReceiver<AgentProcessDirective>,
        ),
        AgentOutcome,
    >
    where
        Config: Send + Sync + 'static,
        Limits: Send + Sync + 'static,
        Sink: AgentObservationSink,
        Plan: StdioLaunchPlan<Config, Limits, Sink> + Send + 'static,
    {
        let cancellation_source = invocation.cancellation().clone();
        let ((invocation, plan, launched), cancelled) =
            run_cancellable_blocking_launch(&cancellation_source, move |cancellation| {
                let launched = plan.launch(&invocation, &cancellation);
                (invocation, plan, launched)
            })
            .await
            .map_err(|_| failed_agent_outcome(join_failure.clone()))?;
        if let Some(reason) = cancelled {
            if let Ok((mut process, _)) = launched {
                let _ = process.child.force_stop(process.process_group).await;
            }
            return Err(AgentOutcome::Cancelled { reason });
        }
        match launched {
            Ok((mut process, stderr)) => {
                let mut invocation = invocation;
                let Some(directives) = take_process_directives(
                    &mut invocation,
                    &mut process.child,
                    process.process_group,
                )
                .await
                else {
                    return Err(failed_agent_outcome(join_failure));
                };
                Ok((invocation, plan, process, stderr, directives))
            }
            Err(cause) => {
                self.diagnostics.record_agent_start_failure(
                    invocation.identity(),
                    self.maximum_diagnostic_stream_bytes,
                    &cause,
                );
                Err(failed_agent_outcome(cause))
            }
        }
    }

    pub(super) async fn prepare_invocation<Config, Limits, Sink, Plan>(
        &self,
        invocation: AgentInvocation<Config, Limits, Sink>,
        prepare: impl FnOnce(&AgentInvocation<Config, Limits, Sink>) -> Result<Plan, AgentFailureCause>
        + Send
        + 'static,
        join_failure: AgentFailureCause,
    ) -> Result<(AgentInvocation<Config, Limits, Sink>, Plan), AgentOutcome>
    where
        Config: Send + Sync + 'static,
        Limits: Send + Sync + 'static,
        Sink: AgentObservationSink,
        Plan: Send + 'static,
    {
        match tokio::task::spawn_blocking(move || {
            let plan = prepare(&invocation);
            (invocation, plan)
        })
        .await
        {
            Ok((invocation, Ok(plan))) => Ok((invocation, plan)),
            Ok((invocation, Err(cause))) => {
                self.diagnostics.record_agent_start_failure(
                    invocation.identity(),
                    self.maximum_diagnostic_stream_bytes,
                    &cause,
                );
                Err(failed_agent_outcome(cause))
            }
            Err(_) => Err(failed_agent_outcome(join_failure)),
        }
    }
}

impl<Clock, Observer, Worker, Profile: Default> AdapterCore<Clock, Observer, Worker, Profile> {
    pub(super) fn with_worker(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
        validation_worker: Worker,
    ) -> Self {
        Self::with_profile(
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            validation_worker,
            Profile::default(),
        )
    }
}

impl<Clock: CoordinatorClock, Observer, Worker: ResultValidationWorker, Profile>
    AdapterCore<Clock, Observer, Worker, Profile>
{
    pub(super) fn start_diagnostic<Config, Limits, Sink: AgentObservationSink>(
        &self,
        invocation: &AgentInvocation<Config, Limits, Sink>,
        standard_error: ChildStderr,
    ) -> PendingStepDiagnostic
    where
        Observer: ExecutionObserver<Clock::Instant>,
    {
        self.diagnostics.start_standard_error_capture(
            invocation.identity().step().to_owned(),
            invocation.identity().invocation(),
            self.maximum_diagnostic_stream_bytes,
            standard_error,
            self.observer.clone(),
        )
    }

    pub(super) fn result_validator<Config, Limits, Sink: AgentObservationSink>(
        &self,
        invocation: &AgentInvocation<Config, Limits, Sink>,
    ) -> Option<AuthoritativeResultValidator<Clock, Worker>> {
        let AgentValueMode::Result { schema, .. } = invocation.value_mode() else {
            return None;
        };
        Some(AuthoritativeResultValidator::new(
            schema.clone(),
            invocation.limits().maximum_result_bytes(),
            invocation
                .limits()
                .maximum_result_rejection_feedback_bytes(),
            invocation.limits().result_validation_deadline(),
            self.clock.clone(),
            self.validation_worker.clone(),
        ))
    }
}

pub(super) struct GuardedProcess {
    pub(super) child: GuardedChild,
    pub(super) process_group: Pid,
    pub(super) standard_input: Option<UnixStream>,
    pub(super) standard_output: ChildStdout,
    pub(super) standard_error: ChildStderr,
}

pub(super) struct StdioProcess {
    pub(super) child: GuardedChild,
    pub(super) process_group: Pid,
    pub(super) standard_input: UnixStream,
    pub(super) standard_output: ChildStdout,
}

impl StdioProcess {
    pub(super) fn split(self) -> (UnixStream, ProcessOutput) {
        (
            self.standard_input,
            ProcessOutput {
                child: self.child,
                process_group: self.process_group,
                standard_output: self.standard_output,
            },
        )
    }
}

impl GuardedProcess {
    pub(super) fn require_standard_input(
        self,
        missing: impl FnOnce() -> AgentFailureCause,
    ) -> Result<(StdioProcess, ChildStderr), AgentFailureCause> {
        let Self {
            mut child,
            process_group,
            standard_input,
            standard_output,
            standard_error,
        } = self;
        let Some(standard_input) = standard_input else {
            child.force_stop_blocking();
            return Err(missing());
        };
        Ok((
            StdioProcess {
                child,
                process_group,
                standard_input,
                standard_output,
            },
            standard_error,
        ))
    }
}

pub(super) async fn take_process_directives<Config, Limits, Sink: AgentObservationSink>(
    invocation: &mut AgentInvocation<Config, Limits, Sink>,
    child: &mut GuardedChild,
    process_group: Pid,
) -> Option<mpsc::UnboundedReceiver<AgentProcessDirective>> {
    if let Some(directives) = invocation.take_process_directives() {
        Some(directives)
    } else {
        let _ = child.force_stop(process_group).await;
        None
    }
}

pub(super) fn verify_session_binding<E: std::fmt::Display>(
    binding: Result<(), E>,
    stage: &'static str,
) -> Result<(), AgentFailureCause> {
    binding.map_err(|error| AgentFailureCause::start_failure(stage, error))
}

pub(super) fn bind_agent_command<Config, Limits, Sink: AgentObservationSink>(
    invocation: &AgentInvocation<Config, Limits, Sink>,
    command: &mut std::process::Command,
) -> io::Result<()> {
    invocation
        .process()
        .bind_command(command)
        .map_err(|_| io::Error::other("agent working directory is unavailable"))
}

pub(super) fn require_native_profile<Config, Limits, Sink: AgentObservationSink>(
    invocation: &AgentInvocation<Config, Limits, Sink>,
    expected: AgentCompatibilityProfile,
    compatible_version: bool,
    failure: impl FnOnce() -> AgentFailureCause,
) -> Result<(), AgentFailureCause> {
    if invocation.adapter().profile() != expected
        || !compatible_version
        || !invocation.adapter().executable().is_absolute()
    {
        Err(failure())
    } else {
        Ok(())
    }
}

pub(super) fn check_prompt_bounds<Config, Limits, Sink: AgentObservationSink>(
    invocation: &AgentInvocation<Config, Limits, Sink>,
) -> Result<(), AgentFailureCause> {
    check_agent_input_bound(
        invocation.prompt().system_prompt(),
        invocation.limits().maximum_system_prompt_bytes(),
        AgentInputKind::SystemPrompt,
    )?;
    check_agent_input_bound(
        invocation.prompt().message(),
        invocation.limits().maximum_message_bytes(),
        AgentInputKind::Message,
    )
}

pub(super) trait StdioLaunchPlan<Config, Limits, Sink: AgentObservationSink> {
    fn arguments(&self) -> &[OsString];
    fn environment(
        &self,
        invocation: &AgentInvocation<Config, Limits, Sink>,
    ) -> Vec<(OsString, OsString)>;
    fn verify_binding(
        &self,
        invocation: &AgentInvocation<Config, Limits, Sink>,
    ) -> Result<(), AgentFailureCause>;
    fn spawn_stage(&self) -> &'static str;
    fn release_stage(&self) -> &'static str;
    fn guard_failure(&self) -> AgentFailureCause;

    fn launch(
        &self,
        invocation: &AgentInvocation<Config, Limits, Sink>,
        cancellation: &ChildGuardCancellation,
    ) -> Result<(StdioProcess, ChildStderr), AgentFailureCause> {
        Launch::for_invocation(
            invocation,
            self.arguments(),
            &self.environment(invocation),
            cancellation,
            true,
        )
        .spawn(
            |command| bind_agent_command(invocation, command),
            || self.verify_binding(invocation),
            |error| AgentFailureCause::start_failure(self.spawn_stage(), error),
            |error| AgentFailureCause::start_failure(self.release_stage(), error),
            || self.guard_failure(),
        )?
        .require_standard_input(|| self.guard_failure())
    }
}

pub(super) fn invocation_environment<Config, Limits, Sink: AgentObservationSink>(
    invocation: &AgentInvocation<Config, Limits, Sink>,
) -> BTreeMap<OsString, OsString> {
    invocation
        .process()
        .environment()
        .variables()
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

pub(super) struct GuardedChild {
    child: StoppedChildGuard,
    registration: Option<ProcessGuardRegistration>,
}

impl GuardedChild {
    pub(super) fn force_stop_blocking(&mut self) {
        let _ = self.child.force_stop_blocking();
        if let Some(registration) = self.registration.as_mut() {
            let _ = registration.mark_quiesced();
        }
    }

    pub(super) fn force_process_group(&self, _process_group: Pid) {
        let _ = terminate_authenticated_process_group(self.child.identity());
    }

    pub(super) async fn wait(&mut self) -> Result<ExitStatus, ()> {
        let status = self.child.wait().await.map_err(|_| ())?;
        mark_process_guard_quiesced(&mut self.registration).await?;
        Ok(status)
    }

    pub(super) async fn force_stop(&mut self, process_group: Pid) -> Result<(), ()> {
        self.force_process_group(process_group);
        self.child.force_stop().await.map_err(|_| ())?;
        mark_process_guard_quiesced(&mut self.registration).await?;
        if process_group_is_quiescent(process_group) {
            Ok(())
        } else {
            Err(())
        }
    }
}

/// A terminal report must follow child reaping. A failed wait still forces the
/// authenticated group before the protocol chooses its own failure classification.
pub(super) async fn settle_child(
    child: &mut GuardedChild,
    process_group: Pid,
    completion: &mut Option<ExitStatus>,
) {
    if completion.is_none() {
        *completion = child.wait().await.ok();
        if completion.is_none() {
            let _ = child.force_stop(process_group).await;
        }
    }
}

pub(super) fn group_is_quiescent(process_group: Pid) -> bool {
    reap_process_group_children(process_group);
    process_group_is_quiescent(process_group)
}

/// All three stream protocols use the same EOF and interrupted-read boundary.
/// A read error is returned intact so each parser can classify its own phase.
pub(super) async fn read_standard_output(
    output: &mut ChildStdout,
    buffer: &mut [u8],
) -> io::Result<Option<usize>> {
    loop {
        match output.read(buffer).await {
            Ok(0) => return Ok(None),
            Ok(count) => return Ok(Some(count)),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

pub(super) struct Launch<'a> {
    pub(super) executable: &'a Path,
    pub(super) arguments: &'a [OsString],
    pub(super) environment: &'a [(OsString, OsString)],
    pub(super) guards: &'a ProcessGuardRegistry,
    pub(super) step: &'a str,
    pub(super) sequence: u64,
    pub(super) cancellation: &'a ChildGuardCancellation,
    pub(super) stdin: bool,
}

impl<'a> Launch<'a> {
    pub(super) fn for_invocation<Config, Limits, Sink: AgentObservationSink>(
        invocation: &'a AgentInvocation<Config, Limits, Sink>,
        arguments: &'a [OsString],
        environment: &'a [(OsString, OsString)],
        cancellation: &'a ChildGuardCancellation,
        stdin: bool,
    ) -> Self {
        Self {
            executable: invocation.adapter().executable(),
            arguments,
            environment,
            guards: invocation.process_guards(),
            step: invocation.identity().step(),
            sequence: invocation.identity().invocation().transition_sequence.get(),
            cancellation,
            stdin,
        }
    }

    /// Register before releasing the stopped child, including when the registry has no
    /// durable backing store. On any failed handshake, stop the child before returning.
    pub(super) fn spawn(
        self,
        bind: impl FnOnce(&mut std::process::Command) -> io::Result<()>,
        verify_binding: impl FnOnce() -> Result<(), AgentFailureCause>,
        spawn_error: impl FnOnce(io::Error) -> AgentFailureCause,
        release_error: impl FnOnce(io::Error) -> AgentFailureCause,
        guard_error: impl Fn() -> AgentFailureCause,
    ) -> Result<GuardedProcess, AgentFailureCause> {
        let (mut child, standard_input) = if self.stdin {
            let (child, stdin) = StoppedChildGuard::spawn_with_stdin_cancellable(
                self.executable,
                self.arguments,
                self.environment,
                self.cancellation,
                bind,
            )
            .map_err(spawn_error)?;
            (child, Some(stdin))
        } else {
            (
                StoppedChildGuard::spawn_cancellable(
                    self.executable,
                    self.arguments,
                    self.environment,
                    self.cancellation,
                    bind,
                )
                .map_err(spawn_error)?,
                None,
            )
        };
        let process_group = child.identity().process_group();
        let (Some(standard_output), Some(standard_error)) =
            (child.take_stdout(), child.take_stderr())
        else {
            let _ = child.force_stop_blocking();
            return Err(guard_error());
        };
        let mut registration =
            match self
                .guards
                .register(self.step, self.sequence, child.identity())
            {
                Ok(registration) => registration,
                Err(_) => {
                    let _ = child.force_stop_blocking();
                    return Err(guard_error());
                }
            };
        let released = verify_binding().and_then(|()| {
            child
                .continue_execution_cancellable(self.cancellation)
                .map_err(release_error)?;
            registration.mark_released().map_err(|_| guard_error())
        });
        if let Err(cause) = released {
            let _ = child.force_stop_blocking();
            let _ = registration.mark_quiesced();
            return Err(cause);
        }
        Ok(GuardedProcess {
            child: GuardedChild {
                child,
                registration: Some(registration),
            },
            process_group,
            standard_input,
            standard_output,
            standard_error,
        })
    }
}

/// The text-attachment classification and visible reference text are common to the
/// native transports; binary frame formats remain the responsibility of each adapter.
pub(super) fn validate_staged_attachments<'a>(
    attachments: &'a [StagedAgentAttachment],
    staging: &Path,
    maximum_count: usize,
    maximum_bytes: u64,
    failure: impl Fn(Option<io::Error>) -> AgentFailureCause,
) -> Result<Vec<(&'a StagedAgentAttachment, String, u64)>, AgentFailureCause> {
    if attachments.len() > maximum_count {
        return Err(failure(None));
    }
    let root = staging
        .parent()
        .map(|parent| parent.join("attachments"))
        .ok_or_else(|| failure(None))?;
    let mut validated = Vec::new();
    validated
        .try_reserve_exact(attachments.len())
        .map_err(|_| failure(None))?;
    let mut total_bytes = 0_u64;
    for (index, attachment) in attachments.iter().enumerate() {
        let identity = format!("{index:06}");
        let path = attachment.path();
        if !path.is_absolute()
            || path.parent() != Some(root.as_path())
            || path.file_name().and_then(|name| name.to_str()) != Some(identity.as_str())
        {
            return Err(failure(None));
        }
        let metadata = std::fs::symlink_metadata(path).map_err(|error| failure(Some(error)))?;
        if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o377 != 0 {
            return Err(failure(None));
        }
        total_bytes = total_bytes
            .checked_add(metadata.len())
            .filter(|total| *total <= maximum_bytes)
            .ok_or_else(|| failure(None))?;
        validated.push((attachment, identity, metadata.len()));
    }
    Ok(validated)
}

pub(super) fn attachment_media_type(media_type: &str) -> (&str, bool) {
    let base = media_type
        .split_once(';')
        .map_or(media_type, |(base, _)| base)
        .trim();
    let text = (base.len() > "text/".len() && base[.."text/".len()].eq_ignore_ascii_case("text/"))
        || base.eq_ignore_ascii_case("application/json");
    (base, text)
}

pub(super) fn attachment_text_content(
    identity: &str,
    media_type: &str,
    text: &str,
) -> serde_json::Value {
    serde_json::json!({
        "type": "text",
        "text": attachment_text(identity, media_type, text),
    })
}

pub(super) fn staged_attachment_reference(
    attachment: &StagedAgentAttachment,
    identity: &str,
    failure: impl FnOnce() -> AgentFailureCause,
) -> Result<serde_json::Value, AgentFailureCause> {
    let path = attachment.path().to_str().ok_or_else(failure)?;
    Ok(attachment_reference_content(
        identity,
        attachment.media_type(),
        path,
    ))
}

pub(super) fn attachment_reference_content(
    identity: &str,
    media_type: &str,
    path: &str,
) -> serde_json::Value {
    serde_json::json!({
        "type": "text",
        "text": attachment_reference(identity, media_type, path),
    })
}

pub(super) fn attachment_text(identity: &str, media_type: &str, text: &str) -> String {
    format!("Scherzo attachment {identity} ({media_type}) follows:\n{text}")
}

pub(super) fn attachment_reference(identity: &str, media_type: &str, path: &str) -> String {
    format!(
        "Scherzo attachment {identity} has media type {media_type} and is available to runner tools at {path}."
    )
}

pub(super) fn collect_stdout_observations<T>(
    parse: impl FnOnce(&mut dyn FnMut(AgentObservation)) -> T,
) -> (T, Vec<AgentObservation>) {
    let mut observations = Vec::new();
    let parsed = parse(&mut |observation| observations.push(observation));
    (parsed, observations)
}

pub(super) fn read_staged_attachment(
    attachment: &StagedAgentAttachment,
    expected_bytes: u64,
    allocation_error: impl Fn(&dyn std::error::Error) -> AgentFailureCause,
    open_error: impl Fn(io::Error) -> AgentFailureCause,
    read_error: impl Fn(io::Error) -> AgentFailureCause,
    length_error: impl Fn(usize) -> AgentFailureCause,
) -> Result<Vec<u8>, AgentFailureCause> {
    let capacity = usize::try_from(expected_bytes).map_err(|error| allocation_error(&error))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|error| allocation_error(&error))?;
    let mut file = File::open(attachment.path()).map_err(open_error)?;
    file.read_to_end(&mut bytes).map_err(read_error)?;
    if u64::try_from(bytes.len()) != Ok(expected_bytes) {
        return Err(length_error(bytes.len()));
    }
    Ok(bytes)
}

pub(super) enum WriteDeadline<E> {
    Failed(E),
    TimedOut,
}

pub(super) async fn write_until<Clock: CoordinatorClock, Output, Error>(
    clock: &mut Clock,
    timeout: Duration,
    write: impl Future<Output = Result<Output, Error>>,
) -> Result<Output, WriteDeadline<Error>> {
    let deadline = clock.now() + timeout;
    let deadline_clock = clock.clone();
    tokio::select! {
        biased;
        result = write => result.map_err(WriteDeadline::Failed),
        () = deadline_clock.wait_until(deadline) => Err(WriteDeadline::TimedOut),
    }
}

pub(super) async fn close_standard_input(input: &mut Option<UnixStream>) -> Result<(), ()> {
    let Some(input) = input.take() else {
        return Ok(());
    };
    match rustix::net::shutdown(&input, rustix::net::Shutdown::Write) {
        Ok(()) => Ok(()),
        Err(error) if error == rustix::io::Errno::NOTCONN => Ok(()),
        Err(_) => Err(()),
    }
}

pub(super) const PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(10);

pub(super) async fn wait_for_process_group_probe<Clock: CoordinatorClock>(clock: &mut Clock) {
    let deadline = clock.now() + PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL;
    clock.clone().wait_until(deadline).await;
}

/// Force requests remain effective even when the driver is blocked writing to stdin or
/// delivering an observation. The per-protocol interrupt action is supplied by the caller.
pub(super) struct Settlement<Clock> {
    pub(super) clock: Clock,
    pub(super) grace: PositiveDuration,
    pub(super) starts: mpsc::UnboundedReceiver<()>,
    pub(super) expired: mpsc::UnboundedSender<()>,
}

async fn wait_for_deadline(wait: &mut Option<Pin<Box<dyn Future<Output = ()> + Send>>>) {
    match wait {
        Some(wait) => wait.await,
        None => pending().await,
    }
}

pub(super) async fn supervise_process_group<Clock: CoordinatorClock>(
    process_group: Pid,
    cancellation: CancellationSource,
    mut directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    interrupt: impl Fn() + Send + 'static,
    mut shutdown: oneshot::Receiver<()>,
    mut settlement: Option<Settlement<Clock>>,
) {
    let mut settlement_deadline: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = None;
    let accepted_cancellation = cancellation.wait_for_cancellation();
    tokio::pin!(accepted_cancellation);
    let mut cancellation_observed = false;
    let mut interrupted = false;
    let mut directives_open = true;
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return,
            _ = &mut accepted_cancellation, if !cancellation_observed => {
                cancellation_observed = true;
                settlement_deadline = None;
                if !interrupted { interrupt(); interrupted = true; }
            }
            directive = directives.recv(), if directives_open => {
                match directive {
                    Some(AgentProcessDirective::Interrupt) if !interrupted => {
                        interrupt();
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
            start = async {
                match settlement.as_mut() {
                    Some(settlement) => settlement.starts.recv().await,
                    None => pending().await,
                }
            }, if settlement.is_some() => {
                match start {
                    Some(()) if settlement_deadline.is_none() && !cancellation_observed => {
                        if let Some(settlement) = settlement.as_mut() {
                            let deadline = settlement.clock.now().add(settlement.grace.get());
                            let clock = settlement.clock.clone();
                            settlement_deadline = Some(Box::pin(async move { clock.wait_until(deadline).await }));
                        }
                    }
                    Some(()) => {
                        terminate_process_group(process_group);
                        if let Some(settlement) = settlement.as_ref() { let _ = settlement.expired.send(()); }
                        return;
                    }
                    None => settlement = None,
                }
            }
            () = wait_for_deadline(&mut settlement_deadline), if settlement_deadline.is_some() => {
                terminate_process_group(process_group);
                if let Some(settlement) = settlement.as_ref() { let _ = settlement.expired.send(()); }
                return;
            }
        }
    }
}

pub(super) fn signal_interrupt(process_group: Pid) {
    interrupt_process_group(process_group);
}
