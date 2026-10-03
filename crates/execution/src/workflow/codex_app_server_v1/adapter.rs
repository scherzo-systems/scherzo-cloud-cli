use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::future::{Future, pending};
use std::io;
use std::num::NonZeroU64;
use std::ops::Add as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt as _;
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use super::input::initial_turn_input;
use super::{
    CodexAppServerV1Parser, CodexAppServerV1ProtocolLimits, CodexAppServerV1RejectionReason,
    ParserProgress,
};
use crate::codex::{CodexCompatibilityProfile, compatibility_profile_for_version};
use crate::workflow::agent::{
    AgentCompatibilityProfile, AgentFailure, AgentFailureCause, AgentHarnessSetupStage,
    AgentInvocation, AgentLifecycleMilestone, AgentObservation, AgentObservationSink, AgentOutcome,
    AgentProcessDirective, AgentStartCallback, PositiveDuration, failed_agent_outcome,
    finish_agent_diagnostic_capture,
};
use crate::workflow::agent_process_driver::{
    self, StdioProcess, WriteDeadline, close_standard_input,
};
use crate::workflow::codex::CodexConfig;
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::observation::ExecutionObserver;
use crate::workflow::result_validation::{
    AuthoritativeResultValidator, ProcessResultValidationWorker, ResultValidationDecision,
    ResultValidationOutcome, ResultValidationWorker,
};

#[derive(Clone)]
pub(crate) struct CodexProfile {
    client_version: Arc<str>,
    model_provider_override: Option<Arc<dyn Fn() -> Option<Arc<str>> + Send + Sync>>,
}

impl CodexProfile {
    fn selected_model_provider(&self) -> Option<Arc<str>> {
        self.model_provider_override
            .as_ref()
            .and_then(|provide| provide())
    }
}

pub(crate) type CodexAppServerV1Adapter<Clock, Observer, Worker = ProcessResultValidationWorker> =
    agent_process_driver::AdapterCore<Clock, Observer, Worker, CodexProfile>;

impl<Clock, Observer>
    agent_process_driver::AdapterCore<Clock, Observer, ProcessResultValidationWorker, CodexProfile>
{
    pub(crate) fn new(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
        client_version: Arc<str>,
    ) -> io::Result<Self> {
        Ok(Self::with_profile(
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            ProcessResultValidationWorker::for_current_executable()?,
            CodexProfile {
                client_version,
                model_provider_override: None,
            },
        ))
    }
}

impl<Clock, Observer, Worker>
    agent_process_driver::AdapterCore<Clock, Observer, Worker, CodexProfile>
{
    #[cfg(test)]
    pub(super) fn with_validation_worker(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
        validation_worker: Worker,
        client_version: Arc<str>,
        synthetic_model_provider: Option<Arc<str>>,
    ) -> Self {
        Self::with_profile(
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            validation_worker,
            CodexProfile {
                client_version,
                model_provider_override: synthetic_model_provider.map(|provider| {
                    Arc::new(move || Some(Arc::clone(&provider)))
                        as Arc<dyn Fn() -> Option<Arc<str>> + Send + Sync>
                }),
            },
        )
    }
}

impl<Clock, Observer, Worker, Sink>
    agent_process_driver::NativeProcessProfile<Clock, Observer, Worker, Sink> for CodexProfile
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type Configuration = CodexConfig;
    type Limits = CodexAppServerV1ProtocolLimits;
    const NAME: &'static str = "codex_app_server_v1";

    async fn invoke(
        adapter: &agent_process_driver::AdapterCore<Clock, Observer, Worker, Self>,
        invocation: AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
        started: &AgentStartCallback,
    ) -> AgentOutcome {
        adapter.invoke_inner(invocation, started).await
    }
}

// Codex setup and result-validator preparation precede the shared process loop;
// Codex's protocol hooks own native setup and correction transitions.
impl<Clock, Observer, Worker>
    agent_process_driver::AdapterCore<Clock, Observer, Worker, CodexProfile>
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
        let (invocation, plan) = match self
            .prepare_invocation(
                invocation,
                prepare_launch,
                setup_failure(AgentHarnessSetupStage::ExecutableLaunch),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        let result_validator = self.result_validator(&invocation);
        let (invocation, mut plan, process, standard_error, process_directives) = match self
            .launch_stdio(
                invocation,
                plan,
                setup_failure(AgentHarnessSetupStage::ExecutableLaunch),
            )
            .await
        {
            Ok(launched) => launched,
            Err(outcome) => return outcome,
        };
        let diagnostic = self.start_diagnostic(&invocation, standard_error);
        let configuration = invocation.adapter().native_configuration();
        let expected_cwd = Arc::clone(&plan.expected_cwd);
        let codex_home = Arc::clone(&plan.codex_home);
        let sqlite_home = Arc::clone(&plan.sqlite_home);
        let initial_input = std::mem::take(&mut plan.initial_input);
        let parser = match CodexAppServerV1Parser::profile(
            expected_cwd,
            codex_home,
            sqlite_home,
            Arc::clone(&self.profile.client_version),
            Arc::from(invocation.adapter().version()),
            Arc::from(configuration.model.as_str()),
            Arc::from(configuration.effort.as_str()),
            Arc::from(invocation.prompt().system_prompt()),
            initial_input,
            self.profile.selected_model_provider(),
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
            ProcessTimingConfiguration {
                clock: self.clock.clone(),
                result_settlement_grace: invocation.limits().result_settlement_grace(),
                standard_input_write_timeout: invocation
                    .limits()
                    .adapter_protocol()
                    .standard_input_write_timeout(),
                post_failure_cleanup_timeout: invocation
                    .limits()
                    .adapter_protocol()
                    .post_failure_cleanup_timeout(),
            },
        )
        .await;
        finish_agent_diagnostic_capture(invocation.diagnostic_session(), diagnostic, &outcome)
            .await;
        if let AgentOutcome::Failed(failure) = &outcome
            && let AgentFailureCause::HarnessSetupRejected { stage, message } = failure.cause()
        {
            self.diagnostics.record_adapter_error(
                invocation.identity().step().to_owned(),
                invocation.identity().invocation(),
                self.maximum_diagnostic_stream_bytes,
                &format!("{stage:?} rejected"),
                message,
            );
        }
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
    agent_process_driver::check_prompt_bounds(invocation)?;
    agent_process_driver::require_native_profile(
        invocation,
        AgentCompatibilityProfile::CodexAppServerV1,
        compatibility_profile_for_version(invocation.adapter().version())
            == Some(CodexCompatibilityProfile::CodexAppServerV1),
        || setup_failure(AgentHarnessSetupStage::ExecutableLaunch),
    )?;
    agent_process_driver::verify_session_binding(
        invocation.diagnostic_session().verify_path_binding(),
        "codex diagnostic session binding",
    )?;
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
        .map_err(|error| AgentFailureCause::start_failure("codex staging directory", error))?;
    let canonical_codex_home = std::fs::canonicalize(codex_home)
        .map_err(|error| AgentFailureCause::start_failure("codex home", error))?;
    if canonical_staging != staging
        || !canonical_staging.is_absolute()
        || canonical_staging.starts_with(canonical_codex_home)
    {
        return Err(setup_failure(AgentHarnessSetupStage::ExecutableLaunch));
    }
    let sqlite_state = tempfile::Builder::new()
        .prefix("codex-sqlite-")
        .tempdir_in(canonical_staging)
        .map_err(|error| AgentFailureCause::start_failure("codex sqlite tempdir", error))?;
    std::fs::set_permissions(sqlite_state.path(), std::fs::Permissions::from_mode(0o700))
        .map_err(|error| AgentFailureCause::start_failure("codex sqlite permissions", error))?;
    Ok(sqlite_state)
}

type LaunchedCodexProcess = StdioProcess;

impl<Sink: AgentObservationSink>
    agent_process_driver::StdioLaunchPlan<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>
    for CodexAppServerV1LaunchPlan
{
    fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
    fn environment(
        &self,
        invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
    ) -> Vec<(OsString, OsString)> {
        agent_process_driver::invocation_environment(invocation)
            .into_iter()
            .collect()
    }
    fn verify_binding(
        &self,
        invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
    ) -> Result<(), AgentFailureCause> {
        agent_process_driver::verify_session_binding(
            invocation.diagnostic_session().verify_path_binding(),
            "codex diagnostic session binding",
        )
    }
    fn spawn_stage(&self) -> &'static str {
        "codex process spawn"
    }
    fn release_stage(&self) -> &'static str {
        "codex process release"
    }
    fn guard_failure(&self) -> AgentFailureCause {
        setup_failure(AgentHarnessSetupStage::ExecutableLaunch)
    }
}

struct ProcessTimingConfiguration<Clock> {
    clock: Clock,
    result_settlement_grace: PositiveDuration,
    standard_input_write_timeout: Duration,
    post_failure_cleanup_timeout: Duration,
}

type ResultSettlementWait = Pin<Box<dyn Future<Output = ()> + Send>>;

enum CodexExtra {
    Interrupt(Option<()>),
    CleanupDeadline,
    SettlementDeadline,
}

struct CodexProtocol<'a, Clock, Worker, Sink> {
    invocation: &'a AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
    started: &'a AgentStartCallback,
    parser: CodexAppServerV1Parser,
    result_validator: Option<AuthoritativeResultValidator<Clock, Worker>>,
    standard_input: Option<UnixStream>,
    cooperative_interrupts: mpsc::UnboundedReceiver<()>,
    cooperative_interrupt_started: bool,
    cooperative_interrupts_open: bool,
    cleanup_deadline: ResultSettlementWait,
    cleanup_deadline_armed: bool,
    result_settlement_wait: Option<ResultSettlementWait>,
    write_timeout: Duration,
    cleanup_timeout: Duration,
    settlement_grace: PositiveDuration,
    start_reported: bool,
    failure: Option<AgentFailureCause>,
}

impl<Clock, Worker, Sink> CodexProtocol<'_, Clock, Worker, Sink>
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    fn arm_cleanup(&mut self, mut clock: Clock) {
        let deadline = clock.now() + self.cleanup_timeout;
        let deadline_clock = clock;
        self.cleanup_deadline = Box::pin(async move { deadline_clock.wait_until(deadline).await });
        self.cleanup_deadline_armed = true;
    }

    async fn interrupt(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if self.cooperative_interrupt_started {
            return;
        }
        self.cooperative_interrupt_started = true;
        if begin_cooperative_interrupt(
            &mut self.standard_input,
            &mut self.parser,
            &mut state.clock,
            self.write_timeout,
        )
        .await
        .is_err()
        {
            state.parser_enabled = false;
            let _ = close_standard_input(&mut self.standard_input).await;
        }
    }
}

impl<Clock, Worker, Sink> agent_process_driver::Protocol<Clock>
    for CodexProtocol<'_, Clock, Worker, Sink>
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type Extra = CodexExtra;

    async fn on_start(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if let Err(cause) = write_pending_frames(
            &mut self.standard_input,
            &mut self.parser,
            &mut state.clock,
            self.write_timeout,
        )
        .await
        {
            self.failure = Some(cause);
            state.parser_enabled = false;
            self.arm_cleanup(state.clock.clone());
        }
    }

    fn extra_enabled(&self, _state: &agent_process_driver::State<Clock>) -> bool {
        (self.cooperative_interrupts_open && !self.cooperative_interrupt_started)
            || self.cleanup_deadline_armed
            || self.result_settlement_wait.is_some()
    }

    async fn extra(&mut self) -> CodexExtra {
        tokio::select! {
            biased;
            requested = self.cooperative_interrupts.recv(), if self.cooperative_interrupts_open && !self.cooperative_interrupt_started => CodexExtra::Interrupt(requested),
            () = &mut self.cleanup_deadline, if self.cleanup_deadline_armed => CodexExtra::CleanupDeadline,
            () = wait_for_result_settlement(&mut self.result_settlement_wait), if self.result_settlement_wait.is_some() => CodexExtra::SettlementDeadline,
        }
    }

    async fn on_extra(
        &mut self,
        event: CodexExtra,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        match event {
            CodexExtra::Interrupt(Some(())) => {
                if let Some(reason) = state.cancellation.cancellation_reason() {
                    state.cancelled = Some(reason);
                }
                self.interrupt(state).await;
            }
            CodexExtra::Interrupt(None) => self.cooperative_interrupts_open = false,
            CodexExtra::CleanupDeadline => {
                self.cleanup_deadline_armed = false;
                state.force_group();
            }
            CodexExtra::SettlementDeadline => {
                self.result_settlement_wait = None;
                self.failure = Some(AgentFailureCause::ResultSettlementFailed);
                state.parser_enabled = false;
                self.standard_input.take();
                state.force_group();
            }
        }
    }

    async fn on_cancel(
        &mut self,
        _reason: crate::workflow::admission::CancellationReason,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        self.interrupt(state).await;
    }

    async fn on_stdout(&mut self, bytes: &[u8], state: &mut agent_process_driver::State<Clock>) {
        let (parsed, observations) = agent_process_driver::collect_stdout_observations(|emit| {
            self.parser.push_stdout(bytes, emit)
        });
        match parsed {
            Ok(progress) => {
                if state.cancelled.is_none()
                    && let Some(reason) = state.cancellation.cancellation_reason()
                {
                    state.cancelled = Some(reason);
                    self.interrupt(state).await;
                }
                if state.cancelled.is_none() && progress.start_acknowledged {
                    if self.start_reported || self.started.report().is_err() {
                        self.parser.record_rejection(
                            CodexAppServerV1RejectionReason::StartAcknowledgementFailed,
                        );
                        self.failure = Some(AgentFailureCause::HarnessSetupFailed {
                            stage: AgentHarnessSetupStage::StartAcknowledgement,
                        });
                        state.parser_enabled = false;
                        state.force_group();
                    } else {
                        self.start_reported = true;
                    }
                }
                if state.parser_enabled
                    && state.cancelled.is_none()
                    && emit_observations(self.invocation, observations)
                        .await
                        .is_err()
                {
                    self.failure =
                        Some(self.parser.failure_for(
                            CodexAppServerV1RejectionReason::ObservationDeliveryFailed,
                        ));
                    state.parser_enabled = false;
                    state.force_group();
                }
                if state.parser_enabled
                    && let Err(cause) = write_pending_frames(
                        &mut self.standard_input,
                        &mut self.parser,
                        &mut state.clock,
                        self.write_timeout,
                    )
                    .await
                {
                    if state.cancelled.is_none() {
                        self.failure = Some(cause);
                        self.arm_cleanup(state.clock.clone());
                    }
                    state.parser_enabled = false;
                }
                if progress.close_standard_input
                    && close_standard_input(&mut self.standard_input)
                        .await
                        .is_err()
                {
                    if state.cancelled.is_none() {
                        self.failure = Some(self.parser.failure_for(
                            CodexAppServerV1RejectionReason::StandardInputCloseFailed,
                        ));
                        state.force_group();
                    }
                    state.parser_enabled = false;
                }
            }
            Err(mut cause) => {
                if state.cancelled.is_none()
                    && !self.start_reported
                    && self.parser.start_acknowledged()
                {
                    if self.started.report().is_err() {
                        self.parser.record_rejection(
                            CodexAppServerV1RejectionReason::StartAcknowledgementFailed,
                        );
                        cause = AgentFailureCause::HarnessSetupFailed {
                            stage: AgentHarnessSetupStage::StartAcknowledgement,
                        };
                    } else {
                        self.start_reported = true;
                    }
                }
                if state.cancelled.is_none()
                    && emit_observations(self.invocation, observations)
                        .await
                        .is_err()
                {
                    cause = self
                        .parser
                        .failure_for(CodexAppServerV1RejectionReason::ObservationDeliveryFailed);
                }
                self.parser.prevent_value_commit();
                let _ = self.parser.request_turn_interrupt();
                let _ = write_pending_frames(
                    &mut self.standard_input,
                    &mut self.parser,
                    &mut state.clock,
                    self.write_timeout,
                )
                .await;
                let _ = close_standard_input(&mut self.standard_input).await;
                state.parser_enabled = false;
                if state.cancelled.is_none() {
                    self.failure = Some(cause);
                    self.arm_cleanup(state.clock.clone());
                }
            }
        }
    }

    fn classify_read_failure(&mut self) {
        self.failure = Some(
            self.parser
                .failure_for(CodexAppServerV1RejectionReason::ProcessOutputReadFailed),
        );
    }

    async fn on_wait_error(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if state.cancelled.is_none() {
            self.failure.get_or_insert_with(|| {
                self.parser
                    .failure_for(CodexAppServerV1RejectionReason::ProcessWaitFailed)
            });
        }
        state.force_group();
    }

    async fn after_event(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if state.parser_enabled
            && let Some(candidate) = self.parser.take_result_candidate()
        {
            let Some(validator) = self.result_validator.as_mut() else {
                self.failure = Some(
                    self.parser
                        .failure_for(CodexAppServerV1RejectionReason::ResultValidatorMissing),
                );
                state.parser_enabled = false;
                self.standard_input.take();
                state.force_group();
                return;
            };
            let progress = match validator.validate(candidate, &state.cancellation).await {
                ResultValidationOutcome::Cancelled { reason } => {
                    state.cancelled = Some(reason);
                    state.parser_enabled = false;
                    self.standard_input.take();
                    None
                }
                ResultValidationOutcome::Decided(ResultValidationDecision::Fatal(fatal)) => {
                    self.failure = Some(AgentFailureCause::from(fatal));
                    state.parser_enabled = false;
                    self.standard_input.take();
                    state.force_group();
                    None
                }
                ResultValidationOutcome::Decided(ResultValidationDecision::Rejected {
                    feedback,
                }) => {
                    let progress = self.parser.reject_result(Arc::clone(&feedback));
                    let mut observations = vec![AgentObservation::ValueRejected {
                        kind: crate::workflow::agent::AgentValueKind::Result,
                        feedback,
                    }];
                    observations.extend(self.parser.take_observations());
                    Some((progress, observations, false))
                }
                ResultValidationOutcome::Decided(ResultValidationDecision::Valid(result)) => {
                    let progress = self.parser.accept_result(result);
                    Some((progress, self.parser.take_observations(), true))
                }
            };
            if let Some((progress, observations, accepted)) = progress {
                if emit_observations(self.invocation, observations)
                    .await
                    .is_err()
                {
                    self.failure =
                        Some(self.parser.failure_for(
                            CodexAppServerV1RejectionReason::ObservationDeliveryFailed,
                        ));
                    state.parser_enabled = false;
                    self.standard_input.take();
                    state.force_group();
                } else {
                    if accepted {
                        let mut settlement_clock = state.clock.clone();
                        let deadline = settlement_clock.now().add(self.settlement_grace.get());
                        self.result_settlement_wait = Some(Box::pin(async move {
                            settlement_clock.wait_until(deadline).await;
                        }));
                    }
                    if let Err(cause) = apply_result_progress(
                        progress,
                        &mut self.standard_input,
                        &mut self.parser,
                        &mut state.clock,
                        self.write_timeout,
                    )
                    .await
                    {
                        self.failure = Some(cause);
                        state.parser_enabled = false;
                        self.standard_input.take();
                        state.force_group();
                    }
                }
            }
        }
    }

    fn needs_group(&self, state: &agent_process_driver::State<Clock>) -> bool {
        self.result_settlement_wait.is_some() && !state.group_quiescent
    }
    fn probe_group(&self, state: &agent_process_driver::State<Clock>) -> bool {
        self.result_settlement_wait.is_some() && state.output_closed && state.completion.is_some()
    }
    fn on_group_quiescent(&mut self, _state: &mut agent_process_driver::State<Clock>) {
        self.result_settlement_wait = None;
    }
    fn force_before_settle(&self, _state: &agent_process_driver::State<Clock>) -> bool {
        false
    }

    async fn finish(
        mut self,
        mut state: agent_process_driver::State<Clock>,
        supervisor_quiesced: bool,
    ) -> AgentOutcome {
        if state.cancelled.is_none() {
            state.cancelled = state.cancellation.cancellation_reason();
        }
        if state.cancelled.is_none() && self.failure.is_none() {
            let settlement_failed = state.wait_failed
                || !supervisor_quiesced
                || !agent_process_driver::group_is_quiescent(state.process_group)
                || !state.output_closed
                || emit_observations(
                    self.invocation,
                    vec![AgentObservation::Lifecycle {
                        milestone: AgentLifecycleMilestone::HarnessQuiescent,
                    }],
                )
                .await
                .is_err();
            if settlement_failed {
                self.failure = Some(
                    self.parser
                        .failure_for(CodexAppServerV1RejectionReason::ProcessSettlementFailed),
                );
            } else if state.completion.is_none() {
                self.failure = Some(
                    self.parser
                        .failure_for(CodexAppServerV1RejectionReason::ProcessWaitFailed),
                );
            }
        }
        let outcome = if let Some(reason) = state.cancelled {
            AgentOutcome::Cancelled { reason }
        } else if let Some(cause) = self.failure {
            failed_agent_outcome(cause)
        } else if let Some(status) = state.completion {
            self.parser.finish(status.success())
        } else {
            failed_agent_outcome(
                self.parser
                    .failure_for(CodexAppServerV1RejectionReason::ProcessWaitFailed),
            )
        };
        self.parser.prepare_completion_rejection();
        let protocol_rejection = self.parser.protocol_rejection();
        match outcome {
            AgentOutcome::Failed(failure)
                if matches!(
                    failure.cause(),
                    AgentFailureCause::HarnessSetupFailed { .. }
                        | AgentFailureCause::HarnessSetupRejected { .. }
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
}

async fn drive_process<Clock, Worker, Sink>(
    invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
    started: &AgentStartCallback,
    process: LaunchedCodexProcess,
    parser: CodexAppServerV1Parser,
    process_directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    result_validator: Option<AuthoritativeResultValidator<Clock, Worker>>,
    timing: ProcessTimingConfiguration<Clock>,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    let (cooperative_interrupt, cooperative_interrupts) = mpsc::unbounded_channel();
    let protocol = CodexProtocol {
        invocation,
        started,
        parser,
        result_validator,
        standard_input: Some(process.standard_input),
        cooperative_interrupts,
        cooperative_interrupt_started: false,
        cooperative_interrupts_open: true,
        cleanup_deadline: Box::pin(pending()),
        cleanup_deadline_armed: false,
        result_settlement_wait: None,
        write_timeout: timing.standard_input_write_timeout,
        cleanup_timeout: timing.post_failure_cleanup_timeout,
        settlement_grace: timing.result_settlement_grace,
        start_reported: false,
        failure: None,
    };
    let state = agent_process_driver::State::new(
        agent_process_driver::ProcessOutput {
            child: process.child,
            process_group: process.process_group,
            standard_output: process.standard_output,
        },
        invocation.cancellation().clone(),
        timing.clock,
    );
    agent_process_driver::drive(
        state,
        protocol,
        agent_process_driver::Supervisor {
            directives: process_directives,
            interrupt: Box::new(move || {
                let _ = cooperative_interrupt.send(());
            }),
            settlement: None,
        },
    )
    .await
}

async fn begin_cooperative_interrupt<Clock: CoordinatorClock>(
    standard_input: &mut Option<UnixStream>,
    parser: &mut CodexAppServerV1Parser,
    clock: &mut Clock,
    write_timeout: Duration,
) -> Result<(), AgentFailureCause> {
    let active_turn = parser.request_turn_interrupt()?;
    write_pending_frames(standard_input, parser, clock, write_timeout).await?;
    if !active_turn {
        close_standard_input(standard_input).await.map_err(|()| {
            parser.failure_for(CodexAppServerV1RejectionReason::StandardInputCloseFailed)
        })?;
    }
    Ok(())
}

// Codex applies correction frames and accepted-result stdin closure inside its JSON-RPC
// driver; sharing Claude's exchange progress helper would couple distinct protocols.
async fn apply_result_progress<Clock: CoordinatorClock>(
    progress: Result<ParserProgress, AgentFailureCause>,
    standard_input: &mut Option<UnixStream>,
    parser: &mut CodexAppServerV1Parser,
    clock: &mut Clock,
    write_timeout: Duration,
) -> Result<(), AgentFailureCause> {
    let progress = progress?;
    write_pending_frames(standard_input, parser, clock, write_timeout).await?;
    if progress.close_standard_input {
        close_standard_input(standard_input).await.map_err(|()| {
            parser.failure_for(CodexAppServerV1RejectionReason::StandardInputCloseFailed)
        })?;
    }
    Ok(())
}

// The native accepted-turn settlement deadline is a Codex protocol hook; the
// shared loop owns its wait and group probe.
async fn wait_for_result_settlement(wait: &mut Option<ResultSettlementWait>) {
    match wait {
        Some(wait) => wait.await,
        None => pending().await,
    }
}

async fn write_pending_frames<Clock: CoordinatorClock>(
    standard_input: &mut Option<UnixStream>,
    parser: &mut CodexAppServerV1Parser,
    clock: &mut Clock,
    write_timeout: Duration,
) -> Result<(), AgentFailureCause> {
    let write = async {
        while let Some(frame) = parser.take_outbound() {
            let Some(input) = standard_input.as_mut() else {
                return Err(());
            };
            input.write_all(&frame).await.map_err(|_| ())?;
        }
        Ok(())
    };
    let reason = match agent_process_driver::write_until(clock, write_timeout, write).await {
        Ok(()) => return Ok(()),
        Err(WriteDeadline::Failed(())) => CodexAppServerV1RejectionReason::StandardInputWriteFailed,
        Err(WriteDeadline::TimedOut) => CodexAppServerV1RejectionReason::StandardInputWriteTimedOut,
    };
    let failure = parser.failure_for(reason);
    let _ = close_standard_input(standard_input).await;
    Err(failure)
}

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

fn setup_failure(stage: AgentHarnessSetupStage) -> AgentFailureCause {
    AgentFailureCause::HarnessSetupFailed { stage }
}
