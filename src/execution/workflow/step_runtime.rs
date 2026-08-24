use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};

use rustix::process::{Pid, Signal, kill_process_group};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, watch};

use super::admission::{AdmittedWorkflow, CancellationReason, EnvironmentSnapshot};
use super::agent::dispatch::{AgentInvocationDispatcher, invoke_agent_dispatcher};
use super::agent::{
    AgentFailure, AgentFailureCause, AgentInvocationIdentity, AgentObservationEnvelope,
    AgentObservationSink, AgentOutcome, AgentProcessControl, AgentStartReceiveError,
    AgentTerminalReceiveError, CompletedAgentInvocation, WorkflowRunId, agent_start_channel,
    agent_terminal_channel, failed_agent_outcome,
};
use super::agent_diagnostics::AgentDiagnosticSessionStore;
use super::agent_input::{
    AgentInputMaterializationError, AgentInputStaging, AgentInputStagingLease,
    AgentInputStartFailure, ClosedAgentInvocation, MaterializedAgentInvocation,
    materialize_agent_invocation, materialize_recovery_agent_invocation,
};
#[cfg(test)]
use super::artifact::CaptureBoundaryObserver;
use super::artifact::{
    ArtifactStaging, CaptureAttemptFailure, CaptureCancellation, CaptureCandidateSet,
    CaptureDeclaration, CaptureFailure,
};
use super::child_guard::{StoppedChildGuard, force_stop_direct_child};
use super::coordinator::{
    ActionPort, CommitPort, CommittedReduction, CoordinationError, CoordinationResult, Coordinator,
    CoordinatorClock, DriverOccurrence, DriverOccurrenceAcceptance, DriverOccurrenceClaim,
    OccurrenceSender, occurrence_channel,
};
use super::diagnostic::{PendingStepDiagnostic, StepDiagnosticLog};
use super::document::Output;
use super::execution_root::{
    AdmittedExecutionRoot, AdmittedWorkingDirectory, ExecutableCandidate,
    WorkingDirectorySelectionFailure,
};
use super::git_capture::{GitAwareCaptureDeclaration, GitCaptureFailure};
use super::input::{InputPreparationFailure, InputStaging, InputValue, InputView};
use super::invocation_accounting::InvocationAccountingLog;
use super::observation::{ExecutionObservation, ExecutionObserver, NoopExecutionObserver};
use super::process_group::{
    AuthenticatedProcessGroup, ProcessGuardRegistration, ProcessGuardRegistry,
    capture_process_group_identity, interrupt_authenticated_process_group,
};
use super::recovery::{
    RECOVERY_CONTEXT_VARIABLE, RECOVERY_RESULT_VARIABLE, RecoveryHandlerFailure, RecoveryStaging,
    parse_recovery_decision, recovery_definition, recovery_handler_cwd,
};
use super::runtime::{
    Action, ActionId, ActionInput, RecoveryDecision, RecoveryRoundNumber, RecoveryRoundRecord,
    RequestedAction,
};
use super::validated::{
    ResolvedOutputSource, ResolvedValueSource, ValidatedAgentStep, ValidatedCommandStep,
    ValidatedCommonStep, ValidatedMessageSource, ValidatedRecoveryHandler, ValidatedStep,
    WorkflowImport,
};
use super::value::CapturedValue;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkingDirectoryFailure {
    ExecutionRootRebound,
    Unavailable,
    EscapesExecutionRoot,
    NotDirectory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandPreparationFailure {
    InvalidArgv,
    PathNotConfigured,
    ExecutableNotFound,
    ExecutableUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandLaunchFailure {
    NotFound,
    PermissionDenied,
    InvalidInput,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StepStartFailure {
    StepUnavailable,
    PreparationTaskUnavailable,
    InputsUnavailable,
    InputPreparation(InputPreparationFailure),
    AgentInput(Box<AgentInputStartFailure>),
    Agent(AgentFailure),
    AgentRuntimeUnavailable,
    OutputsUnsupported,
    WorkingDirectory(WorkingDirectoryFailure),
    CommandPreparation(CommandPreparationFailure),
    CommandLaunch(CommandLaunchFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandExecutionFailure {
    UnsuccessfulExit { code: Option<i32> },
    Wait,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StepExecutionFailure {
    Command(CommandExecutionFailure),
    Agent(AgentFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OutputCaptureFailure {
    StepUnavailable,
    UnsupportedOutput,
    Capture(CaptureFailure),
    Git {
        output: String,
        failure: GitCaptureFailure,
    },
    TaskUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StepFailureCause {
    Start(StepStartFailure),
    Execution(StepExecutionFailure),
    OutputCapture(OutputCaptureFailure),
    RecoveryHandler(RecoveryHandlerFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepRuntimeError {
    OccurrenceReceiverClosed,
}

#[derive(Clone)]
pub(crate) struct ProvisionalStepOutputs {
    values: BTreeMap<String, CapturedValue>,
    agent_staging: Option<Arc<AgentInputStagingLease>>,
}

impl ProvisionalStepOutputs {
    fn command() -> Self {
        Self {
            values: BTreeMap::new(),
            agent_staging: None,
        }
    }

    fn agent(values: BTreeMap<String, CapturedValue>, staging: AgentInputStagingLease) -> Self {
        Self {
            values,
            agent_staging: Some(Arc::new(staging)),
        }
    }

    fn value_names(&self) -> BTreeSet<String> {
        self.values.keys().cloned().collect()
    }
}

impl Default for ProvisionalStepOutputs {
    fn default() -> Self {
        Self::command()
    }
}

impl std::fmt::Debug for ProvisionalStepOutputs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProvisionalStepOutputs")
            .field("values", &self.values)
            .field("has_agent_staging", &self.agent_staging.is_some())
            .finish()
    }
}

impl PartialEq for ProvisionalStepOutputs {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values && self.agent_staging.is_some() == other.agent_staging.is_some()
    }
}

impl Eq for ProvisionalStepOutputs {}

#[derive(Clone, Copy)]
pub(crate) struct NoAgentDispatcher;

impl<Sink> AgentInvocationDispatcher<Sink> for NoAgentDispatcher
where
    Sink: AgentObservationSink,
{
    async fn invoke(
        &self,
        _invocation: ClosedAgentInvocation<Sink>,
        _started: super::agent::AgentStartCallback,
        terminal: super::agent::AgentTerminalCallback,
    ) {
        let _ = terminal.report(failed_agent_outcome(
            AgentFailureCause::HarnessProtocolFailed,
        ));
    }
}

#[derive(Clone)]
pub(crate) enum AgentExecution<Dispatcher> {
    Disabled,
    Enabled {
        run: WorkflowRunId,
        staging: AgentInputStaging,
        diagnostic_sessions: AgentDiagnosticSessionStore,
        dispatcher: Dispatcher,
        accounting: InvocationAccountingLog,
    },
}

impl AgentExecution<NoAgentDispatcher> {
    pub(crate) fn disabled() -> Self {
        Self::Disabled
    }
}

impl<Dispatcher> AgentExecution<Dispatcher> {
    pub(crate) fn enabled(
        run: WorkflowRunId,
        staging: AgentInputStaging,
        diagnostic_sessions: AgentDiagnosticSessionStore,
        dispatcher: Dispatcher,
    ) -> Self {
        Self::enabled_with_accounting(
            run,
            staging,
            diagnostic_sessions,
            dispatcher,
            InvocationAccountingLog::default(),
        )
    }

    pub(crate) fn enabled_with_accounting(
        run: WorkflowRunId,
        staging: AgentInputStaging,
        diagnostic_sessions: AgentDiagnosticSessionStore,
        dispatcher: Dispatcher,
        accounting: InvocationAccountingLog,
    ) -> Self {
        Self::Enabled {
            run,
            staging,
            diagnostic_sessions,
            dispatcher,
            accounting,
        }
    }
}

pub(crate) struct AgentExecutionObservationSink<Deadline, Observer> {
    observer: Observer,
    accounting: InvocationAccountingLog,
    deadline: PhantomData<fn() -> Deadline>,
}

impl<Deadline, Observer> Clone for AgentExecutionObservationSink<Deadline, Observer>
where
    Observer: Clone,
{
    fn clone(&self) -> Self {
        Self {
            observer: self.observer.clone(),
            accounting: self.accounting.clone(),
            deadline: PhantomData,
        }
    }
}

impl<Deadline, Observer> AgentObservationSink for AgentExecutionObservationSink<Deadline, Observer>
where
    Deadline: Send + Sync + 'static,
    Observer: ExecutionObserver<Deadline>,
{
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        self.accounting.record_observation(&observation);
        self.observer
            .observe(ExecutionObservation::Agent(observation))
    }
}

pub(crate) trait WorkflowAgentDispatcher<Deadline, Observer>:
    AgentInvocationDispatcher<AgentExecutionObservationSink<Deadline, Observer>>
where
    Deadline: Send + Sync + 'static,
    Observer: ExecutionObserver<Deadline>,
{
}

impl<Deadline, Observer, Dispatcher> WorkflowAgentDispatcher<Deadline, Observer> for Dispatcher
where
    Deadline: Send + Sync + 'static,
    Observer: ExecutionObserver<Deadline>,
    Dispatcher: AgentInvocationDispatcher<AgentExecutionObservationSink<Deadline, Observer>>,
{
}

#[derive(Clone)]
struct OwnedTasks {
    active: watch::Sender<usize>,
}

struct OwnedTaskGuard(OwnedTasks);

impl Drop for OwnedTaskGuard {
    fn drop(&mut self) {
        self.0
            .active
            .send_modify(|active| *active = active.saturating_sub(1));
    }
}

impl OwnedTasks {
    fn new() -> Self {
        let (active, _) = watch::channel(0);
        Self { active }
    }

    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.active
            .send_modify(|active| *active = active.saturating_add(1));
        let guard = OwnedTaskGuard(self.clone());
        drop(tokio::spawn(async move {
            let _guard = guard;
            future.await;
        }));
    }

    async fn wait_until_idle(&self) {
        let mut active = self.active.subscribe();
        while *active.borrow_and_update() != 0 {
            if active.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct StepRuntime<
    Clock,
    Observer = NoopExecutionObserver,
    Dispatcher = NoAgentDispatcher,
> where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Dispatcher: Clone,
{
    admitted: AdmittedWorkflow,
    artifacts: ArtifactStaging,
    diagnostics: StepDiagnosticLog,
    occurrences: OccurrenceSender<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
    inputs: InputStaging,
    agents: AgentExecution<Dispatcher>,
    clock: Clock,
    observer: Observer,
    work: Arc<Mutex<CommandWorkRegistry<Clock::Instant>>>,
    capture_work: Arc<Mutex<CaptureWorkRegistry>>,
    capture_requests: mpsc::UnboundedSender<CaptureWorkerMessage>,
    tasks: OwnedTasks,
    process_guards: ProcessGuardRegistry,
}

struct CaptureRequest {
    step: String,
    action: ActionId,
    provisional: ProvisionalStepOutputs,
}

enum CaptureWorkerMessage {
    Capture(CaptureRequest),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Clone)]
struct CaptureWorker {
    admitted: AdmittedWorkflow,
    artifacts: ArtifactStaging,
    occurrences: OccurrenceSender<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
    work: Arc<Mutex<CaptureWorkRegistry>>,
}

enum CaptureWorkerFailure {
    Cancelled,
    Failed(OutputCaptureFailure),
}

impl<Clock> StepRuntime<Clock>
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
{
    #[cfg(test)]
    fn new(
        admitted: AdmittedWorkflow,
        artifacts: ArtifactStaging,
        inputs: InputStaging,
        occurrences: OccurrenceSender<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
        clock: Clock,
    ) -> Self {
        Self::with_diagnostics(
            admitted,
            artifacts,
            inputs,
            StepDiagnosticLog::default(),
            occurrences,
            clock,
        )
    }

    fn with_diagnostics(
        admitted: AdmittedWorkflow,
        artifacts: ArtifactStaging,
        inputs: InputStaging,
        diagnostics: StepDiagnosticLog,
        occurrences: OccurrenceSender<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
        clock: Clock,
    ) -> Self {
        Self::with_observer(
            admitted,
            artifacts,
            inputs,
            diagnostics,
            occurrences,
            clock,
            NoopExecutionObserver,
            AgentExecution::disabled(),
            ProcessGuardRegistry::default(),
        )
    }
}

impl<Clock, Observer, Dispatcher> StepRuntime<Clock, Observer, Dispatcher>
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
    Observer: ExecutionObserver<Clock::Instant>,
    Dispatcher: WorkflowAgentDispatcher<Clock::Instant, Observer>,
{
    #[expect(
        clippy::too_many_arguments,
        reason = "construction keeps every execution-owned resource and adapter explicit"
    )]
    fn with_observer(
        admitted: AdmittedWorkflow,
        artifacts: ArtifactStaging,
        inputs: InputStaging,
        diagnostics: StepDiagnosticLog,
        occurrences: OccurrenceSender<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
        clock: Clock,
        observer: Observer,
        agents: AgentExecution<Dispatcher>,
        process_guards: ProcessGuardRegistry,
    ) -> Self {
        let capture_work = Arc::new(Mutex::new(CaptureWorkRegistry::new()));
        let (capture_requests, mut queued_captures) =
            mpsc::unbounded_channel::<CaptureWorkerMessage>();
        let capture_worker = CaptureWorker {
            admitted: admitted.clone(),
            artifacts: artifacts.clone(),
            occurrences: occurrences.clone(),
            work: Arc::clone(&capture_work),
        };
        let tasks = OwnedTasks::new();
        let capture_tasks = tasks.clone();
        tasks.spawn(async move {
            while let Some(message) = queued_captures.recv().await {
                let request = match message {
                    CaptureWorkerMessage::Capture(request) => request,
                    CaptureWorkerMessage::Shutdown(finished) => {
                        let _ = finished.send(());
                        return;
                    }
                };
                let begin = capture_worker.with_work(|work| work.begin(request.action));
                match begin {
                    BeginCapture::Capture(cancellation) => {
                        let blocking_worker = capture_worker.clone();
                        let step = request.step.clone();
                        let provisional_values = request.provisional.value_names();
                        let result = tokio::task::spawn_blocking(move || {
                            blocking_worker.capture_outputs_blocking(
                                &step,
                                &provisional_values,
                                &cancellation,
                            )
                        })
                        .await
                        .unwrap_or(Err(CaptureWorkerFailure::Failed(
                            OutputCaptureFailure::TaskUnavailable,
                        )));
                        let settling_worker = capture_worker.clone();
                        let (started, settlement_started) = oneshot::channel();
                        capture_tasks.spawn(async move {
                            settling_worker.settle(request, result, started).await;
                        });
                        let _ = settlement_started.await;
                    }
                    BeginCapture::Cancelled => {
                        let settling_worker = capture_worker.clone();
                        let (started, settlement_started) = oneshot::channel();
                        capture_tasks.spawn(async move {
                            settling_worker
                                .settle_cancelled(request.action, started)
                                .await;
                        });
                        let _ = settlement_started.await;
                    }
                    BeginCapture::Gone => {}
                }
            }
        });
        Self {
            admitted,
            artifacts,
            diagnostics,
            occurrences,
            inputs,
            agents,
            clock,
            observer,
            work: Arc::new(Mutex::new(CommandWorkRegistry::new())),
            capture_work,
            capture_requests,
            tasks,
            process_guards,
        }
    }

    pub(crate) async fn execute_step(
        &self,
        step: String,
        action: ActionId,
    ) -> Result<(), StepRuntimeError> {
        let Some(cancellation) = self.register_start(step.clone(), action) else {
            return Ok(());
        };
        self.execute_registered_step(step, action, BTreeMap::new(), cancellation)
            .await
    }

    async fn execute_registered_step(
        &self,
        step: String,
        action: ActionId,
        inputs: BTreeMap<String, ActionInput<CapturedValue>>,
        cancellation: oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        let preparation_runtime = self.clone();
        let preparation_step = step.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            preparation_runtime.prepare_step(&preparation_step, action, &inputs)
        })
        .await
        .unwrap_or(Err(StepStartFailure::PreparationTaskUnavailable));
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(failure) => return self.settle_start_failure(step, action, failure).await,
        };

        match self.with_work(|work| work.begin_launch(action)) {
            BeginLaunch::Launch => {}
            BeginLaunch::Cancelled(cancellation) => {
                drop(prepared);
                return self.quiesce_unlaunched(action, cancellation).await;
            }
            BeginLaunch::Gone => {
                drop(prepared);
                return Ok(());
            }
        }

        match prepared.body {
            PreparedStepBody::Command(command) => {
                self.execute_prepared_command(step, action, command, cancellation)
                    .await
            }
            PreparedStepBody::Agent(agent) => {
                self.execute_prepared_agent(step, action, *agent, cancellation)
                    .await
            }
        }
    }

    // Keep recovery preparation separate from ordinary DAG input preparation: sharing this
    // lifecycle-shaped code would make graph inputs representable at the handler boundary.
    // jscpd:ignore-start
    async fn execute_recovery_handler(
        &self,
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        history: Vec<RecoveryRoundRecord<StepFailureCause>>,
        cancellation: oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        self.diagnostics.mark_recovery_handler(&step, action);
        let preparation_runtime = self.clone();
        let preparation_step = step.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            preparation_runtime.prepare_recovery_handler(&preparation_step, round, action, &history)
        })
        .await
        .unwrap_or(Err(RecoveryHandlerFailure::ContextUnavailable));
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(failure) => {
                return self
                    .settle_recovery_start_failure(step, round, action, failure)
                    .await;
            }
        };

        match self.with_work(|work| work.begin_launch(action)) {
            BeginLaunch::Launch => {}
            BeginLaunch::Cancelled(cancellation) => {
                drop(prepared);
                return self.quiesce_unlaunched(action, cancellation).await;
            }
            BeginLaunch::Gone => {
                drop(prepared);
                return Ok(());
            }
        }

        match prepared {
            PreparedRecoveryHandler::Command { command, context } => {
                self.execute_recovery_command(step, round, action, command, context, cancellation)
                    .await
            }
            PreparedRecoveryHandler::Agent { agent, context } => {
                self.execute_recovery_agent(step, round, action, *agent, context, cancellation)
                    .await
            }
        }
    }

    // jscpd:ignore-end
    fn prepare_recovery_handler(
        &self,
        step: &str,
        round: RecoveryRoundNumber,
        action: ActionId,
        history: &[RecoveryRoundRecord<StepFailureCause>],
    ) -> Result<
        PreparedRecoveryHandler<AgentExecutionObservationSink<Clock::Instant, Observer>>,
        RecoveryHandlerFailure,
    > {
        let staging = RecoveryStaging::create(self.admitted.execution().root())?;
        let context =
            staging.materialize(&self.admitted, step, round, history, &self.diagnostics)?;
        let handler = recovery_definition(&self.admitted, step)
            .and_then(|recovery| recovery.handler.as_ref())
            .ok_or(RecoveryHandlerFailure::HandlerUnavailable)?;
        match handler {
            ValidatedRecoveryHandler::Command { argv, cwd } => {
                let cwd = recovery_handler_cwd(&self.admitted, step, cwd.as_deref())
                    .map_err(|()| RecoveryHandlerFailure::HandlerUnavailable)?;
                let cwd = resolve_working_directory(self.admitted.execution().root_identity(), cwd)
                    .map_err(|_| RecoveryHandlerFailure::WorkingDirectoryUnavailable)?;
                let mut command =
                    prepare_command(argv, cwd, self.admitted.execution().environment().clone())
                        .map_err(|_| RecoveryHandlerFailure::CommandPreparationFailed)?;
                command.environment = self
                    .admitted
                    .execution()
                    .environment()
                    .with_variable(
                        RECOVERY_CONTEXT_VARIABLE.into(),
                        context.context_path().as_os_str().to_owned(),
                    )
                    .with_variable(
                        RECOVERY_RESULT_VARIABLE.into(),
                        context.result_path().as_os_str().to_owned(),
                    );
                Ok(PreparedRecoveryHandler::Command { command, context })
            }
            ValidatedRecoveryHandler::Agent { .. } => {
                let AgentExecution::Enabled {
                    run,
                    staging,
                    diagnostic_sessions,
                    accounting,
                    ..
                } = &self.agents
                else {
                    return Err(RecoveryHandlerFailure::AgentInputFailed);
                };
                let identity = AgentInvocationIdentity::new(run.clone(), Arc::from(step), action);
                let agent = materialize_recovery_agent_invocation(
                    &self.admitted,
                    staging,
                    diagnostic_sessions,
                    identity,
                    step,
                    context.context_path(),
                    self.admitted.execution().cancellation().source().clone(),
                    self.process_guards.clone(),
                    accounting,
                    AgentExecutionObservationSink {
                        observer: self.observer.clone(),
                        accounting: accounting.clone(),
                        deadline: PhantomData,
                    },
                )
                .map_err(|_| RecoveryHandlerFailure::AgentInputFailed)?;
                Ok(PreparedRecoveryHandler::Agent {
                    agent: Box::new(agent),
                    context,
                })
            }
        }
    }

    // Recovery commands share containment mechanics with targets but have private result
    // authority and handler occurrences, so combining the two paths would blur settlement.
    // jscpd:ignore-start
    async fn execute_recovery_command(
        &self,
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        command: PreparedCommand,
        context: super::recovery::RecoveryInvocationStaging,
        mut cancellation: oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        // Recovery commands always cross the authenticated child-guard boundary,
        // including source-neutral tests whose commit port has no durable guard store.
        let mut launched = match command.launch::<Clock::Instant, _>(
            true,
            step.clone(),
            action,
            &self.diagnostics,
            self.admitted.execution().limits().maximum_step_log_bytes(),
            self.observer.clone(),
        ) {
            Ok(launched) => launched,
            Err(_) => {
                drop(context);
                return self
                    .settle_recovery_start_failure(
                        step,
                        round,
                        action,
                        RecoveryHandlerFailure::CommandLaunchFailed,
                    )
                    .await;
            }
        };
        let Some(process_group) = launched.process_group().cloned() else {
            let _ = launched.force_stop().await;
            launched.finish_resources().await;
            drop(context);
            return self
                .settle_recovery_start_failure(
                    step,
                    round,
                    action,
                    RecoveryHandlerFailure::CommandLaunchFailed,
                )
                .await;
        };
        let registration = match self.process_guards.register(
            &step,
            action.transition_sequence.get(),
            &process_group,
        ) {
            Ok(registration) => registration,
            Err(()) => {
                let _ = launched.force_stop().await;
                launched.finish_resources().await;
                drop(context);
                return self
                    .settle_recovery_start_failure(
                        step,
                        round,
                        action,
                        RecoveryHandlerFailure::CommandLaunchFailed,
                    )
                    .await;
            }
        };
        launched.install_registration(registration);
        if launched.release().is_err() {
            let _ = launched.force_stop().await;
            launched.finish_resources().await;
            drop(context);
            return self
                .settle_recovery_start_failure(
                    step,
                    round,
                    action,
                    RecoveryHandlerFailure::CommandLaunchFailed,
                )
                .await;
        }
        match self.with_work(|work| work.record_launch(action, process_group)) {
            RecordLaunch::Running => {}
            RecordLaunch::Cancelled {
                cancellation,
                interrupt,
            } => {
                if let Some(group) = interrupt {
                    let _ = interrupt_authenticated_process_group(&group);
                }
                let result = self
                    .cancel_launched(action, &mut launched, cancellation)
                    .await;
                drop(context);
                return result;
            }
            RecordLaunch::Gone => {
                let _ = launched.force_stop().await;
                launched.finish_resources().await;
                drop(context);
                return Ok(());
            }
        }
        match self
            .report_recovery_handler_started(&step, round, action, &mut cancellation)
            .await
        {
            Ok(StartDelivery::Published) => {}
            Ok(StartDelivery::Cancelled(cancellation)) => {
                let result = self
                    .cancel_launched(action, &mut launched, cancellation)
                    .await;
                drop(context);
                return result;
            }
            Ok(StartDelivery::Gone) => {
                self.abandon_launched(action, &mut launched).await;
                drop(context);
                return Ok(());
            }
            Err(failure) => {
                self.abandon_launched(action, &mut launched).await;
                drop(context);
                return Err(failure);
            }
        }

        let waited = tokio::select! {
            biased;
            _ = &mut cancellation => None,
            result = launched.wait() => Some(result),
        };
        if waited.is_none()
            && let Some(cancellation) = self.cancellation_for(action)
        {
            let result = self
                .cancel_launched(action, &mut launched, cancellation)
                .await;
            drop(context);
            return result;
        }
        let waited = match waited {
            Some(result) => result,
            None => launched.wait().await,
        };
        let mut outcome = match waited {
            Ok(status) if status.success() => context
                .read_decision()
                .map_err(RecoveryHandlerFailure::from)
                .and_then(|bytes| {
                    parse_recovery_decision(&bytes).map_err(RecoveryHandlerFailure::DecisionInvalid)
                }),
            Ok(status) => Err(RecoveryHandlerFailure::CommandExitFailed {
                code: status.code(),
            }),
            Err(()) => {
                if launched.force_stop().await.is_err() {
                    Err(RecoveryHandlerFailure::ProcessQuiescenceFailed)
                } else {
                    Err(RecoveryHandlerFailure::CommandWaitFailed)
                }
            }
        };
        launched.finish_resources().await;
        if context.release().is_err() {
            outcome = Err(RecoveryHandlerFailure::SettlementFailed);
        }
        self.settle_recovery_completion(step, round, action, outcome)
            .await
    }

    // jscpd:ignore-end
    // Recovery agents share adapter quiescence with targets but exclude DAG values and accept
    // only the recovery result schema; keep that authority path independently reviewable.
    // jscpd:ignore-start
    async fn execute_recovery_agent(
        &self,
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        agent: MaterializedAgentInvocation<AgentExecutionObservationSink<Clock::Instant, Observer>>,
        context: super::recovery::RecoveryInvocationStaging,
        mut cancellation: oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        let process_control = agent.invocation().process_control().clone();
        match self.with_work(|work| work.record_agent_launch(action, process_control)) {
            RecordLaunch::Running => {}
            RecordLaunch::Cancelled { cancellation, .. } => {
                drop(agent);
                drop(context);
                return self.quiesce_unlaunched(action, cancellation).await;
            }
            RecordLaunch::Gone => return Ok(()),
        }
        let (invocation, staging) = agent.into_parts();
        let (started_callback, started) = agent_start_channel();
        let (terminal, outcome) = agent_terminal_channel(invocation.value_mode());
        let dispatcher = match &self.agents {
            AgentExecution::Enabled { dispatcher, .. } => dispatcher.clone(),
            AgentExecution::Disabled => {
                drop(staging);
                drop(context);
                return self
                    .settle_recovery_start_failure(
                        step,
                        round,
                        action,
                        RecoveryHandlerFailure::AgentInputFailed,
                    )
                    .await;
            }
        };
        let mut invocation_task = tokio::spawn(async move {
            invoke_agent_dispatcher(&dispatcher, invocation, started_callback, terminal).await;
        });
        let started = started.receive();
        tokio::pin!(started);
        let outcome = outcome.receive();
        tokio::pin!(outcome);
        let boundary = tokio::select! {
            biased;
            result = &mut started => AgentLifecycleBoundary::Started(result),
            result = &mut outcome => AgentLifecycleBoundary::Terminal(result),
        };
        let (lifecycle_started, outcome) = match boundary {
            AgentLifecycleBoundary::Started(Ok(())) => {
                match self
                    .report_recovery_handler_started(&step, round, action, &mut cancellation)
                    .await
                {
                    Ok(StartDelivery::Published) => {}
                    Ok(StartDelivery::Cancelled(cancellation)) => {
                        let _ = tokio::join!(&mut invocation_task, &mut outcome);
                        drop(staging);
                        drop(context);
                        return self.finish_cancellation(action, cancellation).await;
                    }
                    Ok(StartDelivery::Gone) => {
                        let _ = tokio::join!(&mut invocation_task, &mut outcome);
                        drop(staging);
                        drop(context);
                        self.with_work(|work| work.abandon(action));
                        return Ok(());
                    }
                    Err(failure) => {
                        let _ = tokio::join!(&mut invocation_task, &mut outcome);
                        drop(staging);
                        drop(context);
                        self.with_work(|work| work.abandon(action));
                        return Err(failure);
                    }
                }
                let (_, outcome) = tokio::join!(&mut invocation_task, &mut outcome);
                (true, outcome)
            }
            AgentLifecycleBoundary::Started(Err(_)) => {
                let (_, outcome) = tokio::join!(&mut invocation_task, &mut outcome);
                (false, outcome)
            }
            AgentLifecycleBoundary::Terminal(outcome) => {
                let _ = invocation_task.await;
                (false, outcome)
            }
        };
        let outcome = outcome
            .unwrap_or_else(|_| failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed));
        if matches!(outcome, AgentOutcome::Cancelled { .. })
            && self
                .admitted
                .execution()
                .cancellation()
                .source()
                .cancellation_reason()
                .is_some()
        {
            if self.cancellation_for(action).is_none() {
                let _ = cancellation.await;
            }
            if let Some(cancellation) = self.cancellation_for(action) {
                drop(staging);
                drop(context);
                return self.finish_cancellation(action, cancellation).await;
            }
        }
        if !lifecycle_started {
            drop(staging);
            drop(context);
            return self
                .settle_recovery_start_failure(
                    step,
                    round,
                    action,
                    RecoveryHandlerFailure::AgentFailed,
                )
                .await;
        }
        let mut decision = match outcome {
            AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) => {
                parse_recovery_decision(result.canonical_json())
                    .map_err(RecoveryHandlerFailure::AgentResultInvalid)
            }
            AgentOutcome::Completed(
                CompletedAgentInvocation::NoValue
                | CompletedAgentInvocation::NoResponse
                | CompletedAgentInvocation::Response(_),
            ) => Err(RecoveryHandlerFailure::AgentResultMissing),
            AgentOutcome::Failed(_) | AgentOutcome::Cancelled { .. } => {
                Err(RecoveryHandlerFailure::AgentFailed)
            }
        };
        if staging.release().is_err() || context.release().is_err() {
            decision = Err(RecoveryHandlerFailure::SettlementFailed);
        }
        self.settle_recovery_completion(step, round, action, decision)
            .await
    }

    // jscpd:ignore-end
    async fn report_recovery_handler_started(
        &self,
        step: &str,
        round: RecoveryRoundNumber,
        action: ActionId,
        cancellation: &mut oneshot::Receiver<()>,
    ) -> Result<StartDelivery<Clock::Instant>, StepRuntimeError> {
        self.report_started(
            DriverOccurrence::recovery_handler_started(step.to_owned(), round, action),
            action,
            cancellation,
        )
        .await
    }

    // Handler settlement intentionally mirrors target settlement while emitting a distinct
    // closed occurrence set that can never publish target values.
    // jscpd:ignore-start
    async fn settle_recovery_start_failure(
        &self,
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        failure: RecoveryHandlerFailure,
    ) -> Result<(), StepRuntimeError> {
        self.settle_unlaunched(
            action,
            DriverOccurrence::recovery_handler_start_failed(
                step,
                round,
                action,
                StepFailureCause::RecoveryHandler(failure),
            ),
        )
        .await
    }

    async fn settle_recovery_completion(
        &self,
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        outcome: Result<RecoveryDecision, RecoveryHandlerFailure>,
    ) -> Result<(), StepRuntimeError> {
        let occurrence = match outcome {
            Ok(decision) => {
                DriverOccurrence::recovery_handler_completed(step, round, action, decision)
            }
            Err(failure) => DriverOccurrence::recovery_handler_execution_failed(
                step,
                round,
                action,
                StepFailureCause::RecoveryHandler(failure),
            ),
        };
        self.settle_agent_occurrence(action, occurrence).await
    }

    // jscpd:ignore-end
    async fn report_step_started(
        &self,
        step: &str,
        action: ActionId,
        cancellation: &mut oneshot::Receiver<()>,
    ) -> Result<StartDelivery<Clock::Instant>, StepRuntimeError> {
        self.report_started(
            DriverOccurrence::step_started(step.to_owned(), action),
            action,
            cancellation,
        )
        .await
    }

    async fn report_started(
        &self,
        occurrence: DriverOccurrence<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
        action: ActionId,
        cancellation: &mut oneshot::Receiver<()>,
    ) -> Result<StartDelivery<Clock::Instant>, StepRuntimeError> {
        let send = self.send(occurrence);
        tokio::pin!(send);
        tokio::select! {
            biased;
            _ = cancellation => Ok(self
                .cancellation_for(action)
                .map_or(StartDelivery::Gone, StartDelivery::Cancelled)),
            result = &mut send => result.map(|()| StartDelivery::Published),
        }
    }

    async fn abandon_launched(&self, action: ActionId, launched: &mut LaunchedStepBody) {
        let _ = launched.force_stop().await;
        launched.finish_resources().await;
        self.with_work(|work| work.abandon(action));
    }

    async fn execute_prepared_command(
        &self,
        step: String,
        action: ActionId,
        command: PreparedCommand,
        mut cancellation: oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        let mut launched = match command.launch::<Clock::Instant, _>(
            self.process_guards.is_durable(),
            step.clone(),
            action,
            &self.diagnostics,
            self.admitted.execution().limits().maximum_step_log_bytes(),
            self.observer.clone(),
        ) {
            Ok(launched) => launched,
            Err(failure) => return self.settle_start_failure(step, action, failure).await,
        };
        let Some(process_group) = launched.process_group().cloned() else {
            let _ = launched.force_stop().await;
            launched.finish_resources().await;
            return self
                .settle_start_failure(
                    step,
                    action,
                    StepStartFailure::CommandLaunch(CommandLaunchFailure::Other),
                )
                .await;
        };
        let registration = match self.process_guards.register(
            &step,
            action.transition_sequence.get(),
            &process_group,
        ) {
            Ok(registration) => registration,
            Err(()) => {
                let _ = launched.force_stop().await;
                launched.finish_resources().await;
                return self
                    .settle_start_failure(
                        step,
                        action,
                        StepStartFailure::CommandLaunch(CommandLaunchFailure::Other),
                    )
                    .await;
            }
        };
        launched.install_registration(registration);
        if let Err(failure) = launched.release() {
            let _ = launched.force_stop().await;
            launched.finish_resources().await;
            return self
                .settle_start_failure(step, action, StepStartFailure::CommandLaunch(failure))
                .await;
        }

        match self.with_work(|work| work.record_launch(action, process_group)) {
            RecordLaunch::Running => {}
            RecordLaunch::Cancelled {
                cancellation,
                interrupt,
            } => {
                if let Some(group) = interrupt {
                    let _ = interrupt_authenticated_process_group(&group);
                }
                return self
                    .cancel_launched(action, &mut launched, cancellation)
                    .await;
            }
            RecordLaunch::Gone => {
                let _ = launched.force_stop().await;
                launched.finish_resources().await;
                return Ok(());
            }
        }

        let start_delivery = self
            .report_step_started(&step, action, &mut cancellation)
            .await;
        match start_delivery {
            Ok(StartDelivery::Published) => {}
            Ok(StartDelivery::Cancelled(cancellation)) => {
                return self
                    .cancel_launched(action, &mut launched, cancellation)
                    .await;
            }
            Ok(StartDelivery::Gone) => {
                self.abandon_launched(action, &mut launched).await;
                return Ok(());
            }
            Err(failure) => {
                self.abandon_launched(action, &mut launched).await;
                return Err(failure);
            }
        }

        let waited = tokio::select! {
            biased;
            _ = &mut cancellation => None,
            result = launched.wait() => Some(result),
        };
        if waited.is_none()
            && let Some(cancellation) = self.cancellation_for(action)
        {
            return self
                .cancel_launched(action, &mut launched, cancellation)
                .await;
        }

        self.settle_launched(step, action, &mut launched, waited)
            .await
    }

    async fn execute_prepared_agent(
        &self,
        step: String,
        action: ActionId,
        agent: PreparedAgent<AgentExecutionObservationSink<Clock::Instant, Observer>>,
        mut cancellation: oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        let process_control = agent.materialized.invocation().process_control().clone();
        match self.with_work(|work| work.record_agent_launch(action, process_control)) {
            RecordLaunch::Running => {}
            RecordLaunch::Cancelled { cancellation, .. } => {
                drop(agent);
                return self.quiesce_unlaunched(action, cancellation).await;
            }
            RecordLaunch::Gone => return Ok(()),
        }

        let PreparedAgent { materialized } = agent;
        let output = materialized
            .invocation()
            .value_mode()
            .output()
            .map(str::to_owned);
        let (invocation, staging) = materialized.into_parts();
        let (started_callback, started) = agent_start_channel();
        let (terminal, outcome) = agent_terminal_channel(invocation.value_mode());
        let dispatcher = match &self.agents {
            AgentExecution::Enabled { dispatcher, .. } => dispatcher.clone(),
            AgentExecution::Disabled => {
                drop(staging);
                return self
                    .settle_start_failure(
                        step,
                        action,
                        StepStartFailure::Agent(AgentFailureCause::HarnessProtocolFailed.into()),
                    )
                    .await;
            }
        };
        let mut invocation_task = tokio::spawn(async move {
            invoke_agent_dispatcher(&dispatcher, invocation, started_callback, terminal).await;
        });
        let started = started.receive();
        tokio::pin!(started);
        let outcome = outcome.receive();
        tokio::pin!(outcome);

        let boundary = tokio::select! {
            biased;
            result = &mut started => AgentLifecycleBoundary::Started(result),
            result = &mut outcome => AgentLifecycleBoundary::Terminal(result),
        };

        let (lifecycle_started, outcome) = match boundary {
            AgentLifecycleBoundary::Started(Ok(())) => {
                match self
                    .report_step_started(&step, action, &mut cancellation)
                    .await
                {
                    Ok(StartDelivery::Published) => {}
                    Ok(StartDelivery::Cancelled(cancellation)) => {
                        let _ = tokio::join!(&mut invocation_task, &mut outcome);
                        drop(staging);
                        return self.finish_cancellation(action, cancellation).await;
                    }
                    Ok(StartDelivery::Gone) => {
                        let _ = tokio::join!(&mut invocation_task, &mut outcome);
                        drop(staging);
                        self.with_work(|work| work.abandon(action));
                        return Ok(());
                    }
                    Err(failure) => {
                        let _ = tokio::join!(&mut invocation_task, &mut outcome);
                        drop(staging);
                        self.with_work(|work| work.abandon(action));
                        return Err(failure);
                    }
                }
                let (_, outcome) = tokio::join!(&mut invocation_task, &mut outcome);
                (true, outcome)
            }
            AgentLifecycleBoundary::Started(Err(_)) => {
                let (_, outcome) = tokio::join!(&mut invocation_task, &mut outcome);
                (false, outcome)
            }
            AgentLifecycleBoundary::Terminal(outcome) => {
                let _ = invocation_task.await;
                (false, outcome)
            }
        };
        let outcome = outcome
            .unwrap_or_else(|_| failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed));

        if matches!(outcome, AgentOutcome::Cancelled { .. }) {
            drop(staging);
            return self
                .settle_agent_cancellation(step, action, lifecycle_started, &mut cancellation)
                .await;
        }

        if !lifecycle_started {
            drop(staging);
            let failure = match outcome {
                AgentOutcome::Failed(failure) => failure,
                AgentOutcome::Completed(_) | AgentOutcome::Cancelled { .. } => {
                    AgentFailureCause::HarnessProtocolFailed.into()
                }
            };
            return self
                .settle_start_failure(step, action, StepStartFailure::Agent(failure))
                .await;
        }

        let occurrence = match outcome {
            AgentOutcome::Completed(completed) => {
                let values = match completed_agent_outputs(output, completed) {
                    Ok(values) => values,
                    Err(cause) => {
                        drop(staging);
                        return self.settle_execution_failure(step, action, cause).await;
                    }
                };
                DriverOccurrence::step_execution_completed(
                    step,
                    action,
                    ProvisionalStepOutputs::agent(values, staging),
                )
            }
            AgentOutcome::Failed(failure) => {
                drop(staging);
                DriverOccurrence::step_execution_failed(
                    step,
                    action,
                    StepFailureCause::Execution(StepExecutionFailure::Agent(failure)),
                )
            }
            AgentOutcome::Cancelled { .. } => unreachable!("cancelled outcomes settle above"),
        };
        self.settle_agent_occurrence(action, occurrence).await
    }

    async fn settle_agent_cancellation(
        &self,
        step: String,
        action: ActionId,
        lifecycle_started: bool,
        cancellation: &mut oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        if self
            .admitted
            .execution()
            .cancellation()
            .source()
            .cancellation_reason()
            .is_some()
        {
            if self.cancellation_for(action).is_none() {
                let _ = cancellation.await;
            }
            if let Some(cancellation) = self.cancellation_for(action) {
                return self.finish_cancellation(action, cancellation).await;
            }
        }

        if lifecycle_started {
            self.settle_execution_failure(step, action, AgentFailureCause::HarnessProtocolFailed)
                .await
        } else {
            self.settle_start_failure(
                step,
                action,
                StepStartFailure::Agent(AgentFailureCause::HarnessProtocolFailed.into()),
            )
            .await
        }
    }

    async fn settle_execution_failure(
        &self,
        step: String,
        action: ActionId,
        cause: AgentFailureCause,
    ) -> Result<(), StepRuntimeError> {
        self.settle_agent_occurrence(
            action,
            DriverOccurrence::step_execution_failed(
                step,
                action,
                StepFailureCause::Execution(StepExecutionFailure::Agent(cause.into())),
            ),
        )
        .await
    }

    async fn claim_completion_occurrence(
        &self,
        action: ActionId,
        occurrence: DriverOccurrence<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
    ) -> Result<ClaimedCompletion<Clock::Instant>, StepRuntimeError> {
        match self.with_work(|work| work.begin_completion(action)) {
            CompletionClaim::Lifecycle => match self.claim(occurrence).await {
                Ok(claim) => Ok(ClaimedCompletion::Lifecycle(claim)),
                Err(failure) => {
                    self.with_work(|work| work.abandon(action));
                    Err(failure)
                }
            },
            CompletionClaim::Cancelled(cancellation) => {
                Ok(ClaimedCompletion::Cancelled(cancellation))
            }
            CompletionClaim::Gone => Ok(ClaimedCompletion::Gone),
        }
    }

    async fn finalize_claimed_completion(
        &self,
        action: ActionId,
        completion: ClaimedCompletion<Clock::Instant>,
    ) -> Result<(), StepRuntimeError> {
        let claim = match completion {
            ClaimedCompletion::Lifecycle(claim) => claim,
            ClaimedCompletion::Cancelled(cancellation) => {
                return self.finish_cancellation(action, cancellation).await;
            }
            ClaimedCompletion::Gone => return Ok(()),
        };
        match self.with_work(|work| work.finish_completion(action)) {
            CompletionClaim::Lifecycle => claim
                .publish()
                .map_err(|_| StepRuntimeError::OccurrenceReceiverClosed),
            CompletionClaim::Cancelled(cancellation) => {
                claim.discard();
                self.finish_cancellation(action, cancellation).await
            }
            CompletionClaim::Gone => {
                claim.discard();
                Ok(())
            }
        }
    }

    async fn settle_agent_occurrence(
        &self,
        action: ActionId,
        occurrence: DriverOccurrence<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
    ) -> Result<(), StepRuntimeError> {
        let completion = self.claim_completion_occurrence(action, occurrence).await?;
        self.finalize_claimed_completion(action, completion).await
    }

    async fn settle_start_failure(
        &self,
        step: String,
        action: ActionId,
        failure: StepStartFailure,
    ) -> Result<(), StepRuntimeError> {
        self.settle_unlaunched(
            action,
            DriverOccurrence::step_start_failed(step, action, StepFailureCause::Start(failure)),
        )
        .await
    }

    async fn settle_unlaunched(
        &self,
        action: ActionId,
        occurrence: DriverOccurrence<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
    ) -> Result<(), StepRuntimeError> {
        match self.with_work(|work| work.claim_completion(action)) {
            CompletionClaim::Lifecycle => self.send(occurrence).await,
            CompletionClaim::Cancelled(cancellation) => {
                self.quiesce_unlaunched(action, cancellation).await
            }
            CompletionClaim::Gone => Ok(()),
        }
    }

    async fn settle_launched(
        &self,
        step: String,
        action: ActionId,
        launched: &mut LaunchedStepBody,
        waited: Option<Result<ExitStatus, ()>>,
    ) -> Result<(), StepRuntimeError> {
        let waited = match waited {
            Some(result) => result,
            None => launched.wait().await,
        };
        if waited.is_err() {
            let _ = launched.force_stop().await;
        }
        launched.force_process_group();
        let occurrence = match waited {
            Ok(status) if status.success() => DriverOccurrence::step_execution_completed(
                step,
                action,
                ProvisionalStepOutputs::command(),
            ),
            Ok(status) => DriverOccurrence::step_execution_failed(
                step,
                action,
                StepFailureCause::Execution(StepExecutionFailure::Command(unsuccessful_exit(
                    status,
                ))),
            ),
            Err(()) => DriverOccurrence::step_execution_failed(
                step,
                action,
                StepFailureCause::Execution(StepExecutionFailure::Command(
                    CommandExecutionFailure::Wait,
                )),
            ),
        };
        let completion = self.claim_completion_occurrence(action, occurrence).await;
        launched.finish_resources().await;
        self.finalize_claimed_completion(action, completion?).await
    }

    async fn cancel_launched(
        &self,
        action: ActionId,
        launched: &mut LaunchedStepBody,
        cancellation: CommandCancellation<Clock::Instant>,
    ) -> Result<(), StepRuntimeError> {
        if let Some(group) = self.with_work(|work| work.ensure_interrupt(action)) {
            let _ = interrupt_authenticated_process_group(&group);
        }

        let deadline = self.clock.wait_until(cancellation.deadline.clone());
        tokio::pin!(deadline);
        tokio::select! {
            biased;
            () = &mut deadline => {
                let _ = launched.force_stop().await;
            }
            waited = launched.wait() => {
                if waited.is_err() {
                    let _ = launched.force_stop().await;
                } else {
                    launched.force_process_group();
                }
            }
        }
        launched.finish_resources().await;
        self.finish_cancellation(action, cancellation).await
    }

    async fn quiesce_unlaunched(
        &self,
        action: ActionId,
        cancellation: CommandCancellation<Clock::Instant>,
    ) -> Result<(), StepRuntimeError> {
        self.finish_cancellation(action, cancellation).await
    }

    async fn finish_cancellation(
        &self,
        action: ActionId,
        cancellation: CommandCancellation<Clock::Instant>,
    ) -> Result<(), StepRuntimeError> {
        let (cancellation, deadline_finished) =
            self.with_work(|work| work.finish_cancellation(action, cancellation));
        if let Some(deadline_finished) = deadline_finished {
            let _ = deadline_finished.await;
        }
        self.send(DriverOccurrence::step_quiesced(
            cancellation.step,
            cancellation.action,
        ))
        .await
    }

    fn prepare_step(
        &self,
        step: &str,
        action: ActionId,
        action_inputs: &BTreeMap<String, ActionInput<CapturedValue>>,
    ) -> Result<
        PreparedStep<AgentExecutionObservationSink<Clock::Instant, Observer>>,
        StepStartFailure,
    > {
        let definition =
            workflow_node(&self.admitted, step).ok_or(StepStartFailure::StepUnavailable)?;
        let body = match StepBody::from(definition) {
            StepBody::Command(command) => {
                if command.common.outputs.values().any(|output| {
                    !matches!(&output.definition, Output::File { .. } | Output::GitBranch)
                }) {
                    return Err(StepStartFailure::OutputsUnsupported);
                }
                let cwd = resolve_working_directory(
                    self.admitted.execution().root_identity(),
                    command.common.cwd.as_deref(),
                )
                .map_err(StepStartFailure::WorkingDirectory)?;
                let mut prepared = prepare_command(
                    command.argv.as_slice(),
                    cwd,
                    self.admitted.execution().environment().clone(),
                )?;
                if command.inputs.keys().ne(action_inputs.keys()) {
                    return Err(StepStartFailure::InputsUnavailable);
                }
                if !command.inputs.is_empty() {
                    let values = self.resolve_input_values(command, action_inputs)?;
                    let view = self
                        .inputs
                        .materialize(&values, &self.artifacts)
                        .map_err(StepStartFailure::InputPreparation)?;
                    prepared.environment = prepared.environment.with_variable(
                        OsString::from("SCHERZO_STEP_INPUTS"),
                        view.path().as_os_str().to_owned(),
                    );
                    prepared.inputs = Some(view);
                }
                PreparedStepBody::Command(prepared)
            }
            StepBody::Agent(agent) => {
                let AgentExecution::Enabled {
                    run,
                    staging,
                    diagnostic_sessions,
                    accounting,
                    ..
                } = &self.agents
                else {
                    return Err(StepStartFailure::AgentRuntimeUnavailable);
                };
                let upstream = resolve_agent_upstream_outputs(agent, action_inputs)?;
                let finalization_context = action_inputs.values().find_map(|input| match input {
                    ActionInput::FinalizationContext(bytes) => Some(bytes.as_ref()),
                    ActionInput::Import | ActionInput::Output(_) | ActionInput::Unavailable => None,
                });
                let identity = AgentInvocationIdentity::new(run.clone(), Arc::from(step), action);
                let materialized = materialize_agent_invocation(
                    &self.admitted,
                    &self.artifacts,
                    staging,
                    diagnostic_sessions,
                    identity,
                    &upstream,
                    finalization_context,
                    self.admitted.execution().cancellation().source().clone(),
                    self.process_guards.clone(),
                    AgentExecutionObservationSink {
                        observer: self.observer.clone(),
                        accounting: accounting.clone(),
                        deadline: PhantomData,
                    },
                )
                .map_err(|failure| match failure {
                    AgentInputMaterializationError::Start(failure) => {
                        StepStartFailure::AgentInput(Box::new(failure))
                    }
                    AgentInputMaterializationError::Cancelled { .. } => {
                        StepStartFailure::InputsUnavailable
                    }
                })?;
                accounting.record_native_session(
                    materialized.invocation().identity(),
                    materialized.invocation().profile(),
                    materialized.invocation().diagnostic_session(),
                );
                PreparedStepBody::Agent(Box::new(PreparedAgent { materialized }))
            }
        };
        Ok(PreparedStep { body })
    }

    fn resolve_input_values<'a>(
        &'a self,
        command: &'a ValidatedCommandStep,
        action_inputs: &'a BTreeMap<String, ActionInput<CapturedValue>>,
    ) -> Result<BTreeMap<String, InputValue<'a>>, StepStartFailure> {
        command
            .inputs
            .iter()
            .map(|(input_identity, reference)| {
                let action_input = action_inputs
                    .get(input_identity)
                    .ok_or(StepStartFailure::InputsUnavailable)?;
                let value = match (&reference.source, action_input) {
                    (ResolvedValueSource::Import(WorkflowImport::Prompt), ActionInput::Import) => {
                        InputValue::Prompt(
                            self.admitted
                                .imports()
                                .prompt()
                                .ok_or(StepStartFailure::InputsUnavailable)?,
                        )
                    }
                    (
                        ResolvedValueSource::Import(WorkflowImport::Attachments),
                        ActionInput::Import,
                    ) => InputValue::Attachments(self.admitted.imports().attachments()),
                    (
                        ResolvedValueSource::FinalizationContext,
                        ActionInput::FinalizationContext(bytes),
                    ) => InputValue::CanonicalJson(bytes),
                    (ResolvedValueSource::Output(source), ActionInput::Output(value)) => {
                        if let CapturedValue::File(file) = value
                            && file.output_identity() != source.output
                        {
                            return Err(StepStartFailure::InputsUnavailable);
                        }
                        InputValue::Captured {
                            expected_type: reference.value_type,
                            value,
                        }
                    }
                    _ => return Err(StepStartFailure::InputsUnavailable),
                };
                Ok((input_identity.clone(), value))
            })
            .collect()
    }

    fn register_start(&self, step: String, action: ActionId) -> Option<oneshot::Receiver<()>> {
        self.with_work(|work| work.register_start(step, action))
    }

    fn request_cancellation(
        &self,
        step: String,
        action: ActionId,
        deadline: Clock::Instant,
    ) -> CancellationRegistration<Clock::Instant> {
        self.with_work(|work| work.cancel(step, action, deadline))
    }

    fn cancellation_for(&self, action: ActionId) -> Option<CommandCancellation<Clock::Instant>> {
        self.with_work(|work| work.cancellation_for(action))
    }

    fn request_force_abort(
        &self,
        step: String,
        action: ActionId,
        deadline: Clock::Instant,
    ) -> ForceAbortRegistration {
        self.with_work(|work| work.force_abort(step, action, deadline))
    }

    fn with_work<Output>(
        &self,
        operation: impl FnOnce(&mut CommandWorkRegistry<Clock::Instant>) -> Output,
    ) -> Output {
        operation(&mut lock_registry(&self.work))
    }

    fn register_capture(
        &self,
        step: String,
        action: ActionId,
        provisional: ProvisionalStepOutputs,
    ) {
        if !self.with_capture_work(|work| work.register(step.clone(), action)) {
            return;
        }
        if self
            .capture_requests
            .send(CaptureWorkerMessage::Capture(CaptureRequest {
                step,
                action,
                provisional,
            }))
            .is_err()
        {
            self.with_capture_work(|work| work.finish(action));
        }
    }

    fn request_capture_cancellation(
        &self,
        step: &str,
        action: ActionId,
    ) -> CaptureCancellationRegistration {
        self.with_capture_work(|work| work.cancel(step, action))
    }

    fn request_capture_force_abort(
        &self,
        step: &str,
        action: ActionId,
    ) -> CaptureCancellationRegistration {
        self.with_capture_work(|work| work.force_abort(step, action))
    }

    fn with_capture_work<Output>(
        &self,
        operation: impl FnOnce(&mut CaptureWorkRegistry) -> Output,
    ) -> Output {
        operation(&mut lock_registry(&self.capture_work))
    }

    async fn quiesce_after_commit_failure(&self) {
        self.admitted
            .execution()
            .cancellation()
            .source()
            .request_cancellation(CancellationReason::RunnerShutdown);
        let mut clock = self.clock.clone();
        let deadline = clock.now();
        let revocations = self.with_work(|work| work.revoke_all_for_commit_failure(deadline));
        self.with_capture_work(CaptureWorkRegistry::revoke_all_for_commit_failure);
        for (wake, interrupt) in revocations {
            if let Some(group) = interrupt {
                let _ = interrupt_authenticated_process_group(&group);
            }
            if let Some(wake) = wake {
                let _ = wake.send(());
            }
        }
        self.shutdown().await;
    }

    fn schedule_quiesced_report(&self, step: String, action: ActionId) {
        let runtime = self.clone();
        let tasks = self.tasks.clone();
        tasks.spawn(async move {
            let _ = runtime
                .send(DriverOccurrence::step_quiesced(step, action))
                .await;
        });
    }

    async fn shutdown(&self) {
        let (finished, completion) = oneshot::channel();
        if self
            .capture_requests
            .send(CaptureWorkerMessage::Shutdown(finished))
            .is_ok()
        {
            let _ = completion.await;
        }
        self.tasks.wait_until_idle().await;
    }

    #[cfg(test)]
    async fn capture_outputs(
        &self,
        step: String,
        action: ActionId,
    ) -> Result<(), StepRuntimeError> {
        self.register_capture(step, action, ProvisionalStepOutputs::command());
        Ok(())
    }

    #[cfg(test)]
    fn set_capture_observer(&self, observer: Arc<dyn CaptureBoundaryObserver>) {
        self.with_capture_work(|work| work.observer = Some(observer));
    }

    async fn send(
        &self,
        occurrence: DriverOccurrence<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
    ) -> Result<(), StepRuntimeError> {
        self.occurrences
            .send(occurrence)
            .await
            .map_err(|_| StepRuntimeError::OccurrenceReceiverClosed)
    }

    async fn claim(
        &self,
        occurrence: DriverOccurrence<ProvisionalStepOutputs, StepFailureCause, CapturedValue>,
    ) -> Result<DriverOccurrenceClaim, StepRuntimeError> {
        self.occurrences
            .claim(occurrence)
            .await
            .map_err(|_| StepRuntimeError::OccurrenceReceiverClosed)
    }

    #[cfg(test)]
    fn active_work_count(&self) -> usize {
        self.with_work(|work| work.active.len()) + self.with_capture_work(|work| work.active.len())
    }
}

impl CaptureWorker {
    fn with_work<Output>(
        &self,
        operation: impl FnOnce(&mut CaptureWorkRegistry) -> Output,
    ) -> Output {
        operation(&mut lock_registry(&self.work))
    }

    fn capture_outputs_blocking(
        &self,
        step: &str,
        provisional_values: &BTreeSet<String>,
        cancellation: &CaptureCancellation,
    ) -> Result<CaptureCandidateSet, CaptureWorkerFailure> {
        let definition = workflow_node(&self.admitted, step).ok_or(
            CaptureWorkerFailure::Failed(OutputCaptureFailure::StepUnavailable),
        )?;
        let body = StepBody::from(definition);
        let common = body.common();
        let declared_values = common
            .outputs
            .iter()
            .filter(|(_, output)| {
                matches!(
                    output.definition,
                    Output::AgentResponse | Output::AgentResult { .. }
                )
            })
            .map(|(output_identity, _)| output_identity.clone())
            .collect::<BTreeSet<_>>();
        let required_results = common
            .outputs
            .iter()
            .filter(|(_, output)| matches!(output.definition, Output::AgentResult { .. }))
            .map(|(output_identity, _)| output_identity.clone())
            .collect::<BTreeSet<_>>();
        if !provisional_values.is_subset(&declared_values)
            || !required_results.is_subset(provisional_values)
        {
            return Err(CaptureWorkerFailure::Failed(
                OutputCaptureFailure::UnsupportedOutput,
            ));
        }
        let declarations = common
            .outputs
            .iter()
            .filter_map(|(output_identity, output)| match &output.definition {
                Output::File { path, media_type } => Some(GitAwareCaptureDeclaration::File(
                    CaptureDeclaration::new(output_identity, Path::new(path), media_type),
                )),
                Output::GitBranch => Some(GitAwareCaptureDeclaration::GitBranch(output_identity)),
                Output::AgentResponse | Output::AgentResult { .. } => None,
            })
            .collect::<Vec<_>>();
        if declarations
            .iter()
            .any(|declaration| matches!(declaration, GitAwareCaptureDeclaration::GitBranch(_)))
        {
            let Some(git) = self.admitted.git_capture() else {
                return Err(CaptureWorkerFailure::Failed(
                    OutputCaptureFailure::UnsupportedOutput,
                ));
            };
            return git
                .capture_step(&declarations, &self.artifacts, cancellation)
                .map_err(|failure| match failure {
                    GitCaptureFailure::Cancelled => CaptureWorkerFailure::Cancelled,
                    GitCaptureFailure::Artifact(failure) => {
                        CaptureWorkerFailure::Failed(OutputCaptureFailure::Capture(failure))
                    }
                    failure => {
                        let output = declarations
                            .iter()
                            .find_map(|declaration| match declaration {
                                GitAwareCaptureDeclaration::GitBranch(output) => {
                                    Some((*output).to_owned())
                                }
                                GitAwareCaptureDeclaration::File(_) => None,
                            })
                            .unwrap_or_default();
                        CaptureWorkerFailure::Failed(OutputCaptureFailure::Git { output, failure })
                    }
                });
        }
        let files = declarations
            .iter()
            .filter_map(|declaration| match declaration {
                GitAwareCaptureDeclaration::File(declaration) => Some(*declaration),
                GitAwareCaptureDeclaration::GitBranch(_) => None,
            })
            .collect::<Vec<_>>();
        self.artifacts
            .capture_file_candidates(&files, cancellation)
            .map_err(|failure| match failure {
                CaptureAttemptFailure::Cancelled => CaptureWorkerFailure::Cancelled,
                CaptureAttemptFailure::Capture(failure) => {
                    CaptureWorkerFailure::Failed(OutputCaptureFailure::Capture(failure))
                }
            })
    }

    async fn settle(
        &self,
        request: CaptureRequest,
        result: Result<CaptureCandidateSet, CaptureWorkerFailure>,
        started: oneshot::Sender<()>,
    ) {
        match result {
            Ok(candidates) => self.settle_candidates(request, candidates, started).await,
            Err(CaptureWorkerFailure::Cancelled) => {
                let action = request.action;
                drop(request.provisional);
                self.settle_cancelled(action, started).await;
            }
            Err(CaptureWorkerFailure::Failed(failure)) => {
                self.settle_failure(request, failure, started).await;
            }
        }
    }

    async fn settle_candidates(
        &self,
        request: CaptureRequest,
        candidates: CaptureCandidateSet,
        started: oneshot::Sender<()>,
    ) {
        let CaptureRequest {
            step,
            action,
            provisional,
        } = request;
        let ProvisionalStepOutputs {
            mut values,
            agent_staging,
        } = provisional;
        if self.with_work(|work| work.is_cancelled(action)) {
            candidates.abort();
            drop(agent_staging);
            self.settle_cancelled(action, started).await;
            return;
        }
        self.with_work(|work| work.begin_delivery(action));
        values.extend(
            candidates
                .outputs()
                .iter()
                .map(|(name, output)| (name.clone(), output.clone())),
        );
        let occurrence = DriverOccurrence::outputs_captured(step, action, values);
        let _ = started.send(());
        match self.occurrences.send_acknowledged(occurrence).await {
            Ok(DriverOccurrenceAcceptance::Accepted(commit)) => {
                drop(candidates.commit());
                self.with_work(|work| work.finish(action));
                drop(agent_staging);
                commit.finalize();
            }
            Ok(DriverOccurrenceAcceptance::Rejected(finalization)) => {
                candidates.abort();
                let cancellation = self.with_work(|work| work.finish(action));
                drop(agent_staging);
                finalization.finalize();
                if let Some(cancellation) = cancellation {
                    self.send_quiesced(cancellation).await;
                }
            }
            Err(_) => {
                candidates.abort();
                self.with_work(|work| work.finish(action));
                drop(agent_staging);
            }
        }
    }

    async fn settle_failure(
        &self,
        request: CaptureRequest,
        failure: OutputCaptureFailure,
        started: oneshot::Sender<()>,
    ) {
        let CaptureRequest {
            step,
            action,
            provisional,
        } = request;
        if self.with_work(|work| work.is_cancelled(action)) {
            drop(provisional);
            self.settle_cancelled(action, started).await;
            return;
        }
        self.with_work(|work| work.begin_delivery(action));
        let occurrence = DriverOccurrence::output_capture_failed(
            step,
            action,
            StepFailureCause::OutputCapture(failure),
        );
        let _ = started.send(());
        match self.occurrences.send_acknowledged(occurrence).await {
            Ok(DriverOccurrenceAcceptance::Accepted(commit)) => {
                self.with_work(|work| work.finish(action));
                drop(provisional);
                commit.finalize();
            }
            Ok(DriverOccurrenceAcceptance::Rejected(finalization)) => {
                let cancellation = self.with_work(|work| work.finish(action));
                drop(provisional);
                finalization.finalize();
                if let Some(cancellation) = cancellation {
                    self.send_quiesced(cancellation).await;
                }
            }
            Err(_) => {
                self.with_work(|work| work.finish(action));
                drop(provisional);
            }
        }
    }

    async fn settle_cancelled(&self, action: ActionId, started: oneshot::Sender<()>) {
        let cancellation = self.with_work(|work| work.finish(action));
        let _ = started.send(());
        if let Some(cancellation) = cancellation {
            self.send_quiesced(cancellation).await;
        }
    }

    async fn send_quiesced(&self, cancellation: CaptureRevocation) {
        let _ = self
            .occurrences
            .send(DriverOccurrence::step_quiesced(
                cancellation.step,
                cancellation.action,
            ))
            .await;
    }
}

#[derive(Clone)]
struct CaptureRevocation {
    step: String,
    action: ActionId,
}

#[derive(Clone, Copy)]
enum CapturePhase {
    Queued,
    Copying,
    Delivering,
}

struct CaptureWork {
    step: String,
    phase: CapturePhase,
    cancellation: CaptureCancellation,
    revocation: Option<CaptureRevocation>,
}

struct CaptureWorkRegistry {
    known_actions: BTreeSet<ActionId>,
    known_cancellations: BTreeSet<ActionId>,
    active_by_step: BTreeMap<String, ActionId>,
    active: BTreeMap<ActionId, CaptureWork>,
    #[cfg(test)]
    observer: Option<Arc<dyn CaptureBoundaryObserver>>,
}

impl CaptureWorkRegistry {
    fn new() -> Self {
        Self {
            known_actions: BTreeSet::new(),
            known_cancellations: BTreeSet::new(),
            active_by_step: BTreeMap::new(),
            active: BTreeMap::new(),
            #[cfg(test)]
            observer: None,
        }
    }

    fn register(&mut self, step: String, action: ActionId) -> bool {
        if !self.known_actions.insert(action) || self.active_by_step.contains_key(&step) {
            return false;
        }
        self.active_by_step.insert(step.clone(), action);
        #[cfg(test)]
        let cancellation = self
            .observer
            .as_ref()
            .map_or_else(CaptureCancellation::default, |observer| {
                CaptureCancellation::with_observer(Arc::clone(observer))
            });
        #[cfg(not(test))]
        let cancellation = CaptureCancellation::default();
        self.active.insert(
            action,
            CaptureWork {
                step,
                phase: CapturePhase::Queued,
                cancellation,
                revocation: None,
            },
        );
        true
    }

    fn begin(&mut self, action: ActionId) -> BeginCapture {
        let Some(work) = self.active.get_mut(&action) else {
            return BeginCapture::Gone;
        };
        if work.revocation.is_some() {
            return BeginCapture::Cancelled;
        }
        work.phase = CapturePhase::Copying;
        BeginCapture::Capture(work.cancellation.clone())
    }

    fn begin_delivery(&mut self, action: ActionId) {
        if let Some(work) = self.active.get_mut(&action) {
            work.phase = CapturePhase::Delivering;
        }
    }

    fn cancel(&mut self, step: &str, action: ActionId) -> CaptureCancellationRegistration {
        self.revoke(step, action, false)
    }

    fn force_abort(&mut self, step: &str, action: ActionId) -> CaptureCancellationRegistration {
        self.revoke(step, action, true)
    }

    fn revoke(
        &mut self,
        step: &str,
        action: ActionId,
        replace_existing: bool,
    ) -> CaptureCancellationRegistration {
        if !self.known_cancellations.insert(action) {
            return CaptureCancellationRegistration::Duplicate;
        }
        let Some(capture_action) = self.active_by_step.get(step).copied() else {
            return CaptureCancellationRegistration::NotFound;
        };
        let Some(work) = self.active.get_mut(&capture_action) else {
            return CaptureCancellationRegistration::NotFound;
        };
        if work.revocation.is_some() && !replace_existing {
            return CaptureCancellationRegistration::Duplicate;
        }
        work.revocation = Some(CaptureRevocation {
            step: step.to_owned(),
            action,
        });
        work.cancellation.cancel();
        CaptureCancellationRegistration::Active
    }

    fn is_cancelled(&self, action: ActionId) -> bool {
        self.active
            .get(&action)
            .is_some_and(|work| work.revocation.is_some())
    }

    fn revoke_all_for_commit_failure(&mut self) {
        for (&action, work) in &mut self.active {
            if work.revocation.is_none() {
                work.revocation = Some(CaptureRevocation {
                    step: work.step.clone(),
                    action,
                });
            }
            work.cancellation.cancel();
        }
    }

    fn finish(&mut self, action: ActionId) -> Option<CaptureRevocation> {
        let work = self.active.remove(&action)?;
        if self.active_by_step.get(&work.step) == Some(&action) {
            self.active_by_step.remove(&work.step);
        }
        work.revocation
    }
}

enum BeginCapture {
    Capture(CaptureCancellation),
    Cancelled,
    Gone,
}

enum CaptureCancellationRegistration {
    Active,
    NotFound,
    Duplicate,
}

#[derive(Clone)]
struct CommandCancellation<Deadline> {
    step: String,
    action: ActionId,
    deadline: Deadline,
}

#[derive(Clone, Copy)]
enum WorkPhase {
    Prelaunch,
    Launching,
    Running,
    Completing,
}

struct CommandWork<Deadline> {
    step: String,
    phase: WorkPhase,
    process_group: Option<AuthenticatedProcessGroup>,
    agent_process_control: Option<AgentProcessControl>,
    agent_deadline_shutdown: Option<oneshot::Sender<()>>,
    agent_deadline_finished: Option<oneshot::Receiver<()>>,
    cancellation: Option<CommandCancellation<Deadline>>,
    cancellation_wake: Option<oneshot::Sender<()>>,
    interrupt_sent: bool,
}

struct CommandWorkRegistry<Deadline> {
    known_actions: BTreeSet<ActionId>,
    cancelled_steps: BTreeSet<String>,
    active_by_step: BTreeMap<String, ActionId>,
    active: BTreeMap<ActionId, CommandWork<Deadline>>,
}

impl<Deadline> CommandWorkRegistry<Deadline>
where
    Deadline: Clone,
{
    fn new() -> Self {
        Self {
            known_actions: BTreeSet::new(),
            cancelled_steps: BTreeSet::new(),
            active_by_step: BTreeMap::new(),
            active: BTreeMap::new(),
        }
    }

    fn register_start(&mut self, step: String, action: ActionId) -> Option<oneshot::Receiver<()>> {
        if !self.known_actions.insert(action)
            || self.cancelled_steps.contains(&step)
            || self.active_by_step.contains_key(&step)
        {
            return None;
        }

        let (cancellation_wake, cancellation) = oneshot::channel();
        self.active_by_step.insert(step.clone(), action);
        self.active.insert(
            action,
            CommandWork {
                step,
                phase: WorkPhase::Prelaunch,
                process_group: None,
                agent_process_control: None,
                agent_deadline_shutdown: None,
                agent_deadline_finished: None,
                cancellation: None,
                cancellation_wake: Some(cancellation_wake),
                interrupt_sent: false,
            },
        );
        Some(cancellation)
    }

    fn begin_launch(&mut self, action: ActionId) -> BeginLaunch<Deadline> {
        let Some(work) = self.active.get_mut(&action) else {
            return BeginLaunch::Gone;
        };
        if let Some(cancellation) = work.cancellation.clone() {
            return BeginLaunch::Cancelled(cancellation);
        }
        work.phase = WorkPhase::Launching;
        BeginLaunch::Launch
    }

    fn record_launch(
        &mut self,
        action: ActionId,
        process_group: AuthenticatedProcessGroup,
    ) -> RecordLaunch<Deadline> {
        let Some(work) = self.active.get_mut(&action) else {
            return RecordLaunch::Gone;
        };
        work.phase = WorkPhase::Running;
        work.process_group = Some(process_group.clone());
        let Some(cancellation) = work.cancellation.clone() else {
            return RecordLaunch::Running;
        };
        let interrupt = (!work.interrupt_sent).then_some(process_group);
        work.interrupt_sent = true;
        RecordLaunch::Cancelled {
            cancellation,
            interrupt,
        }
    }

    fn record_agent_launch(
        &mut self,
        action: ActionId,
        process_control: AgentProcessControl,
    ) -> RecordLaunch<Deadline> {
        let Some(work) = self.active.get_mut(&action) else {
            return RecordLaunch::Gone;
        };
        work.phase = WorkPhase::Running;
        work.agent_process_control = Some(process_control);
        match work.cancellation.clone() {
            Some(cancellation) => RecordLaunch::Cancelled {
                cancellation,
                interrupt: None,
            },
            None => RecordLaunch::Running,
        }
    }

    fn cancel(
        &mut self,
        step: String,
        action: ActionId,
        deadline: Deadline,
    ) -> CancellationRegistration<Deadline> {
        if !self.known_actions.insert(action) {
            return CancellationRegistration::Duplicate;
        }
        self.cancelled_steps.insert(step.clone());
        let Some(start_action) = self.active_by_step.get(&step).copied() else {
            return CancellationRegistration::Quiesced;
        };
        let Some(work) = self.active.get_mut(&start_action) else {
            return CancellationRegistration::Quiesced;
        };
        if work.cancellation.is_some() {
            return CancellationRegistration::Duplicate;
        }

        work.cancellation = Some(CommandCancellation {
            step,
            action,
            deadline: deadline.clone(),
        });
        let interrupt = match (work.phase, work.process_group.clone()) {
            (WorkPhase::Running, Some(process_group)) if !work.interrupt_sent => {
                work.interrupt_sent = true;
                Some(process_group)
            }
            _ => None,
        };
        let agent_deadline = work.agent_process_control.clone().map(|process_control| {
            process_control.interrupt();
            let (shutdown, deadline_shutdown) = oneshot::channel();
            let (deadline_finished, finished) = oneshot::channel();
            work.agent_deadline_shutdown = Some(shutdown);
            work.agent_deadline_finished = Some(finished);
            AgentCancellationDeadline {
                process_control,
                deadline,
                shutdown: deadline_shutdown,
                finished: deadline_finished,
            }
        });
        CancellationRegistration::Active {
            wake: work.cancellation_wake.take(),
            interrupt,
            agent_deadline,
        }
    }

    fn cancellation_for(&self, action: ActionId) -> Option<CommandCancellation<Deadline>> {
        self.active.get(&action)?.cancellation.clone()
    }

    fn force_abort(
        &mut self,
        step: String,
        action: ActionId,
        deadline: Deadline,
    ) -> ForceAbortRegistration {
        if !self.known_actions.insert(action) {
            return ForceAbortRegistration::Duplicate;
        }
        self.cancelled_steps.insert(step.clone());
        let Some(start_action) = self.active_by_step.get(&step).copied() else {
            return ForceAbortRegistration::Quiesced;
        };
        let Some(work) = self.active.get_mut(&start_action) else {
            return ForceAbortRegistration::Quiesced;
        };
        match work.cancellation.as_mut() {
            Some(cancellation) => {
                cancellation.action = action;
                cancellation.deadline = deadline;
            }
            None => {
                work.cancellation = Some(CommandCancellation {
                    step: step.clone(),
                    action,
                    deadline,
                });
            }
        }
        ForceAbortRegistration::Active {
            wake: work.cancellation_wake.take(),
            process_group: work.process_group.clone(),
            agent_process_control: work.agent_process_control.clone(),
        }
    }

    fn revoke_all_for_commit_failure(
        &mut self,
        deadline: Deadline,
    ) -> Vec<(
        Option<oneshot::Sender<()>>,
        Option<AuthenticatedProcessGroup>,
    )> {
        let mut revocations = Vec::with_capacity(self.active.len());
        for (&start_action, work) in &mut self.active {
            self.cancelled_steps.insert(work.step.clone());
            let action = work
                .cancellation
                .as_ref()
                .map_or(start_action, |cancellation| cancellation.action);
            work.cancellation = Some(CommandCancellation {
                step: work.step.clone(),
                action,
                deadline: deadline.clone(),
            });
            let interrupt = match (work.phase, work.process_group.clone()) {
                (WorkPhase::Running, Some(process_group)) if !work.interrupt_sent => {
                    work.interrupt_sent = true;
                    Some(process_group)
                }
                _ => None,
            };
            revocations.push((work.cancellation_wake.take(), interrupt));
        }
        revocations
    }

    fn ensure_interrupt(&mut self, action: ActionId) -> Option<AuthenticatedProcessGroup> {
        let work = self.active.get_mut(&action)?;
        if work.interrupt_sent {
            return None;
        }
        let process_group = work.process_group.clone()?;
        work.interrupt_sent = true;
        Some(process_group)
    }

    fn claim_completion(&mut self, action: ActionId) -> CompletionClaim<Deadline> {
        let claim = self.completion_status(action);
        if matches!(claim, CompletionClaim::Lifecycle) {
            let _ = self.remove_active(action);
        }
        claim
    }

    fn begin_completion(&mut self, action: ActionId) -> CompletionClaim<Deadline> {
        let claim = self.completion_status(action);
        if matches!(claim, CompletionClaim::Lifecycle) {
            let Some(work) = self.active.get_mut(&action) else {
                return CompletionClaim::Gone;
            };
            work.phase = WorkPhase::Completing;
        }
        claim
    }

    fn finish_completion(&mut self, action: ActionId) -> CompletionClaim<Deadline> {
        self.claim_completion(action)
    }

    fn completion_status(&self, action: ActionId) -> CompletionClaim<Deadline> {
        let Some(work) = self.active.get(&action) else {
            return CompletionClaim::Gone;
        };
        match work.cancellation.clone() {
            Some(cancellation) => CompletionClaim::Cancelled(cancellation),
            None => CompletionClaim::Lifecycle,
        }
    }

    fn finish_cancellation(
        &mut self,
        action: ActionId,
        fallback: CommandCancellation<Deadline>,
    ) -> (CommandCancellation<Deadline>, Option<oneshot::Receiver<()>>) {
        let cancellation = self
            .active
            .get(&action)
            .and_then(|work| work.cancellation.clone())
            .unwrap_or(fallback);
        let deadline_finished = self.remove_active(action);
        (cancellation, deadline_finished)
    }

    fn abandon(&mut self, action: ActionId) {
        let _ = self.remove_active(action);
    }

    fn remove_active(&mut self, action: ActionId) -> Option<oneshot::Receiver<()>> {
        let mut work = self.active.remove(&action)?;
        if let Some(shutdown) = work.agent_deadline_shutdown.take() {
            let _ = shutdown.send(());
        }
        if self.active_by_step.get(&work.step) == Some(&action) {
            self.active_by_step.remove(&work.step);
        }
        work.agent_deadline_finished.take()
    }
}

enum StartDelivery<Deadline> {
    Published,
    Cancelled(CommandCancellation<Deadline>),
    Gone,
}

enum AgentLifecycleBoundary {
    Started(Result<(), AgentStartReceiveError>),
    Terminal(Result<AgentOutcome, AgentTerminalReceiveError>),
}

enum BeginLaunch<Deadline> {
    Launch,
    Cancelled(CommandCancellation<Deadline>),
    Gone,
}

enum RecordLaunch<Deadline> {
    Running,
    Cancelled {
        cancellation: CommandCancellation<Deadline>,
        interrupt: Option<AuthenticatedProcessGroup>,
    },
    Gone,
}

enum ClaimedCompletion<Deadline> {
    Lifecycle(DriverOccurrenceClaim),
    Cancelled(CommandCancellation<Deadline>),
    Gone,
}

enum CompletionClaim<Deadline> {
    Lifecycle,
    Cancelled(CommandCancellation<Deadline>),
    Gone,
}

struct AgentCancellationDeadline<Deadline> {
    process_control: AgentProcessControl,
    deadline: Deadline,
    shutdown: oneshot::Receiver<()>,
    finished: oneshot::Sender<()>,
}

enum ForceAbortRegistration {
    Active {
        wake: Option<oneshot::Sender<()>>,
        process_group: Option<AuthenticatedProcessGroup>,
        agent_process_control: Option<AgentProcessControl>,
    },
    Quiesced,
    Duplicate,
}

enum CancellationRegistration<Deadline> {
    Active {
        wake: Option<oneshot::Sender<()>>,
        interrupt: Option<AuthenticatedProcessGroup>,
        agent_deadline: Option<AgentCancellationDeadline<Deadline>>,
    },
    Quiesced,
    Duplicate,
}

fn lock_registry<Registry>(registry: &Mutex<Registry>) -> MutexGuard<'_, Registry> {
    match registry.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl<Clock, Observer, Dispatcher>
    ActionPort<
        RequestedAction<ProvisionalStepOutputs, StepFailureCause, CapturedValue, Clock::Instant>,
    > for StepRuntime<Clock, Observer, Dispatcher>
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
    Observer: ExecutionObserver<Clock::Instant>,
    Dispatcher: WorkflowAgentDispatcher<Clock::Instant, Observer>,
{
    fn release(
        &mut self,
        requested: RequestedAction<
            ProvisionalStepOutputs,
            StepFailureCause,
            CapturedValue,
            Clock::Instant,
        >,
    ) -> impl Future<Output = ()> {
        let runtime = self.clone();
        async move {
            match requested.action {
                Action::StartStep {
                    step,
                    execution_number: _,
                    inputs,
                } => {
                    let Some(cancellation) = runtime.register_start(step.clone(), requested.id)
                    else {
                        return;
                    };
                    let tasks = runtime.tasks.clone();
                    tasks.spawn(async move {
                        let _ = runtime
                            .execute_registered_step(step, requested.id, inputs, cancellation)
                            .await;
                    });
                }
                // A recovery action intentionally has the same delivery deduplication shell as
                // a target action but dispatches a graph-input-free physical invocation.
                // jscpd:ignore-start
                Action::StartRecoveryHandler {
                    step,
                    round,
                    history,
                    ..
                } => {
                    let Some(cancellation) = runtime.register_start(step.clone(), requested.id)
                    else {
                        return;
                    };
                    let tasks = runtime.tasks.clone();
                    tasks.spawn(async move {
                        let _ = runtime
                            .execute_recovery_handler(
                                step,
                                round,
                                requested.id,
                                history,
                                cancellation,
                            )
                            .await;
                    });
                }
                // jscpd:ignore-end
                Action::CancelStep { step, deadline, .. } => {
                    match runtime.request_capture_cancellation(&step, requested.id) {
                        CaptureCancellationRegistration::Active
                        | CaptureCancellationRegistration::Duplicate => {}
                        CaptureCancellationRegistration::NotFound => {
                            match runtime.request_cancellation(step.clone(), requested.id, deadline)
                            {
                                CancellationRegistration::Active {
                                    wake,
                                    interrupt,
                                    agent_deadline,
                                } => {
                                    if let Some(group) = interrupt {
                                        let _ = interrupt_authenticated_process_group(&group);
                                    }
                                    if let Some(agent_deadline) = agent_deadline {
                                        let clock = runtime.clock.clone();
                                        let tasks = runtime.tasks.clone();
                                        tasks.spawn(async move {
                                            let AgentCancellationDeadline {
                                                process_control,
                                                deadline,
                                                mut shutdown,
                                                finished,
                                            } = agent_deadline;
                                            let deadline = clock.wait_until(deadline);
                                            tokio::pin!(deadline);
                                            tokio::select! {
                                                biased;
                                                () = &mut deadline => process_control.force(),
                                                _ = &mut shutdown => {}
                                            }
                                            let _ = finished.send(());
                                        });
                                    }
                                    if let Some(wake) = wake {
                                        let _ = wake.send(());
                                    }
                                }
                                CancellationRegistration::Quiesced => {
                                    runtime.schedule_quiesced_report(step, requested.id);
                                }
                                CancellationRegistration::Duplicate => {}
                            }
                        }
                    }
                }
                Action::ForceAbortStep { step, deadline, .. } => {
                    match runtime.request_capture_force_abort(&step, requested.id) {
                        CaptureCancellationRegistration::Active
                        | CaptureCancellationRegistration::Duplicate => {}
                        CaptureCancellationRegistration::NotFound => {
                            match runtime.request_force_abort(step.clone(), requested.id, deadline)
                            {
                                ForceAbortRegistration::Active {
                                    wake,
                                    process_group,
                                    agent_process_control,
                                } => {
                                    if let Some(group) = process_group {
                                        let _ = super::process_group::terminate_authenticated_process_group(
                                            &group,
                                        );
                                    }
                                    if let Some(control) = agent_process_control {
                                        control.force();
                                    }
                                    if let Some(wake) = wake {
                                        let _ = wake.send(());
                                    }
                                }
                                ForceAbortRegistration::Quiesced => {
                                    runtime.schedule_quiesced_report(step, requested.id);
                                }
                                ForceAbortRegistration::Duplicate => {}
                            }
                        }
                    }
                }
                Action::CaptureOutputs { step, provisional } => {
                    // Registration preserves reducer action order while the queue remains
                    // independently revocable and occurrence delivery waits for acceptance.
                    runtime.register_capture(step, requested.id, provisional);
                }
                Action::FinishRun { .. } => {}
            }
        }
    }
}

pub(crate) trait WorkflowCommitPort<Clock>:
    CommitPort<CommittedReduction<StepFailureCause, CapturedValue, Clock::Instant>>
where
    Clock: CoordinatorClock,
{
}

impl<Clock, Commits> WorkflowCommitPort<Clock> for Commits
where
    Clock: CoordinatorClock,
    Commits: CommitPort<CommittedReduction<StepFailureCause, CapturedValue, Clock::Instant>>,
{
}

pub(crate) async fn execute_workflow<Clock, Commits>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    inputs: &InputStaging,
    diagnostics: &StepDiagnosticLog,
    clock: Clock,
    commits: Commits,
) -> Result<CoordinationResult<StepFailureCause, CapturedValue, Clock::Instant>, CoordinationError>
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
    Commits: WorkflowCommitPort<Clock>,
{
    execute_workflow_observed(
        admitted,
        artifacts,
        inputs,
        diagnostics,
        clock,
        commits,
        NoopExecutionObserver,
        AgentExecution::disabled(),
        ProcessGuardRegistry::default(),
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "the shared operation receives each adapter-owned resource explicitly"
)]
pub(super) async fn execute_workflow_observed<Clock, Commits, Observer, Dispatcher>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    inputs: &InputStaging,
    diagnostics: &StepDiagnosticLog,
    clock: Clock,
    commits: Commits,
    observer: Observer,
    agents: AgentExecution<Dispatcher>,
    process_guards: ProcessGuardRegistry,
) -> Result<CoordinationResult<StepFailureCause, CapturedValue, Clock::Instant>, CoordinationError>
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
    Commits: WorkflowCommitPort<Clock>,
    Observer: ExecutionObserver<Clock::Instant>,
    Dispatcher: WorkflowAgentDispatcher<Clock::Instant, Observer>,
{
    if !artifacts.is_bound_to(admitted.execution()) {
        return Err(CoordinationError::ArtifactStagingMismatch);
    }
    if !inputs.is_bound_to(admitted.execution()) {
        return Err(CoordinationError::InputStagingMismatch);
    }
    match &agents {
        AgentExecution::Disabled
            if !admitted.agent_steps().is_empty() || !admitted.recovery_handlers().is_empty() =>
        {
            return Err(CoordinationError::AgentRuntimeUnavailable);
        }
        AgentExecution::Enabled { staging, .. } if !staging.is_bound_to(admitted.execution()) => {
            return Err(CoordinationError::AgentInputStagingMismatch);
        }
        AgentExecution::Disabled | AgentExecution::Enabled { .. } => {}
    }
    let channel_capacity = admitted.execution().limits().maximum_parallel_steps();
    let (sender, receiver) = occurrence_channel(channel_capacity);
    let actions = StepRuntime::with_observer(
        admitted.clone(),
        artifacts.clone(),
        inputs.clone(),
        diagnostics.clone(),
        sender,
        clock.clone(),
        observer,
        agents,
        process_guards,
    );
    let lifecycle = actions.clone();
    let result = Coordinator::new(admitted, receiver, clock, commits, actions)
        .run()
        .await;
    if matches!(result, Err(CoordinationError::CommitFailed)) {
        lifecycle.quiesce_after_commit_failure().await;
    } else {
        lifecycle.shutdown().await;
    }
    result
}

enum StepBody<'a> {
    Command(&'a super::validated::ValidatedCommandStep),
    Agent(&'a super::validated::ValidatedAgentStep),
}

impl<'a> From<&'a ValidatedStep> for StepBody<'a> {
    fn from(step: &'a ValidatedStep) -> Self {
        match step {
            ValidatedStep::Command(command) => Self::Command(command),
            ValidatedStep::Agent(agent) => Self::Agent(agent),
        }
    }
}

impl StepBody<'_> {
    fn common(&self) -> &ValidatedCommonStep {
        match self {
            Self::Command(command) => &command.common,
            Self::Agent(agent) => &agent.common,
        }
    }
}

fn workflow_node<'a>(admitted: &'a AdmittedWorkflow, id: &str) -> Option<&'a ValidatedStep> {
    admitted.workflow().definition.steps.get(id).or_else(|| {
        admitted
            .workflow()
            .definition
            .finalizers
            .get(id)
            .map(|finalizer| &finalizer.body)
    })
}

fn resolve_agent_upstream_outputs(
    agent: &ValidatedAgentStep,
    action_inputs: &BTreeMap<String, ActionInput<CapturedValue>>,
) -> Result<BTreeMap<ResolvedOutputSource, CapturedValue>, StepStartFailure> {
    let mut message_sources = agent
        .agent
        .message
        .text
        .iter()
        .chain(&agent.agent.message.attachments);
    let sources = message_sources
        .clone()
        .filter_map(|source| match source {
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Output(source),
                ..
            } => Some(source.clone()),
            ValidatedMessageSource::File { .. }
            | ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Import(_) | ResolvedValueSource::FinalizationContext,
                ..
            } => None,
        })
        .collect::<BTreeSet<_>>();
    let consumes_finalization_context = message_sources.any(|source| {
        matches!(
            source,
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::FinalizationContext,
                ..
            }
        )
    });
    let expected_input_count = sources
        .len()
        .saturating_add(usize::from(consumes_finalization_context));
    if expected_input_count != action_inputs.len()
        || (consumes_finalization_context
            && !matches!(
                action_inputs.get("finalization.context"),
                Some(ActionInput::FinalizationContext(_))
            ))
    {
        return Err(StepStartFailure::InputsUnavailable);
    }
    sources
        .into_iter()
        .map(|source| {
            let value = match action_inputs.get(&source.reference()) {
                Some(ActionInput::Output(value)) => value.clone(),
                Some(
                    ActionInput::Import
                    | ActionInput::FinalizationContext(_)
                    | ActionInput::Unavailable,
                )
                | None => {
                    return Err(StepStartFailure::InputsUnavailable);
                }
            };
            Ok((source, value))
        })
        .collect()
}

fn completed_agent_outputs(
    output: Option<String>,
    completed: CompletedAgentInvocation,
) -> Result<BTreeMap<String, CapturedValue>, AgentFailureCause> {
    let output = match (output, completed) {
        (None, CompletedAgentInvocation::NoValue)
        | (Some(_), CompletedAgentInvocation::NoResponse) => return Ok(BTreeMap::new()),
        (Some(output), CompletedAgentInvocation::Response(response)) => {
            (output, CapturedValue::Text(response.into_text()))
        }
        (Some(output), CompletedAgentInvocation::Result(result)) => {
            (output, CapturedValue::Json(result.into_value()))
        }
        (
            None,
            CompletedAgentInvocation::NoResponse
            | CompletedAgentInvocation::Response(_)
            | CompletedAgentInvocation::Result(_),
        )
        | (Some(_), CompletedAgentInvocation::NoValue) => {
            return Err(AgentFailureCause::HarnessProtocolFailed);
        }
    };
    Ok(BTreeMap::from([output]))
}

enum PreparedRecoveryHandler<Sink>
where
    Sink: AgentObservationSink,
{
    Command {
        command: PreparedCommand,
        context: super::recovery::RecoveryInvocationStaging,
    },
    Agent {
        agent: Box<MaterializedAgentInvocation<Sink>>,
        context: super::recovery::RecoveryInvocationStaging,
    },
}

struct PreparedStep<Sink>
where
    Sink: AgentObservationSink,
{
    body: PreparedStepBody<Sink>,
}

enum PreparedStepBody<Sink>
where
    Sink: AgentObservationSink,
{
    Command(PreparedCommand),
    Agent(Box<PreparedAgent<Sink>>),
}

struct PreparedAgent<Sink>
where
    Sink: AgentObservationSink,
{
    materialized: MaterializedAgentInvocation<Sink>,
}

struct PreparedCommand {
    program: PathBuf,
    arguments: Vec<OsString>,
    cwd: AdmittedWorkingDirectory,
    environment: EnvironmentSnapshot,
    inputs: Option<InputView>,
}

impl PreparedCommand {
    fn launch<Deadline, Observer>(
        self,
        guarded: bool,
        step: String,
        invocation: ActionId,
        diagnostic_log: &StepDiagnosticLog,
        maximum_stream_bytes: NonZeroU64,
        observer: Observer,
    ) -> Result<LaunchedStepBody, StepStartFailure>
    where
        Deadline: Send + 'static,
        Observer: ExecutionObserver<Deadline>,
    {
        let Self {
            program,
            arguments,
            cwd,
            environment,
            inputs,
        } = self;
        if !cwd.validate_execution_root() {
            return Err(StepStartFailure::WorkingDirectory(
                WorkingDirectoryFailure::ExecutionRootRebound,
            ));
        }
        let environment = environment
            .variables()
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        let child = if guarded {
            CommandChild::Guarded(
                StoppedChildGuard::spawn(&program, &arguments, &environment, |command| {
                    cwd.bind_command(command);
                    Ok(())
                })
                .map_err(|failure| {
                    StepStartFailure::CommandLaunch(classify_launch_failure(&failure))
                })?,
            )
        } else {
            let mut command = Command::new(program);
            command
                .args(arguments)
                .env_clear()
                .envs(environment)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            command.as_std_mut().process_group(0);
            cwd.bind_command(command.as_std_mut());
            let child = command.spawn().map_err(|failure| {
                StepStartFailure::CommandLaunch(classify_launch_failure(&failure))
            })?;
            CommandChild::direct(child)?
        };
        LaunchedStepBody::command(
            child,
            step,
            invocation,
            diagnostic_log,
            maximum_stream_bytes,
            inputs,
            observer,
        )
    }
}

enum CommandChild {
    Guarded(StoppedChildGuard),
    Direct {
        child: Child,
        identity: Option<AuthenticatedProcessGroup>,
    },
}

impl CommandChild {
    fn direct(mut child: Child) -> Result<Self, StepStartFailure> {
        let process_group = child
            .id()
            .and_then(|process_id| i32::try_from(process_id).ok())
            .and_then(Pid::from_raw);
        let Some(identity) = process_group.and_then(capture_process_group_identity) else {
            if let Some(process_group) = process_group {
                let _ = kill_process_group(process_group, Signal::KILL);
            }
            let _ = child.start_kill();
            return Err(StepStartFailure::CommandLaunch(CommandLaunchFailure::Other));
        };
        Ok(Self::Direct {
            child,
            identity: Some(identity),
        })
    }

    fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        match self {
            Self::Guarded(child) => child.take_stdout(),
            Self::Direct { child, .. } => child.stdout.take(),
        }
    }

    fn take_stderr(&mut self) -> Option<tokio::process::ChildStderr> {
        match self {
            Self::Guarded(child) => child.take_stderr(),
            Self::Direct { child, .. } => child.stderr.take(),
        }
    }
}

enum LaunchedStepBody {
    Command {
        child: CommandChild,
        registration: Option<ProcessGuardRegistration>,
        diagnostic: Option<PendingStepDiagnostic>,
        inputs: Option<InputView>,
    },
}

impl LaunchedStepBody {
    fn command<Deadline, Observer>(
        mut child: CommandChild,
        step: String,
        invocation: ActionId,
        diagnostic_log: &StepDiagnosticLog,
        maximum_stream_bytes: NonZeroU64,
        inputs: Option<InputView>,
        observer: Observer,
    ) -> Result<Self, StepStartFailure>
    where
        Deadline: Send + 'static,
        Observer: ExecutionObserver<Deadline>,
    {
        let (Some(standard_output), Some(standard_error)) =
            (child.take_stdout(), child.take_stderr())
        else {
            return Err(StepStartFailure::CommandLaunch(CommandLaunchFailure::Other));
        };
        let diagnostic = diagnostic_log.start_capture(
            step,
            invocation,
            maximum_stream_bytes,
            standard_output,
            standard_error,
            observer,
        );
        Ok(Self::Command {
            child,
            registration: None,
            diagnostic: Some(diagnostic),
            inputs,
        })
    }

    #[cfg(test)]
    fn fixture(child: Child, diagnostic: PendingStepDiagnostic) -> Self {
        Self::Command {
            child: CommandChild::Direct {
                child,
                identity: None,
            },
            registration: None,
            diagnostic: Some(diagnostic),
            inputs: None,
        }
    }

    fn process_group(&self) -> Option<&AuthenticatedProcessGroup> {
        match self {
            Self::Command {
                child: CommandChild::Guarded(child),
                ..
            } => Some(child.identity()),
            Self::Command {
                child: CommandChild::Direct { identity, .. },
                ..
            } => identity.as_ref(),
        }
    }

    fn install_registration(&mut self, registration: ProcessGuardRegistration) {
        match self {
            Self::Command {
                registration: current,
                ..
            } => *current = Some(registration),
        }
    }

    fn release(&mut self) -> Result<(), CommandLaunchFailure> {
        match self {
            Self::Command {
                child,
                registration: Some(registration),
                ..
            } => {
                if let CommandChild::Guarded(child) = child {
                    child
                        .continue_execution()
                        .map_err(|failure| classify_launch_failure(&failure))?;
                }
                registration
                    .mark_released()
                    .map_err(|()| CommandLaunchFailure::Other)
            }
            Self::Command { .. } => Err(CommandLaunchFailure::Other),
        }
    }

    async fn wait(&mut self) -> Result<ExitStatus, ()> {
        let status = match self {
            Self::Command {
                child: CommandChild::Guarded(child),
                ..
            } => child.wait().await.map_err(|_| ()),
            Self::Command {
                child: CommandChild::Direct { child, .. },
                ..
            } => child.wait().await.map_err(|_| ()),
        }?;
        self.mark_quiesced()?;
        Ok(status)
    }

    fn force_process_group(&self) {
        match self {
            Self::Command {
                child: CommandChild::Guarded(child),
                ..
            } => {
                let _ =
                    super::process_group::terminate_authenticated_process_group(child.identity());
            }
            Self::Command {
                child:
                    CommandChild::Direct {
                        identity: Some(identity),
                        ..
                    },
                ..
            } => {
                // Non-durable adapters retain their legacy direct-child lifecycle;
                // local durable execution always uses the authenticated guard branch.
                let _ = kill_process_group(identity.process_group(), Signal::KILL);
            }
            Self::Command { .. } => {}
        }
    }

    async fn force_stop(&mut self) -> Result<(), ()> {
        self.force_process_group();
        match self {
            Self::Command {
                child: CommandChild::Guarded(child),
                ..
            } => child.force_stop().await.map_err(|_| ())?,
            Self::Command {
                child: CommandChild::Direct { child, .. },
                ..
            } => force_stop_direct_child(child).await?,
        }
        self.mark_quiesced()
    }

    fn mark_quiesced(&mut self) -> Result<(), ()> {
        match self {
            Self::Command { registration, .. } => match registration {
                Some(registration) => registration.mark_quiesced(),
                None => Ok(()),
            },
        }
    }

    async fn finish_resources(&mut self) {
        let (diagnostic, inputs) = match self {
            Self::Command {
                diagnostic, inputs, ..
            } => (diagnostic.take(), inputs.take()),
        };
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish().await;
        }
        drop(inputs);
    }
}

pub(super) fn resolve_working_directory(
    execution_root: &AdmittedExecutionRoot,
    declared_cwd: Option<&str>,
) -> Result<AdmittedWorkingDirectory, WorkingDirectoryFailure> {
    execution_root
        .select_working_directory(declared_cwd)
        .map_err(|failure| match failure {
            WorkingDirectorySelectionFailure::ExecutionRootRebound => {
                WorkingDirectoryFailure::ExecutionRootRebound
            }
            WorkingDirectorySelectionFailure::Unavailable => WorkingDirectoryFailure::Unavailable,
            WorkingDirectorySelectionFailure::EscapesExecutionRoot => {
                WorkingDirectoryFailure::EscapesExecutionRoot
            }
            WorkingDirectorySelectionFailure::NotDirectory => WorkingDirectoryFailure::NotDirectory,
        })
}

fn prepare_command(
    argv: &[String],
    cwd: AdmittedWorkingDirectory,
    environment: EnvironmentSnapshot,
) -> Result<PreparedCommand, StepStartFailure> {
    let (program, arguments) = argv
        .split_first()
        .ok_or(StepStartFailure::CommandPreparation(
            CommandPreparationFailure::InvalidArgv,
        ))?;
    let program = match resolve_program(program, &cwd, &environment) {
        Ok(program) => program,
        Err(failure) if cwd.validate_execution_root() => return Err(failure),
        Err(_) => {
            return Err(StepStartFailure::WorkingDirectory(
                WorkingDirectoryFailure::ExecutionRootRebound,
            ));
        }
    };
    Ok(PreparedCommand {
        program,
        arguments: arguments.iter().map(OsString::from).collect(),
        cwd,
        environment,
        inputs: None,
    })
}

fn resolve_program(
    program: &str,
    cwd: &AdmittedWorkingDirectory,
    environment: &EnvironmentSnapshot,
) -> Result<PathBuf, StepStartFailure> {
    let program_path = Path::new(program);
    if program_path.is_absolute() {
        return Ok(program_path.to_owned());
    }
    if program.contains(std::path::MAIN_SEPARATOR) {
        return Ok(program_path.to_owned());
    }

    let search_path =
        environment
            .variable(OsStr::new("PATH"))
            .ok_or(StepStartFailure::CommandPreparation(
                CommandPreparationFailure::PathNotConfigured,
            ))?;
    let mut unavailable_candidate = false;
    for directory in env::split_paths(search_path) {
        let child_candidate = directory.join(program_path);
        match cwd.executable_candidate(&child_candidate) {
            ExecutableCandidate::Executable => return Ok(child_candidate),
            ExecutableCandidate::Missing => {}
            ExecutableCandidate::Unavailable => unavailable_candidate = true,
        }
    }

    let failure = if unavailable_candidate {
        CommandPreparationFailure::ExecutableUnavailable
    } else {
        CommandPreparationFailure::ExecutableNotFound
    };
    Err(StepStartFailure::CommandPreparation(failure))
}

fn classify_launch_failure(failure: &io::Error) -> CommandLaunchFailure {
    match failure.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => CommandLaunchFailure::NotFound,
        io::ErrorKind::PermissionDenied => CommandLaunchFailure::PermissionDenied,
        io::ErrorKind::InvalidInput => CommandLaunchFailure::InvalidInput,
        _ => CommandLaunchFailure::Other,
    }
}

fn unsuccessful_exit(status: ExitStatus) -> CommandExecutionFailure {
    CommandExecutionFailure::UnsuccessfulExit {
        code: status.code(),
    }
}

#[cfg(test)]
mod tests;
