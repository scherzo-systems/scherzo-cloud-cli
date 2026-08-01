use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::future::Future;
use std::io;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::{Arc, Mutex, MutexGuard};

use rustix::fs::{Access, AtFlags, CWD, accessat};
use rustix::process::{Pid, Signal, kill_process_group};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;

use super::admission::{AdmittedWorkflow, EnvironmentSnapshot};
use super::artifact::{ArtifactStaging, CaptureFailure, CapturedArtifact};
use super::coordinator::{
    ActionPort, CommitPort, CommittedReduction, CoordinationError, CoordinationResult, Coordinator,
    CoordinatorClock, DriverOccurrence, OccurrenceSender, occurrence_channel,
};
use super::document::Output;
use super::runtime::{Action, ActionId, OutputSet, RequestedAction};
use super::validated::{ValidatedCommonStep, ValidatedStep};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepBodyKind {
    Command,
    Agent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkingDirectoryFailure {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepStartFailure {
    StepUnavailable,
    InputsUnsupported,
    OutputsUnsupported,
    WorkingDirectory(WorkingDirectoryFailure),
    UnsupportedBody(StepBodyKind),
    CommandPreparation(CommandPreparationFailure),
    CommandLaunch(CommandLaunchFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandExecutionFailure {
    UnsuccessfulExit { code: Option<i32> },
    Wait,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepExecutionFailure {
    Command(CommandExecutionFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OutputCaptureFailure {
    StepUnavailable,
    UnsupportedOutput,
    Capture(CaptureFailure),
    TaskUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StepFailureCause {
    Start(StepStartFailure),
    Execution(StepExecutionFailure),
    OutputCapture(OutputCaptureFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepRuntimeError {
    OccurrenceReceiverClosed,
}

#[derive(Clone)]
pub(crate) struct StepRuntime<Clock>
where
    Clock: CoordinatorClock,
{
    admitted: AdmittedWorkflow,
    artifacts: ArtifactStaging,
    occurrences: OccurrenceSender<(), StepFailureCause, CapturedArtifact>,
    clock: Clock,
    work: Arc<Mutex<CommandWorkRegistry<Clock::Instant>>>,
}

impl<Clock> StepRuntime<Clock>
where
    Clock: CoordinatorClock,
{
    pub(crate) fn new(
        admitted: AdmittedWorkflow,
        artifacts: ArtifactStaging,
        occurrences: OccurrenceSender<(), StepFailureCause, CapturedArtifact>,
        clock: Clock,
    ) -> Self {
        Self {
            admitted,
            artifacts,
            occurrences,
            clock,
            work: Arc::new(Mutex::new(CommandWorkRegistry::new())),
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
        self.execute_registered_step(step, action, cancellation)
            .await
    }

    async fn execute_registered_step(
        &self,
        step: String,
        action: ActionId,
        mut cancellation: oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        let prepared = match self.prepare_step(&step) {
            Ok(prepared) => prepared,
            Err(failure) => return self.settle_start_failure(step, action, failure).await,
        };

        match self.with_work(|work| work.begin_launch(action)) {
            BeginLaunch::Launch => {}
            BeginLaunch::Cancelled(cancellation) => {
                return self.quiesce_unlaunched(action, cancellation).await;
            }
            BeginLaunch::Gone => return Ok(()),
        }

        let mut launched = match prepared.body.launch() {
            Ok(launched) => launched,
            Err(failure) => return self.settle_start_failure(step, action, failure).await,
        };
        let Some(process_group) = launched.process_group() else {
            launched.force_stop().await;
            return self
                .settle_start_failure(
                    step,
                    action,
                    StepStartFailure::CommandLaunch(CommandLaunchFailure::Other),
                )
                .await;
        };

        match self.with_work(|work| work.record_launch(action, process_group)) {
            RecordLaunch::Running => {}
            RecordLaunch::Cancelled {
                cancellation,
                interrupt,
            } => {
                if let Some(group) = interrupt {
                    interrupt_process_group(group);
                }
                return self
                    .cancel_launched(action, &mut launched, cancellation)
                    .await;
            }
            RecordLaunch::Gone => {
                launched.force_stop().await;
                return Ok(());
            }
        }

        let started = {
            let send = self.send(DriverOccurrence::step_started(step.clone(), action));
            tokio::pin!(send);
            tokio::select! {
                biased;
                _ = &mut cancellation => None,
                result = &mut send => Some(result),
            }
        };
        match started {
            None => {
                if let Some(cancellation) = self.cancellation_for(action) {
                    return self
                        .cancel_launched(action, &mut launched, cancellation)
                        .await;
                }
            }
            Some(Err(failure)) => {
                launched.force_stop().await;
                self.with_work(|work| work.abandon(action));
                return Err(failure);
            }
            Some(Ok(())) => {}
        }

        let waited = tokio::select! {
            biased;
            _ = &mut cancellation => None,
            result = launched.wait() => Some(result),
        };
        if waited.is_none() {
            if let Some(cancellation) = self.cancellation_for(action) {
                return self
                    .cancel_launched(action, &mut launched, cancellation)
                    .await;
            }
        }

        self.settle_launched(step, action, &mut launched, waited)
            .await
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
        occurrence: DriverOccurrence<(), StepFailureCause, CapturedArtifact>,
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
            launched.force_stop().await;
        }
        launched.force_process_group();

        match self.with_work(|work| work.claim_completion(action)) {
            CompletionClaim::Lifecycle => {
                let occurrence = match waited {
                    Ok(status) if status.success() => {
                        DriverOccurrence::step_execution_completed(step, action, ())
                    }
                    Ok(status) => DriverOccurrence::step_execution_failed(
                        step,
                        action,
                        StepFailureCause::Execution(StepExecutionFailure::Command(
                            unsuccessful_exit(status),
                        )),
                    ),
                    Err(()) => DriverOccurrence::step_execution_failed(
                        step,
                        action,
                        StepFailureCause::Execution(StepExecutionFailure::Command(
                            CommandExecutionFailure::Wait,
                        )),
                    ),
                };
                self.send(occurrence).await
            }
            CompletionClaim::Cancelled(cancellation) => {
                launched.force_process_group();
                self.finish_cancellation(action, cancellation).await
            }
            CompletionClaim::Gone => Ok(()),
        }
    }

    async fn cancel_launched(
        &self,
        action: ActionId,
        launched: &mut LaunchedStepBody,
        cancellation: CommandCancellation<Clock::Instant>,
    ) -> Result<(), StepRuntimeError> {
        if let Some(group) = self.with_work(|work| work.ensure_interrupt(action)) {
            interrupt_process_group(group);
        }

        let deadline = self.clock.wait_until(cancellation.deadline.clone());
        tokio::pin!(deadline);
        tokio::select! {
            biased;
            () = &mut deadline => launched.force_stop().await,
            waited = launched.wait() => {
                if waited.is_err() {
                    launched.force_stop().await;
                } else {
                    launched.force_process_group();
                }
            }
        }
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
        self.with_work(|work| work.finish_cancellation(action));
        self.send(DriverOccurrence::step_quiesced(
            cancellation.step,
            cancellation.action,
        ))
        .await
    }

    fn prepare_step(&self, step: &str) -> Result<PreparedStep, StepStartFailure> {
        let definition = self
            .admitted
            .workflow()
            .definition
            .steps
            .get(step)
            .ok_or(StepStartFailure::StepUnavailable)?;
        let body = StepBody::from(definition);
        let common = body.common();
        if !common.inputs.is_empty() {
            return Err(StepStartFailure::InputsUnsupported);
        }
        if common
            .outputs
            .values()
            .any(|output| !matches!(&output.definition, Output::File { .. }))
        {
            return Err(StepStartFailure::OutputsUnsupported);
        }

        let cwd =
            prepare_working_directory(self.admitted.execution().root(), common.cwd.as_deref())?;
        let body = match body {
            StepBody::Command(command) => PreparedStepBody::Command(prepare_command(
                command.argv.as_slice(),
                cwd,
                self.admitted.execution().environment().clone(),
            )?),
            StepBody::Agent(_) => PreparedStepBody::Agent,
        };
        Ok(PreparedStep { body })
    }

    fn register_start(&self, step: String, action: ActionId) -> Option<oneshot::Receiver<()>> {
        self.with_work(|work| work.register_start(step, action))
    }

    fn request_cancellation(
        &self,
        step: String,
        action: ActionId,
        deadline: Clock::Instant,
    ) -> CancellationRegistration {
        self.with_work(|work| work.cancel(step, action, deadline))
    }

    fn cancellation_for(&self, action: ActionId) -> Option<CommandCancellation<Clock::Instant>> {
        self.with_work(|work| work.cancellation_for(action))
    }

    fn with_work<Output>(
        &self,
        operation: impl FnOnce(&mut CommandWorkRegistry<Clock::Instant>) -> Output,
    ) -> Output {
        operation(&mut lock_registry(&self.work))
    }

    async fn capture_outputs(
        &self,
        step: String,
        action: ActionId,
    ) -> Result<(), StepRuntimeError> {
        let runtime = self.clone();
        let capture_step = step.clone();
        let captured = tokio::task::spawn_blocking(move || {
            runtime.capture_outputs_blocking(capture_step.as_str())
        })
        .await
        .unwrap_or(Err(OutputCaptureFailure::TaskUnavailable));
        match captured {
            Ok(outputs) => {
                let unreachable = outputs.values().cloned().collect::<Vec<_>>();
                let result = self
                    .send(DriverOccurrence::outputs_captured(step, action, outputs))
                    .await;
                if result.is_err() {
                    self.discard_outputs(&unreachable);
                }
                result
            }
            Err(failure) => {
                self.send(DriverOccurrence::output_capture_failed(
                    step,
                    action,
                    StepFailureCause::OutputCapture(failure),
                ))
                .await
            }
        }
    }

    fn capture_outputs_blocking(
        &self,
        step: &str,
    ) -> Result<OutputSet<CapturedArtifact>, OutputCaptureFailure> {
        let definition = self
            .admitted
            .workflow()
            .definition
            .steps
            .get(step)
            .ok_or(OutputCaptureFailure::StepUnavailable)?;
        let body = StepBody::from(definition);
        let common = body.common();
        let mut captured = OutputSet::new();
        for (output_identity, output) in &common.outputs {
            let Output::File { path, media_type } = &output.definition else {
                self.discard_outputs(captured.values());
                return Err(OutputCaptureFailure::UnsupportedOutput);
            };
            match self.artifacts.capture(
                output_identity.as_str(),
                Path::new(path),
                media_type.as_str(),
            ) {
                Ok(artifact) => {
                    captured.insert(output_identity.clone(), artifact);
                }
                Err(failure) => {
                    self.discard_outputs(captured.values());
                    return Err(OutputCaptureFailure::Capture(failure));
                }
            }
        }
        Ok(captured)
    }

    fn discard_outputs<'a>(&self, outputs: impl IntoIterator<Item = &'a CapturedArtifact>) {
        for output in outputs {
            self.artifacts.discard(output);
        }
    }

    async fn send(
        &self,
        occurrence: DriverOccurrence<(), StepFailureCause, CapturedArtifact>,
    ) -> Result<(), StepRuntimeError> {
        self.occurrences
            .send(occurrence)
            .await
            .map_err(|_| StepRuntimeError::OccurrenceReceiverClosed)
    }

    #[cfg(test)]
    fn active_work_count(&self) -> usize {
        self.with_work(|work| work.active.len())
    }
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
}

struct CommandWork<Deadline> {
    step: String,
    phase: WorkPhase,
    process_group: Option<Pid>,
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

    fn record_launch(&mut self, action: ActionId, process_group: Pid) -> RecordLaunch<Deadline> {
        let Some(work) = self.active.get_mut(&action) else {
            return RecordLaunch::Gone;
        };
        work.phase = WorkPhase::Running;
        work.process_group = Some(process_group);
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

    fn cancel(
        &mut self,
        step: String,
        action: ActionId,
        deadline: Deadline,
    ) -> CancellationRegistration {
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
            deadline,
        });
        let interrupt = match (work.phase, work.process_group) {
            (WorkPhase::Running, Some(process_group)) if !work.interrupt_sent => {
                work.interrupt_sent = true;
                Some(process_group)
            }
            _ => None,
        };
        CancellationRegistration::Active {
            wake: work.cancellation_wake.take(),
            interrupt,
        }
    }

    fn cancellation_for(&self, action: ActionId) -> Option<CommandCancellation<Deadline>> {
        self.active.get(&action)?.cancellation.clone()
    }

    fn ensure_interrupt(&mut self, action: ActionId) -> Option<Pid> {
        let work = self.active.get_mut(&action)?;
        if work.interrupt_sent {
            return None;
        }
        let process_group = work.process_group?;
        work.interrupt_sent = true;
        Some(process_group)
    }

    fn claim_completion(&mut self, action: ActionId) -> CompletionClaim<Deadline> {
        let Some(work) = self.active.get(&action) else {
            return CompletionClaim::Gone;
        };
        if let Some(cancellation) = work.cancellation.clone() {
            return CompletionClaim::Cancelled(cancellation);
        }
        self.remove_active(action);
        CompletionClaim::Lifecycle
    }

    fn finish_cancellation(&mut self, action: ActionId) {
        self.remove_active(action);
    }

    fn abandon(&mut self, action: ActionId) {
        self.remove_active(action);
    }

    fn remove_active(&mut self, action: ActionId) {
        let Some(work) = self.active.remove(&action) else {
            return;
        };
        if self.active_by_step.get(&work.step) == Some(&action) {
            self.active_by_step.remove(&work.step);
        }
    }
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
        interrupt: Option<Pid>,
    },
    Gone,
}

enum CompletionClaim<Deadline> {
    Lifecycle,
    Cancelled(CommandCancellation<Deadline>),
    Gone,
}

enum CancellationRegistration {
    Active {
        wake: Option<oneshot::Sender<()>>,
        interrupt: Option<Pid>,
    },
    Quiesced,
    Duplicate,
}

fn lock_registry<Deadline>(
    registry: &Mutex<CommandWorkRegistry<Deadline>>,
) -> MutexGuard<'_, CommandWorkRegistry<Deadline>> {
    match registry.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl<Clock> ActionPort<RequestedAction<(), StepFailureCause, CapturedArtifact, Clock::Instant>>
    for StepRuntime<Clock>
where
    Clock: CoordinatorClock,
{
    fn release(
        &mut self,
        requested: RequestedAction<(), StepFailureCause, CapturedArtifact, Clock::Instant>,
    ) -> impl Future<Output = ()> {
        let runtime = self.clone();
        async move {
            match requested.action {
                Action::StartStep { step } => {
                    let Some(cancellation) = runtime.register_start(step.clone(), requested.id)
                    else {
                        return;
                    };
                    drop(tokio::spawn(async move {
                        let _ = runtime
                            .execute_registered_step(step, requested.id, cancellation)
                            .await;
                    }));
                }
                Action::CancelStep { step, deadline, .. } => {
                    match runtime.request_cancellation(step.clone(), requested.id, deadline) {
                        CancellationRegistration::Active { wake, interrupt } => {
                            if let Some(group) = interrupt {
                                interrupt_process_group(group);
                            }
                            if let Some(wake) = wake {
                                let _ = wake.send(());
                            }
                        }
                        CancellationRegistration::Quiesced => {
                            drop(tokio::spawn(async move {
                                let _ = runtime
                                    .send(DriverOccurrence::step_quiesced(step, requested.id))
                                    .await;
                            }));
                        }
                        CancellationRegistration::Duplicate => {}
                    }
                }
                Action::CaptureOutputs { step, .. } => {
                    drop(tokio::spawn(async move {
                        let _ = runtime.capture_outputs(step, requested.id).await;
                    }));
                }
                Action::FinishRun { .. } => {}
            }
        }
    }
}

pub(crate) async fn execute_workflow<Clock, Commits>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    clock: Clock,
    commits: Commits,
) -> Result<CoordinationResult<StepFailureCause, CapturedArtifact>, CoordinationError>
where
    Clock: CoordinatorClock,
    Commits: CommitPort<CommittedReduction<StepFailureCause, CapturedArtifact>>,
{
    if !artifacts.is_bound_to(admitted.execution()) {
        return Err(CoordinationError::ArtifactStagingMismatch);
    }
    let channel_capacity = admitted.execution().limits().maximum_parallel_steps();
    let (sender, receiver) = occurrence_channel(channel_capacity);
    let actions = StepRuntime::new(admitted.clone(), artifacts.clone(), sender, clock.clone());
    Coordinator::new(admitted, receiver, clock, commits, actions)
        .run()
        .await
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

struct PreparedStep {
    body: PreparedStepBody,
}

enum PreparedStepBody {
    Command(PreparedCommand),
    Agent,
}

impl PreparedStepBody {
    fn launch(self) -> Result<LaunchedStepBody, StepStartFailure> {
        match self {
            Self::Command(command) => command.launch().map(LaunchedStepBody::command),
            Self::Agent => Err(StepStartFailure::UnsupportedBody(StepBodyKind::Agent)),
        }
    }
}

struct PreparedCommand {
    program: PathBuf,
    arguments: Vec<OsString>,
    cwd: PathBuf,
    environment: EnvironmentSnapshot,
}

impl PreparedCommand {
    fn launch(self) -> Result<Child, StepStartFailure> {
        let mut command = Command::new(self.program);
        command
            .args(self.arguments)
            .current_dir(self.cwd)
            .env_clear()
            .envs(self.environment.variables());
        command.as_std_mut().process_group(0);
        command
            .spawn()
            .map_err(|failure| StepStartFailure::CommandLaunch(classify_launch_failure(&failure)))
    }
}

enum LaunchedStepBody {
    Command {
        child: Child,
        process_group: Option<Pid>,
    },
}

impl LaunchedStepBody {
    fn command(child: Child) -> Self {
        let process_group = child
            .id()
            .and_then(|process_id| i32::try_from(process_id).ok())
            .and_then(Pid::from_raw);
        Self::Command {
            child,
            process_group,
        }
    }

    fn process_group(&self) -> Option<Pid> {
        match self {
            Self::Command { process_group, .. } => *process_group,
        }
    }

    async fn wait(&mut self) -> Result<ExitStatus, ()> {
        match self {
            Self::Command { child, .. } => child.wait().await.map_err(|_| ()),
        }
    }

    fn force_process_group(&self) {
        if let Some(process_group) = self.process_group() {
            terminate_process_group(process_group);
        }
    }

    async fn force_stop(&mut self) {
        self.force_process_group();
        match self {
            Self::Command { child, .. } => {
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
        }
    }
}

fn interrupt_process_group(process_group: Pid) {
    let _ = kill_process_group(process_group, Signal::INT);
}

fn terminate_process_group(process_group: Pid) {
    let _ = kill_process_group(process_group, Signal::KILL);
}

fn prepare_working_directory(
    execution_root: &Path,
    declared_cwd: Option<&str>,
) -> Result<PathBuf, StepStartFailure> {
    let candidate =
        declared_cwd.map_or_else(|| execution_root.to_owned(), |cwd| execution_root.join(cwd));
    let canonical = fs::canonicalize(candidate)
        .map_err(|_| StepStartFailure::WorkingDirectory(WorkingDirectoryFailure::Unavailable))?;
    if !canonical.starts_with(execution_root) {
        return Err(StepStartFailure::WorkingDirectory(
            WorkingDirectoryFailure::EscapesExecutionRoot,
        ));
    }
    if !canonical.is_dir() {
        return Err(StepStartFailure::WorkingDirectory(
            WorkingDirectoryFailure::NotDirectory,
        ));
    }
    Ok(canonical)
}

fn prepare_command(
    argv: &[String],
    cwd: PathBuf,
    environment: EnvironmentSnapshot,
) -> Result<PreparedCommand, StepStartFailure> {
    let (program, arguments) = argv
        .split_first()
        .ok_or(StepStartFailure::CommandPreparation(
            CommandPreparationFailure::InvalidArgv,
        ))?;
    let program = resolve_program(program, &cwd, &environment)?;
    Ok(PreparedCommand {
        program,
        arguments: arguments.iter().map(OsString::from).collect(),
        cwd,
        environment,
    })
}

fn resolve_program(
    program: &str,
    cwd: &Path,
    environment: &EnvironmentSnapshot,
) -> Result<PathBuf, StepStartFailure> {
    let program_path = Path::new(program);
    if program_path.is_absolute() {
        return Ok(program_path.to_owned());
    }
    if program.contains(std::path::MAIN_SEPARATOR) {
        return Ok(cwd.join(program_path));
    }

    let search_path =
        environment
            .variable(OsStr::new("PATH"))
            .ok_or(StepStartFailure::CommandPreparation(
                CommandPreparationFailure::PathNotConfigured,
            ))?;
    let mut unavailable_candidate = false;
    for directory in env::split_paths(search_path) {
        let directory = if directory.is_absolute() {
            directory
        } else {
            cwd.join(directory)
        };
        let candidate = directory.join(program_path);
        match candidate.metadata() {
            Ok(metadata) if metadata.is_file() => {
                if accessat(CWD, &candidate, Access::EXEC_OK, AtFlags::EACCESS).is_ok() {
                    return Ok(candidate);
                }
                unavailable_candidate = true;
            }
            Ok(_) => unavailable_candidate = true,
            Err(failure)
                if matches!(
                    failure.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) => {}
            Err(_) => unavailable_candidate = true,
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
