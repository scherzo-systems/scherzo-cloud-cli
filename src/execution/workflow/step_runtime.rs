use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::future::Future;
use std::io;
use std::num::NonZeroU64;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};

use rustix::fs::{Access, AtFlags, CWD, accessat};
use rustix::process::{Pid, Signal, kill_process_group};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

use super::admission::{AdmittedWorkflow, EnvironmentSnapshot};
#[cfg(test)]
use super::artifact::CaptureBoundaryObserver;
use super::artifact::{
    ArtifactStaging, CaptureAttemptFailure, CaptureCancellation, CaptureCandidateSet,
    CaptureDeclaration, CaptureFailure,
};
use super::coordinator::{
    ActionPort, CommitPort, CommittedReduction, CoordinationError, CoordinationResult, Coordinator,
    CoordinatorClock, DriverOccurrence, DriverOccurrenceAcceptance, DriverOccurrenceClaim,
    OccurrenceSender, occurrence_channel,
};
use super::diagnostic::{PendingStepDiagnostic, StepDiagnosticLog};
use super::document::Output;
use super::input::{InputPreparationFailure, InputStaging, InputValue, InputView};
use super::runtime::{Action, ActionId, ActionInput, RequestedAction};
use super::validated::{
    ResolvedValueSource, ValidatedCommandStep, ValidatedCommonStep, ValidatedStep, WorkflowImport,
};
use super::value::CapturedValue;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StepStartFailure {
    StepUnavailable,
    PreparationTaskUnavailable,
    InputsUnavailable,
    InputPreparation(InputPreparationFailure),
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
    diagnostics: StepDiagnosticLog,
    occurrences: OccurrenceSender<(), StepFailureCause, CapturedValue>,
    inputs: InputStaging,
    clock: Clock,
    work: Arc<Mutex<CommandWorkRegistry<Clock::Instant>>>,
    capture_work: Arc<Mutex<CaptureWorkRegistry>>,
    capture_requests: mpsc::UnboundedSender<CaptureRequest>,
}

struct CaptureRequest {
    step: String,
    action: ActionId,
}

#[derive(Clone)]
struct CaptureWorker {
    admitted: AdmittedWorkflow,
    artifacts: ArtifactStaging,
    occurrences: OccurrenceSender<(), StepFailureCause, CapturedValue>,
    work: Arc<Mutex<CaptureWorkRegistry>>,
}

enum CaptureWorkerFailure {
    Cancelled,
    Failed(OutputCaptureFailure),
}

impl<Clock> StepRuntime<Clock>
where
    Clock: CoordinatorClock,
{
    #[cfg(test)]
    fn new(
        admitted: AdmittedWorkflow,
        artifacts: ArtifactStaging,
        inputs: InputStaging,
        occurrences: OccurrenceSender<(), StepFailureCause, CapturedValue>,
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
        occurrences: OccurrenceSender<(), StepFailureCause, CapturedValue>,
        clock: Clock,
    ) -> Self {
        let capture_work = Arc::new(Mutex::new(CaptureWorkRegistry::new()));
        let (capture_requests, mut queued_captures) = mpsc::unbounded_channel::<CaptureRequest>();
        let capture_worker = CaptureWorker {
            admitted: admitted.clone(),
            artifacts: artifacts.clone(),
            occurrences: occurrences.clone(),
            work: Arc::clone(&capture_work),
        };
        drop(tokio::spawn(async move {
            while let Some(request) = queued_captures.recv().await {
                let begin = capture_worker.with_work(|work| work.begin(request.action));
                match begin {
                    BeginCapture::Capture(cancellation) => {
                        let blocking_worker = capture_worker.clone();
                        let step = request.step.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            blocking_worker.capture_outputs_blocking(&step, &cancellation)
                        })
                        .await
                        .unwrap_or(Err(CaptureWorkerFailure::Failed(
                            OutputCaptureFailure::TaskUnavailable,
                        )));
                        let settling_worker = capture_worker.clone();
                        drop(tokio::spawn(async move {
                            settling_worker.settle(request, result).await;
                        }));
                    }
                    BeginCapture::Cancelled => {
                        let settling_worker = capture_worker.clone();
                        drop(tokio::spawn(async move {
                            settling_worker.settle_cancelled(request.action).await;
                        }));
                    }
                    BeginCapture::Gone => {}
                }
            }
        }));
        Self {
            admitted,
            artifacts,
            diagnostics,
            occurrences,
            inputs,
            clock,
            work: Arc::new(Mutex::new(CommandWorkRegistry::new())),
            capture_work,
            capture_requests,
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
        mut cancellation: oneshot::Receiver<()>,
    ) -> Result<(), StepRuntimeError> {
        let preparation_runtime = self.clone();
        let preparation_step = step.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            preparation_runtime.prepare_step(&preparation_step, &inputs)
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

        let mut launched = match prepared.body.launch(
            step.clone(),
            &self.diagnostics,
            self.admitted.execution().limits().maximum_step_log_bytes(),
        ) {
            Ok(launched) => launched,
            Err(failure) => return self.settle_start_failure(step, action, failure).await,
        };
        let Some(process_group) = launched.process_group() else {
            launched.force_stop().await;
            launched.finish_resources().await;
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
                launched.finish_resources().await;
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
                launched.finish_resources().await;
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
        occurrence: DriverOccurrence<(), StepFailureCause, CapturedValue>,
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
        let occurrence = match waited {
            Ok(status) if status.success() => {
                DriverOccurrence::step_execution_completed(step, action, ())
            }
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
        match self.with_work(|work| work.begin_completion(action)) {
            CompletionClaim::Lifecycle => {}
            CompletionClaim::Cancelled(cancellation) => {
                launched.finish_resources().await;
                return self.finish_cancellation(action, cancellation).await;
            }
            CompletionClaim::Gone => {
                launched.finish_resources().await;
                return Ok(());
            }
        }
        let claim = match self.claim(occurrence).await {
            Ok(claim) => claim,
            Err(failure) => {
                launched.finish_resources().await;
                self.with_work(|work| work.abandon(action));
                return Err(failure);
            }
        };
        launched.finish_resources().await;

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
        self.with_work(|work| work.finish_cancellation(action));
        self.send(DriverOccurrence::step_quiesced(
            cancellation.step,
            cancellation.action,
        ))
        .await
    }

    fn prepare_step(
        &self,
        step: &str,
        action_inputs: &BTreeMap<String, ActionInput<CapturedValue>>,
    ) -> Result<PreparedStep, StepStartFailure> {
        let definition = self
            .admitted
            .workflow()
            .definition
            .steps
            .get(step)
            .ok_or(StepStartFailure::StepUnavailable)?;
        let body = StepBody::from(definition);
        let common = body.common();
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
            StepBody::Command(command) => {
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
            StepBody::Agent(_) => {
                if !action_inputs.is_empty() {
                    return Err(StepStartFailure::InputsUnavailable);
                }
                PreparedStepBody::Agent
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

    fn register_capture(&self, step: String, action: ActionId) {
        if !self.with_capture_work(|work| work.register(step.clone(), action)) {
            return;
        }
        if self
            .capture_requests
            .send(CaptureRequest { step, action })
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

    fn with_capture_work<Output>(
        &self,
        operation: impl FnOnce(&mut CaptureWorkRegistry) -> Output,
    ) -> Output {
        operation(&mut lock_registry(&self.capture_work))
    }

    #[cfg(test)]
    async fn capture_outputs(
        &self,
        step: String,
        action: ActionId,
    ) -> Result<(), StepRuntimeError> {
        self.register_capture(step, action);
        Ok(())
    }

    #[cfg(test)]
    fn set_capture_observer(&self, observer: Arc<dyn CaptureBoundaryObserver>) {
        self.with_capture_work(|work| work.observer = Some(observer));
    }

    async fn send(
        &self,
        occurrence: DriverOccurrence<(), StepFailureCause, CapturedValue>,
    ) -> Result<(), StepRuntimeError> {
        self.occurrences
            .send(occurrence)
            .await
            .map_err(|_| StepRuntimeError::OccurrenceReceiverClosed)
    }

    async fn claim(
        &self,
        occurrence: DriverOccurrence<(), StepFailureCause, CapturedValue>,
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
        cancellation: &CaptureCancellation,
    ) -> Result<CaptureCandidateSet, CaptureWorkerFailure> {
        let definition = self.admitted.workflow().definition.steps.get(step).ok_or(
            CaptureWorkerFailure::Failed(OutputCaptureFailure::StepUnavailable),
        )?;
        let body = StepBody::from(definition);
        let common = body.common();
        let declarations = common
            .outputs
            .iter()
            .map(|(output_identity, output)| {
                let Output::File { path, media_type } = &output.definition else {
                    return Err(CaptureWorkerFailure::Failed(
                        OutputCaptureFailure::UnsupportedOutput,
                    ));
                };
                Ok(CaptureDeclaration::new(
                    output_identity,
                    Path::new(path),
                    media_type,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.artifacts
            .capture_file_candidates(&declarations, cancellation)
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
    ) {
        match result {
            Ok(candidates) => self.settle_candidates(request, candidates).await,
            Err(CaptureWorkerFailure::Cancelled) => {
                self.settle_cancelled(request.action).await;
            }
            Err(CaptureWorkerFailure::Failed(failure)) => {
                self.settle_failure(request, failure).await;
            }
        }
    }

    async fn settle_candidates(&self, request: CaptureRequest, candidates: CaptureCandidateSet) {
        if self.with_work(|work| work.is_cancelled(request.action)) {
            candidates.abort();
            self.settle_cancelled(request.action).await;
            return;
        }
        self.with_work(|work| work.begin_delivery(request.action));
        let outputs = candidates
            .outputs()
            .iter()
            .map(|(name, output)| (name.clone(), CapturedValue::file(output.clone())))
            .collect();
        let occurrence = DriverOccurrence::outputs_captured(request.step, request.action, outputs);
        match self.occurrences.send_acknowledged(occurrence).await {
            Ok(DriverOccurrenceAcceptance::Accepted(commit)) => {
                drop(candidates.commit());
                self.with_work(|work| work.finish(request.action));
                commit.finalize();
            }
            Ok(DriverOccurrenceAcceptance::Rejected(finalization)) => {
                candidates.abort();
                let cancellation = self.with_work(|work| work.finish(request.action));
                finalization.finalize();
                if let Some(cancellation) = cancellation {
                    self.send_quiesced(cancellation).await;
                }
            }
            Err(_) => {
                candidates.abort();
                self.with_work(|work| work.finish(request.action));
            }
        }
    }

    async fn settle_failure(&self, request: CaptureRequest, failure: OutputCaptureFailure) {
        if self.with_work(|work| work.is_cancelled(request.action)) {
            self.settle_cancelled(request.action).await;
            return;
        }
        self.with_work(|work| work.begin_delivery(request.action));
        let occurrence = DriverOccurrence::output_capture_failed(
            request.step,
            request.action,
            StepFailureCause::OutputCapture(failure),
        );
        match self.occurrences.send_acknowledged(occurrence).await {
            Ok(DriverOccurrenceAcceptance::Accepted(commit)) => {
                self.with_work(|work| work.finish(request.action));
                commit.finalize();
            }
            Ok(DriverOccurrenceAcceptance::Rejected(finalization)) => {
                let cancellation = self.with_work(|work| work.finish(request.action));
                finalization.finalize();
                if let Some(cancellation) = cancellation {
                    self.send_quiesced(cancellation).await;
                }
            }
            Err(_) => {
                self.with_work(|work| work.finish(request.action));
            }
        }
    }

    async fn settle_cancelled(&self, action: ActionId) {
        let cancellation = self.with_work(|work| work.finish(action));
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
        if self.known_cancellations.contains(&action) {
            return CaptureCancellationRegistration::Duplicate;
        }
        let Some(capture_action) = self.active_by_step.get(step).copied() else {
            return CaptureCancellationRegistration::NotFound;
        };
        let Some(work) = self.active.get_mut(&capture_action) else {
            return CaptureCancellationRegistration::NotFound;
        };
        self.known_cancellations.insert(action);
        if work.revocation.is_some() {
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
        let claim = self.completion_status(action);
        if matches!(claim, CompletionClaim::Lifecycle) {
            self.remove_active(action);
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

fn lock_registry<Registry>(registry: &Mutex<Registry>) -> MutexGuard<'_, Registry> {
    match registry.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl<Clock> ActionPort<RequestedAction<(), StepFailureCause, CapturedValue, Clock::Instant>>
    for StepRuntime<Clock>
where
    Clock: CoordinatorClock,
{
    fn release(
        &mut self,
        requested: RequestedAction<(), StepFailureCause, CapturedValue, Clock::Instant>,
    ) -> impl Future<Output = ()> {
        let runtime = self.clone();
        async move {
            match requested.action {
                Action::StartStep { step, inputs } => {
                    let Some(cancellation) = runtime.register_start(step.clone(), requested.id)
                    else {
                        return;
                    };
                    drop(tokio::spawn(async move {
                        let _ = runtime
                            .execute_registered_step(step, requested.id, inputs, cancellation)
                            .await;
                    }));
                }
                Action::CancelStep { step, deadline, .. } => {
                    match runtime.request_capture_cancellation(&step, requested.id) {
                        CaptureCancellationRegistration::Active
                        | CaptureCancellationRegistration::Duplicate => {}
                        CaptureCancellationRegistration::NotFound => {
                            match runtime.request_cancellation(step.clone(), requested.id, deadline)
                            {
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
                                            .send(DriverOccurrence::step_quiesced(
                                                step,
                                                requested.id,
                                            ))
                                            .await;
                                    }));
                                }
                                CancellationRegistration::Duplicate => {}
                            }
                        }
                    }
                }
                Action::CaptureOutputs { step, .. } => {
                    // Registration preserves reducer action order while the queue remains
                    // independently revocable and occurrence delivery waits for acceptance.
                    runtime.register_capture(step, requested.id);
                }
                Action::FinishRun { .. } => {}
            }
        }
    }
}

pub(crate) async fn execute_workflow<Clock, Commits>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    inputs: &InputStaging,
    diagnostics: &StepDiagnosticLog,
    clock: Clock,
    commits: Commits,
) -> Result<CoordinationResult<StepFailureCause, CapturedValue>, CoordinationError>
where
    Clock: CoordinatorClock,
    Commits: CommitPort<CommittedReduction<StepFailureCause, CapturedValue>>,
{
    if !artifacts.is_bound_to(admitted.execution()) {
        return Err(CoordinationError::ArtifactStagingMismatch);
    }
    if !inputs.is_bound_to(admitted.execution()) {
        return Err(CoordinationError::InputStagingMismatch);
    }
    let channel_capacity = admitted.execution().limits().maximum_parallel_steps();
    let (sender, receiver) = occurrence_channel(channel_capacity);
    let actions = StepRuntime::with_diagnostics(
        admitted.clone(),
        artifacts.clone(),
        inputs.clone(),
        diagnostics.clone(),
        sender,
        clock.clone(),
    );
    let result = Coordinator::new(admitted, receiver, clock, commits, actions)
        .run()
        .await?;
    inputs
        .release()
        .map_err(|_| CoordinationError::InputStagingCleanup)?;
    Ok(result)
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
    fn launch(
        self,
        step: String,
        diagnostic_log: &StepDiagnosticLog,
        maximum_stream_bytes: NonZeroU64,
    ) -> Result<LaunchedStepBody, StepStartFailure> {
        match self {
            Self::Command(command) => command.launch(step, diagnostic_log, maximum_stream_bytes),
            Self::Agent => Err(StepStartFailure::UnsupportedBody(StepBodyKind::Agent)),
        }
    }
}

struct PreparedCommand {
    program: PathBuf,
    arguments: Vec<OsString>,
    cwd: PathBuf,
    environment: EnvironmentSnapshot,
    inputs: Option<InputView>,
}

impl PreparedCommand {
    fn launch(
        self,
        step: String,
        diagnostic_log: &StepDiagnosticLog,
        maximum_stream_bytes: NonZeroU64,
    ) -> Result<LaunchedStepBody, StepStartFailure> {
        let Self {
            program,
            arguments,
            cwd,
            environment,
            inputs,
        } = self;
        let mut command = Command::new(program);
        command
            .args(arguments)
            .current_dir(cwd)
            .env_clear()
            .envs(environment.variables())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.as_std_mut().process_group(0);
        let child = command.spawn().map_err(|failure| {
            StepStartFailure::CommandLaunch(classify_launch_failure(&failure))
        })?;
        LaunchedStepBody::command(child, step, diagnostic_log, maximum_stream_bytes, inputs)
    }
}

enum LaunchedStepBody {
    Command {
        child: Child,
        process_group: Option<Pid>,
        diagnostic: Option<PendingStepDiagnostic>,
        inputs: Option<InputView>,
    },
}

impl LaunchedStepBody {
    fn command(
        mut child: Child,
        step: String,
        diagnostic_log: &StepDiagnosticLog,
        maximum_stream_bytes: NonZeroU64,
        inputs: Option<InputView>,
    ) -> Result<Self, StepStartFailure> {
        let process_group = child
            .id()
            .and_then(|process_id| i32::try_from(process_id).ok())
            .and_then(Pid::from_raw);
        let (Some(standard_output), Some(standard_error)) =
            (child.stdout.take(), child.stderr.take())
        else {
            if let Some(process_group) = process_group {
                terminate_process_group(process_group);
            }
            let _ = child.start_kill();
            return Err(StepStartFailure::CommandLaunch(CommandLaunchFailure::Other));
        };
        let diagnostic = diagnostic_log.start_capture(
            step,
            maximum_stream_bytes,
            standard_output,
            standard_error,
        );
        Ok(Self::Command {
            child,
            process_group,
            diagnostic: Some(diagnostic),
            inputs,
        })
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
        inputs: None,
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
