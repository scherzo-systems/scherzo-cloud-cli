use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read};
use std::ops::Add;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::Args;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::unix::AsyncFd;

use crate::exit_code::{ExitCode, OutcomeClass};
#[cfg(test)]
use um_execution::admit_workflow;
use um_execution::{
    ActionId, AdmittedWorkflow, AgentExecution, AgentHarnessInstallationFailure, AgentInputStaging,
    ArtifactStaging, CancellationPolicy, CancellationReason, CancellationSource, ColorChoice,
    CoordinationError, CoordinatorClock, DisplayDeadline, DurableDeadline,
    DurableInvocationStateV1, DurableInvocationV1, EnvironmentSnapshot, ExecutionContext,
    ExecutionObservation, ExecutionObserver, FailurePolicy, InitialLocalRun, InputStaging,
    InvocationAccountingLog, LocalAttemptOwner, LocalAttemptOwnershipReleased,
    LocalPublicationError, LocalPublicationPhase, MAXIMUM_PARALLEL_STEPS, ObservationClock,
    PresentationConfig, PresentationFailure, PresentationFailureOperation, PresentationMode,
    PublicationFailurePhaseV1, PublicationPresentation, RecoveryDiagnosticKindV1,
    RecoveryInvocationDiagnosticV1, RecoveryInvocationStateV1, RecoveryInvocationV1,
    RequestedPresentationMode, ResolvedAttachment, ResolvedFile, ResolvedInput, ResolvedInputs,
    ResolvedJsonInput, ResolvedWorkflow, RunOutcome, RunTimingObservation, RunTimingSnapshot,
    StepDiagnosticLog, SystemObservationClock, TerminalCapabilities, TerminalHostExit,
    TransitionSequence, ValidatedHarness, ValidatedRecoveryHandler, ValidatedStep,
    WorkflowExecutionResult, WorkflowExecutionStart, WorkflowNodeRole, WorkflowRunCancellation,
    WorkflowRunCleanupResult, WorkflowRunFinalization, WorkflowRunFinalizationCancellation,
    WorkflowRunId, WorkflowRunOutput, WorkflowRunPresentation, WorkflowRunPresentationResult,
    WorkflowRunPublicationResult, WorkflowRunResult, WorkflowRunStep, WorkflowRunStepKind,
    WorkflowRunTerminalResultV1, WorkflowRunTiming, WorkflowRunViewModel, WorkflowStepTiming,
    WorkflowTerminalHost, admit_local_workflow, command_output_v1, default_execution_policy_limits,
    discover_and_validate_claude_code_installation, discover_and_validate_codex_installation,
    discover_and_validate_pi_installation, execute_workflow, prepare_attempt_result_destination,
    production_agent_dispatcher, publish_prepared_workflow_result, resolve_workflow_file,
    step_recovery_summary_v1, summary_disposition_matches,
};

pub(super) const ABOUT: &str = "Run a local command and agent workflow";
pub(super) const AFTER_HELP: &str = "Interactive mode:
  Automatic mode uses the terminal interface only when stdin and stdout are terminals,
  TERM is usable, and stdin is not reserved by a Text or JSON input. Resize keeps the
  interface active; undersized terminals show a resize notice without changing modes.
  Use Up/Down or j/k to select steps, Enter to inspect logs, ? for complete help,
  Ctrl-C to request cancellation, and q to leave only after publication and cleanup.
  After q, Scherzo restores the terminal and prints the standard plain summary.";

const MAXIMUM_INPUTS: usize = 256;
const MAXIMUM_TEXT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_ATTACHMENTS: usize = 256;
const MAXIMUM_ATTACHMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const CANCELLATION_GRACE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutionLeaf {
    Run,
    Retry,
    Continue,
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    source: super::LocalWorkflowSource,

    #[command(flatten)]
    execution: super::LocalExecutionRoot,

    #[arg(
        long,
        value_name = "PATH",
        help = "Directory to create for this run (must not already exist)"
    )]
    run_dir: PathBuf,

    #[command(flatten)]
    inputs: super::super::NamedInputArgs,

    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = 1,
        value_parser = parse_parallelism,
        help = "Maximum simultaneous workflow steps"
    )]
    max_parallel: usize,

    #[command(flatten)]
    presentation: super::PresentationOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        execute_with_runtime("start local workflow runtime", self.execute_async())
    }

    async fn execute_async(self) -> super::super::CommandResult {
        let input_plan = self.input_plan().map_err(|error| {
            super::super::CommandFailure::with_exit_code(error, ExitCode::UsageError)
        })?;
        let presentation_config = self.presentation_config_with_input_plan(&input_plan);
        let cancellation = CancellationSource::new();
        let mut signal_task = AbortOnDrop(Some(start_signal_observation(
            cancellation.clone(),
            UnixSignals::new()?,
        )));

        let inputs = acquire_inputs(&input_plan, &cancellation)
            .await
            .map_err(|error| match cancellation.cancellation_reason() {
                Some(CancellationReason::UserRequest) => {
                    super::super::CommandFailure::for_outcome(error, OutcomeClass::Interrupted)
                }
                Some(CancellationReason::TerminationRequest) => {
                    super::super::CommandFailure::for_outcome(error, OutcomeClass::Terminated)
                }
                _ => error.into(),
            })?;
        let source_root = self.source.source_root.clone();
        let workflow_file = self.source.workflow_file.clone();
        let workflow =
            match blocking_operation(move || resolve_workflow_file(&source_root, &workflow_file))
                .await
            {
                Ok(workflow) => workflow,
                Err(BlockingOperationError::Operation(failure)) => {
                    return rejection_output(presentation_config, |output| {
                        output.render_resolution_rejection(&failure)
                    });
                }
                Err(BlockingOperationError::WorkerUnavailable) => {
                    return Err(anyhow!("resolve local workflow definition").into());
                }
            };
        let workflow_for_context = workflow.clone();
        let execution_root = self.execution.execution_root;
        let maximum_parallel_steps = self.max_parallel;
        let context_cancellation = cancellation.clone();
        let context = match blocking_operation(move || {
            execution_context_for_workflow(
                &workflow_for_context,
                execution_root,
                maximum_parallel_steps,
                context_cancellation,
            )
        })
        .await
        {
            Ok(context) => context,
            Err(BlockingOperationError::Operation(failure)) => {
                return rejection_output(presentation_config, |output| {
                    output.render_agent_harness_installation_rejection(&workflow, &failure)
                });
            }
            Err(BlockingOperationError::WorkerUnavailable) => {
                return Err(anyhow!("prepare local workflow execution context").into());
            }
        };
        let workflow_for_admission = workflow.clone();
        let admitted = match blocking_operation(move || {
            admit_local_workflow(workflow_for_admission, inputs, context)
        })
        .await
        {
            Ok(admitted) => admitted,
            Err(BlockingOperationError::Operation(failure)) => {
                return rejection_output(presentation_config, |output| {
                    output.render_admission_rejection(&workflow, &failure)
                });
            }
            Err(BlockingOperationError::WorkerUnavailable) => {
                return Err(anyhow!("admit local workflow").into());
            }
        };

        if workflow.source.source_root.to_str().is_none()
            || admitted.execution().root().to_str().is_none()
        {
            return Err(anyhow!(
                "prepare local workflow paths: an authoritative path is not valid UTF-8"
            )
            .into());
        }
        let run_directory = self.run_dir.clone();
        let admitted_for_creation = admitted.clone();
        let owned_run = tokio::task::spawn_blocking(move || {
            InitialLocalRun::create(&run_directory, &admitted_for_creation)
        })
        .await
        .map_err(anyhow::Error::new)
        .and_then(|result| result.map_err(anyhow::Error::new))
        .with_context(|| format!("create workflow run {}", self.run_dir.display()))?;
        execute_owned_attempt(
            workflow,
            admitted,
            owned_run,
            cancellation,
            signal_task
                .0
                .take()
                .ok_or_else(|| anyhow!("local workflow signal observation unavailable"))?,
            presentation_config,
            ExecutionLeaf::Run,
        )
        .await
    }

    fn input_plan(&self) -> anyhow::Result<InputPlan> {
        plan_inputs(
            &self.inputs.input_text,
            &self.inputs.input_text_file,
            &self.inputs.input_json,
            &self.inputs.input_json_file,
            &self.inputs.input_file,
            &self.inputs.input_attachment,
            &self.inputs.input_attachments_empty,
        )
    }

    fn presentation_config_with_input_plan(&self, input_plan: &InputPlan) -> PresentationConfig {
        presentation_config_with(
            &self.presentation,
            input_plan.standard_input_reserved,
            TerminalCapabilities::detect(),
        )
    }

    #[cfg(test)]
    fn presentation_config_with(&self, capabilities: TerminalCapabilities) -> PresentationConfig {
        let standard_input_reserved = self
            .input_plan()
            .is_ok_and(|plan| plan.standard_input_reserved);
        presentation_config_with(&self.presentation, standard_input_reserved, capabilities)
    }
}

pub(super) enum BlockingOperationError<Error> {
    Operation(Error),
    WorkerUnavailable,
}

pub(super) async fn blocking_operation<Value, Error, Operation>(
    operation: Operation,
) -> Result<Value, BlockingOperationError<Error>>
where
    Value: Send + 'static,
    Error: Send + 'static,
    Operation: FnOnce() -> Result<Value, Error> + Send + 'static,
{
    match tokio::task::spawn_blocking(operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(BlockingOperationError::Operation(error)),
        Err(_) => Err(BlockingOperationError::WorkerUnavailable),
    }
}

pub(super) fn execute_with_runtime(
    failure_context: &str,
    execution: impl Future<Output = super::super::CommandResult>,
) -> super::super::CommandResult {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .with_context(|| failure_context.to_owned())?;
    runtime.block_on(execution)
}

pub(super) fn presentation_config(presentation: &super::PresentationOptions) -> PresentationConfig {
    presentation_config_with(presentation, false, TerminalCapabilities::detect())
}

pub(super) fn presentation_config_with(
    presentation: &super::PresentationOptions,
    standard_input_reserved: bool,
    capabilities: TerminalCapabilities,
) -> PresentationConfig {
    PresentationConfig {
        requested_mode: if presentation.output.json {
            RequestedPresentationMode::Json
        } else if presentation.plain {
            RequestedPresentationMode::Plain
        } else {
            RequestedPresentationMode::Automatic
        },
        color: match presentation.color {
            super::ColorArgument::Auto => ColorChoice::Auto,
            super::ColorArgument::Always => ColorChoice::Always,
            super::ColorArgument::Never => ColorChoice::Never,
        },
        capabilities,
        standard_input_reserved,
    }
}

struct PreparedExecutionPresentation<Observer, Host> {
    observer: Observer,
    host: Host,
    timing: RunTimingObservation,
}

fn initialize_execution_presentation<Observer, Host, Clock>(
    clock: Clock,
    initialize: impl FnOnce() -> Result<
        PreparedExecutionPresentation<Observer, Host>,
        PresentationFailure,
    >,
) -> Result<PreparedExecutionPresentation<Observer, Host>, PresentationFailure>
where
    Clock: ObservationClock,
{
    let prepared = initialize()?;
    prepared.timing.mark_execution_started(clock.sample());
    Ok(prepared)
}

struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

struct AttemptTeardown {
    run: Option<LocalAttemptOwner>,
    signal_task: Option<tokio::task::JoinHandle<()>>,
    private_staging: Option<um_execution::AttemptPrivateStaging>,
    artifacts: Option<ArtifactStaging>,
    inputs: Option<InputStaging>,
    agents: Option<AgentInputStaging>,
    host: Option<ActiveRunHost>,
    execution_started: bool,
    armed: bool,
}

impl AttemptTeardown {
    fn new(run: LocalAttemptOwner, signal_task: tokio::task::JoinHandle<()>) -> Self {
        Self {
            run: Some(run),
            signal_task: Some(signal_task),
            private_staging: None,
            artifacts: None,
            inputs: None,
            agents: None,
            host: None,
            execution_started: false,
            armed: true,
        }
    }

    fn run(&self) -> anyhow::Result<&LocalAttemptOwner> {
        self.run
            .as_ref()
            .ok_or_else(|| anyhow!("local attempt ownership unavailable"))
    }

    fn host(&mut self) -> anyhow::Result<&mut ActiveRunHost> {
        self.host
            .as_mut()
            .ok_or_else(|| anyhow!("local attempt presentation unavailable"))
    }

    fn disarm(&mut self) {
        if let Some(signal_task) = self.signal_task.take() {
            signal_task.abort();
        }
        self.armed = false;
    }

    async fn teardown(&mut self) {
        if let Some(signal_task) = self.signal_task.take() {
            signal_task.abort();
        }
        if !self.execution_started
            && let Some(run) = self.run.as_ref()
        {
            settle_before_execution_failure(run).await;
        }
        let inputs = self.inputs.take();
        let agents = self.agents.take();
        let artifacts = self.artifacts.take();
        let private_staging = self.private_staging.take();
        let cleanup = blocking_operation(move || {
            let staging_failed = match (inputs.as_ref(), artifacts.as_ref()) {
                (Some(inputs), Some(artifacts)) => {
                    release_execution_staging(inputs, agents.as_ref(), artifacts)
                }
                (None, Some(artifacts)) => artifacts.release().is_err(),
                (Some(inputs), None) => inputs.release().is_err(),
                (None, None) => false,
            };
            let agent_failed = if inputs.is_none() || artifacts.is_none() {
                agents
                    .as_ref()
                    .is_some_and(|agents| agents.release().is_err())
            } else {
                false
            };
            Ok::<_, std::convert::Infallible>(
                staging_failed
                    | agent_failed
                    | private_staging.is_some_and(|staging| staging.release().is_err()),
            )
        })
        .await;
        if let Some(run) = self.run.take() {
            let cleanup_failed = !matches!(cleanup, Ok(false));
            if let Ok(run) = blocking_operation(move || {
                record_private_cleanup_failure(&run, cleanup_failed);
                Ok::<_, std::convert::Infallible>(run)
            })
            .await
            {
                self.run = Some(run);
            }
        }
        if let Some(host) = self.host.as_mut() {
            host.stop_terminal().await;
        }
        self.disarm();
    }
}

impl Drop for AttemptTeardown {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // A cancelled attempt cannot await its cleanup. Transfer the owned resources to
        // a task; the same teardown ordering applies as for an ordinary phase failure.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let mut pending = Self {
                run: self.run.take(),
                signal_task: self.signal_task.take(),
                private_staging: self.private_staging.take(),
                artifacts: self.artifacts.take(),
                inputs: self.inputs.take(),
                agents: self.agents.take(),
                host: self.host.take(),
                execution_started: self.execution_started,
                armed: false,
            };
            runtime.spawn(async move { pending.teardown().await });
        } else if let Some(signal_task) = self.signal_task.take() {
            signal_task.abort();
            // The staging owners' Drop implementations are the last-resort cleanup
            // when no executor remains to drive an asynchronous terminal shutdown.
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AttemptCheckpoint {
    ArtifactsStaged,
    InputsStaged,
    BeforeHostStart,
    AfterHostStart,
}

struct AttemptSettings<Hooks> {
    presentation_config: PresentationConfig,
    leaf: ExecutionLeaf,
    hooks: Hooks,
}

trait AttemptHooks {
    fn checkpoint(&self, point: AttemptCheckpoint) -> anyhow::Result<()>;
    fn start_terminal(
        &self,
        view: WorkflowRunViewModel<SystemObservationClock>,
        cancellation: CancellationSource,
        color: bool,
    ) -> Result<WorkflowTerminalHost, PresentationFailure>;
    fn execution_failure(&mut self) -> Option<CoordinationError>;
}

impl<Check, Start, Fail> AttemptHooks for (Check, Start, Fail)
where
    Check: Fn(AttemptCheckpoint) -> anyhow::Result<()>,
    Start: Fn(
        WorkflowRunViewModel<SystemObservationClock>,
        CancellationSource,
        bool,
    ) -> Result<WorkflowTerminalHost, PresentationFailure>,
    Fail: FnMut() -> Option<CoordinationError>,
{
    fn checkpoint(&self, point: AttemptCheckpoint) -> anyhow::Result<()> {
        self.0(point)
    }

    fn start_terminal(
        &self,
        view: WorkflowRunViewModel<SystemObservationClock>,
        cancellation: CancellationSource,
        color: bool,
    ) -> Result<WorkflowTerminalHost, PresentationFailure> {
        self.1(view, cancellation, color)
    }

    fn execution_failure(&mut self) -> Option<CoordinationError> {
        self.2()
    }
}

pub(super) async fn execute_owned_attempt(
    workflow: ResolvedWorkflow,
    admitted: AdmittedWorkflow,
    owned_run: LocalAttemptOwner,
    cancellation: CancellationSource,
    signal_task: tokio::task::JoinHandle<()>,
    presentation_config: PresentationConfig,
    leaf: ExecutionLeaf,
) -> super::super::CommandResult {
    execute_owned_attempt_with(
        workflow,
        admitted,
        owned_run,
        cancellation,
        signal_task,
        AttemptSettings {
            presentation_config,
            leaf,
            hooks: (|_| Ok(()), WorkflowTerminalHost::start, || None),
        },
    )
    .await
}

async fn execute_owned_attempt_with(
    workflow: ResolvedWorkflow,
    admitted: AdmittedWorkflow,
    owned_run: LocalAttemptOwner,
    cancellation: CancellationSource,
    signal_task: tokio::task::JoinHandle<()>,
    settings: AttemptSettings<impl AttemptHooks>,
) -> super::super::CommandResult {
    let mut attempt = AttemptTeardown::new(owned_run, signal_task);
    let result =
        execute_attempt_phases(&workflow, &admitted, &cancellation, &mut attempt, settings).await;
    let result = result.unwrap_or_else(|error| Err(error.into()));
    if result.is_err() {
        attempt.teardown().await;
    }
    result
}

async fn execute_attempt_phases(
    workflow: &ResolvedWorkflow,
    admitted: &AdmittedWorkflow,
    cancellation: &CancellationSource,
    attempt: &mut AttemptTeardown,
    settings: AttemptSettings<impl AttemptHooks>,
) -> anyhow::Result<super::super::CommandResult> {
    let AttemptSettings {
        presentation_config,
        leaf,
        mut hooks,
    } = settings;
    let run_directory = attempt
        .run()?
        .run_directory()
        .to_str()
        .ok_or_else(|| {
            anyhow!("prepare local workflow paths: an authoritative path is not valid UTF-8")
        })?
        .to_owned();
    let destination = prepare_attempt_result_destination(
        attempt.run()?.result_directory(),
        attempt.run()?.private_directory(),
        attempt.run()?.attempt_directory_handle(),
        attempt.run()?.private_directory_handle(),
    )?;
    attempt.private_staging = Some(attempt.run()?.create_private_staging()?);
    let staging = attempt
        .private_staging
        .as_ref()
        .ok_or_else(|| anyhow!("prepare private local workflow staging"))?;
    let artifacts =
        ArtifactStaging::create_bound(admitted.execution(), staging.path(), staging.root_handle())?;
    attempt.artifacts = Some(artifacts.clone());
    hooks.checkpoint(AttemptCheckpoint::ArtifactsStaged)?;
    let inputs =
        InputStaging::create_bound(admitted.execution(), staging.path(), staging.root_handle())?;
    attempt.inputs = Some(inputs.clone());
    hooks.checkpoint(AttemptCheckpoint::InputsStaged)?;
    let agent_staging = if admitted.agent_steps().is_empty() {
        None
    } else {
        Some(
            AgentInputStaging::create(admitted.execution(), staging.path())
                .map_err(anyhow::Error::new)
                .context("prepare private local agent staging")?,
        )
    };
    attempt.agents = agent_staging.clone();
    hooks.checkpoint(AttemptCheckpoint::BeforeHostStart)?;

    let run_clock = SystemObservationClock;
    let run_for_output = attempt.run()?;
    let prepared =
        initialize_execution_presentation(run_clock, || match presentation_config.mode() {
            PresentationMode::Tui => {
                let presentation_opened = run_clock.sample();
                let timing_observation = RunTimingObservation::new(presentation_opened);
                let view = WorkflowRunViewModel::new(
                    workflow,
                    admitted.execution().limits().maximum_parallel_steps().get(),
                    timing_observation.clone(),
                    run_clock,
                );
                let terminal = hooks.start_terminal(
                    view.clone(),
                    cancellation.clone(),
                    presentation_config.color_enabled(),
                )?;
                Ok(PreparedExecutionPresentation {
                    observer: RunExecutionObserver::Tui(view.clone()),
                    host: ActiveRunHost::Tui {
                        view,
                        terminal: Some(terminal),
                        failure: None,
                        config: Box::new(presentation_config),
                        leaf,
                    },
                    timing: timing_observation,
                })
            }
            PresentationMode::Plain | PresentationMode::Json => {
                let output = execution_output(presentation_config, run_for_output, leaf);
                let presentation = output.start_for_result(
                    workflow,
                    &run_directory,
                    admitted.execution().limits().maximum_parallel_steps().get(),
                    run_clock,
                )?;
                let timing_observation = RunTimingObservation::new(presentation.opened_at());
                let timing = TimingObserver::new(
                    presentation.clone(),
                    cancellation.clone(),
                    timing_observation.clone(),
                    run_clock,
                );
                Ok(PreparedExecutionPresentation {
                    observer: RunExecutionObserver::Standard(timing),
                    host: ActiveRunHost::Standard(presentation),
                    timing: timing_observation,
                })
            }
        })?;
    let observer = prepared.observer;
    attempt.host = Some(prepared.host);
    attempt.host()?.await_ready().await?;
    attempt.host()?.activate_execution()?;
    hooks.checkpoint(AttemptCheckpoint::AfterHostStart)?;

    if let Some(ActiveRunHost::Standard(presentation)) = attempt.host.as_ref() {
        let mut failures = presentation.subscribe_failures();
        let output_cancellation = cancellation.clone();
        tokio::spawn(async move {
            loop {
                if failures.borrow_and_update().is_some() {
                    output_cancellation
                        .request_cancellation(CancellationReason::CallerOutputFailure);
                    break;
                }
                if failures.changed().await.is_err() {
                    break;
                }
            }
        });
    }
    let agent_diagnostic_sessions = if agent_staging.is_some() {
        Some(attempt.run()?.create_agent_diagnostic_sessions()?)
    } else {
        None
    };
    let diagnostics = StepDiagnosticLog::default();
    let accounting = InvocationAccountingLog::default();
    let agents = match (agent_staging.as_ref(), agent_diagnostic_sessions) {
        (Some(staging), Some(diagnostic_sessions)) => {
            let dispatcher = production_agent_dispatcher(
                diagnostics.clone(),
                admitted.execution().limits().maximum_step_log_bytes(),
                SystemExecutionClock,
                observer.clone(),
                crate::build_info::VERSION,
            )
            .map_err(|error| anyhow!("prepare local agent runtimes: {error:?}"))?;
            AgentExecution::enabled_with_accounting(
                WorkflowRunId::from(Arc::from(run_directory.as_str())),
                staging.clone(),
                diagnostic_sessions,
                dispatcher,
                accounting.clone(),
            )
        }
        (None, None) => AgentExecution::Disabled,
        _ => return Err(anyhow!("prepare local agent diagnostic retention")),
    };
    let seed = attempt
        .run()?
        .execution_seed(admitted.clone(), artifacts.clone())
        .await
        .map_err(anyhow::Error::new)
        .context("load retained workflow values")?;
    let execution_start =
        WorkflowExecutionStart::seeded(attempt.run()?.process_guard_registry(), seed);
    attempt.execution_started = true;
    let execution = if let Some(error) = hooks.execution_failure() {
        Err(error)
    } else {
        execute_workflow(
            admitted.clone(),
            &artifacts,
            &inputs,
            &diagnostics,
            agents,
            SystemExecutionClock,
            attempt
                .run()?
                .commit_port(diagnostics.clone(), accounting, artifacts.clone()),
            observer.clone(),
            execution_start,
        )
        .await
    };
    if let Some(signal_task) = attempt.signal_task.take() {
        signal_task.abort();
    }
    let execution = match execution {
        Ok(execution) => execution,
        Err(error) => {
            if error == CoordinationError::CommitFailed {
                let _ = attempt
                    .run()?
                    .record_state_persistence_failure_async()
                    .await;
            }
            return Err(anyhow!("execute admitted local workflow: {error:?}"));
        }
    };
    let durable_invocations = attempt.run()?.durable_invocations()?;
    let observed_timing = observer.snapshot();
    let run_timing = observed_run_timing(&observed_timing)
        .ok_or_else(|| anyhow!("prepare authoritative local workflow terminal result"))?;
    let run = build_run_result(
        workflow,
        admitted,
        execution,
        LocalRunEvidence {
            diagnostics: &diagnostics,
            durable_invocations: &durable_invocations,
            timing: observed_timing,
        },
        run_timing,
        attempt.run()?,
    )?;
    attempt.host()?.reconcile_and_mark_quiescent(&run)?;
    attempt.host()?.begin_publication();
    let publication_run = run.clone();
    let publication_artifacts = artifacts.clone();
    let owned_run = attempt
        .run
        .take()
        .ok_or_else(|| anyhow!("local attempt ownership unavailable"))?;
    let (owned_run, publication, state_publication) = complete_blocking_phase(
        blocking_operation(move || {
            let mut publication = publish_prepared_workflow_result(
                &destination,
                &publication_artifacts,
                &publication_run,
            );
            if let Ok(terminal) = &mut publication {
                match leaf {
                    ExecutionLeaf::Retry => terminal.mark_retry(),
                    ExecutionLeaf::Continue => terminal.mark_continue(),
                    ExecutionLeaf::Run => {}
                }
            }
            let state_publication = match &publication {
                Ok(_) => owned_run.record_result_published(),
                Err(error) => owned_run.record_result_publication_failed(
                    publication_failure_phase(error.phase()),
                    error.invariant(),
                ),
            };
            Ok::<_, std::convert::Infallible>((owned_run, publication, state_publication))
        })
        .await,
        attempt.host()?,
        "publish terminal local workflow result",
    )
    .await?;
    attempt.run = Some(owned_run);
    attempt.host()?.complete_publication(&publication);
    attempt.host()?.begin_cleanup();
    let private_staging = attempt
        .private_staging
        .take()
        .ok_or_else(|| anyhow!("private local workflow staging unavailable"))?;
    let owned_run = attempt
        .run
        .take()
        .ok_or_else(|| anyhow!("local attempt ownership unavailable"))?;
    let (cleanup_failed, cleanup_state, released_ownership) = complete_blocking_phase(
        blocking_operation(move || {
            let execution_staging_failed =
                release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
            let private_staging_failed = private_staging.release().is_err();
            let cleanup_failed = execution_staging_failed || private_staging_failed;
            let cleanup_state = if cleanup_failed {
                owned_run.record_private_cleanup_failure()
            } else {
                Ok(())
            };
            let released_ownership = owned_run.release();
            Ok::<_, std::convert::Infallible>((cleanup_failed, cleanup_state, released_ownership))
        })
        .await,
        attempt.host()?,
        "release private local workflow staging",
    )
    .await?;
    attempt.host()?.complete_cleanup(cleanup_failed);
    let state_commit_failed = state_publication.is_err() || cleanup_state.is_err();
    attempt
        .host()?
        .mark_adapter_lifecycle_completed(released_ownership);
    attempt.disarm();
    Ok(attempt
        .host()?
        .finish(
            workflow,
            &run,
            &publication,
            cleanup_failed,
            state_commit_failed,
        )
        .await)
}

async fn complete_blocking_phase<Value>(
    completion: Result<Value, BlockingOperationError<std::convert::Infallible>>,
    host: &mut ActiveRunHost,
    failure_context: &str,
) -> anyhow::Result<Value> {
    match completion {
        Ok(value) => Ok(value),
        Err(BlockingOperationError::Operation(never)) => match never {},
        Err(BlockingOperationError::WorkerUnavailable) => {
            host.stop_terminal().await;
            Err(anyhow!(failure_context.to_owned()))
        }
    }
}

fn release_execution_staging(
    inputs: &InputStaging,
    agents: Option<&AgentInputStaging>,
    artifacts: &ArtifactStaging,
) -> bool {
    let input_failed = inputs.release().is_err();
    let agent_failed = agents.is_some_and(|staging| staging.release().is_err());
    let artifact_failed = artifacts.release().is_err();
    input_failed || agent_failed || artifact_failed
}

fn execution_output(
    config: PresentationConfig,
    owned_run: &LocalAttemptOwner,
    leaf: ExecutionLeaf,
) -> WorkflowRunOutput<io::Stdout, io::Stderr> {
    let output = WorkflowRunOutput::new(config, io::stdout(), io::stderr());
    match leaf {
        ExecutionLeaf::Run => output,
        ExecutionLeaf::Retry => output.for_retry(owned_run.run_directory()),
        ExecutionLeaf::Continue => output.for_continue(owned_run.run_directory()),
    }
}

fn rejection_output(
    config: PresentationConfig,
    render: impl FnOnce(WorkflowRunOutput<io::Stdout, io::Stderr>) -> WorkflowRunPresentationResult,
) -> super::super::CommandResult {
    rejection_exit(render(WorkflowRunOutput::new(
        config,
        io::stdout(),
        io::stderr(),
    )))
}

pub(super) fn rejection_exit(result: WorkflowRunPresentationResult) -> super::super::CommandResult {
    match result {
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: Some(diagnostic),
        } => Err(anyhow!(diagnostic).into()),
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: None,
        } => Ok(ExitCode::GeneralFailure),
        WorkflowRunPresentationResult::Failed(failure) => Err(anyhow::Error::new(failure).into()),
        WorkflowRunPresentationResult::PublicationFailed(error) => {
            Err(anyhow::Error::new(error).into())
        }
        WorkflowRunPresentationResult::Published { .. } => Ok(ExitCode::GeneralFailure),
    }
}

fn presentation_exit_code(result: WorkflowRunPresentationResult) -> super::super::CommandResult {
    match result {
        WorkflowRunPresentationResult::Published { outcome, .. } => Ok(outcome.into()),
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: Some(diagnostic),
        } => Err(anyhow!(diagnostic).into()),
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: None,
        } => Ok(ExitCode::GeneralFailure),
        WorkflowRunPresentationResult::PublicationFailed(error) => {
            Err(anyhow::Error::new(error).into())
        }
        WorkflowRunPresentationResult::Failed(failure) => Err(anyhow::Error::new(failure).into()),
    }
}

fn parse_parallelism(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| (1..=MAXIMUM_PARALLEL_STEPS).contains(value))
        .ok_or_else(|| format!("value must be between 1 and {MAXIMUM_PARALLEL_STEPS}"))
}

fn required_agent_harnesses(
    workflow: &ResolvedWorkflow,
) -> impl Iterator<Item = &ValidatedHarness> {
    let node_harnesses = workflow
        .definition
        .source_order
        .iter()
        .chain(&workflow.definition.finalizer_source_order)
        .filter_map(|step_name| {
            let step = workflow.definition.steps.get(step_name).or_else(|| {
                workflow
                    .definition
                    .finalizers
                    .get(step_name)
                    .map(|finalizer| &finalizer.body)
            })?;
            let ValidatedStep::Agent(step) = step else {
                return None;
            };
            Some(&step.agent.harness)
        });
    let recovery_harnesses = workflow
        .definition
        .source_order
        .iter()
        .filter_map(|step_name| {
            let handler = workflow
                .definition
                .recoveries
                .get(step_name)?
                .as_ref()?
                .handler
                .as_ref()?;
            let ValidatedRecoveryHandler::Agent { harness, .. } = handler else {
                return None;
            };
            Some(harness)
        });
    node_harnesses.chain(recovery_harnesses)
}

pub(super) fn execution_context_for_workflow(
    workflow: &ResolvedWorkflow,
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
) -> Result<ExecutionContext, AgentHarnessInstallationFailure> {
    let (context, mut failures) = execution_context_with_profile_failures(
        workflow,
        root,
        maximum_parallel_steps,
        cancellation,
        false,
    );
    match failures.pop() {
        Some(failure) => Err(failure),
        None => Ok(context),
    }
}

pub(super) fn continuation_execution_context(
    workflow: &ResolvedWorkflow,
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
) -> (ExecutionContext, Vec<AgentHarnessInstallationFailure>) {
    execution_context_with_profile_failures(
        workflow,
        root,
        maximum_parallel_steps,
        cancellation,
        true,
    )
}

fn execution_context_with_profile_failures(
    workflow: &ResolvedWorkflow,
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
    collect_all: bool,
) -> (ExecutionContext, Vec<AgentHarnessInstallationFailure>) {
    let environment = EnvironmentSnapshot::new(env::vars_os());
    let mut context = ExecutionContext::new(
        root,
        default_execution_policy_limits(maximum_parallel_steps),
        environment.clone(),
        CancellationPolicy::new(cancellation, CANCELLATION_GRACE),
    );
    if workflow.requires_git_capture() {
        context = context.with_local_git_capture();
    }

    let mut pi_validated = false;
    let mut claude_code_validated = false;
    let mut codex_validated = false;
    let mut failures = Vec::new();
    for harness in required_agent_harnesses(workflow) {
        let failure = match harness {
            ValidatedHarness::Pi(_) if !pi_validated => {
                pi_validated = true;
                match discover_and_validate_pi_installation() {
                    Ok(installation) => {
                        context = context.with_pi_installation(installation);
                        None
                    }
                    Err(error) => Some(AgentHarnessInstallationFailure::Pi(error)),
                }
            }
            ValidatedHarness::ClaudeCode(_) if !claude_code_validated => {
                claude_code_validated = true;
                match discover_and_validate_claude_code_installation() {
                    Ok(installation) => {
                        context = context.with_claude_code_installation(installation);
                        None
                    }
                    Err(error) => Some(AgentHarnessInstallationFailure::ClaudeCode(error)),
                }
            }
            ValidatedHarness::Codex(_) if !codex_validated => {
                codex_validated = true;
                match discover_and_validate_codex_installation() {
                    Ok(installation) => {
                        context = context.with_codex_installation(installation);
                        None
                    }
                    Err(error) => Some(AgentHarnessInstallationFailure::Codex(error)),
                }
            }
            ValidatedHarness::Pi(_)
            | ValidatedHarness::ClaudeCode(_)
            | ValidatedHarness::Codex(_) => None,
        };
        if let Some(failure) = failure {
            failures.push(failure);
            if !collect_all {
                break;
            }
        }
    }
    (context, failures)
}

#[derive(Debug)]
enum PlannedInput {
    TextInline(Arc<str>),
    TextFile(PathBuf),
    JsonInline(Arc<[u8]>),
    JsonFile(PathBuf),
    File(PlannedAttachment),
    Attachments(Vec<PlannedAttachment>),
}

#[derive(Debug)]
struct PlannedAttachment {
    media_type: Arc<str>,
    path: PathBuf,
}

#[derive(Debug)]
struct InputPlan {
    values: std::collections::BTreeMap<String, PlannedInput>,
    standard_input_reserved: bool,
}

fn plan_inputs(
    text_values: &[OsString],
    text_files: &[OsString],
    json_values: &[OsString],
    json_files: &[OsString],
    files: &[OsString],
    attachments: &[OsString],
    empty_attachments: &[String],
) -> anyhow::Result<InputPlan> {
    let mut values = std::collections::BTreeMap::new();
    let inline = text_values.chunks_exact(2);
    if !inline.remainder().is_empty() {
        return Err(anyhow!("invalid --input-text binding"));
    }
    for binding in inline {
        let name = input_argument(binding.first(), "input name")?;
        let text = input_argument(binding.get(1), "Text value")?;
        insert_scalar_input(&mut values, name, PlannedInput::TextInline(Arc::from(text)))?;
    }
    let text_file_bindings = text_files.chunks_exact(2);
    if !text_file_bindings.remainder().is_empty() {
        return Err(anyhow!("invalid --input-text-file binding"));
    }
    let mut standard_input_reserved = false;
    for binding in text_file_bindings {
        let name = input_argument(binding.first(), "input name")?;
        let path = PathBuf::from(binding.get(1).ok_or_else(|| anyhow!("missing Text path"))?);
        claim_standard_input(&path, &mut standard_input_reserved)?;
        insert_scalar_input(&mut values, name, PlannedInput::TextFile(path))?;
    }
    let inline = json_values.chunks_exact(2);
    if !inline.remainder().is_empty() {
        return Err(anyhow!("invalid --input-json binding"));
    }
    for binding in inline {
        let name = input_argument(binding.first(), "input name")?;
        let json = input_argument(binding.get(1), "JSON value")?;
        insert_scalar_input(
            &mut values,
            name,
            PlannedInput::JsonInline(Arc::from(json.as_bytes())),
        )?;
    }
    let json_file_bindings = json_files.chunks_exact(2);
    if !json_file_bindings.remainder().is_empty() {
        return Err(anyhow!("invalid --input-json-file binding"));
    }
    for binding in json_file_bindings {
        let name = input_argument(binding.first(), "input name")?;
        let path = PathBuf::from(binding.get(1).ok_or_else(|| anyhow!("missing JSON path"))?);
        claim_standard_input(&path, &mut standard_input_reserved)?;
        insert_scalar_input(&mut values, name, PlannedInput::JsonFile(path))?;
    }
    let scalar_files = files.chunks_exact(3);
    if !scalar_files.remainder().is_empty() {
        return Err(anyhow!("invalid --input-file binding"));
    }
    for binding in scalar_files {
        let name = input_argument(binding.first(), "input name")?;
        let media_type = input_argument(binding.get(1), "File media type")?;
        let path = PathBuf::from(binding.get(2).ok_or_else(|| anyhow!("missing File path"))?);
        insert_scalar_input(
            &mut values,
            name,
            PlannedInput::File(PlannedAttachment {
                media_type: Arc::from(media_type),
                path,
            }),
        )?;
    }
    let members = attachments.chunks_exact(3);
    if !members.remainder().is_empty() || members.len() > MAXIMUM_ATTACHMENTS {
        return Err(anyhow!("invalid named attachment bindings"));
    }
    for binding in members {
        let name = input_argument(binding.first(), "input name")?;
        let media_type = input_argument(binding.get(1), "attachment media type")?;
        let path = PathBuf::from(
            binding
                .get(2)
                .ok_or_else(|| anyhow!("missing attachment path"))?,
        );
        match values.entry(name.to_owned()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(PlannedInput::Attachments(vec![PlannedAttachment {
                    media_type: Arc::from(media_type),
                    path,
                }]));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let PlannedInput::Attachments(items) = entry.get_mut() else {
                    return Err(anyhow!("one input name has incompatible bindings"));
                };
                items.push(PlannedAttachment {
                    media_type: Arc::from(media_type),
                    path,
                });
            }
        }
    }
    for name in empty_attachments {
        validate_input_name(name)?;
        insert_scalar_input(&mut values, name, PlannedInput::Attachments(Vec::new()))?;
    }
    if values.len() > MAXIMUM_INPUTS {
        return Err(anyhow!("named input count exceeds {MAXIMUM_INPUTS}"));
    }
    Ok(InputPlan {
        values,
        standard_input_reserved,
    })
}

fn claim_standard_input(path: &Path, reserved: &mut bool) -> anyhow::Result<()> {
    if path != Path::new("-") {
        return Ok(());
    }
    if *reserved {
        return Err(anyhow!(
            "standard input may supply only one named Text or JSON input"
        ));
    }
    *reserved = true;
    Ok(())
}

fn input_argument<'a>(value: Option<&'a OsString>, description: &str) -> anyhow::Result<&'a str> {
    let value = value
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("{description} is not valid UTF-8"))?;
    if description == "input name" {
        validate_input_name(value)?;
    }
    Ok(value)
}

fn validate_input_name(name: &str) -> anyhow::Result<()> {
    if !um_execution::is_input_name(name) {
        return Err(anyhow!("invalid Workflow V1 input name"));
    }
    Ok(())
}

fn insert_scalar_input(
    values: &mut std::collections::BTreeMap<String, PlannedInput>,
    name: &str,
    input: PlannedInput,
) -> anyhow::Result<()> {
    validate_input_name(name)?;
    if values.insert(name.to_owned(), input).is_some() {
        return Err(anyhow!(
            "one input name has duplicate or incompatible bindings"
        ));
    }
    Ok(())
}

async fn acquire_inputs(
    plan: &InputPlan,
    cancellation: &CancellationSource,
) -> anyhow::Result<ResolvedInputs> {
    let mut total_bytes = 0_u64;
    let mut values = std::collections::BTreeMap::new();
    for (name, input) in &plan.values {
        if cancellation.is_cancelled() {
            return Err(input_error(
                InputAcquisitionFailureKind::Interrupted,
                name,
                None,
            ));
        }
        let value = match input {
            PlannedInput::TextInline(text) => {
                account_input_bytes(&mut total_bytes, text.len() as u64, MAXIMUM_TEXT_BYTES)?;
                ResolvedInput::Text(Arc::clone(text))
            }
            PlannedInput::TextFile(path) => {
                let bytes = acquire_scalar_file(path, name, &mut total_bytes, cancellation).await?;
                let text = String::from_utf8(bytes).map(Arc::from).map_err(|_| {
                    input_error(InputAcquisitionFailureKind::InvalidUtf8, name, Some(path))
                })?;
                ResolvedInput::Text(text)
            }
            PlannedInput::JsonInline(source) => {
                account_input_bytes(
                    &mut total_bytes,
                    u64::try_from(source.len()).map_err(|_| input_bytes_error())?,
                    MAXIMUM_TEXT_BYTES,
                )?;
                ResolvedInput::Json(ResolvedJsonInput::from_source(Arc::clone(source)).map_err(
                    |_| input_error(InputAcquisitionFailureKind::InvalidJson, name, None),
                )?)
            }
            PlannedInput::JsonFile(path) => {
                let bytes = acquire_scalar_file(path, name, &mut total_bytes, cancellation).await?;
                ResolvedInput::Json(ResolvedJsonInput::from_source(Arc::from(bytes)).map_err(
                    |_| input_error(InputAcquisitionFailureKind::InvalidJson, name, Some(path)),
                )?)
            }
            PlannedInput::File(item) => {
                let file = open_regular_input(&item.path)
                    .map_err(|kind| input_error(kind, name, Some(&item.path)))?;
                let bytes = read_bounded(
                    file,
                    remaining_input_bytes(total_bytes, MAXIMUM_ATTACHMENT_BYTES),
                    cancellation,
                )
                .map_err(|kind| input_error(kind, name, Some(&item.path)))?;
                account_input_bytes(
                    &mut total_bytes,
                    u64::try_from(bytes.len()).map_err(|_| input_bytes_error())?,
                    MAXIMUM_ATTACHMENT_BYTES,
                )?;
                ResolvedInput::File(ResolvedFile::new(
                    Arc::clone(&item.media_type),
                    Arc::from(bytes),
                ))
            }
            PlannedInput::Attachments(items) => {
                let mut resolved = Vec::with_capacity(items.len());
                for item in items {
                    let file = open_regular_input(&item.path)
                        .map_err(|kind| input_error(kind, name, Some(&item.path)))?;
                    let bytes = read_bounded(
                        file,
                        remaining_input_bytes(total_bytes, MAXIMUM_ATTACHMENT_BYTES),
                        cancellation,
                    )
                    .map_err(|kind| input_error(kind, name, Some(&item.path)))?;
                    account_input_bytes(
                        &mut total_bytes,
                        u64::try_from(bytes.len()).map_err(|_| input_bytes_error())?,
                        MAXIMUM_ATTACHMENT_BYTES,
                    )?;
                    resolved.push(ResolvedAttachment::new(
                        Arc::clone(&item.media_type),
                        Arc::from(bytes),
                    ));
                }
                ResolvedInput::Attachments(Arc::from(resolved))
            }
        };
        values.insert(name.clone(), value);
    }
    Ok(ResolvedInputs::new(values))
}

async fn acquire_scalar_file(
    path: &Path,
    name: &str,
    total_bytes: &mut u64,
    cancellation: &CancellationSource,
) -> anyhow::Result<Vec<u8>> {
    let bytes = if path == Path::new("-") {
        read_stdin_bounded(
            remaining_input_bytes(*total_bytes, MAXIMUM_TEXT_BYTES),
            cancellation,
        )
        .await
        .map_err(|kind| input_error(kind, name, None))?
    } else {
        let file = open_regular_input(path).map_err(|kind| input_error(kind, name, Some(path)))?;
        read_bounded(
            file,
            remaining_input_bytes(*total_bytes, MAXIMUM_TEXT_BYTES),
            cancellation,
        )
        .map_err(|kind| input_error(kind, name, Some(path)))?
    };
    account_input_bytes(
        total_bytes,
        u64::try_from(bytes.len()).map_err(|_| input_bytes_error())?,
        MAXIMUM_TEXT_BYTES,
    )?;
    Ok(bytes)
}

fn remaining_input_bytes(total: u64, per_value_limit: u64) -> u64 {
    per_value_limit.min(MAXIMUM_TOTAL_INPUT_BYTES.saturating_sub(total))
}

fn account_input_bytes(total: &mut u64, size: u64, per_value_limit: u64) -> anyhow::Result<()> {
    if size > per_value_limit {
        return Err(input_bytes_error());
    }
    *total = total
        .checked_add(size)
        .filter(|total| *total <= MAXIMUM_TOTAL_INPUT_BYTES)
        .ok_or_else(input_bytes_error)?;
    Ok(())
}

fn input_error(
    kind: InputAcquisitionFailureKind,
    name: &str,
    path: Option<&Path>,
) -> anyhow::Error {
    let context = path.map_or_else(
        || format!("acquire local workflow input {name}"),
        |path| format!("acquire local workflow input {name} from {path:?}"),
    );
    anyhow::Error::new(kind).context(context)
}

fn input_bytes_error() -> anyhow::Error {
    anyhow!("acquire local workflow inputs: an input byte bound was exceeded")
}

fn open_regular_input(path: &Path) -> Result<File, InputAcquisitionFailureKind> {
    super::super::open_regular_file_nonblocking(path).map_err(|error| match error {
        super::super::OpenRegularFileError::NotRegular => {
            InputAcquisitionFailureKind::NotRegularFile
        }
        super::super::OpenRegularFileError::Open(_)
        | super::super::OpenRegularFileError::Metadata(_) => {
            InputAcquisitionFailureKind::Unavailable
        }
    })
}

fn read_bounded(
    mut reader: impl Read,
    maximum: u64,
    cancellation: &CancellationSource,
) -> Result<Vec<u8>, InputAcquisitionFailureKind> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if cancellation.is_cancelled() {
            return Err(InputAcquisitionFailureKind::Interrupted);
        }
        let remaining = maximum.saturating_sub(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let permitted = usize::try_from(remaining.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        match reader.read(&mut buffer[..permitted]) {
            Ok(0) => return Ok(bytes),
            Ok(read) => {
                bytes.extend_from_slice(&buffer[..read]);
                if u64::try_from(bytes.len()).map_or(true, |length| length > maximum) {
                    return Err(InputAcquisitionFailureKind::TooLarge);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return Err(InputAcquisitionFailureKind::Read),
        }
    }
}

async fn read_stdin_bounded(
    maximum: u64,
    cancellation: &CancellationSource,
) -> Result<Vec<u8>, InputAcquisitionFailureKind> {
    read_stdin_from(&io::stdin(), maximum, cancellation).await
}

async fn read_stdin_from(
    standard_input: &impl AsFd,
    maximum: u64,
    cancellation: &CancellationSource,
) -> Result<Vec<u8>, InputAcquisitionFailureKind> {
    let input = rustix::io::dup(standard_input.as_fd())
        .map_err(|_| InputAcquisitionFailureKind::Unavailable)?;
    let original_flags =
        fcntl_getfl(standard_input).map_err(|_| InputAcquisitionFailureKind::Unavailable)?;
    fcntl_setfl(standard_input, original_flags | OFlags::NONBLOCK)
        .map_err(|_| InputAcquisitionFailureKind::Unavailable)?;
    let flags = StdinFlags {
        input: &standard_input,
        original_flags,
    };
    let input = File::from(input);
    let async_input = match AsyncFd::new(input) {
        Ok(input) => input,
        Err(_) => {
            flags
                .restore()
                .map_err(|_| InputAcquisitionFailureKind::Read)?;
            let input = rustix::io::dup(standard_input.as_fd())
                .map_err(|_| InputAcquisitionFailureKind::Unavailable)?;
            return read_bounded(File::from(input), maximum, cancellation);
        }
    };
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    let result = loop {
        if cancellation.is_cancelled() {
            break Err(InputAcquisitionFailureKind::Interrupted);
        }
        let remaining = maximum.saturating_sub(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let permitted = usize::try_from(remaining.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let mut ready = tokio::select! {
            biased;
            _ = cancellation.wait_for_cancellation() => {
                break Err(InputAcquisitionFailureKind::Interrupted);
            }
            ready = async_input.readable() => match ready {
                Ok(ready) => ready,
                Err(_) => break Err(InputAcquisitionFailureKind::Read),
            }
        };
        match ready.try_io(|inner| inner.get_ref().read(&mut buffer[..permitted])) {
            Ok(Ok(0)) => break Ok(bytes),
            Ok(Ok(read)) => {
                bytes.extend_from_slice(&buffer[..read]);
                if u64::try_from(bytes.len()).map_or(true, |length| length > maximum) {
                    break Err(InputAcquisitionFailureKind::TooLarge);
                }
            }
            Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
            Ok(Err(_)) => break Err(InputAcquisitionFailureKind::Read),
            Err(_) => {}
        }
    };
    drop(async_input);
    flags
        .restore()
        .map_err(|_| InputAcquisitionFailureKind::Read)?;
    result
}

struct StdinFlags<'a, T: AsFd> {
    input: &'a T,
    original_flags: OFlags,
}

impl<T: AsFd> StdinFlags<'_, T> {
    fn restore(&self) -> rustix::io::Result<()> {
        fcntl_setfl(self.input, self.original_flags)
    }
}

impl<T: AsFd> Drop for StdinFlags<'_, T> {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

pub(super) struct UnixSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl UnixSignals {
    pub(super) fn new() -> anyhow::Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .context("install local workflow interrupt observation")?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("install local workflow termination observation")?,
        })
    }
}

pub(super) trait SignalEvents: Send + 'static {
    fn next(&mut self) -> impl Future<Output = Option<CancellationReason>> + Send;
}

impl SignalEvents for UnixSignals {
    async fn next(&mut self) -> Option<CancellationReason> {
        tokio::select! {
            biased;
            signal = self.interrupt.recv() => signal.map(|()| CancellationReason::UserRequest),
            signal = self.terminate.recv() => signal.map(|()| CancellationReason::TerminationRequest),
        }
    }
}

pub(super) fn start_signal_observation(
    cancellation: CancellationSource,
    mut signals: impl SignalEvents,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(reason) = signals.next().await {
            if handle_observed_signal(&cancellation, reason) {
                return;
            }
        }
    })
}

fn handle_observed_signal(cancellation: &CancellationSource, reason: CancellationReason) -> bool {
    if cancellation.request_cancellation(reason) {
        return false;
    }
    cancellation.finalization_cancellation_requested() && cancellation.request_force_abort()
}

#[derive(Clone, Copy, Debug)]
struct ExecutionInstant {
    monotonic: Instant,
    utc: OffsetDateTime,
}

impl Add<Duration> for ExecutionInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self {
            monotonic: self.monotonic + duration,
            utc: self.utc + duration,
        }
    }
}

impl DisplayDeadline for ExecutionInstant {
    fn deadline_utc(&self) -> OffsetDateTime {
        self.utc
    }
}

impl DurableDeadline for ExecutionInstant {
    fn deadline_utc(&self) -> OffsetDateTime {
        self.utc
    }
}

#[derive(Clone, Copy)]
struct SystemExecutionClock;

impl CoordinatorClock for SystemExecutionClock {
    type Instant = ExecutionInstant;

    fn now(&mut self) -> Self::Instant {
        let point = SystemObservationClock.sample();
        ExecutionInstant {
            monotonic: point.monotonic,
            utc: point.utc,
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "SystemExecutionClock is the workflow adapter boundary for deadline waits"
    )]
    fn wait_until(&self, deadline: Self::Instant) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.monotonic))
    }
}

type SystemPresentation = WorkflowRunPresentation<io::Stdout, io::Stderr, SystemObservationClock>;

#[derive(Clone)]
enum RunExecutionObserver {
    Standard(TimingObserver<SystemPresentation, SystemObservationClock>),
    Tui(WorkflowRunViewModel<SystemObservationClock>),
}

impl RunExecutionObserver {
    fn snapshot(&self) -> RunTimingSnapshot {
        match self {
            Self::Standard(observer) => observer.snapshot(),
            Self::Tui(view) => view.timing_observation().snapshot(),
        }
    }
}

impl ExecutionObserver<ExecutionInstant> for RunExecutionObserver {
    fn observe(
        &self,
        observation: ExecutionObservation<ExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        let observer = self.clone();
        async move {
            match observer {
                Self::Standard(observer) => observer.observe(observation).await,
                Self::Tui(view) => view.observe(observation).await,
            }
        }
    }
}

enum ActiveRunHost {
    Standard(SystemPresentation),
    Tui {
        view: WorkflowRunViewModel<SystemObservationClock>,
        terminal: Option<WorkflowTerminalHost>,
        failure: Option<PresentationFailure>,
        config: Box<PresentationConfig>,
        leaf: ExecutionLeaf,
    },
}

impl ActiveRunHost {
    async fn await_ready(&mut self) -> Result<(), PresentationFailure> {
        match self {
            Self::Standard(presentation) => {
                // Let an immediately failing header reject the attempt before execution.
                // A stalled output consumer must not hold up the workflow indefinitely.
                let mut clock = SystemExecutionClock;
                let deadline = clock.now() + Duration::from_millis(100);
                tokio::select! {
                    () = presentation.flush_pending() => {}
                    () = clock.wait_until(deadline) => {}
                }
                presentation.failure().map_or(Ok(()), Err)
            }
            Self::Tui {
                terminal: Some(terminal),
                ..
            } => terminal.await_ready().await,
            Self::Tui { terminal: None, .. } => Err(PresentationFailure::operation(
                PresentationFailureOperation::TerminalTask,
            )),
        }
    }

    fn activate_execution(&mut self) -> Result<(), PresentationFailure> {
        match self {
            Self::Standard(_) => Ok(()),
            Self::Tui { terminal, .. } => terminal.as_mut().map_or_else(
                || {
                    Err(PresentationFailure::operation(
                        PresentationFailureOperation::TerminalTask,
                    ))
                },
                WorkflowTerminalHost::activate_execution,
            ),
        }
    }

    async fn stop_terminal(&mut self) {
        if let Self::Tui {
            terminal, failure, ..
        } = self
            && let Some(active) = terminal.take()
            && let Err(terminal_failure) = active.stop().await
            && failure.is_none()
        {
            *failure = Some(terminal_failure);
        }
    }

    fn reconcile_and_mark_quiescent(&self, run: &WorkflowRunResult) -> anyhow::Result<()> {
        if let Self::Tui { view, .. } = self {
            view.reconcile_terminal_result(run)
                .map_err(|_| anyhow!("prepare authoritative local workflow terminal result"))?;
            view.mark_quiescent();
        }
        Ok(())
    }

    fn begin_publication(&self) {
        if let Self::Tui { view, .. } = self {
            view.begin_publication();
        }
    }

    fn complete_publication(
        &self,
        publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
    ) {
        if let Self::Tui { view, .. } = self {
            let result = match publication {
                Ok(terminal) => WorkflowRunPublicationResult::Succeeded {
                    result_directory: terminal.result_directory().to_owned(),
                },
                Err(error) => WorkflowRunPublicationResult::Failed(error.into()),
            };
            view.complete_publication(result);
        }
    }

    fn begin_cleanup(&self) {
        if let Self::Tui { view, .. } = self {
            view.begin_cleanup();
        }
    }

    fn complete_cleanup(&self, failed: bool) {
        if let Self::Tui { view, .. } = self {
            view.complete_cleanup(if failed {
                WorkflowRunCleanupResult::Failed
            } else {
                WorkflowRunCleanupResult::Succeeded
            });
        }
    }

    fn mark_adapter_lifecycle_completed(&self, _released_ownership: LocalAttemptOwnershipReleased) {
        if let Self::Tui { view, .. } = self {
            view.mark_adapter_lifecycle_completed();
        }
    }

    async fn finish(
        &mut self,
        workflow: &ResolvedWorkflow,
        run: &WorkflowRunResult,
        publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
        cleanup_failed: bool,
        state_commit_failed: bool,
    ) -> super::super::CommandResult {
        match self {
            Self::Standard(presentation) => {
                if cleanup_failed || state_commit_failed {
                    if render_without_terminal_json(presentation, run, publication)
                        .await
                        .cannot_report_failure()
                    {
                        // A blocked presentation stream may also be stderr. Do not
                        // render a second, unbounded diagnostic on that stream.
                        return Ok(ExitCode::GeneralFailure);
                    }
                    return Err(if state_commit_failed {
                        state_commit_failure()
                    } else {
                        cleanup_failure(publication)
                    }
                    .into());
                }
                let presented = match publication {
                    Ok(terminal) => {
                        present_standard(
                            presentation,
                            run,
                            PublicationPresentation::Published(terminal),
                            true,
                        )
                        .await
                    }
                    Err(error) => {
                        present_standard(
                            presentation,
                            run,
                            PublicationPresentation::Failed(error),
                            true,
                        )
                        .await
                    }
                };
                match presented {
                    StandardPresentation::DrainTimedOut => Ok(ExitCode::GeneralFailure),
                    StandardPresentation::Completed(WorkflowRunPresentationResult::Failed(
                        failure,
                    )) if failure.error_kind == Some(io::ErrorKind::WouldBlock) => {
                        Ok(ExitCode::GeneralFailure)
                    }
                    StandardPresentation::Completed(presented) => presentation_exit_code(presented),
                }
            }
            Self::Tui {
                terminal,
                failure,
                config,
                leaf,
                ..
            } => {
                if let Some(active) = terminal.take() {
                    match active.wait().await {
                        Ok(TerminalHostExit::Quit) => {}
                        Ok(TerminalHostExit::Stopped) => {
                            if failure.is_none() {
                                *failure = Some(PresentationFailure {
                                    operation: PresentationFailureOperation::TerminalTask,
                                    error_kind: None,
                                    result_directory: None,
                                });
                            }
                        }
                        Err(terminal_failure) => {
                            if failure.is_none() {
                                *failure = Some(terminal_failure);
                            }
                        }
                    }
                }
                if let Some(mut terminal_failure) = failure.clone() {
                    terminal_failure.result_directory = publication
                        .as_ref()
                        .ok()
                        .map(|terminal| terminal.result_directory().to_owned());
                    let mut error = anyhow::Error::new(terminal_failure);
                    if let Err(publication_error) = publication {
                        error = error.context(publication_error.to_string());
                    }
                    if state_commit_failed {
                        error = error.context("commit terminal local run state");
                    } else if cleanup_failed {
                        error = error.context(cleanup_failure_message(publication));
                    }
                    return Err(error.into());
                }

                let output =
                    WorkflowRunOutput::new(config.as_ref().clone(), io::stdout(), io::stderr());
                let output = match *leaf {
                    ExecutionLeaf::Run => output,
                    ExecutionLeaf::Retry => output.for_retry(&run.run_directory),
                    ExecutionLeaf::Continue => output.for_continue(&run.run_directory),
                };
                let presented = match publication {
                    Ok(terminal) => output.render_standard_summary(
                        workflow,
                        run,
                        PublicationPresentation::Published(terminal),
                    ),
                    Err(error) => output.render_standard_summary(
                        workflow,
                        run,
                        PublicationPresentation::Failed(error),
                    ),
                };
                if state_commit_failed {
                    Err(state_commit_failure().into())
                } else if cleanup_failed {
                    Err(cleanup_failure(publication).into())
                } else {
                    presentation_exit_code(presented)
                }
            }
        }
    }
}

enum StandardPresentation {
    Completed(WorkflowRunPresentationResult),
    DrainTimedOut,
}

impl StandardPresentation {
    fn cannot_report_failure(&self) -> bool {
        // An exhausted queue or timed-out drain may have stderr blocked too.
        matches!(self, Self::DrainTimedOut)
            || matches!(self, Self::Completed(WorkflowRunPresentationResult::Failed(failure))
                if failure.error_kind == Some(io::ErrorKind::WouldBlock))
    }
}

async fn present_standard(
    presentation: &SystemPresentation,
    run: &WorkflowRunResult,
    publication: PublicationPresentation<'_>,
    emit_terminal_json: bool,
) -> StandardPresentation {
    let output = async {
        if emit_terminal_json {
            presentation.finish(run, publication).await
        } else {
            presentation
                .finish_without_terminal_json(run, publication)
                .await
        }
    };
    if run.cancellation.is_none()
        && run.force_abort.is_none()
        && !run.finalization.as_ref().is_some_and(|finalization| {
            finalization.cancellation.is_some() || finalization.force_abort
        })
    {
        return StandardPresentation::Completed(output.await);
    }
    // The workflow clock adapter bounds the final output wait for cancellation
    // in either phase, including force-aborted finalizers;
    // the presentation thread may be stuck in an unread pipe indefinitely.
    let mut clock = SystemExecutionClock;
    let deadline = clock.now() + Duration::from_millis(100);
    tokio::select! {
        presented = output => StandardPresentation::Completed(presented),
        () = clock.wait_until(deadline) => StandardPresentation::DrainTimedOut,
    }
}

async fn render_without_terminal_json(
    presentation: &SystemPresentation,
    run: &WorkflowRunResult,
    publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
) -> StandardPresentation {
    let publication = match publication {
        Ok(terminal) => PublicationPresentation::Published(terminal),
        Err(error) => PublicationPresentation::Failed(error),
    };
    present_standard(presentation, run, publication, false).await
}

fn cleanup_failure(
    publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
) -> anyhow::Error {
    anyhow!(cleanup_failure_message(publication))
}

fn cleanup_failure_message(
    publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
) -> String {
    publication.as_ref().map_or_else(
        |_| "release private workflow staging".to_owned(),
        |terminal| {
            format!(
                "release private workflow staging; result published at {}",
                terminal.result_directory()
            )
        },
    )
}

fn state_commit_failure() -> anyhow::Error {
    anyhow!("commit terminal local run state")
}

trait PresentationFailureState: Clone + Send + Sync + 'static {
    fn presentation_failed(&self) -> bool;
}

impl PresentationFailureState for SystemPresentation {
    fn presentation_failed(&self) -> bool {
        self.failure().is_some()
    }
}

#[derive(Clone)]
struct TimingObserver<Presentation, Clock> {
    presentation: Presentation,
    cancellation: CancellationSource,
    timing: RunTimingObservation,
    clock: Clock,
}

impl<Presentation, Clock> TimingObserver<Presentation, Clock>
where
    Clock: ObservationClock,
{
    fn new(
        presentation: Presentation,
        cancellation: CancellationSource,
        timing: RunTimingObservation,
        clock: Clock,
    ) -> Self {
        Self {
            presentation,
            cancellation,
            timing,
            clock,
        }
    }

    fn snapshot(&self) -> RunTimingSnapshot {
        self.timing.snapshot()
    }

    fn record(&self, observation: &ExecutionObservation<ExecutionInstant>) {
        self.timing.observe(observation, &self.clock);
    }
}

impl<Presentation, Clock> ExecutionObserver<ExecutionInstant>
    for TimingObserver<Presentation, Clock>
where
    Presentation: ExecutionObserver<ExecutionInstant> + PresentationFailureState,
    Clock: ObservationClock,
{
    fn observe(
        &self,
        observation: ExecutionObservation<ExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        self.record(&observation);
        let presentation = self.presentation.clone();
        let cancellation = self.cancellation.clone();
        async move {
            presentation.observe(observation).await;
            if presentation.presentation_failed() {
                cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
            }
        }
    }
}

fn observed_run_timing(timing: &RunTimingSnapshot) -> Option<WorkflowRunTiming> {
    let started = timing.execution_started?;
    let finished = timing.terminal?;
    Some(WorkflowRunTiming {
        started_at: started.utc,
        finished_at: finished.utc,
        duration: finished
            .monotonic
            .saturating_duration_since(started.monotonic),
    })
}

async fn settle_before_execution_failure(run: &InitialLocalRun) {
    let _ = run.record_executor_fault_before_execution_async().await;
}

fn record_private_cleanup_failure(run: &InitialLocalRun, cleanup_failed: bool) {
    if cleanup_failed {
        let _ = run.record_private_cleanup_failure();
    }
}

fn publication_failure_phase(phase: LocalPublicationPhase) -> PublicationFailurePhaseV1 {
    match phase {
        LocalPublicationPhase::ExportCopy => PublicationFailurePhaseV1::ExportCopy,
        LocalPublicationPhase::Serialization => PublicationFailurePhaseV1::Serialization,
        LocalPublicationPhase::Close => PublicationFailurePhaseV1::Close,
        LocalPublicationPhase::Verification => PublicationFailurePhaseV1::Verification,
        LocalPublicationPhase::TargetValidation
        | LocalPublicationPhase::Staging
        | LocalPublicationPhase::Commit => PublicationFailurePhaseV1::Rename,
    }
}

struct LocalRunEvidence<'a> {
    diagnostics: &'a StepDiagnosticLog,
    durable_invocations: &'a [DurableInvocationV1],
    timing: RunTimingSnapshot,
}

fn build_run_result(
    workflow: &ResolvedWorkflow,
    admitted: &AdmittedWorkflow,
    execution: WorkflowExecutionResult<ExecutionInstant>,
    evidence: LocalRunEvidence<'_>,
    run_timing: WorkflowRunTiming,
    local_run: &InitialLocalRun,
) -> anyhow::Result<WorkflowRunResult> {
    let diagnostics = evidence.diagnostics;
    let durable_invocations = evidence.durable_invocations;
    let continuation = local_run
        .continuation_record()
        .map_err(|_| invalid_terminal_result_error())?;
    let timing = &evidence.timing;
    let cancellation = match timing.cancellation {
        None => None,
        Some((reason, deadline)) => {
            let retained_by_outcome = match &execution.outcome {
                RunOutcome::Succeeded => false,
                RunOutcome::Failed {
                    later_cancellation, ..
                } => *later_cancellation == Some(reason),
                RunOutcome::Cancelled {
                    reason: outcome_reason,
                } => *outcome_reason == reason,
            };
            if !retained_by_outcome {
                return Err(invalid_terminal_result_error());
            }
            Some(WorkflowRunCancellation {
                reason,
                force_stop_deadline: deadline,
            })
        }
    };
    let mut states = execution.steps;
    let mut recoveries = execution.recoveries;
    let mut steps = Vec::with_capacity(states.len());
    for id in &workflow.definition.presentation_order {
        let state = states
            .remove(id)
            .ok_or_else(invalid_terminal_result_error)?;
        let timing = match timing.steps.get(id) {
            None => None,
            Some(timing) => {
                let finished = timing.finished.ok_or_else(invalid_terminal_result_error)?;
                Some(WorkflowStepTiming {
                    started_at: timing.started.utc,
                    duration: finished.saturating_duration_since(timing.started.monotonic),
                })
            }
        };
        let (kind, failure_policy) = match workflow.definition.steps.get(id) {
            Some(ValidatedStep::Command(command)) => {
                (WorkflowRunStepKind::Command, command.common.failure_policy)
            }
            Some(ValidatedStep::Agent(agent)) => {
                (WorkflowRunStepKind::Agent, agent.common.failure_policy)
            }
            None => return Err(invalid_terminal_result_error()),
        };
        let recovery_state = recoveries
            .remove(id)
            .ok_or_else(invalid_terminal_result_error)?;
        let recovery = step_recovery_summary_v1(recovery_state.as_ref())
            .map_err(|_| invalid_terminal_result_error())?;
        let invocations = if recovery.is_some()
            || kind == WorkflowRunStepKind::Agent
                && durable_invocations
                    .iter()
                    .any(|invocation| invocation.step_id == *id)
        {
            project_recovery_invocations(id, durable_invocations, diagnostics)?
        } else {
            Vec::new()
        };
        steps.push(WorkflowRunStep {
            id: id.clone(),
            role: WorkflowNodeRole::Step,
            kind,
            failure_policy,
            state,
            timing,
            command_output: (kind == WorkflowRunStepKind::Command)
                .then(|| diagnostics.get(id))
                .flatten(),
            recovery,
            invocations,
        });
    }
    let finalization = match (
        workflow.definition.finalizers.is_empty(),
        execution.finalization_summary,
    ) {
        (true, None) => None,
        (false, Some(summary)) => {
            let mut retained = summary
                .finalizers
                .into_iter()
                .map(|finalizer| (finalizer.finalizer.clone(), finalizer))
                .collect::<std::collections::BTreeMap<_, _>>();
            let mut finalizers = Vec::with_capacity(retained.len());
            for id in &workflow.definition.finalizer_presentation_order {
                let state = states
                    .remove(id)
                    .ok_or_else(invalid_terminal_result_error)?;
                let summarized = retained
                    .remove(id)
                    .ok_or_else(invalid_terminal_result_error)?;
                if summarized.failure_policy
                    != finalizer_failure_policy(workflow, id)
                        .ok_or_else(invalid_terminal_result_error)?
                    || !summary_disposition_matches(&summarized.disposition, &state)
                {
                    return Err(invalid_terminal_result_error());
                }
                let timing = match timing.steps.get(id) {
                    None => None,
                    Some(timing) => {
                        let finished = timing.finished.ok_or_else(invalid_terminal_result_error)?;
                        Some(WorkflowStepTiming {
                            started_at: timing.started.utc,
                            duration: finished.saturating_duration_since(timing.started.monotonic),
                        })
                    }
                };
                let (kind, failure_policy) = finalizer_kind_and_policy(workflow, id)
                    .ok_or_else(invalid_terminal_result_error)?;
                let recovery = recoveries
                    .remove(id)
                    .ok_or_else(invalid_terminal_result_error)?;
                if recovery.is_some() {
                    return Err(invalid_terminal_result_error());
                }
                finalizers.push(WorkflowRunStep {
                    id: id.clone(),
                    role: WorkflowNodeRole::Finalizer,
                    kind,
                    failure_policy,
                    state,
                    timing,
                    command_output: (kind == WorkflowRunStepKind::Command)
                        .then(|| diagnostics.get(id))
                        .flatten(),
                    recovery: None,
                    invocations: if kind == WorkflowRunStepKind::Agent
                        && durable_invocations
                            .iter()
                            .any(|invocation| invocation.step_id == *id)
                    {
                        project_recovery_invocations(id, durable_invocations, diagnostics)?
                    } else {
                        Vec::new()
                    },
                });
            }
            if !retained.is_empty() {
                return Err(invalid_terminal_result_error());
            }
            Some(WorkflowRunFinalization {
                trigger: summary.trigger,
                finalizers,
                cancellation: summary.cancellation.map(|cancellation| {
                    WorkflowRunFinalizationCancellation {
                        reason: cancellation.reason,
                        force_stop_deadline: cancellation.deadline.map(|deadline| deadline.utc),
                    }
                }),
                force_abort: summary.force_abort,
            })
        }
        (true, Some(_)) | (false, None) => return Err(invalid_terminal_result_error()),
    };
    if !states.is_empty() || !recoveries.is_empty() {
        return Err(invalid_terminal_result_error());
    }
    Ok(WorkflowRunResult {
        run_directory: local_run.run_directory().to_owned(),
        attempt_number: local_run.attempt_number(),
        continuation,
        output_producers: execution.output_producers.into_iter().fold(
            BTreeMap::new(),
            |mut producers, ((node, output), producer)| {
                producers.entry(node).or_default().insert(output, producer);
                producers
            },
        ),
        workflow_path: execution.provenance.workflow_path,
        source_root: execution.provenance.source_root,
        content_digest: execution.content_digest,
        execution_root: admitted.execution().root().to_owned(),
        maximum_parallel_steps: admitted.execution().limits().maximum_parallel_steps(),
        maximum_retained_bytes_per_stream: admitted
            .execution()
            .limits()
            .maximum_step_log_bytes()
            .get(),
        cloud_capacity: None,
        timing: run_timing,
        outcome: execution.outcome,
        cancellation,
        force_abort: execution.force_abort,
        steps,
        finalization,
        exports: execution.exports,
        export_sources: workflow.definition.exports.clone(),
    })
}

fn project_recovery_invocations(
    step_id: &str,
    durable: &[DurableInvocationV1],
    diagnostics: &StepDiagnosticLog,
) -> anyhow::Result<Vec<RecoveryInvocationV1>> {
    let mut projected = Vec::new();
    for invocation in durable
        .iter()
        .filter(|invocation| invocation.step_id == step_id)
    {
        let finished_at = invocation
            .finished_at
            .as_deref()
            .ok_or_else(invalid_terminal_result_error)?;
        let started = OffsetDateTime::parse(&invocation.started_at, &Rfc3339)
            .map_err(|_| invalid_terminal_result_error())?;
        let finished = OffsetDateTime::parse(finished_at, &Rfc3339)
            .map_err(|_| invalid_terminal_result_error())?;
        let duration = (finished - started).whole_milliseconds();
        let duration_milliseconds =
            u64::try_from(duration).map_err(|_| invalid_terminal_result_error())?;
        let action = ActionId {
            transition_sequence: TransitionSequence(invocation.invocation_id),
        };
        let retained = diagnostics.get_invocation(step_id, action);
        let command_output = retained
            .as_ref()
            .map(command_output_v1)
            .transpose()
            .map_err(|_| invalid_terminal_result_error())?;
        if command_output.is_some() != !invocation.diagnostics.is_empty() {
            return Err(invalid_terminal_result_error());
        }
        let mut invocation_diagnostics = Vec::with_capacity(invocation.diagnostics.len());
        for diagnostic in &invocation.diagnostics {
            let output = command_output
                .as_ref()
                .ok_or_else(invalid_terminal_result_error)?;
            let stream = match diagnostic.kind {
                RecoveryDiagnosticKindV1::CommandStdout
                | RecoveryDiagnosticKindV1::AgentHarnessStdout => output.stdout.clone(),
                RecoveryDiagnosticKindV1::CommandStderr
                | RecoveryDiagnosticKindV1::AgentHarnessStderr => output.stderr.clone(),
            };
            if stream.retained_bytes != diagnostic.retained_bytes
                || stream.discarded_bytes != diagnostic.discarded_bytes
                || stream.truncated != diagnostic.truncated
                || stream.fully_drained != diagnostic.fully_drained
            {
                return Err(invalid_terminal_result_error());
            }
            invocation_diagnostics.push(RecoveryInvocationDiagnosticV1 {
                kind: diagnostic.kind,
                reference: diagnostic.reference.clone(),
                stream,
            });
        }
        projected.push(RecoveryInvocationV1 {
            invocation_id: invocation.invocation_id,
            role: invocation.role,
            target_execution: invocation.target_execution,
            recovery_round: invocation.recovery_round,
            state: match invocation.state {
                DurableInvocationStateV1::Settled => RecoveryInvocationStateV1::Settled,
                DurableInvocationStateV1::Cancelled => RecoveryInvocationStateV1::Cancelled,
                DurableInvocationStateV1::Active => {
                    return Err(invalid_terminal_result_error());
                }
            },
            started_at: invocation.started_at.clone(),
            finished_at: finished_at.to_owned(),
            duration_milliseconds,
            usage: invocation.usage,
            diagnostics: invocation_diagnostics,
            diagnostic_reference: invocation.diagnostic_reference.clone(),
        });
    }
    if projected.is_empty() {
        return Err(invalid_terminal_result_error());
    }
    projected.sort_by_key(|invocation| invocation.invocation_id);
    Ok(projected)
}

fn finalizer_kind_and_policy(
    workflow: &ResolvedWorkflow,
    id: &str,
) -> Option<(WorkflowRunStepKind, FailurePolicy)> {
    let finalizer = workflow.definition.finalizers.get(id)?;
    let (kind, policy) = match &finalizer.body {
        ValidatedStep::Command(command) => {
            (WorkflowRunStepKind::Command, command.common.failure_policy)
        }
        ValidatedStep::Agent(agent) => (WorkflowRunStepKind::Agent, agent.common.failure_policy),
    };
    Some((kind, policy))
}

fn finalizer_failure_policy(workflow: &ResolvedWorkflow, id: &str) -> Option<FailurePolicy> {
    finalizer_kind_and_policy(workflow, id).map(|(_, policy)| policy)
}

fn invalid_terminal_result_error() -> anyhow::Error {
    anyhow!("prepare authoritative local workflow terminal result")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InputAcquisitionFailureKind {
    Unavailable,
    NotRegularFile,
    Interrupted,
    Read,
    TooLarge,
    InvalidUtf8,
    InvalidJson,
}

impl std::fmt::Display for InputAcquisitionFailureKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "input is unavailable",
            Self::NotRegularFile => "input is not a regular file",
            Self::Interrupted => "input acquisition was interrupted",
            Self::Read => "input read unavailable",
            Self::TooLarge => "input exceeds the byte limit",
            Self::InvalidUtf8 => "input is not valid UTF-8",
            Self::InvalidJson => "input is not valid JSON",
        })
    }
}

impl Error for InputAcquisitionFailureKind {}

pub(super) fn diagnose(error: impl Error + Send + Sync + 'static) -> super::super::CommandResult {
    Err(anyhow::Error::new(error).into())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::ready;
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::process::{Command as ProcessCommand, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};

    use nix::sys::stat::Mode;
    use nix::unistd::mkfifo;
    use rustix::fs::{FlockOperation, fcntl_lock};
    use time::format_description::well_known::Rfc3339;

    use super::*;
    use um_execution::{
        ObservationTime, SchedulingGate, StepStateKind, TransitionEvent, TransitionObservation,
        TransitionSequence, WorkflowState, resolve,
    };

    #[derive(Clone)]
    struct ScriptedClock {
        points: Arc<Mutex<VecDeque<ObservationTime>>>,
    }

    impl ScriptedClock {
        fn new(points: impl IntoIterator<Item = ObservationTime>) -> Self {
            Self {
                points: Arc::new(Mutex::new(points.into_iter().collect())),
            }
        }
    }

    impl ObservationClock for ScriptedClock {
        fn sample(&self) -> ObservationTime {
            self.points.lock().unwrap().pop_front().unwrap()
        }
    }

    #[derive(Clone)]
    struct ControlledObservationClock {
        current: Arc<Mutex<ObservationTime>>,
    }

    impl ControlledObservationClock {
        fn new(current: ObservationTime) -> Self {
            Self {
                current: Arc::new(Mutex::new(current)),
            }
        }

        fn set(&self, current: ObservationTime) {
            *self.current.lock().unwrap() = current;
        }
    }

    impl ObservationClock for ControlledObservationClock {
        fn sample(&self) -> ObservationTime {
            *self.current.lock().unwrap()
        }
    }

    struct DelayedHeaderWriter {
        clock: ControlledObservationClock,
        completed_at: ObservationTime,
        flushed: Arc<AtomicBool>,
        entered: std::sync::mpsc::Sender<()>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl Write for DelayedHeaderWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            let _ = self.entered.send(());
            let (lock, wake) = &*self.gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
            self.clock.set(self.completed_at);
            self.flushed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RecordingPresentation {
        observations: Arc<AtomicUsize>,
        failed: bool,
    }

    impl PresentationFailureState for RecordingPresentation {
        fn presentation_failed(&self) -> bool {
            self.failed
        }
    }

    impl ExecutionObserver<ExecutionInstant> for RecordingPresentation {
        fn observe(
            &self,
            _observation: ExecutionObservation<ExecutionInstant>,
        ) -> impl Future<Output = ()> + Send {
            self.observations.fetch_add(1, Ordering::SeqCst);
            ready(())
        }
    }

    fn os_arguments(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn typed_diagnostic_keeps_its_source_chain() {
        #[derive(Debug)]
        struct StageFailure(io::Error);
        impl std::fmt::Display for StageFailure {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("stage failed")
            }
        }
        impl Error for StageFailure {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                Some(&self.0)
            }
        }
        let result = diagnose(StageFailure(io::Error::other("underlying cause")));
        let failure = result.err().unwrap();
        assert_eq!(failure.error().chain().count(), 2);
    }

    struct RestoringBoundary(Arc<AtomicBool>);

    impl um_execution::TerminalBoundary for RestoringBoundary {
        fn setup(&mut self) -> io::Result<um_execution::TerminalRect> {
            Ok(um_execution::TerminalRect::new(0, 0, 120, 24))
        }
        async fn next_event(&mut self) -> io::Result<um_execution::TerminalInputEvent> {
            std::future::pending().await
        }
        fn resize(&mut self) -> io::Result<um_execution::TerminalRect> {
            Ok(um_execution::TerminalRect::new(0, 0, 120, 24))
        }
        fn restore(&mut self) -> io::Result<()> {
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl um_execution::WorkflowTerminalBoundary for RestoringBoundary {
        fn draw_workflow(
            &mut self,
            _snapshot: &um_execution::WorkflowRunViewSnapshot,
            _interaction: &mut um_execution::HostInteraction,
            _color: bool,
        ) -> io::Result<()> {
            Ok(())
        }
    }

    fn private_staging_paths(root: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("workflow-")
            })
            .collect()
    }

    #[tokio::test]
    async fn attempt_failure_releases_staging_and_restores_active_terminal() {
        for (phase, target, start_host) in [
            (
                "after artifact staging",
                Some(AttemptCheckpoint::ArtifactsStaged),
                false,
            ),
            (
                "after input staging",
                Some(AttemptCheckpoint::InputsStaged),
                false,
            ),
            (
                "before host start",
                Some(AttemptCheckpoint::BeforeHostStart),
                false,
            ),
            (
                "after host start",
                Some(AttemptCheckpoint::AfterHostStart),
                true,
            ),
            ("execution failure", None, true),
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let source = temporary.path().join("source");
            let execution_root = temporary.path().join("execution");
            std::fs::create_dir(&source).unwrap();
            std::fs::create_dir(&execution_root).unwrap();
            std::fs::write(
                source.join("workflow.yaml"),
                "schemaVersion: 1\nsteps:\n  task:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
            ).unwrap();
            let workflow = resolve(&source, Path::new("workflow.yaml")).unwrap();
            let admitted = admit_workflow(
                workflow.clone(),
                ResolvedInputs::default(),
                execution_context_for_workflow(
                    &workflow,
                    execution_root,
                    1,
                    CancellationSource::new(),
                )
                .unwrap(),
            )
            .unwrap();
            let run = InitialLocalRun::create(&temporary.path().join("run"), &admitted).unwrap();
            let private_root = run.private_directory().to_owned();
            let reached = Arc::new(AtomicBool::new(false));
            let reached_checkpoint = reached.clone();
            let reached_execution = reached.clone();
            let private_at_checkpoint = private_root.clone();
            let private_root_for_execution = private_root.clone();
            let restored = Arc::new(AtomicBool::new(false));
            let restored_for_terminal = restored.clone();
            let result = execute_owned_attempt_with(
                workflow,
                admitted,
                run,
                CancellationSource::new(),
                tokio::spawn(std::future::pending()),
                AttemptSettings {
                    presentation_config: PresentationConfig {
                        requested_mode: RequestedPresentationMode::Automatic,
                        color: ColorChoice::Never,
                        capabilities: TerminalCapabilities {
                            stdin_is_terminal: true,
                            stdout_is_terminal: true,
                            stderr_is_terminal: true,
                            stdout_width: Some(120),
                            stderr_width: Some(120),
                            term: Some("xterm".into()),
                            no_color: None,
                        },
                        standard_input_reserved: false,
                    },
                    leaf: ExecutionLeaf::Run,
                    hooks: (
                        move |point| {
                            if Some(point) == target {
                                assert_eq!(private_staging_paths(&private_at_checkpoint).len(), 1);
                                reached_checkpoint.store(true, Ordering::SeqCst);
                                anyhow::bail!("injected phase failure")
                            }
                            Ok(())
                        },
                        move |view, cancellation, color| {
                            WorkflowTerminalHost::start_with_boundary(
                                view,
                                cancellation,
                                color,
                                RestoringBoundary(restored_for_terminal.clone()),
                            )
                        },
                        || {
                            // Return an execution error after ownership has entered the
                            // execution phase, without depending on a real child process.
                            if target.is_none() {
                                assert_eq!(
                                    private_staging_paths(&private_root_for_execution).len(),
                                    1
                                );
                                reached_execution.store(true, Ordering::SeqCst);
                                Some(CoordinationError::ReducerStateUnavailable)
                            } else {
                                None
                            }
                        },
                    ),
                },
            )
            .await;
            assert!(reached.load(Ordering::SeqCst), "{phase} was not reached");
            assert!(result.is_err(), "{phase} must fail");
            assert!(private_staging_paths(&private_root).is_empty(), "{phase}");
            assert_eq!(restored.load(Ordering::SeqCst), start_host, "{phase}");
        }
    }

    #[tokio::test]
    async fn cancelling_stdin_read_restores_shared_file_description_flags() {
        use std::future::Future as _;
        use std::os::unix::net::UnixStream;
        use std::task::{Context, Poll, Waker};

        let (input, _writer) = UnixStream::pair().unwrap();
        let original = fcntl_getfl(&input).unwrap();
        let cancellation = CancellationSource::new();
        let mut read = Box::pin(read_stdin_from(&input, 64, &cancellation));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(read.as_mut().poll(&mut context), Poll::Pending));
        assert!(fcntl_getfl(&input).unwrap().contains(OFlags::NONBLOCK));
        drop(read);
        assert_eq!(fcntl_getfl(&input).unwrap(), original);
    }

    #[test]
    fn named_input_planning_rejects_conflicts_before_source_io() {
        let missing = "/path/that/must/not/be/read";
        for (text, text_files, json, json_files, attachments, empty) in [
            (
                os_arguments(&["request", "inline"]),
                os_arguments(&["request", missing]),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            (
                Vec::new(),
                os_arguments(&["first", "-"]),
                Vec::new(),
                os_arguments(&["second", "-"]),
                Vec::new(),
                Vec::new(),
            ),
            (
                Vec::new(),
                Vec::new(),
                os_arguments(&["request", "null"]),
                os_arguments(&["request", missing]),
                Vec::new(),
                Vec::new(),
            ),
            (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                os_arguments(&["evidence", "text/plain", missing]),
                vec!["evidence".to_owned()],
            ),
        ] {
            assert!(
                plan_inputs(
                    &text,
                    &text_files,
                    &json,
                    &json_files,
                    &[],
                    &attachments,
                    &empty,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn named_input_planning_enforces_inclusive_name_and_member_bounds() {
        let mut text = Vec::new();
        for index in 0..MAXIMUM_INPUTS {
            text.push(OsString::from(format!("input{index}")));
            text.push(OsString::from("value"));
        }
        assert!(plan_inputs(&text, &[], &[], &[], &[], &[], &[]).is_ok());
        text.push(OsString::from("oneOver"));
        text.push(OsString::from("value"));
        assert!(plan_inputs(&text, &[], &[], &[], &[], &[], &[]).is_err());

        let mut attachments = Vec::new();
        for index in 0..MAXIMUM_ATTACHMENTS {
            attachments.extend([
                OsString::from("evidence"),
                OsString::from("application/octet-stream"),
                OsString::from(format!("member-{index}")),
            ]);
        }
        assert!(plan_inputs(&[], &[], &[], &[], &[], &attachments, &[]).is_ok());
        attachments.extend([
            OsString::from("evidence"),
            OsString::from("application/octet-stream"),
            OsString::from("member-over"),
        ]);
        assert!(plan_inputs(&[], &[], &[], &[], &[], &attachments, &[]).is_err());
    }

    #[tokio::test]
    async fn named_input_acquisition_preserves_empty_values_and_member_order() {
        let temporary = tempfile::tempdir().unwrap();
        let first = temporary.path().join("first");
        let second = temporary.path().join("second");
        std::fs::write(&first, b"distinct-first").unwrap();
        std::fs::write(&second, b"second-value").unwrap();
        let file_value = temporary.path().join("payload");
        std::fs::write(&file_value, b"file-value").unwrap();
        let files = vec![
            OsString::from("payload"),
            OsString::from("application/octet-stream"),
            file_value.into_os_string(),
        ];
        let attachments = vec![
            OsString::from("evidence"),
            OsString::from("text/plain"),
            first.into_os_string(),
            OsString::from("evidence"),
            OsString::from("application/octet-stream"),
            second.into_os_string(),
        ];
        let plan = plan_inputs(
            &os_arguments(&["request", ""]),
            &[],
            &os_arguments(&["settings", "{ \"z\": null, \"n\": 1.2300 }"]),
            &[],
            &files,
            &attachments,
            &["emptyEvidence".to_owned()],
        )
        .unwrap();
        let inputs = acquire_inputs(&plan, &CancellationSource::new())
            .await
            .unwrap();
        assert!(matches!(
            inputs.get("request"),
            Some(ResolvedInput::Text(value)) if value.is_empty()
        ));
        let Some(ResolvedInput::Json(settings)) = inputs.get("settings") else {
            panic!("named JSON value is missing");
        };
        assert_eq!(settings.source(), b"{ \"z\": null, \"n\": 1.2300 }");
        assert_eq!(settings.canonical(), b"{\"n\":1.2300,\"z\":null}");
        assert!(settings.value()["z"].is_null());
        let Some(ResolvedInput::File(value)) = inputs.get("payload") else {
            panic!("named File value is missing");
        };
        assert_eq!(value.media_type(), "application/octet-stream");
        assert_eq!(value.bytes(), b"file-value");
        let Some(ResolvedInput::Attachments(values)) = inputs.get("evidence") else {
            panic!("named attachment collection is missing");
        };
        assert_eq!(values[0].bytes(), b"distinct-first");
        assert_eq!(values[1].bytes(), b"second-value");
        assert!(matches!(
            inputs.get("emptyEvidence"),
            Some(ResolvedInput::Attachments(values)) if values.is_empty()
        ));
    }

    #[tokio::test]
    async fn named_json_acquisition_rejects_duplicate_keys_without_a_fallback() {
        for source in ["{\"safe\":1,\"safe\":2}", "", "null true"] {
            let plan = plan_inputs(
                &[],
                &[],
                &os_arguments(&["request", source]),
                &[],
                &[],
                &[],
                &[],
            )
            .unwrap();
            assert!(
                acquire_inputs(&plan, &CancellationSource::new())
                    .await
                    .is_err()
            );
        }
    }

    #[test]
    fn named_input_paths_follow_regular_symlinks_and_reject_nonregular_files() {
        let temporary = tempfile::tempdir().unwrap();
        let regular = temporary.path().join("regular");
        let link = temporary.path().join("link");
        let fifo = temporary.path().join("fifo");
        std::fs::write(&regular, b"regular-bytes").unwrap();
        symlink(&regular, &link).unwrap();
        mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();

        let file = open_regular_input(&link).unwrap();
        assert_eq!(
            read_bounded(file, 32, &CancellationSource::new()).unwrap(),
            b"regular-bytes"
        );
        assert_eq!(
            open_regular_input(temporary.path()).unwrap_err(),
            InputAcquisitionFailureKind::NotRegularFile
        );
        assert_eq!(
            open_regular_input(&fifo).unwrap_err(),
            InputAcquisitionFailureKind::NotRegularFile
        );
    }

    #[tokio::test]
    async fn json_file_acquisition_accepts_exact_source_bound_and_rejects_one_over() {
        let temporary = tempfile::tempdir().unwrap();
        let exact_path = temporary.path().join("exact.json");
        let one_over_path = temporary.path().join("one-over.json");
        let mut exact = Vec::with_capacity(usize::try_from(MAXIMUM_TEXT_BYTES).unwrap());
        exact.push(b'"');
        exact.extend(std::iter::repeat_n(
            b'x',
            usize::try_from(MAXIMUM_TEXT_BYTES).unwrap() - 2,
        ));
        exact.push(b'"');
        let mut one_over = exact.clone();
        one_over.insert(one_over.len() - 1, b'y');
        std::fs::write(&exact_path, &exact).unwrap();
        std::fs::write(&one_over_path, &one_over).unwrap();

        let exact_plan = plan_inputs(
            &[],
            &[],
            &[],
            &[OsString::from("request"), exact_path.into_os_string()],
            &[],
            &[],
            &[],
        )
        .unwrap();
        let exact_inputs = acquire_inputs(&exact_plan, &CancellationSource::new())
            .await
            .unwrap();
        assert!(matches!(
            exact_inputs.get("request"),
            Some(ResolvedInput::Json(value)) if value.source().len() == exact.len()
        ));

        let one_over_plan = plan_inputs(
            &[],
            &[],
            &[],
            &[OsString::from("request"), one_over_path.into_os_string()],
            &[],
            &[],
            &[],
        )
        .unwrap();
        assert!(
            acquire_inputs(&one_over_plan, &CancellationSource::new())
                .await
                .is_err()
        );
    }

    #[test]
    fn input_reader_accepts_exact_limit_and_rejects_one_over() {
        let cancellation = CancellationSource::new();
        let exact = vec![b'x'; 17];
        assert_eq!(
            read_bounded(io::Cursor::new(exact.clone()), 17, &cancellation).unwrap(),
            exact
        );
        assert_eq!(
            read_bounded(io::Cursor::new(vec![b'y'; 18]), 17, &cancellation),
            Err(InputAcquisitionFailureKind::TooLarge)
        );

        let mut aggregate = MAXIMUM_TOTAL_INPUT_BYTES - 1;
        account_input_bytes(&mut aggregate, 1, MAXIMUM_ATTACHMENT_BYTES).unwrap();
        assert_eq!(aggregate, MAXIMUM_TOTAL_INPUT_BYTES);
        assert!(account_input_bytes(&mut aggregate, 1, MAXIMUM_ATTACHMENT_BYTES).is_err());
    }

    #[test]
    fn local_context_discovers_a_recovery_only_agent_harness() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        std::fs::write(source_root.join("recovery.md"), "Repair the target.\n").unwrap();
        std::fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nagentProfiles:\n  repair:\n    harness:\n      kind: pi\n      config: {model: openai/gpt-5, thinking: high}\nsteps:\n  check:\n    kind: cmd\n    recovery:\n      retries: 1\n      handler:\n        kind: agent\n        profile: repair\n        prompt: recovery.md\n    command: {argv: [\"true\"]}\n",
        )
        .unwrap();
        let workflow = resolve(&source_root, Path::new("workflow.yaml")).unwrap();

        let harnesses = required_agent_harnesses(&workflow).collect::<Vec<_>>();
        assert_eq!(harnesses.len(), 1);
        assert!(matches!(harnesses[0], ValidatedHarness::Pi(_)));
    }

    #[test]
    fn presentation_flags_forward_injected_terminal_capabilities() {
        let capabilities = TerminalCapabilities {
            stdin_is_terminal: true,
            stdout_is_terminal: true,
            stderr_is_terminal: false,
            stdout_width: Some(100),
            stderr_width: None,
            term: Some("xterm".into()),
            no_color: Some("1".into()),
        };
        let command = Command {
            source: super::super::LocalWorkflowSource {
                source_root: PathBuf::from("source"),
                workflow_file: PathBuf::from("workflow.yaml"),
            },
            execution: super::super::LocalExecutionRoot {
                execution_root: PathBuf::from("execution"),
            },
            run_dir: PathBuf::from("run"),
            inputs: super::super::super::NamedInputArgs {
                input_text: Vec::new(),
                input_text_file: Vec::new(),
                input_json: Vec::new(),
                input_json_file: Vec::new(),
                input_file: Vec::new(),
                input_attachment: Vec::new(),
                input_attachments_empty: Vec::new(),
            },
            max_parallel: 2,
            presentation: super::super::PresentationOptions {
                plain: false,
                output: super::super::super::JsonArgs {
                    json: true,
                    output: std::marker::PhantomData,
                },
                color: super::super::ColorArgument::Always,
            },
        };

        assert_eq!(
            command.presentation_config_with(capabilities.clone()),
            PresentationConfig {
                requested_mode: RequestedPresentationMode::Json,
                color: ColorChoice::Always,
                capabilities,
                standard_input_reserved: false,
            }
        );
    }

    #[test]
    fn repeated_signals_only_force_abort_a_cancelling_finalization() {
        let ordinary = CancellationSource::new();
        assert!(!handle_observed_signal(
            &ordinary,
            CancellationReason::UserRequest
        ));
        assert!(!handle_observed_signal(
            &ordinary,
            CancellationReason::TerminationRequest
        ));
        assert!(ordinary.request_force_abort());

        let finalization = CancellationSource::new();
        assert!(finalization.fixture_begin_finalization_arm());
        assert!(finalization.fixture_complete_finalization_arm());
        assert!(!handle_observed_signal(
            &finalization,
            CancellationReason::UserRequest
        ));
        assert!(handle_observed_signal(
            &finalization,
            CancellationReason::TerminationRequest
        ));
        assert!(!finalization.request_force_abort());
    }

    #[tokio::test]
    async fn injected_signals_map_once_to_the_closed_cancellation_reason() {
        let cancellation = CancellationSource::new();
        struct InjectedSignals(tokio::sync::mpsc::UnboundedReceiver<CancellationReason>);
        impl SignalEvents for InjectedSignals {
            async fn next(&mut self) -> Option<CancellationReason> {
                self.0.recv().await
            }
        }
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer = start_signal_observation(cancellation.clone(), InjectedSignals(receiver));
        sender.send(CancellationReason::TerminationRequest).unwrap();
        assert_eq!(
            cancellation.wait_for_cancellation().await,
            CancellationReason::TerminationRequest
        );
        assert!(!cancellation.request_cancellation(CancellationReason::UserRequest));
        drop(sender);
        observer.await.unwrap();

        let finalization = CancellationSource::new();
        assert!(finalization.fixture_begin_finalization_arm());
        assert!(finalization.fixture_complete_finalization_arm());
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer = start_signal_observation(finalization.clone(), InjectedSignals(receiver));
        sender.send(CancellationReason::UserRequest).unwrap();
        assert_eq!(
            finalization.wait_for_cancellation().await,
            CancellationReason::UserRequest
        );
        sender.send(CancellationReason::TerminationRequest).unwrap();
        observer.await.unwrap();
        assert!(!finalization.request_force_abort());
    }

    #[test]
    fn adapter_completion_follows_attempt_ownership_release() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let run_parent = temporary.path().join("runs");
        for directory in [&source_root, &execution_root, &run_parent] {
            std::fs::create_dir(directory).unwrap();
        }
        std::fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  task:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let workflow = resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let admitted = admit_workflow(
            workflow.clone(),
            ResolvedInputs::default(),
            execution_context_for_workflow(&workflow, execution_root, 1, CancellationSource::new())
                .unwrap(),
        )
        .unwrap();
        let run_directory = run_parent.join("owned");
        let owned_run = InitialLocalRun::create(&run_directory, &admitted).unwrap();
        let lock_path = run_directory.join("run.lock");
        assert_run_lock_available(&lock_path, false);

        let clock = SystemObservationClock;
        let view = WorkflowRunViewModel::new(
            &workflow,
            1,
            RunTimingObservation::new(clock.sample()),
            clock,
        );
        let host = ActiveRunHost::Tui {
            view: view.clone(),
            terminal: None,
            failure: None,
            config: Box::new(PresentationConfig {
                requested_mode: RequestedPresentationMode::Automatic,
                color: ColorChoice::Never,
                capabilities: TerminalCapabilities {
                    stdin_is_terminal: true,
                    stdout_is_terminal: true,
                    stderr_is_terminal: true,
                    stdout_width: Some(80),
                    stderr_width: Some(80),
                    term: Some("xterm".into()),
                    no_color: None,
                },
                standard_input_reserved: false,
            }),
            leaf: ExecutionLeaf::Run,
        };
        assert!(!view.snapshot().quit_eligible);

        let released_ownership = owned_run.release();
        assert_run_lock_available(&lock_path, true);
        assert!(!view.snapshot().quit_eligible);

        host.mark_adapter_lifecycle_completed(released_ownership);
        assert!(view.snapshot().quit_eligible);
    }

    fn assert_run_lock_available(path: &Path, expected: bool) {
        let output = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "cli::workflow::run::tests::run_lock_probe_fixture",
            ])
            .env("SCHERZO_TEST_RUN_LOCK_PATH", path)
            .env(
                "SCHERZO_TEST_RUN_LOCK_AVAILABLE",
                if expected { "true" } else { "false" },
            )
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "run.lock probe failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "launched as a run.lock ownership probe"]
    fn run_lock_probe_fixture() {
        let path = std::env::var_os("SCHERZO_TEST_RUN_LOCK_PATH")
            .unwrap_or_else(|| panic!("SCHERZO_TEST_RUN_LOCK_PATH must be set"));
        let expected = std::env::var("SCHERZO_TEST_RUN_LOCK_AVAILABLE").unwrap() == "true";
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let available = fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive).is_ok();
        assert_eq!(available, expected);
    }

    #[tokio::test]
    async fn execution_handoff_does_not_wait_for_a_blocked_plain_header() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        std::fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  step:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let workflow = resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let monotonic = um_support::monotonic_now();
        let opened = timing_point(monotonic, "2026-08-02T12:01:43.5Z", 0);
        let initialized = timing_point(monotonic, "2026-08-02T12:01:44Z", 500);
        let terminal = timing_point(monotonic, "2026-08-02T12:01:44.03Z", 530);
        let clock = ControlledObservationClock::new(opened);
        let flushed = Arc::new(AtomicBool::new(false));
        let (entered, waiting) = std::sync::mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));

        let prepared = initialize_execution_presentation(clock.clone(), || {
            let presentation = WorkflowRunOutput::new(
                PresentationConfig {
                    requested_mode: RequestedPresentationMode::Plain,
                    color: ColorChoice::Never,
                    capabilities: TerminalCapabilities {
                        stdin_is_terminal: false,
                        stdout_is_terminal: false,
                        stderr_is_terminal: false,
                        stdout_width: None,
                        stderr_width: None,
                        term: None,
                        no_color: None,
                    },
                    standard_input_reserved: false,
                },
                DelayedHeaderWriter {
                    clock: clock.clone(),
                    completed_at: initialized,
                    flushed: flushed.clone(),
                    entered,
                    gate: gate.clone(),
                },
                io::sink(),
            )
            .start_for_result(&workflow, "result", 1, clock.clone())?;
            let timing = RunTimingObservation::new(presentation.opened_at());
            Ok(PreparedExecutionPresentation {
                observer: presentation,
                host: (),
                timing,
            })
        })
        .unwrap();

        let wait = tokio::task::spawn_blocking(move || waiting.recv().unwrap());
        wait.await.unwrap();
        assert!(!flushed.load(Ordering::SeqCst));
        prepared.timing.record(&terminal_transition(), terminal);
        let timing = observed_run_timing(&prepared.timing.snapshot()).unwrap();
        assert_eq!(timing.started_at, opened.utc);
        assert_eq!(timing.duration, Duration::from_millis(530));
        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        prepared.observer.flush_pending().await;
        assert!(flushed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn timing_observer_excludes_presentation_opening_and_uses_terminal_transition() {
        let monotonic = um_support::monotonic_now();
        let opened = timing_point(monotonic, "2026-08-02T12:01:43.5Z", 0);
        let started = timing_point(monotonic, "2026-08-02T12:01:44Z", 500);
        let step_started = timing_point(monotonic, "2026-08-02T12:01:44.01Z", 510);
        let step_finished = timing_point(monotonic, "2026-08-02T12:01:44.02Z", 520);
        let terminal = timing_point(monotonic, "2026-08-02T12:01:44.03Z", 530);
        let observations = Arc::new(AtomicUsize::new(0));
        let cancellation = CancellationSource::new();
        let timing = RunTimingObservation::new(opened);
        timing.mark_execution_started(started);
        let observer = TimingObserver::new(
            RecordingPresentation {
                observations: observations.clone(),
                failed: false,
            },
            cancellation,
            timing,
            ScriptedClock::new([step_started, step_finished, terminal]),
        );

        observer
            .observe(step_transition(
                StepStateKind::Pending,
                StepStateKind::Starting,
            ))
            .await;
        observer
            .observe(step_transition(
                StepStateKind::CapturingOutputs,
                StepStateKind::Succeeded,
            ))
            .await;
        observer.observe(terminal_transition()).await;

        assert_eq!(observations.load(Ordering::SeqCst), 3);
        let timing = observer.snapshot();
        let step = timing.steps.get("step").unwrap();
        assert_eq!(step.started.utc, step_started.utc);
        assert_eq!(step.finished, Some(step_finished.monotonic));
        let run = observed_run_timing(&timing).unwrap();
        assert_eq!(run.started_at, started.utc);
        assert_eq!(run.finished_at, terminal.utc);
        assert_eq!(run.duration, Duration::from_millis(30));
    }

    #[tokio::test]
    async fn presentation_failure_requests_cancellation_without_replacing_a_signal() {
        let monotonic = um_support::monotonic_now();
        let cancellation = CancellationSource::new();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        let observed_at = timing_point(monotonic, "2026-08-02T12:01:44Z", 0);
        let timing = RunTimingObservation::new(observed_at);
        timing.mark_execution_started(observed_at);
        let observer = TimingObserver::new(
            RecordingPresentation {
                observations: Arc::new(AtomicUsize::new(0)),
                failed: true,
            },
            cancellation.clone(),
            timing,
            ScriptedClock::new([observed_at]),
        );

        observer
            .observe(step_transition(
                StepStateKind::Pending,
                StepStateKind::Starting,
            ))
            .await;

        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::UserRequest)
        );
    }

    fn timing_point(monotonic: Instant, utc: &str, milliseconds: u64) -> ObservationTime {
        ObservationTime {
            utc: OffsetDateTime::parse(utc, &Rfc3339).unwrap(),
            monotonic: monotonic + Duration::from_millis(milliseconds),
        }
    }

    fn step_transition(
        from: StepStateKind,
        to: StepStateKind,
    ) -> ExecutionObservation<ExecutionInstant> {
        ExecutionObservation::Transition(Box::new(TransitionObservation {
            event: TransitionEvent::Step {
                sequence: TransitionSequence::default(),
                step: "step".to_owned(),
                role: WorkflowNodeRole::Step,
                failure_policy: FailurePolicy::Required,
                from,
                to,
            },
            step: None,
        }))
    }

    fn terminal_transition() -> ExecutionObservation<ExecutionInstant> {
        ExecutionObservation::Transition(Box::new(TransitionObservation {
            event: TransitionEvent::Workflow {
                sequence: TransitionSequence::default(),
                from: WorkflowState::Executing {
                    gate: SchedulingGate::Open,
                },
                to: Box::new(WorkflowState::Succeeded),
            },
            step: None,
        }))
    }
}
