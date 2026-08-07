use std::env;
use std::ffi::OsString;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{self, Read, Write};
use std::ops::Add;
use std::os::fd::AsFd as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Args;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use time::OffsetDateTime;
use tokio::io::unix::AsyncFd;

use crate::execution::pi::{PiInstallationFailure, discover_and_validate_pi_installation};
use crate::execution::workflow::admission::{
    AdmittedWorkflow, CancellationPolicy, CancellationReason, CancellationSource,
    EnvironmentSnapshot, ExecutionContext, ExecutionRootLifecycle, ResolvedAttachment,
    ResolvedImports, admit_workflow, default_execution_policy_limits,
};
use crate::execution::workflow::agent::WorkflowRunId;
use crate::execution::workflow::agent_input::{AgentInputStaging, AgentInputStagingFailure};
use crate::execution::workflow::artifact::ArtifactStaging;
use crate::execution::workflow::coordinator::CoordinationError;
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution::{WorkflowExecutionResult, execute_workflow};
use crate::execution::workflow::input::InputStaging;
use crate::execution::workflow::local_run::{
    DurableDeadline, InitialLocalRun, LocalAttemptOwner, LocalAttemptOwnershipReleased,
    PublicationFailurePhaseV1,
};
use crate::execution::workflow::observation::{ExecutionObservation, ExecutionObserver};
use crate::execution::workflow::pi_json_v1::adapter::PiJsonV1Adapter;
use crate::execution::workflow::presentation::{
    ColorChoice, PresentationConfig, PresentationFailure, PresentationFailureOperation,
    PresentationMode, PublicationPresentation, RequestedPresentationMode, SystemObservationClock,
    TerminalCapabilities, WorkflowRunOutput, WorkflowRunPresentation,
    WorkflowRunPresentationResult,
};
use crate::execution::workflow::presentation_feed::DisplayDeadline;
use crate::execution::workflow::publication::{
    LocalPublicationError, LocalPublicationPhase, WorkflowRunCancellation, WorkflowRunResult,
    WorkflowRunStep, WorkflowRunStepKind, WorkflowRunTerminalResultV1, WorkflowRunTiming,
    WorkflowStepTiming, prepare_attempt_result_destination, publish_prepared_workflow_result,
};
use crate::execution::workflow::resolution::{ResolvedWorkflow, resolve};
use crate::execution::workflow::run_timing::{
    ObservationClock, RunTimingObservation, RunTimingSnapshot,
};
use crate::execution::workflow::run_view_model::{
    StepLogCapacity, WorkflowRunCleanupResult, WorkflowRunPublicationResult, WorkflowRunViewModel,
};
use crate::execution::workflow::runtime::RunOutcome;
use crate::execution::workflow::step_runtime::AgentExecution;
use crate::execution::workflow::terminal_host::{TerminalHostExit, WorkflowTerminalHost};

pub(super) const ABOUT: &str = "Execute a local Workflow V1 command and agent DAG";
pub(super) const AFTER_HELP: &str = "Interactive mode:
  Automatic mode uses the terminal interface only when stdin and stdout are terminals,
  TERM is usable, and stdin is not reserved by --prompt-file -. Resize keeps the
  interface active; undersized terminals show a resize notice without changing modes.
  Use Up/Down or j/k to select steps, Enter to inspect logs, ? for complete help,
  Ctrl-C to request cancellation, and q to leave only after publication and cleanup.
  After q, Scherzo restores the terminal and prints the standard plain summary.";

const MAXIMUM_PARALLEL_STEPS: usize = 256;
const MAXIMUM_PROMPT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_ATTACHMENTS: usize = 256;
const MAXIMUM_ATTACHMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_ATTACHMENT_BYTES: u64 = 256 * 1024 * 1024;
const CANCELLATION_GRACE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutionLeaf {
    Run,
    Retry,
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
        help = "Nonexistent durable directory for exactly one workflow run"
    )]
    run_dir: PathBuf,

    #[arg(
        long,
        value_name = "PATH|-",
        help = "UTF-8 prompt file, or - to read standard input"
    )]
    prompt_file: Option<PathBuf>,

    #[arg(
        long,
        value_names = ["MEDIA_TYPE", "PATH"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help = "Append an immutable attachment with its declared media type"
    )]
    attachment: Vec<OsString>,

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
    pub(super) fn execute(self) -> ExitCode {
        execute_with_runtime("start local workflow runtime", self.execute_async())
    }

    async fn execute_async(self) -> ExitCode {
        let presentation_config = self.presentation_config();
        let cancellation = CancellationSource::new();
        let signal_task = match start_signal_observation(cancellation.clone()) {
            Ok(task) => task,
            Err(error) => return diagnose(error),
        };

        let imports =
            match acquire_imports(self.prompt_file.as_deref(), &self.attachment, &cancellation)
                .await
            {
                Ok(imports) => imports,
                Err(error) => {
                    signal_task.abort();
                    return diagnose(error);
                }
            };
        let workflow = match resolve(&self.source.source_root, &self.source.workflow_path) {
            Ok(workflow) => workflow,
            Err(failure) => {
                signal_task.abort();
                return rejection_output(presentation_config, |output| {
                    output.render_resolution_rejection(&failure)
                });
            }
        };
        let context = match execution_context_for_workflow(
            &workflow,
            self.execution.execution_root,
            self.max_parallel,
            cancellation.clone(),
        ) {
            Ok(context) => context,
            Err(failure) => {
                signal_task.abort();
                return rejection_output(presentation_config, |output| {
                    output.render_pi_installation_rejection(&workflow, &failure)
                });
            }
        };
        let admitted = match admit_workflow(workflow.clone(), imports, context) {
            Ok(admitted) => admitted,
            Err(failure) => {
                signal_task.abort();
                return rejection_output(presentation_config, |output| {
                    output.render_admission_rejection(&workflow, &failure)
                });
            }
        };

        if workflow.source.source_root.to_str().is_none()
            || admitted.execution().root().to_str().is_none()
        {
            signal_task.abort();
            return diagnose(LocalRunError::UnrepresentablePath);
        }
        let owned_run = match InitialLocalRun::create(&self.run_dir, &admitted) {
            Ok(run) => run,
            Err(error) => {
                signal_task.abort();
                return diagnose(error);
            }
        };
        execute_owned_attempt(
            workflow,
            admitted,
            owned_run,
            cancellation,
            signal_task,
            presentation_config,
            ExecutionLeaf::Run,
        )
        .await
    }

    fn presentation_config(&self) -> PresentationConfig {
        self.presentation_config_with(TerminalCapabilities::detect())
    }

    fn presentation_config_with(&self, capabilities: TerminalCapabilities) -> PresentationConfig {
        presentation_config_with(
            &self.presentation,
            self.prompt_file.as_deref() == Some(Path::new("-")),
            capabilities,
        )
    }
}

pub(super) fn execute_with_runtime(
    failure_context: &str,
    execution: impl Future<Output = ExitCode>,
) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return diagnose(format_args!("{failure_context}: {error}")),
    };
    runtime.block_on(execution)
}

pub(super) fn presentation_config(presentation: &super::PresentationOptions) -> PresentationConfig {
    presentation_config_with(presentation, false, TerminalCapabilities::detect())
}

fn presentation_config_with(
    presentation: &super::PresentationOptions,
    standard_input_reserved: bool,
    capabilities: TerminalCapabilities,
) -> PresentationConfig {
    PresentationConfig {
        requested_mode: if presentation.json {
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

pub(super) async fn execute_owned_attempt(
    workflow: ResolvedWorkflow,
    admitted: AdmittedWorkflow,
    owned_run: LocalAttemptOwner,
    cancellation: CancellationSource,
    signal_task: tokio::task::JoinHandle<()>,
    presentation_config: PresentationConfig,
    leaf: ExecutionLeaf,
) -> ExitCode {
    let run_directory = match owned_run.run_directory().to_str() {
        Some(path) => path.to_owned(),
        None => {
            signal_task.abort();
            settle_before_execution_failure(&owned_run);
            return diagnose(LocalRunError::UnrepresentablePath);
        }
    };
    let destination = match prepare_attempt_result_destination(
        owned_run.result_directory(),
        owned_run.private_directory(),
        owned_run.attempt_directory_handle(),
        owned_run.private_directory_handle(),
    ) {
        Ok(destination) => destination,
        Err(error) => {
            signal_task.abort();
            settle_before_execution_failure(&owned_run);
            return diagnose(error);
        }
    };
    let private_staging = match owned_run.create_private_staging() {
        Ok(staging) => staging,
        Err(_) => {
            signal_task.abort();
            settle_before_execution_failure(&owned_run);
            return diagnose(LocalRunError::PrivateStaging);
        }
    };
    let artifacts = match ArtifactStaging::create_bound(
        admitted.execution(),
        private_staging.path(),
        private_staging.root_handle(),
    ) {
        Ok(artifacts) => artifacts,
        Err(error) => {
            signal_task.abort();
            settle_before_execution_failure(&owned_run);
            return diagnose(error);
        }
    };
    let inputs = match InputStaging::create_bound(
        admitted.execution(),
        private_staging.path(),
        private_staging.root_handle(),
    ) {
        Ok(inputs) => inputs,
        Err(error) => {
            signal_task.abort();
            settle_before_execution_failure(&owned_run);
            let cleanup_failed = artifacts.release().is_err();
            record_private_cleanup_failure(&owned_run, cleanup_failed);
            return diagnose(error);
        }
    };
    let agent_staging = if admitted.agent_steps().is_empty() {
        None
    } else {
        match AgentInputStaging::create(admitted.execution(), private_staging.path()) {
            Ok(staging) => Some(staging),
            Err(error) => {
                signal_task.abort();
                settle_before_execution_failure(&owned_run);
                let cleanup_failed = inputs.release().is_err() | artifacts.release().is_err();
                record_private_cleanup_failure(&owned_run, cleanup_failed);
                return diagnose(LocalRunError::AgentInputStaging(error));
            }
        }
    };

    let run_clock = SystemObservationClock;
    let prepared =
        initialize_execution_presentation(run_clock, || match presentation_config.mode() {
            PresentationMode::Tui => {
                let presentation_opened = run_clock.sample();
                let timing_observation = RunTimingObservation::new(presentation_opened);
                let view = WorkflowRunViewModel::new(
                    &workflow,
                    admitted.execution().limits().maximum_parallel_steps().get(),
                    timing_observation.clone(),
                    run_clock,
                    StepLogCapacity::default(),
                );
                let terminal = WorkflowTerminalHost::start(
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
                let output = execution_output(presentation_config, &owned_run, leaf);
                let presentation = output.start_for_result(
                    &workflow,
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
        });
    let PreparedExecutionPresentation {
        observer, mut host, ..
    } = match prepared {
        Ok(prepared) => prepared,
        Err(failure) => {
            signal_task.abort();
            settle_before_execution_failure(&owned_run);
            let cleanup_failed =
                release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
            record_private_cleanup_failure(&owned_run, cleanup_failed);
            return diagnose(failure);
        }
    };

    if let Err(failure) = host.activate_execution() {
        signal_task.abort();
        settle_before_execution_failure(&owned_run);
        let cleanup_failed = release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
        record_private_cleanup_failure(&owned_run, cleanup_failed);
        host.stop_terminal().await;
        return diagnose(failure);
    }

    let diagnostics = StepDiagnosticLog::default();
    let agents = match &agent_staging {
        Some(staging) => {
            let adapter = match PiJsonV1Adapter::new(
                diagnostics.clone(),
                admitted.execution().limits().maximum_step_log_bytes(),
                SystemExecutionClock,
                observer.clone(),
            ) {
                Ok(adapter) => adapter,
                Err(_) => {
                    signal_task.abort();
                    settle_before_execution_failure(&owned_run);
                    let cleanup_failed =
                        release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
                    record_private_cleanup_failure(&owned_run, cleanup_failed);
                    host.stop_terminal().await;
                    return diagnose(LocalRunError::AgentRuntime);
                }
            };
            AgentExecution::enabled(
                WorkflowRunId::from(Arc::from(run_directory.as_str())),
                staging.clone(),
                adapter,
            )
        }
        None => AgentExecution::Disabled,
    };
    let execution = execute_workflow(
        admitted.clone(),
        &artifacts,
        &inputs,
        &diagnostics,
        agents,
        SystemExecutionClock,
        owned_run.commit_port(),
        observer.clone(),
        owned_run.process_guard_registry(),
    )
    .await;
    signal_task.abort();

    let execution = match execution {
        Ok(execution) => execution,
        Err(error) => {
            if error == CoordinationError::CommitFailed {
                let _ = owned_run.record_state_persistence_failure();
            }
            let cleanup_failed =
                release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
            record_private_cleanup_failure(&owned_run, cleanup_failed);
            host.stop_terminal().await;
            return diagnose(LocalRunError::Coordination(error));
        }
    };
    let observed_timing = observer.snapshot();
    let run_timing = match observed_run_timing(&observed_timing) {
        Some(timing) => timing,
        None => {
            let cleanup_failed =
                release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
            record_private_cleanup_failure(&owned_run, cleanup_failed);
            host.stop_terminal().await;
            return diagnose(LocalRunError::InvalidTerminalResult);
        }
    };
    let run = match build_run_result(
        &workflow,
        &admitted,
        &diagnostics,
        execution,
        observed_timing,
        run_timing,
        &owned_run,
    ) {
        Ok(run) => run,
        Err(error) => {
            let cleanup_failed =
                release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
            record_private_cleanup_failure(&owned_run, cleanup_failed);
            host.stop_terminal().await;
            return diagnose(error);
        }
    };
    if let Err(error) = host.reconcile_and_mark_quiescent(&run) {
        let cleanup_failed = release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
        record_private_cleanup_failure(&owned_run, cleanup_failed);
        host.stop_terminal().await;
        return diagnose(error);
    }

    host.begin_publication();
    let mut publication = publish_prepared_workflow_result(&destination, &artifacts, &run);
    if leaf == ExecutionLeaf::Retry
        && let Ok(terminal) = &mut publication
    {
        terminal.mark_retry();
    }
    let state_publication = match &publication {
        Ok(_) => owned_run.record_result_published(),
        Err(error) => {
            owned_run.record_result_publication_failed(publication_failure_phase(error.phase()))
        }
    };
    host.complete_publication(&publication);
    host.begin_cleanup();
    let execution_staging_failed =
        release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
    let private_staging_failed = private_staging.release().is_err();
    let cleanup_failed = execution_staging_failed || private_staging_failed;
    let cleanup_state = if cleanup_failed {
        owned_run.record_private_cleanup_failure()
    } else {
        Ok(())
    };
    host.complete_cleanup(cleanup_failed);
    let state_commit_failed = state_publication.is_err() || cleanup_state.is_err();

    let released_ownership = owned_run.release();
    host.mark_adapter_lifecycle_completed(released_ownership);
    host.finish(
        &workflow,
        &run,
        &publication,
        cleanup_failed,
        state_commit_failed,
    )
    .await
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
    }
}

fn rejection_output(
    config: PresentationConfig,
    render: impl FnOnce(WorkflowRunOutput<io::Stdout, io::Stderr>) -> WorkflowRunPresentationResult,
) -> ExitCode {
    rejection_exit(render(WorkflowRunOutput::new(
        config,
        io::stdout(),
        io::stderr(),
    )))
}

pub(super) fn rejection_exit(result: WorkflowRunPresentationResult) -> ExitCode {
    match result {
        WorkflowRunPresentationResult::Rejected
        | WorkflowRunPresentationResult::Failed(_)
        | WorkflowRunPresentationResult::PublicationFailed
        | WorkflowRunPresentationResult::Published { .. } => ExitCode::FAILURE,
    }
}

fn presentation_exit_code(result: WorkflowRunPresentationResult) -> ExitCode {
    match result {
        WorkflowRunPresentationResult::Published { exit_status, .. } => {
            u8::try_from(exit_status).map_or(ExitCode::FAILURE, ExitCode::from)
        }
        WorkflowRunPresentationResult::Rejected
        | WorkflowRunPresentationResult::PublicationFailed
        | WorkflowRunPresentationResult::Failed(_) => ExitCode::FAILURE,
    }
}

fn parse_parallelism(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| (1..=MAXIMUM_PARALLEL_STEPS).contains(value))
        .ok_or_else(|| format!("value must be between 1 and {MAXIMUM_PARALLEL_STEPS}"))
}

pub(super) fn execution_context_for_workflow(
    workflow: &ResolvedWorkflow,
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
) -> Result<ExecutionContext, PiInstallationFailure> {
    let environment = EnvironmentSnapshot::new(env::vars_os());
    let context = ExecutionContext::new(
        root,
        ExecutionRootLifecycle::CallerOwnedRetained,
        default_execution_policy_limits(maximum_parallel_steps),
        environment.clone(),
        CancellationPolicy::new(cancellation, CANCELLATION_GRACE),
    );
    if workflow.definition.steps.values().any(|step| {
        matches!(
            step,
            crate::execution::workflow::validated::ValidatedStep::Agent(_)
        )
    }) {
        let installation = discover_and_validate_pi_installation()?;
        Ok(context.with_pi_installation(installation))
    } else {
        Ok(context)
    }
}

async fn acquire_imports(
    prompt_path: Option<&Path>,
    attachments: &[OsString],
    cancellation: &CancellationSource,
) -> Result<ResolvedImports, LocalRunError> {
    let prompt = match prompt_path {
        None => None,
        Some(path) if path == Path::new("-") => {
            let bytes = read_stdin_bounded(MAXIMUM_PROMPT_BYTES, cancellation)
                .await
                .map_err(|kind| LocalRunError::Import { kind, path: None })?;
            Some(decode_prompt(bytes, None)?)
        }
        Some(path) => {
            let file = open_regular_import(path).map_err(|kind| LocalRunError::Import {
                kind,
                path: Some(path.to_owned()),
            })?;
            let bytes = read_bounded(file, MAXIMUM_PROMPT_BYTES, cancellation).map_err(|kind| {
                LocalRunError::Import {
                    kind,
                    path: Some(path.to_owned()),
                }
            })?;
            Some(decode_prompt(bytes, Some(path))?)
        }
    };

    let pairs = attachments.chunks_exact(2);
    if !pairs.remainder().is_empty() || pairs.len() > MAXIMUM_ATTACHMENTS {
        return Err(LocalRunError::AttachmentCount);
    }
    let mut total = 0_u64;
    let mut resolved = Vec::with_capacity(pairs.len());
    for pair in pairs {
        if cancellation.is_cancelled() {
            return Err(LocalRunError::Import {
                kind: ImportFailureKind::Interrupted,
                path: None,
            });
        }
        let media_type = pair[0]
            .to_str()
            .ok_or(LocalRunError::AttachmentMediaTypeEncoding)?;
        let path = Path::new(&pair[1]);
        let file = open_regular_import(path).map_err(|kind| LocalRunError::Import {
            kind,
            path: Some(path.to_owned()),
        })?;
        let remaining_total = MAXIMUM_TOTAL_ATTACHMENT_BYTES.saturating_sub(total);
        let maximum = MAXIMUM_ATTACHMENT_BYTES.min(remaining_total);
        let bytes = match read_bounded(file, maximum, cancellation) {
            Err(ImportFailureKind::TooLarge) if maximum < MAXIMUM_ATTACHMENT_BYTES => {
                return Err(LocalRunError::AttachmentBytes);
            }
            Err(kind) => {
                return Err(LocalRunError::Import {
                    kind,
                    path: Some(path.to_owned()),
                });
            }
            Ok(bytes) => bytes,
        };
        let size = u64::try_from(bytes.len()).map_err(|_| LocalRunError::AttachmentBytes)?;
        total = total
            .checked_add(size)
            .filter(|total| *total <= MAXIMUM_TOTAL_ATTACHMENT_BYTES)
            .ok_or(LocalRunError::AttachmentBytes)?;
        let attachment = ResolvedAttachment::new(Arc::from(media_type), Arc::from(bytes));
        let attachment = if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            attachment.with_diagnostic_source_name(Arc::from(name))
        } else {
            attachment
        };
        resolved.push(attachment);
    }
    Ok(ResolvedImports::new(prompt, Arc::from(resolved)))
}

fn decode_prompt(bytes: Vec<u8>, path: Option<&Path>) -> Result<Arc<str>, LocalRunError> {
    String::from_utf8(bytes)
        .map(Arc::from)
        .map_err(|_| LocalRunError::Import {
            kind: ImportFailureKind::InvalidUtf8,
            path: path.map(Path::to_owned),
        })
}

fn open_regular_import(path: &Path) -> Result<File, ImportFailureKind> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| ImportFailureKind::Unavailable)?;
    if !file
        .metadata()
        .map_err(|_| ImportFailureKind::Unavailable)?
        .is_file()
    {
        return Err(ImportFailureKind::NotRegularFile);
    }
    Ok(file)
}

fn read_bounded(
    mut reader: impl Read,
    maximum: u64,
    cancellation: &CancellationSource,
) -> Result<Vec<u8>, ImportFailureKind> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if cancellation.is_cancelled() {
            return Err(ImportFailureKind::Interrupted);
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
                    return Err(ImportFailureKind::TooLarge);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return Err(ImportFailureKind::Read),
        }
    }
}

async fn read_stdin_bounded(
    maximum: u64,
    cancellation: &CancellationSource,
) -> Result<Vec<u8>, ImportFailureKind> {
    let standard_input = io::stdin();
    let input =
        rustix::io::dup(standard_input.as_fd()).map_err(|_| ImportFailureKind::Unavailable)?;
    let original_flags =
        fcntl_getfl(&standard_input).map_err(|_| ImportFailureKind::Unavailable)?;
    fcntl_setfl(&standard_input, original_flags | OFlags::NONBLOCK)
        .map_err(|_| ImportFailureKind::Unavailable)?;
    let input = File::from(input);
    let async_input = match AsyncFd::new(input) {
        Ok(input) => input,
        Err(_) => {
            fcntl_setfl(&standard_input, original_flags).map_err(|_| ImportFailureKind::Read)?;
            let input = rustix::io::dup(standard_input.as_fd())
                .map_err(|_| ImportFailureKind::Unavailable)?;
            return read_bounded(File::from(input), maximum, cancellation);
        }
    };
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    let result = loop {
        if cancellation.is_cancelled() {
            break Err(ImportFailureKind::Interrupted);
        }
        let remaining = maximum.saturating_sub(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let permitted = usize::try_from(remaining.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let mut ready = tokio::select! {
            biased;
            _ = cancellation.wait_for_cancellation() => {
                break Err(ImportFailureKind::Interrupted);
            }
            ready = async_input.readable() => match ready {
                Ok(ready) => ready,
                Err(_) => break Err(ImportFailureKind::Read),
            }
        };
        match ready.try_io(|inner| inner.get_ref().read(&mut buffer[..permitted])) {
            Ok(Ok(0)) => break Ok(bytes),
            Ok(Ok(read)) => {
                bytes.extend_from_slice(&buffer[..read]);
                if u64::try_from(bytes.len()).map_or(true, |length| length > maximum) {
                    break Err(ImportFailureKind::TooLarge);
                }
            }
            Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
            Ok(Err(_)) => break Err(ImportFailureKind::Read),
            Err(_) => {}
        }
    };
    drop(async_input);
    fcntl_setfl(&standard_input, original_flags).map_err(|_| ImportFailureKind::Read)?;
    result
}

pub(super) fn start_signal_observation(
    cancellation: CancellationSource,
) -> Result<tokio::task::JoinHandle<()>, LocalRunError> {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|_| LocalRunError::SignalObservation)?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| LocalRunError::SignalObservation)?;
    Ok(tokio::spawn(observe_first_signal(
        async move {
            let _ = interrupt.recv().await;
        },
        async move {
            let _ = terminate.recv().await;
        },
        cancellation,
    )))
}

async fn observe_first_signal(
    interrupt: impl Future<Output = ()> + Send,
    terminate: impl Future<Output = ()> + Send,
    cancellation: CancellationSource,
) {
    let reason = tokio::select! {
        biased;
        () = interrupt => CancellationReason::UserRequest,
        () = terminate => CancellationReason::TerminationRequest,
    };
    cancellation.request_cancellation(reason);
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
        {
            if failure.is_none() {
                *failure = Some(terminal_failure.clone());
            }
            write_diagnostic(format_args!("{terminal_failure}"));
        }
    }

    fn reconcile_and_mark_quiescent(&self, run: &WorkflowRunResult) -> Result<(), LocalRunError> {
        if let Self::Tui { view, .. } = self {
            view.reconcile_terminal_result(run)
                .map_err(|_| LocalRunError::InvalidTerminalResult)?;
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
    ) -> ExitCode {
        match self {
            Self::Standard(presentation) => {
                if cleanup_failed || state_commit_failed {
                    render_without_terminal_json(presentation, run, publication);
                    if state_commit_failed {
                        report_state_commit_failure();
                    } else {
                        report_cleanup_failure(publication);
                    }
                    return ExitCode::FAILURE;
                }
                let presented = match publication {
                    Ok(terminal) => {
                        presentation.finish(run, PublicationPresentation::Published(terminal))
                    }
                    Err(error) => presentation.finish(run, PublicationPresentation::Failed(error)),
                };
                presentation_exit_code(presented)
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
                    write_diagnostic(format_args!("{terminal_failure}"));
                    if let Err(error) = publication {
                        write_diagnostic(format_args!("{error}"));
                    }
                    if state_commit_failed {
                        report_state_commit_failure();
                    } else if cleanup_failed {
                        report_cleanup_failure(publication);
                    }
                    return ExitCode::FAILURE;
                }

                let output =
                    WorkflowRunOutput::new(config.as_ref().clone(), io::stdout(), io::stderr());
                let output = match *leaf {
                    ExecutionLeaf::Run => output,
                    ExecutionLeaf::Retry => output.for_retry(&run.run_directory),
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
                    report_state_commit_failure();
                    ExitCode::FAILURE
                } else if cleanup_failed {
                    report_cleanup_failure(publication);
                    ExitCode::FAILURE
                } else {
                    presentation_exit_code(presented)
                }
            }
        }
    }
}

fn render_without_terminal_json(
    presentation: &SystemPresentation,
    run: &WorkflowRunResult,
    publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
) {
    match publication {
        Ok(terminal) => {
            let _ = presentation
                .finish_without_terminal_json(run, PublicationPresentation::Published(terminal));
        }
        Err(error) => {
            let _ = presentation
                .finish_without_terminal_json(run, PublicationPresentation::Failed(error));
        }
    }
}

fn report_cleanup_failure(
    publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
) {
    if let Ok(terminal) = publication {
        write_diagnostic(format_args!(
            "release private workflow staging; result published at {}",
            terminal.result_directory()
        ));
    } else {
        write_diagnostic(format_args!("release private workflow staging"));
    }
}

fn report_state_commit_failure() {
    write_diagnostic(format_args!("commit terminal local run state"));
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

fn settle_before_execution_failure(run: &InitialLocalRun) {
    if let Err(error) = run.record_executor_fault_before_execution() {
        write_diagnostic(format_args!(
            "settle published local run before execution: {error}"
        ));
    }
}

fn record_private_cleanup_failure(run: &InitialLocalRun, cleanup_failed: bool) {
    if cleanup_failed && let Err(error) = run.record_private_cleanup_failure() {
        write_diagnostic(format_args!(
            "record private local workflow cleanup failure: {error}"
        ));
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

fn build_run_result(
    workflow: &ResolvedWorkflow,
    admitted: &crate::execution::workflow::admission::AdmittedWorkflow,
    diagnostics: &StepDiagnosticLog,
    execution: WorkflowExecutionResult,
    timing: RunTimingSnapshot,
    run_timing: WorkflowRunTiming,
    local_run: &InitialLocalRun,
) -> Result<WorkflowRunResult, LocalRunError> {
    let expected_cancellation = match &execution.outcome {
        RunOutcome::Succeeded => None,
        RunOutcome::Failed {
            later_cancellation, ..
        } => *later_cancellation,
        RunOutcome::Cancelled { reason } => Some(*reason),
    };
    let cancellation = match (expected_cancellation, timing.cancellation) {
        (None, None) => None,
        (Some(reason), Some((observed, deadline))) if reason == observed => {
            Some(WorkflowRunCancellation {
                reason,
                force_stop_deadline: deadline,
            })
        }
        _ => return Err(LocalRunError::InvalidTerminalResult),
    };
    let mut states = execution.steps;
    let mut steps = Vec::with_capacity(states.len());
    for id in &workflow.definition.presentation_order {
        let state = states
            .remove(id)
            .ok_or(LocalRunError::InvalidTerminalResult)?;
        let timing = match timing.steps.get(id) {
            None => None,
            Some(timing) => {
                let finished = timing
                    .finished
                    .ok_or(LocalRunError::InvalidTerminalResult)?;
                Some(WorkflowStepTiming {
                    started_at: timing.started.utc,
                    duration: finished.saturating_duration_since(timing.started.monotonic),
                })
            }
        };
        let kind = match workflow.definition.steps.get(id) {
            Some(crate::execution::workflow::validated::ValidatedStep::Command(_)) => {
                WorkflowRunStepKind::Command
            }
            Some(crate::execution::workflow::validated::ValidatedStep::Agent(_)) => {
                WorkflowRunStepKind::Agent
            }
            None => return Err(LocalRunError::InvalidTerminalResult),
        };
        steps.push(WorkflowRunStep {
            id: id.clone(),
            kind,
            state,
            timing,
            command_output: (kind == WorkflowRunStepKind::Command)
                .then(|| diagnostics.get(id))
                .flatten(),
        });
    }
    if !states.is_empty() {
        return Err(LocalRunError::InvalidTerminalResult);
    }
    Ok(WorkflowRunResult {
        run_directory: local_run.run_directory().to_owned(),
        attempt_number: local_run.attempt_number(),
        workflow_path: execution.provenance.workflow_path,
        source_root: execution.provenance.source_root,
        content_digest: execution.content_digest,
        execution_root: admitted.execution().root().to_owned(),
        maximum_parallel_steps: admitted.execution().limits().maximum_parallel_steps(),
        timing: run_timing,
        outcome: execution.outcome,
        cancellation,
        steps,
        exports: execution.exports,
        export_sources: workflow.definition.exports.clone(),
    })
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ImportFailureKind {
    Unavailable,
    NotRegularFile,
    Interrupted,
    Read,
    TooLarge,
    InvalidUtf8,
}

#[derive(Debug)]
pub(super) enum LocalRunError {
    Import {
        kind: ImportFailureKind,
        path: Option<PathBuf>,
    },
    AttachmentCount,
    AttachmentBytes,
    AttachmentMediaTypeEncoding,
    UnrepresentablePath,
    PrivateStaging,
    AgentInputStaging(AgentInputStagingFailure),
    AgentRuntime,
    SignalObservation,
    Coordination(CoordinationError),
    InvalidTerminalResult,
}

impl fmt::Display for LocalRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Import { kind, path } => {
                write!(formatter, "acquire local workflow import")?;
                if let Some(path) = path {
                    write!(formatter, " {:?}", path)?;
                }
                write!(formatter, ": {kind:?}")
            }
            Self::AttachmentCount => {
                formatter.write_str("acquire local workflow imports: attachment count exceeds 256")
            }
            Self::AttachmentBytes => formatter.write_str(
                "acquire local workflow imports: total attachment bytes exceed 268435456",
            ),
            Self::AttachmentMediaTypeEncoding => formatter.write_str(
                "acquire local workflow imports: an attachment media type is not valid UTF-8",
            ),
            Self::UnrepresentablePath => formatter.write_str(
                "prepare local workflow paths: an authoritative path is not valid UTF-8",
            ),
            Self::PrivateStaging => formatter.write_str("prepare private local workflow staging"),
            Self::AgentInputStaging(error) => {
                write!(formatter, "prepare private local agent staging: {error}")
            }
            Self::AgentRuntime => formatter.write_str("prepare local PiJsonV1 runtime"),
            Self::SignalObservation => {
                formatter.write_str("install local workflow signal observation")
            }
            Self::Coordination(error) => {
                write!(formatter, "execute admitted local workflow: {error:?}")
            }
            Self::InvalidTerminalResult => {
                formatter.write_str("prepare authoritative local workflow terminal result")
            }
        }
    }
}

impl std::error::Error for LocalRunError {}

pub(super) fn diagnose(error: impl fmt::Display) -> ExitCode {
    write_diagnostic(format_args!("{error}"));
    ExitCode::FAILURE
}

fn write_diagnostic(message: fmt::Arguments<'_>) {
    let standard_error = io::stderr();
    let mut standard_error = standard_error.lock();
    let _ = writeln!(standard_error, "Error: {message}").and_then(|()| standard_error.flush());
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::ready;
    use std::process::{Command as ProcessCommand, Stdio};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use rustix::fs::{FlockOperation, fcntl_lock};
    use time::format_description::well_known::Rfc3339;

    use super::*;
    use crate::execution::workflow::observation::TransitionObservation;
    use crate::execution::workflow::run_timing::ObservationTime;
    use crate::execution::workflow::runtime::{
        SchedulingGate, StepStateKind, TransitionEvent, TransitionSequence, WorkflowState,
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
    }

    impl Write for DelayedHeaderWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
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
                workflow_path: PathBuf::from("workflow.yaml"),
            },
            execution: super::super::LocalExecutionRoot {
                execution_root: PathBuf::from("execution"),
            },
            run_dir: PathBuf::from("run"),
            prompt_file: None,
            attachment: Vec::new(),
            max_parallel: 2,
            presentation: super::super::PresentationOptions {
                plain: false,
                json: true,
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

    #[tokio::test]
    async fn injected_signals_map_once_to_the_closed_cancellation_reason() {
        let cancellation = CancellationSource::new();
        let (interrupt_sender, interrupt) = tokio::sync::oneshot::channel::<()>();
        let (terminate_sender, terminate) = tokio::sync::oneshot::channel::<()>();
        let observer = tokio::spawn(observe_first_signal(
            async move {
                interrupt.await.unwrap();
            },
            async move {
                terminate.await.unwrap();
            },
            cancellation.clone(),
        ));

        terminate_sender.send(()).unwrap();
        assert_eq!(
            cancellation.wait_for_cancellation().await,
            CancellationReason::TerminationRequest
        );
        observer.await.unwrap();
        assert!(interrupt_sender.send(()).is_err());
        assert!(!cancellation.request_cancellation(CancellationReason::UserRequest));
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
            ResolvedImports::default(),
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
            StepLogCapacity::default(),
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
        let Some(path) = std::env::var_os("SCHERZO_TEST_RUN_LOCK_PATH") else {
            return;
        };
        let expected = std::env::var("SCHERZO_TEST_RUN_LOCK_AVAILABLE").unwrap() == "true";
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let available = fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive).is_ok();
        assert_eq!(available, expected);
    }

    #[test]
    fn execution_handoff_samples_timing_after_the_plain_header_is_flushed() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        std::fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  step:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let workflow = resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let monotonic = crate::timing::monotonic_now();
        let opened = timing_point(monotonic, "2026-08-02T12:01:43.5Z", 0);
        let initialized = timing_point(monotonic, "2026-08-02T12:01:44Z", 500);
        let terminal = timing_point(monotonic, "2026-08-02T12:01:44.03Z", 530);
        let clock = ControlledObservationClock::new(opened);
        let flushed = Arc::new(AtomicBool::new(false));

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

        assert!(flushed.load(Ordering::SeqCst));
        prepared.timing.record(&terminal_transition(), terminal);
        let timing = observed_run_timing(&prepared.timing.snapshot()).unwrap();
        assert_eq!(timing.started_at, initialized.utc);
        assert_eq!(timing.duration, Duration::from_millis(30));
    }

    #[tokio::test]
    async fn timing_observer_excludes_presentation_opening_and_uses_terminal_transition() {
        let monotonic = crate::timing::monotonic_now();
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
        let monotonic = crate::timing::monotonic_now();
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
        ExecutionObservation::Transition(TransitionObservation {
            event: TransitionEvent::Step {
                sequence: TransitionSequence::default(),
                step: "step".to_owned(),
                from,
                to,
            },
            step: None,
        })
    }

    fn terminal_transition() -> ExecutionObservation<ExecutionInstant> {
        ExecutionObservation::Transition(TransitionObservation {
            event: TransitionEvent::Workflow {
                sequence: TransitionSequence::default(),
                from: WorkflowState::Executing {
                    gate: SchedulingGate::Open,
                },
                to: WorkflowState::Succeeded,
            },
            step: None,
        })
    }
}
