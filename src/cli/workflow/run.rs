use std::env;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{self, Read};
use std::ops::Add;
use std::os::fd::AsFd as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::Args;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use time::OffsetDateTime;
use tokio::io::unix::AsyncFd;

use crate::execution::AgentHarnessInstallationFailure;
use crate::execution::claude_code::discover_and_validate_claude_code_installation;
use crate::execution::codex::discover_and_validate_codex_installation;
use crate::execution::pi::discover_and_validate_pi_installation;
use crate::execution::workflow::MAXIMUM_PARALLEL_STEPS;
#[cfg(test)]
use crate::execution::workflow::admission::admit_workflow;
use crate::execution::workflow::admission::{
    AdmittedWorkflow, CancellationPolicy, CancellationReason, CancellationSource,
    EnvironmentSnapshot, ExecutionContext, ExecutionRootLifecycle, MAXIMUM_AGENT_PROMPT_BYTES,
    ResolvedAttachment, ResolvedImports, admit_local_workflow, default_execution_policy_limits,
};
use crate::execution::workflow::agent::WorkflowRunId;
use crate::execution::workflow::agent::dispatch::production_agent_dispatcher;
use crate::execution::workflow::agent_input::AgentInputStaging;
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
use crate::execution::workflow::presentation::{
    ColorChoice, PresentationConfig, PresentationFailure, PresentationFailureOperation,
    PresentationMode, PublicationPresentation, RequestedPresentationMode, SystemObservationClock,
    TerminalCapabilities, WorkflowRunOutput, WorkflowRunPresentation,
    WorkflowRunPresentationResult,
};
use crate::execution::workflow::presentation_feed::DisplayDeadline;
use crate::execution::workflow::publication::{
    LocalPublicationError, LocalPublicationPhase, WorkflowRunCancellation, WorkflowRunFinalization,
    WorkflowRunFinalizationCancellation, WorkflowRunResult, WorkflowRunStep, WorkflowRunStepKind,
    WorkflowRunTerminalResultV1, WorkflowRunTiming, WorkflowStepTiming,
    prepare_attempt_result_destination, publish_prepared_workflow_result,
    summary_disposition_matches,
};
use crate::execution::workflow::resolution::{ResolvedWorkflow, resolve_workflow_file};
use crate::execution::workflow::run_timing::{
    ObservationClock, RunTimingObservation, RunTimingSnapshot,
};
use crate::execution::workflow::run_view_model::{
    WorkflowRunCleanupResult, WorkflowRunPublicationResult, WorkflowRunViewModel,
};
use crate::execution::workflow::runtime::RunOutcome;
use crate::execution::workflow::step_runtime::AgentExecution;
use crate::execution::workflow::terminal_host::{TerminalHostExit, WorkflowTerminalHost};
use crate::execution::workflow::validated::WorkflowNodeRole;
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Run a local command and agent workflow";
pub(super) const AFTER_HELP: &str = "Interactive mode:
  Automatic mode uses the terminal interface only when stdin and stdout are terminals,
  TERM is usable, and stdin is not reserved by --prompt-file -. Resize keeps the
  interface active; undersized terminals show a resize notice without changing modes.
  Use Up/Down or j/k to select steps, Enter to inspect logs, ? for complete help,
  Ctrl-C to request cancellation, and q to leave only after publication and cleanup.
  After q, Scherzo restores the terminal and prints the standard plain summary.";

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
        help = "Directory to create for this run (must not already exist)"
    )]
    run_dir: PathBuf,

    #[arg(
        long,
        value_name = "PATH",
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
    pub(super) fn execute(self) -> super::super::CommandResult {
        execute_with_runtime("start local workflow runtime", self.execute_async())
    }

    async fn execute_async(self) -> super::super::CommandResult {
        let presentation_config = self.presentation_config();
        let cancellation = CancellationSource::new();
        let signal_task = match start_signal_observation(cancellation.clone()) {
            Ok(task) => task,
            Err(error) => return Err(error.into()),
        };

        let imports =
            match acquire_imports(self.prompt_file.as_deref(), &self.attachment, &cancellation)
                .await
            {
                Ok(imports) => imports,
                Err(error) => {
                    signal_task.abort();
                    return Err(error.into());
                }
            };
        let workflow =
            match resolve_workflow_file(&self.source.source_root, &self.source.workflow_file) {
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
                    output.render_agent_harness_installation_rejection(&workflow, &failure)
                });
            }
        };
        let admitted = match admit_local_workflow(workflow.clone(), imports, context) {
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
            return diagnose(
                "prepare local workflow paths: an authoritative path is not valid UTF-8",
            );
        }
        let owned_run = match InitialLocalRun::create(&self.run_dir, &admitted)
            .map_err(anyhow::Error::new)
            .with_context(|| format!("create workflow run {}", self.run_dir.display()))
        {
            Ok(run) => run,
            Err(error) => {
                signal_task.abort();
                return Err(error.into());
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
) -> super::super::CommandResult {
    let run_directory = match owned_run.run_directory().to_str() {
        Some(path) => path.to_owned(),
        None => {
            signal_task.abort();
            settle_before_execution_failure(&owned_run);
            return diagnose(
                "prepare local workflow paths: an authoritative path is not valid UTF-8",
            );
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
            return diagnose("prepare private local workflow staging");
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
                return diagnose(format_args!("prepare private local agent staging: {error}"));
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

    let agent_diagnostic_sessions = if agent_staging.is_some() {
        match owned_run.create_agent_diagnostic_sessions() {
            Ok(sessions) => Some(sessions),
            Err(_) => {
                signal_task.abort();
                settle_before_execution_failure(&owned_run);
                let cleanup_failed =
                    release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
                record_private_cleanup_failure(&owned_run, cleanup_failed);
                host.stop_terminal().await;
                return diagnose("prepare local agent diagnostic retention");
            }
        }
    } else {
        None
    };
    let diagnostics = StepDiagnosticLog::default();
    let agents = match (&agent_staging, agent_diagnostic_sessions) {
        (Some(staging), Some(diagnostic_sessions)) => {
            let maximum_log_bytes = admitted.execution().limits().maximum_step_log_bytes();
            let Ok(dispatcher) = production_agent_dispatcher(
                diagnostics.clone(),
                maximum_log_bytes,
                SystemExecutionClock,
                observer.clone(),
            ) else {
                signal_task.abort();
                settle_before_execution_failure(&owned_run);
                let cleanup_failed =
                    release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
                record_private_cleanup_failure(&owned_run, cleanup_failed);
                host.stop_terminal().await;
                return diagnose("prepare local agent runtimes");
            };
            AgentExecution::enabled(
                WorkflowRunId::from(Arc::from(run_directory.as_str())),
                staging.clone(),
                diagnostic_sessions,
                dispatcher,
            )
        }
        (None, None) => AgentExecution::Disabled,
        (Some(_), None) | (None, Some(_)) => {
            signal_task.abort();
            settle_before_execution_failure(&owned_run);
            let cleanup_failed =
                release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
            record_private_cleanup_failure(&owned_run, cleanup_failed);
            host.stop_terminal().await;
            return diagnose("prepare local agent diagnostic retention");
        }
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
            return diagnose(format_args!("execute admitted local workflow: {error:?}"));
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
            return diagnose("prepare authoritative local workflow terminal result");
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
            return Err(error.into());
        }
    };
    if let Err(error) = host.reconcile_and_mark_quiescent(&run) {
        let cleanup_failed = release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
        record_private_cleanup_failure(&owned_run, cleanup_failed);
        host.stop_terminal().await;
        return Err(error.into());
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
        WorkflowRunPresentationResult::Published { exit_status, .. } => {
            Ok(ExitCode::from_u16(exit_status).unwrap_or(ExitCode::GeneralFailure))
        }
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

pub(super) fn execution_context_for_workflow(
    workflow: &ResolvedWorkflow,
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
) -> Result<ExecutionContext, AgentHarnessInstallationFailure> {
    let environment = EnvironmentSnapshot::new(env::vars_os());
    let mut context = ExecutionContext::new(
        root,
        ExecutionRootLifecycle::CallerOwnedRetained,
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
    for step_name in workflow
        .definition
        .source_order
        .iter()
        .chain(&workflow.definition.finalizer_source_order)
    {
        let Some(crate::execution::workflow::validated::ValidatedStep::Agent(step)) =
            workflow.definition.steps.get(step_name).or_else(|| {
                workflow
                    .definition
                    .finalizers
                    .get(step_name)
                    .map(|finalizer| &finalizer.body)
            })
        else {
            continue;
        };
        match &step.agent.harness {
            crate::execution::workflow::validated::ValidatedHarness::Pi(_) if !pi_validated => {
                let installation = discover_and_validate_pi_installation()
                    .map_err(AgentHarnessInstallationFailure::Pi)?;
                context = context.with_pi_installation(installation);
                pi_validated = true;
            }
            crate::execution::workflow::validated::ValidatedHarness::ClaudeCode(_)
                if !claude_code_validated =>
            {
                let installation = discover_and_validate_claude_code_installation()
                    .map_err(AgentHarnessInstallationFailure::ClaudeCode)?;
                context = context.with_claude_code_installation(installation);
                claude_code_validated = true;
            }
            crate::execution::workflow::validated::ValidatedHarness::Codex(_)
                if !codex_validated =>
            {
                let installation = discover_and_validate_codex_installation()
                    .map_err(AgentHarnessInstallationFailure::Codex)?;
                context = context.with_codex_installation(installation);
                codex_validated = true;
            }
            crate::execution::workflow::validated::ValidatedHarness::Pi(_)
            | crate::execution::workflow::validated::ValidatedHarness::ClaudeCode(_)
            | crate::execution::workflow::validated::ValidatedHarness::Codex(_) => {}
        }
    }
    Ok(context)
}

async fn acquire_imports(
    prompt_path: Option<&Path>,
    attachments: &[OsString],
    cancellation: &CancellationSource,
) -> anyhow::Result<ResolvedImports> {
    let prompt = match prompt_path {
        None => None,
        Some(path) if path == Path::new("-") => {
            let bytes = read_stdin_bounded(MAXIMUM_AGENT_PROMPT_BYTES, cancellation)
                .await
                .map_err(|kind| import_error(kind, None))?;
            Some(decode_prompt(bytes, None)?)
        }
        Some(path) => {
            let file = open_regular_import(path).map_err(|kind| import_error(kind, Some(path)))?;
            let bytes = read_bounded(file, MAXIMUM_AGENT_PROMPT_BYTES, cancellation)
                .map_err(|kind| import_error(kind, Some(path)))?;
            Some(decode_prompt(bytes, Some(path))?)
        }
    };

    let pairs = attachments.chunks_exact(2);
    if !pairs.remainder().is_empty() || pairs.len() > MAXIMUM_ATTACHMENTS {
        return Err(anyhow!(
            "acquire local workflow imports: attachment count exceeds 256"
        ));
    }
    let mut total = 0_u64;
    let mut resolved = Vec::with_capacity(pairs.len());
    for pair in pairs {
        if cancellation.is_cancelled() {
            return Err(import_error(ImportFailureKind::Interrupted, None));
        }
        let media_type = pair[0].to_str().ok_or_else(|| {
            anyhow!("acquire local workflow imports: an attachment media type is not valid UTF-8")
        })?;
        let path = Path::new(&pair[1]);
        let file = open_regular_import(path).map_err(|kind| import_error(kind, Some(path)))?;
        let remaining_total = MAXIMUM_TOTAL_ATTACHMENT_BYTES.saturating_sub(total);
        let maximum = MAXIMUM_ATTACHMENT_BYTES.min(remaining_total);
        let bytes = match read_bounded(file, maximum, cancellation) {
            Err(ImportFailureKind::TooLarge) if maximum < MAXIMUM_ATTACHMENT_BYTES => {
                return Err(attachment_bytes_error());
            }
            Err(kind) => return Err(import_error(kind, Some(path))),
            Ok(bytes) => bytes,
        };
        let size = u64::try_from(bytes.len()).map_err(|_| attachment_bytes_error())?;
        total = total
            .checked_add(size)
            .filter(|total| *total <= MAXIMUM_TOTAL_ATTACHMENT_BYTES)
            .ok_or_else(attachment_bytes_error)?;
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

fn import_error(kind: ImportFailureKind, path: Option<&Path>) -> anyhow::Error {
    let context = path.map_or_else(
        || "acquire local workflow import".to_owned(),
        |path| format!("acquire local workflow import {path:?}"),
    );
    anyhow!("{kind:?}").context(context)
}

fn attachment_bytes_error() -> anyhow::Error {
    anyhow!("acquire local workflow imports: total attachment bytes exceed 268435456")
}

fn decode_prompt(bytes: Vec<u8>, path: Option<&Path>) -> anyhow::Result<Arc<str>> {
    String::from_utf8(bytes)
        .map(Arc::from)
        .map_err(|_| import_error(ImportFailureKind::InvalidUtf8, path))
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
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("install local workflow interrupt observation")?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install local workflow termination observation")?;
    Ok(tokio::spawn(async move {
        loop {
            let reason = tokio::select! {
                biased;
                signal = interrupt.recv() => signal.map(|()| CancellationReason::UserRequest),
                signal = terminate.recv() => {
                    signal.map(|()| CancellationReason::TerminationRequest)
                }
            };
            let Some(reason) = reason else {
                return;
            };
            if cancellation.request_cancellation(reason) {
                continue;
            }
            if cancellation.request_force_abort() {
                return;
            }
        }
    }))
}

#[cfg(test)]
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
                    render_without_terminal_json(presentation, run, publication);
                    return Err(if state_commit_failed {
                        state_commit_failure()
                    } else {
                        cleanup_failure(publication)
                    }
                    .into());
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

fn settle_before_execution_failure(run: &InitialLocalRun) {
    let _ = run.record_executor_fault_before_execution();
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

fn build_run_result(
    workflow: &ResolvedWorkflow,
    admitted: &crate::execution::workflow::admission::AdmittedWorkflow,
    diagnostics: &StepDiagnosticLog,
    execution: WorkflowExecutionResult<ExecutionInstant>,
    timing: RunTimingSnapshot,
    run_timing: WorkflowRunTiming,
    local_run: &InitialLocalRun,
) -> anyhow::Result<WorkflowRunResult> {
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
            Some(crate::execution::workflow::validated::ValidatedStep::Command(command)) => {
                (WorkflowRunStepKind::Command, command.common.failure_policy)
            }
            Some(crate::execution::workflow::validated::ValidatedStep::Agent(agent)) => {
                (WorkflowRunStepKind::Agent, agent.common.failure_policy)
            }
            None => return Err(invalid_terminal_result_error()),
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
    if !states.is_empty() {
        return Err(invalid_terminal_result_error());
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
        finalization,
        exports: execution.exports,
        export_sources: workflow.definition.exports.clone(),
    })
}

fn finalizer_kind_and_policy(
    workflow: &ResolvedWorkflow,
    id: &str,
) -> Option<(
    WorkflowRunStepKind,
    crate::execution::workflow::document::FailurePolicy,
)> {
    let finalizer = workflow.definition.finalizers.get(id)?;
    let (kind, policy) = match &finalizer.body {
        crate::execution::workflow::validated::ValidatedStep::Command(command) => {
            (WorkflowRunStepKind::Command, command.common.failure_policy)
        }
        crate::execution::workflow::validated::ValidatedStep::Agent(agent) => {
            (WorkflowRunStepKind::Agent, agent.common.failure_policy)
        }
    };
    Some((kind, policy))
}

fn finalizer_failure_policy(
    workflow: &ResolvedWorkflow,
    id: &str,
) -> Option<crate::execution::workflow::document::FailurePolicy> {
    finalizer_kind_and_policy(workflow, id).map(|(_, policy)| policy)
}

fn invalid_terminal_result_error() -> anyhow::Error {
    anyhow!("prepare authoritative local workflow terminal result")
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

pub(super) fn diagnose(error: impl std::fmt::Display) -> super::super::CommandResult {
    Err(anyhow!(error.to_string()).into())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::ready;
    use std::io::Write;
    use std::process::{Command as ProcessCommand, Stdio};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use rustix::fs::{FlockOperation, fcntl_lock};
    use time::format_description::well_known::Rfc3339;

    use super::*;
    use crate::execution::workflow::observation::TransitionObservation;
    use crate::execution::workflow::resolution::resolve;
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
                workflow_file: PathBuf::from("workflow.yaml"),
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
                role: crate::execution::workflow::validated::WorkflowNodeRole::Step,
                failure_policy: crate::execution::workflow::document::FailurePolicy::Required,
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
