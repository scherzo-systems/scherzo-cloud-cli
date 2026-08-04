use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, Read, Write};
use std::ops::Add;
use std::os::fd::AsFd as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use clap::{Args, ValueEnum};
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use time::OffsetDateTime;
use tokio::io::unix::AsyncFd;

use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, CaptureLimits, EnvironmentSnapshot,
    ExecutionContext, ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits,
    ResolvedAttachment, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::artifact::ArtifactStaging;
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::execution::{WorkflowExecutionResult, execute_workflow};
use crate::execution::workflow::input::InputStaging;
use crate::execution::workflow::observation::{ExecutionObservation, ExecutionObserver};
use crate::execution::workflow::presentation::{
    ColorChoice, DisplayDeadline, PresentationConfig, PublicationPresentation,
    RequestedPresentationMode, SystemObservationClock, TerminalCapabilities, WorkflowRunOutput,
    WorkflowRunPresentation, WorkflowRunPresentationResult,
};
use crate::execution::workflow::publication::{
    WorkflowRunCancellation, WorkflowRunResult, WorkflowRunStep, WorkflowRunTiming,
    WorkflowStepTiming, prepare_result_destination, publish_prepared_workflow_result,
};
use crate::execution::workflow::resolution::{ResolvedWorkflow, resolve};
use crate::execution::workflow::runtime::{
    RunOutcome, StepStateKind, TransitionEvent, WorkflowState,
};

pub(super) const ABOUT: &str = "Execute a local command-only Workflow V1 bundle";

const MAXIMUM_PARALLEL_STEPS: usize = 256;
const MAXIMUM_PROMPT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_ATTACHMENTS: usize = 256;
const MAXIMUM_ATTACHMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_ATTACHMENT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_CAPTURED_FILES: usize = 1024;
const MAXIMUM_CAPTURED_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_CAPTURED_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_INPUT_VALUES: usize = 1024;
const MAXIMUM_INPUT_VALUE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_LIVE_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_STEP_LOG_BYTES: u64 = 64 * 1024;
const CANCELLATION_GRACE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ColorArgument {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    source: super::LocalWorkflowSource,

    #[arg(
        long,
        value_name = "PATH",
        help = "Existing caller-owned workflow execution directory"
    )]
    execution_root: PathBuf,

    #[arg(
        long,
        value_name = "PATH",
        help = "Nonexistent directory to receive the terminal result"
    )]
    result_dir: PathBuf,

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
        help = "Maximum simultaneous command steps"
    )]
    max_parallel: usize,

    #[arg(long, conflicts_with = "json", help = "Force plain human presentation")]
    plain: bool,

    #[arg(long, help = "Print one terminal schema-version-1 JSON result")]
    json: bool,

    #[arg(
        long,
        value_enum,
        value_name = "auto|always|never",
        default_value_t = ColorArgument::Auto,
        help = "Select renderer color behavior"
    )]
    color: ColorArgument,
}

impl Command {
    pub(super) fn execute(self) -> ExitCode {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                write_diagnostic(format_args!("start local workflow runtime: {error}"));
                return ExitCode::FAILURE;
            }
        };
        runtime.block_on(self.execute_async())
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
        let context =
            execution_context(self.execution_root, self.max_parallel, cancellation.clone());
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
        let destination = match prepare_result_destination(&self.result_dir) {
            Ok(destination) => destination,
            Err(error) => {
                signal_task.abort();
                return diagnose(error);
            }
        };
        let private_staging = match create_private_staging(admitted.execution().root()) {
            Ok(staging) => staging,
            Err(error) => {
                signal_task.abort();
                return diagnose(error);
            }
        };
        let artifacts = match ArtifactStaging::create(admitted.execution(), private_staging.path())
        {
            Ok(artifacts) => artifacts,
            Err(error) => {
                signal_task.abort();
                return diagnose(error);
            }
        };
        let inputs = match InputStaging::create(admitted.execution(), private_staging.path()) {
            Ok(inputs) => inputs,
            Err(error) => {
                signal_task.abort();
                let _ = artifacts.release();
                return diagnose(error);
            }
        };

        let output = WorkflowRunOutput::new(presentation_config, io::stdout(), io::stderr());
        let presentation = match output.start_for_result(
            &workflow,
            destination.result_directory(),
            admitted.execution().limits().maximum_parallel_steps().get(),
            SystemObservationClock,
        ) {
            Ok(presentation) => presentation,
            Err(error) => {
                signal_task.abort();
                let _ = inputs.release();
                let _ = artifacts.release();
                return diagnose(error);
            }
        };

        let run_clock = SystemRunClock;
        let started = run_clock.sample();
        let timing = TimingObserver::new(presentation.clone(), cancellation.clone(), run_clock);
        let diagnostics = StepDiagnosticLog::default();
        let execution = execute_workflow(
            admitted.clone(),
            &artifacts,
            &inputs,
            &diagnostics,
            SystemExecutionClock,
            timing.clone(),
        )
        .await;
        signal_task.abort();

        let execution = match execution {
            Ok(execution) => execution,
            Err(error) => {
                let _ = inputs.release();
                let _ = artifacts.release();
                let _ = error;
                return diagnose(LocalRunError::Coordination);
            }
        };
        let observed_timing = timing.snapshot();
        let run_timing = match observed_timing.run_timing(started) {
            Ok(timing) => timing,
            Err(error) => {
                let _ = inputs.release();
                let _ = artifacts.release();
                return diagnose(error);
            }
        };
        let run = match build_run_result(
            &workflow,
            &admitted,
            &diagnostics,
            execution,
            observed_timing,
            run_timing,
        ) {
            Ok(run) => run,
            Err(error) => {
                let _ = inputs.release();
                let _ = artifacts.release();
                return diagnose(error);
            }
        };

        let publication = publish_prepared_workflow_result(&destination, &artifacts, &run);
        let input_release = inputs.release();
        let artifact_release = artifacts.release();
        if input_release.is_err() || artifact_release.is_err() {
            let path = publication
                .as_ref()
                .ok()
                .map(|terminal| terminal.result_directory());
            match &publication {
                Ok(terminal) => {
                    let _ = presentation.finish_without_terminal_json(
                        &run,
                        PublicationPresentation::Published(terminal),
                    );
                }
                Err(error) => {
                    let _ = presentation
                        .finish_without_terminal_json(&run, PublicationPresentation::Failed(error));
                }
            }
            if let Some(path) = path {
                write_diagnostic(format_args!(
                    "release private workflow staging; result published at {path}"
                ));
            } else {
                write_diagnostic(format_args!("release private workflow staging"));
            }
            return ExitCode::FAILURE;
        }

        let presented = match &publication {
            Ok(terminal) => presentation.finish(&run, PublicationPresentation::Published(terminal)),
            Err(error) => presentation.finish(&run, PublicationPresentation::Failed(error)),
        };
        presentation_exit_code(presented)
    }

    fn presentation_config(&self) -> PresentationConfig {
        self.presentation_config_with(TerminalCapabilities::detect())
    }

    fn presentation_config_with(&self, capabilities: TerminalCapabilities) -> PresentationConfig {
        PresentationConfig {
            requested_mode: if self.json {
                RequestedPresentationMode::Json
            } else if self.plain {
                RequestedPresentationMode::Plain
            } else {
                RequestedPresentationMode::Automatic
            },
            color: match self.color {
                ColorArgument::Auto => ColorChoice::Auto,
                ColorArgument::Always => ColorChoice::Always,
                ColorArgument::Never => ColorChoice::Never,
            },
            capabilities,
        }
    }
}

fn rejection_output(
    config: PresentationConfig,
    render: impl FnOnce(WorkflowRunOutput<io::Stdout, io::Stderr>) -> WorkflowRunPresentationResult,
) -> ExitCode {
    let result = render(WorkflowRunOutput::new(config, io::stdout(), io::stderr()));
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

fn execution_context(
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
) -> ExecutionContext {
    ExecutionContext::new(
        root,
        ExecutionRootLifecycle::CallerOwnedRetained,
        ExecutionPolicyLimits::new(
            maximum_parallel_steps,
            CaptureLimits::new(
                MAXIMUM_CAPTURED_FILES,
                MAXIMUM_CAPTURED_FILE_BYTES,
                MAXIMUM_TOTAL_CAPTURED_BYTES,
            ),
            InputLimits::new(
                MAXIMUM_INPUT_VALUES,
                MAXIMUM_INPUT_VALUE_BYTES,
                MAXIMUM_TOTAL_INPUT_BYTES,
                MAXIMUM_LIVE_INPUT_BYTES,
            ),
            MAXIMUM_STEP_LOG_BYTES,
        ),
        EnvironmentSnapshot::new(env::vars_os()),
        CancellationPolicy::new(cancellation, CANCELLATION_GRACE),
    )
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

fn create_private_staging(execution_root: &Path) -> Result<tempfile::TempDir, LocalRunError> {
    let staging = tempfile::Builder::new()
        .prefix(".scherzo-workflow-")
        .tempdir()
        .map_err(|_| LocalRunError::PrivateStaging)?;
    let canonical = fs::canonicalize(staging.path()).map_err(|_| LocalRunError::PrivateStaging)?;
    if !canonical.starts_with(execution_root) {
        return Ok(staging);
    }
    drop(staging);
    let parent = execution_root
        .parent()
        .ok_or(LocalRunError::PrivateStaging)?;
    tempfile::Builder::new()
        .prefix(".scherzo-workflow-")
        .tempdir_in(parent)
        .map_err(|_| LocalRunError::PrivateStaging)
}

fn start_signal_observation(
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

#[derive(Clone, Copy)]
struct SystemRunClock;

impl SystemRunClock {
    fn sample(self) -> TimingPoint {
        TimingPoint {
            wall: crate::timing::utc_now(),
            monotonic: crate::timing::monotonic_now(),
        }
    }
}

#[derive(Clone, Copy)]
struct SystemExecutionClock;

impl CoordinatorClock for SystemExecutionClock {
    type Instant = ExecutionInstant;

    fn now(&mut self) -> Self::Instant {
        let point = SystemRunClock.sample();
        ExecutionInstant {
            monotonic: point.monotonic,
            utc: point.wall,
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

trait RunTimingClock: Clone + Send + Sync + 'static {
    fn sample(&self) -> TimingPoint;
}

impl RunTimingClock for SystemRunClock {
    fn sample(&self) -> TimingPoint {
        (*self).sample()
    }
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
    timing: Arc<Mutex<ObservedTiming>>,
    clock: Clock,
}

#[derive(Clone, Copy, Debug)]
struct TimingPoint {
    wall: OffsetDateTime,
    monotonic: Instant,
}

#[derive(Clone, Copy)]
struct ObservedStepTiming {
    started: TimingPoint,
    finished: Option<Instant>,
}

#[derive(Clone, Default)]
struct ObservedTiming {
    steps: BTreeMap<String, ObservedStepTiming>,
    cancellation: Option<(CancellationReason, ExecutionInstant)>,
    terminal: Option<TimingPoint>,
}

impl ObservedTiming {
    fn run_timing(&self, started: TimingPoint) -> Result<WorkflowRunTiming, LocalRunError> {
        let finished = self.terminal.ok_or(LocalRunError::InvalidTerminalResult)?;
        Ok(WorkflowRunTiming {
            started_at: started.wall,
            finished_at: finished.wall,
            duration: finished
                .monotonic
                .saturating_duration_since(started.monotonic),
        })
    }
}

impl<Presentation, Clock> TimingObserver<Presentation, Clock>
where
    Clock: RunTimingClock,
{
    fn new(presentation: Presentation, cancellation: CancellationSource, clock: Clock) -> Self {
        Self {
            presentation,
            cancellation,
            timing: Arc::new(Mutex::new(ObservedTiming::default())),
            clock,
        }
    }

    fn snapshot(&self) -> ObservedTiming {
        lock_timing(&self.timing).clone()
    }

    fn record(&self, observation: &ExecutionObservation<ExecutionInstant>) {
        let ExecutionObservation::Transition(transition) = observation else {
            return;
        };
        match &transition.event {
            TransitionEvent::Step { step, to, .. } if *to == StepStateKind::Starting => {
                let point = self.clock.sample();
                lock_timing(&self.timing)
                    .steps
                    .entry(step.clone())
                    .or_insert(ObservedStepTiming {
                        started: point,
                        finished: None,
                    });
            }
            TransitionEvent::Step {
                step,
                to:
                    StepStateKind::Succeeded
                    | StepStateKind::Failed
                    | StepStateKind::Blocked
                    | StepStateKind::NotRun
                    | StepStateKind::Cancelled,
                ..
            } => {
                let mut timing = lock_timing(&self.timing);
                if let Some(step) = timing.steps.get_mut(step) {
                    step.finished = Some(self.clock.sample().monotonic);
                }
            }
            TransitionEvent::CancellationAccepted {
                reason, deadline, ..
            } => {
                lock_timing(&self.timing)
                    .cancellation
                    .get_or_insert((*reason, *deadline));
            }
            TransitionEvent::Workflow { to, .. }
                if !matches!(to, WorkflowState::Executing { .. }) =>
            {
                let point = self.clock.sample();
                lock_timing(&self.timing).terminal.get_or_insert(point);
            }
            TransitionEvent::Step { .. } | TransitionEvent::Workflow { .. } => {}
        }
    }
}

impl<Presentation, Clock> ExecutionObserver<ExecutionInstant>
    for TimingObserver<Presentation, Clock>
where
    Presentation: ExecutionObserver<ExecutionInstant> + PresentationFailureState,
    Clock: RunTimingClock,
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

fn lock_timing(timing: &Mutex<ObservedTiming>) -> MutexGuard<'_, ObservedTiming> {
    timing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn build_run_result(
    workflow: &ResolvedWorkflow,
    admitted: &crate::execution::workflow::admission::AdmittedWorkflow,
    diagnostics: &StepDiagnosticLog,
    execution: WorkflowExecutionResult,
    timing: ObservedTiming,
    run_timing: WorkflowRunTiming,
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
                force_stop_deadline: deadline.utc,
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
                    started_at: timing.started.wall,
                    duration: finished.saturating_duration_since(timing.started.monotonic),
                })
            }
        };
        steps.push(WorkflowRunStep {
            id: id.clone(),
            state,
            timing,
            command_output: diagnostics.get(id),
        });
    }
    if !states.is_empty() {
        return Err(LocalRunError::InvalidTerminalResult);
    }
    Ok(WorkflowRunResult {
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
    })
}

#[derive(Clone, Copy, Debug)]
enum ImportFailureKind {
    Unavailable,
    NotRegularFile,
    Interrupted,
    Read,
    TooLarge,
    InvalidUtf8,
}

#[derive(Debug)]
enum LocalRunError {
    Import {
        kind: ImportFailureKind,
        path: Option<PathBuf>,
    },
    AttachmentCount,
    AttachmentBytes,
    AttachmentMediaTypeEncoding,
    UnrepresentablePath,
    PrivateStaging,
    SignalObservation,
    Coordination,
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
            Self::SignalObservation => {
                formatter.write_str("install local workflow signal observation")
            }
            Self::Coordination => formatter.write_str("execute admitted local workflow"),
            Self::InvalidTerminalResult => {
                formatter.write_str("prepare authoritative local workflow terminal result")
            }
        }
    }
}

impl std::error::Error for LocalRunError {}

fn diagnose(error: impl fmt::Display) -> ExitCode {
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use time::format_description::well_known::Rfc3339;

    use super::*;
    use crate::execution::workflow::observation::TransitionObservation;
    use crate::execution::workflow::runtime::{SchedulingGate, TransitionSequence};

    #[derive(Clone)]
    struct ScriptedClock {
        points: Arc<Mutex<VecDeque<TimingPoint>>>,
    }

    impl ScriptedClock {
        fn new(points: impl IntoIterator<Item = TimingPoint>) -> Self {
            Self {
                points: Arc::new(Mutex::new(points.into_iter().collect())),
            }
        }
    }

    impl RunTimingClock for ScriptedClock {
        fn sample(&self) -> TimingPoint {
            self.points.lock().unwrap().pop_front().unwrap()
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
            execution_root: PathBuf::from("execution"),
            result_dir: PathBuf::from("result"),
            prompt_file: None,
            attachment: Vec::new(),
            max_parallel: 2,
            plain: false,
            json: true,
            color: ColorArgument::Always,
        };

        assert_eq!(
            command.presentation_config_with(capabilities.clone()),
            PresentationConfig {
                requested_mode: RequestedPresentationMode::Json,
                color: ColorChoice::Always,
                capabilities,
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

    #[tokio::test]
    async fn timing_observer_forwards_once_and_uses_the_terminal_transition() {
        let monotonic = crate::timing::monotonic_now();
        let started = timing_point(monotonic, "2026-08-02T12:01:44Z", 0);
        let step_started = timing_point(monotonic, "2026-08-02T12:01:44.01Z", 10);
        let step_finished = timing_point(monotonic, "2026-08-02T12:01:44.02Z", 20);
        let terminal = timing_point(monotonic, "2026-08-02T12:01:44.03Z", 30);
        let observations = Arc::new(AtomicUsize::new(0));
        let cancellation = CancellationSource::new();
        let observer = TimingObserver::new(
            RecordingPresentation {
                observations: observations.clone(),
                failed: false,
            },
            cancellation,
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
        assert_eq!(step.started.wall, step_started.wall);
        assert_eq!(step.finished, Some(step_finished.monotonic));
        let run = timing.run_timing(started).unwrap();
        assert_eq!(run.started_at, started.wall);
        assert_eq!(run.finished_at, terminal.wall);
        assert_eq!(run.duration, Duration::from_millis(30));
    }

    #[tokio::test]
    async fn presentation_failure_requests_cancellation_without_replacing_a_signal() {
        let monotonic = crate::timing::monotonic_now();
        let cancellation = CancellationSource::new();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        let observer = TimingObserver::new(
            RecordingPresentation {
                observations: Arc::new(AtomicUsize::new(0)),
                failed: true,
            },
            cancellation.clone(),
            ScriptedClock::new([timing_point(monotonic, "2026-08-02T12:01:44Z", 0)]),
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

    fn timing_point(monotonic: Instant, wall: &str, milliseconds: u64) -> TimingPoint {
        TimingPoint {
            wall: OffsetDateTime::parse(wall, &Rfc3339).unwrap(),
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
