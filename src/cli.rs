macro_rules! impl_organization_human_credential_outcome {
    ($($outcome:ty),+ $(,)?) => {
        $(
            impl super::HumanCredentialOutcome for $outcome {
                type Error = crate::api::OrganizationError;

                fn unauthenticated() -> Self {
                    Self::Common(crate::api::CommonOrganizationFailure::Unauthenticated)
                }

                fn unreachable(category: crate::api::UnreachableCategory) -> Self {
                    Self::Common(crate::api::CommonOrganizationFailure::Unreachable(category))
                }

                fn is_unauthenticated(&self) -> bool {
                    matches!(
                        self,
                        Self::Common(crate::api::CommonOrganizationFailure::Unauthenticated)
                    )
                }

                fn credential_rejected(error: &Self::Error) -> bool {
                    error.credential_rejected()
                }
            }
        )+
    };
}
pub(super) use impl_organization_human_credential_outcome;

mod account;
mod artifact;
mod atomic_directory;
mod auth;
mod deletion;
mod github;
mod invitation;
mod organization;
mod principal;
mod project;
mod publication;
mod run;
mod runner;
mod service_principal;
mod version;
mod workflow;

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::ops::Deref;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::{Args, CommandFactory, Parser, Subcommand};
use serde::Serialize;

use crate::api::{
    HttpClient, HttpTransportPolicy, MembershipRole, MembershipState, UnreachableCategory,
};
use crate::exit_code::{ExitCode, OutcomeClass};
use crate::human_auth::cancellation::Cancellation;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::session::{self, RequiredOperation};
use crate::service_auth::{ServiceApiKey, read_api_key};

pub(crate) type CommandResult = Result<ExitCode, CommandFailure>;

#[derive(Debug, Args)]
struct NamedInputArgs {
    #[arg(
        long,
        value_names = ["NAME", "TEXT"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help = "Supply one required named Text value"
    )]
    input_text: Vec<OsString>,

    #[arg(
        long,
        value_names = ["NAME", "PATH"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help = "Supply one required named Text value from a regular file, or - for standard input"
    )]
    input_text_file: Vec<OsString>,

    #[arg(
        long,
        value_names = ["NAME", "JSON"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help = "Supply one required named JSON value"
    )]
    input_json: Vec<OsString>,

    #[arg(
        long,
        value_names = ["NAME", "PATH"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help = "Supply one required named JSON value from a regular file, or - for standard input"
    )]
    input_json_file: Vec<OsString>,

    #[arg(
        long,
        value_names = ["NAME", "MEDIA_TYPE", "PATH"],
        num_args = 3,
        action = clap::ArgAction::Append,
        help = "Supply one required named File value with an explicit media type (maximum 64 MiB)"
    )]
    input_file: Vec<OsString>,

    #[arg(
        long,
        value_names = ["NAME", "MEDIA_TYPE", "PATH"],
        num_args = 3,
        action = clap::ArgAction::Append,
        help = "Append an immutable member to a named attachment collection"
    )]
    input_attachment: Vec<OsString>,

    #[arg(
        long,
        value_name = "NAME",
        action = clap::ArgAction::Append,
        help = "Supply a present named attachment collection with no members"
    )]
    input_attachments_empty: Vec<String>,
}

impl NamedInputArgs {
    fn is_empty(&self) -> bool {
        self.input_text.is_empty()
            && self.input_text_file.is_empty()
            && self.input_json.is_empty()
            && self.input_json_file.is_empty()
            && self.input_file.is_empty()
            && self.input_attachment.is_empty()
            && self.input_attachments_empty.is_empty()
    }
}

#[derive(Debug)]
enum OpenRegularFileError {
    Open(io::Error),
    Metadata(io::Error),
    NotRegular,
}

fn open_regular_file_nonblocking(path: &Path) -> Result<File, OpenRegularFileError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(OpenRegularFileError::Open)?;
    if !file
        .metadata()
        .map_err(OpenRegularFileError::Metadata)?
        .is_file()
    {
        return Err(OpenRegularFileError::NotRegular);
    }
    Ok(file)
}

#[derive(Clone, Debug)]
struct OrganizationRef(String);

impl FromStr for OrganizationRef {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if crate::public_id::valid_organization_ref(value) {
            Ok(Self(value.to_owned()))
        } else {
            Err("must be an organization ID or lowercase organization slug".to_owned())
        }
    }
}

impl Deref for OrganizationRef {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug)]
struct ContinuationCursor(String);

impl FromStr for ContinuationCursor {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            Err("must be a nonempty opaque cursor".to_owned())
        } else {
            Ok(Self(value.to_owned()))
        }
    }
}

impl Deref for ContinuationCursor {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct CommandFailure {
    error: anyhow::Error,
    exit_code: ExitCode,
}

impl CommandFailure {
    pub(crate) fn new(error: anyhow::Error) -> Self {
        Self {
            error,
            exit_code: ExitCode::GeneralFailure,
        }
    }

    pub(crate) fn with_exit_code(error: anyhow::Error, exit_code: ExitCode) -> Self {
        Self { error, exit_code }
    }

    pub(crate) fn for_outcome(error: anyhow::Error, outcome: OutcomeClass) -> Self {
        Self::with_exit_code(error, outcome.exit_code())
    }

    pub(crate) fn error(&self) -> &anyhow::Error {
        &self.error
    }

    pub(crate) fn exit_code(&self) -> ExitCode {
        self.exit_code
    }
}

impl From<anyhow::Error> for CommandFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::new(error)
    }
}

const AFTER_HELP: &str =
    "Documentation:\n  Public API contract: https://docs.scherzo.dev/openapi/public-api.yaml";

#[derive(Debug, Parser)]
#[command(
    name = "scherzo-cloud",
    about = "Scherzo Cloud CLI",
    version = crate::build_info::VERSION,
    after_help = AFTER_HELP
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Args)]
struct HttpOptions {
    #[arg(
        long,
        help = "Allow this command's Scherzo Cloud requests over insecure HTTP connections"
    )]
    allow_insecure_http: bool,
}

impl HttpOptions {
    fn transport_policy(&self) -> HttpTransportPolicy {
        if self.allow_insecure_http {
            HttpTransportPolicy::AllowInsecureHttp
        } else {
            HttpTransportPolicy::HttpsOnly
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrincipalAuthenticationKind {
    HumanSession,
    ServiceApiKey,
}

impl PrincipalAuthenticationKind {
    const fn rejected_error(self, human_session: &'static str) -> &'static str {
        match self {
            Self::HumanSession => human_session,
            Self::ServiceApiKey => {
                "error: service API key rejected\n\nUse a different active service API key."
            }
        }
    }

    const fn rejected_remedy(self, human_session: &'static str) -> &'static str {
        match self {
            Self::HumanSession => human_session,
            Self::ServiceApiKey => "Use a different active service API key.",
        }
    }

    const fn rejected_notice(self, human_session: &'static str) -> &'static str {
        match self {
            Self::HumanSession => human_session,
            Self::ServiceApiKey => {
                "! The service API key was rejected.\n\nUse a different active service API key."
            }
        }
    }
}

#[derive(Debug, Args, Default)]
struct PrincipalAuthenticationArgs {
    #[arg(
        long,
        value_name = "PATH|-",
        help = "Authenticate with a service API key from a private file, or - for standard input"
    )]
    service_api_key_file: Option<PathBuf>,

    #[arg(skip)]
    resolved_service_api_key: Mutex<Option<Arc<ServiceApiKey>>>,
}

impl PrincipalAuthenticationArgs {
    const fn kind(&self) -> PrincipalAuthenticationKind {
        if self.service_api_key_file.is_some() {
            PrincipalAuthenticationKind::ServiceApiKey
        } else {
            PrincipalAuthenticationKind::HumanSession
        }
    }

    fn service_api_key(&self) -> anyhow::Result<Option<Arc<ServiceApiKey>>> {
        let Some(path) = self.service_api_key_file.as_deref() else {
            return Ok(None);
        };
        let mut resolved = self
            .resolved_service_api_key
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(api_key) = resolved.as_ref() {
            return Ok(Some(Arc::clone(api_key)));
        }
        let api_key = Arc::new(read_api_key(path).context("read service API key")?);
        *resolved = Some(Arc::clone(&api_key));
        Ok(Some(api_key))
    }

    fn required_service_api_key(&self) -> anyhow::Result<Arc<ServiceApiKey>> {
        self.service_api_key()?.ok_or_else(|| {
            anyhow!("--service-api-key-file is required for service credential management")
        })
    }

    fn uses_stdin(&self) -> bool {
        self.service_api_key_file.as_deref() == Some(Path::new("-"))
    }
}

#[derive(Debug, Args)]
struct WaitTimeoutArgs {
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_wait_timeout,
        help = "Stop waiting after a positive duration (units: ms, s, m, or h)"
    )]
    timeout: Option<Duration>,
}

#[derive(Debug, Args)]
struct PaginationArgs {
    #[arg(
        long,
        value_parser = clap::value_parser!(u16).range(1..=200),
        help = "Maximum items to return (1-200)"
    )]
    limit: Option<u16>,

    #[arg(long, help = "Opaque continuation cursor")]
    cursor: Option<ContinuationCursor>,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = account::ABOUT)]
    Account(account::Command),
    #[command(about = artifact::ABOUT)]
    Artifact(artifact::Command),
    #[command(about = auth::ABOUT)]
    Auth(auth::Command),
    #[command(about = github::ABOUT)]
    Github(github::Command),
    #[command(about = invitation::ABOUT)]
    Invitation(invitation::Command),
    #[command(about = organization::ABOUT)]
    Organization(organization::Command),
    #[command(about = project::ABOUT)]
    Project(project::Command),
    #[command(about = publication::ABOUT)]
    Publication(publication::Command),
    #[command(about = run::ABOUT)]
    Run(run::Command),
    #[command(about = version::ABOUT)]
    Version(version::Command),
    #[command(about = runner::ABOUT)]
    Runner(runner::Command),
    #[command(about = service_principal::ABOUT)]
    ServicePrincipal(service_principal::Command),
    #[command(about = workflow::ABOUT)]
    Workflow(workflow::Command),
}

pub(crate) fn parse<I, S>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString> + Clone,
{
    Cli::try_parse_from(args)
}

impl Cli {
    pub(crate) fn execute(self) -> CommandResult {
        match self.command {
            None => print_help(&[]),
            Some(Command::Account(command)) => command.execute(),
            Some(Command::Artifact(command)) => command.execute(),
            Some(Command::Auth(command)) => command.execute(),
            Some(Command::Github(command)) => command.execute(),
            Some(Command::Invitation(command)) => command.execute(),
            Some(Command::Organization(command)) => command.execute(),
            Some(Command::Project(command)) => command.execute(),
            Some(Command::Publication(command)) => command.execute(),
            Some(Command::Run(command)) => command.execute(),
            Some(Command::Version(command)) => command.execute(),
            Some(Command::Runner(command)) => command.execute(),
            Some(Command::ServicePrincipal(command)) => command.execute(),
            Some(Command::Workflow(command)) => command.execute(),
        }
    }
}

pub(crate) const fn unreachable_outcome_class(
    category: crate::api::UnreachableCategory,
) -> OutcomeClass {
    match category {
        crate::api::UnreachableCategory::RateLimited => OutcomeClass::RateLimited,
        crate::api::UnreachableCategory::Dns
        | crate::api::UnreachableCategory::Timeout
        | crate::api::UnreachableCategory::Connection
        | crate::api::UnreachableCategory::Tls
        | crate::api::UnreachableCategory::Server => OutcomeClass::Unreachable,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiFailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloudListResult<'a, T> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [T],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObservationResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    publication_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
}

impl<'a> ApiFailureResult<'a> {
    const fn new(
        deployment: &'a str,
        outcome: &'static str,
        category: Option<&'static str>,
    ) -> Self {
        Self {
            schema_version: 1,
            deployment,
            outcome,
            category,
            retry_after: None,
        }
    }

    const fn with_retry_after(
        deployment: &'a str,
        outcome: &'static str,
        category: Option<&'static str>,
        retry_after: Option<u64>,
    ) -> Self {
        Self {
            schema_version: 1,
            deployment,
            outcome,
            category,
            retry_after,
        }
    }
}

fn write_pretty_json(value: &impl Serialize) -> io::Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout.write_all(&bytes)?;
    stdout.flush()
}

fn write_api_failure(
    deployment: &str,
    outcome: &'static str,
    category: Option<&'static str>,
    retry_after: Option<u64>,
    human: &str,
    outcome_class: OutcomeClass,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_pretty_json(&ApiFailureResult::with_retry_after(
            deployment,
            outcome,
            category,
            retry_after,
        ))?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(outcome_class.exit_code())
}

fn write_cloud_list_json(
    deployment: &str,
    items: &[impl Serialize],
    next_cursor: Option<&str>,
) -> io::Result<()> {
    write_pretty_json(&CloudListResult {
        schema_version: 1,
        deployment,
        outcome: "listed",
        items,
        next_cursor,
    })
}

fn write_cloud_failure_json(
    deployment: &str,
    outcome: &'static str,
    category: Option<&'static str>,
    retry_after: Option<u64>,
) -> io::Result<()> {
    write_pretty_json(&ApiFailureResult::with_retry_after(
        deployment,
        outcome,
        category,
        retry_after,
    ))
}

fn write_page_footer(
    output: &mut impl Write,
    deployment: &str,
    next_cursor: Option<&str>,
) -> io::Result<()> {
    if let Some(next_cursor) = next_cursor {
        writeln!(output, "next cursor: {next_cursor}")?;
    }
    writeln!(output, "deployment: {deployment}")
}

const fn membership_role(role: MembershipRole) -> &'static str {
    match role {
        MembershipRole::Owner => "owner",
        MembershipRole::Member => "member",
    }
}

const fn membership_state(state: MembershipState) -> &'static str {
    match state {
        MembershipState::Active => "active",
        MembershipState::Suspended => "suspended",
        MembershipState::Ended => "ended",
    }
}

struct ProcessSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl ProcessSignals {
    fn install(context: &str) -> anyhow::Result<Self> {
        (|| -> io::Result<_> {
            Ok(Self {
                interrupt: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::interrupt(),
                )?,
                terminate: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?,
            })
        })()
        .with_context(|| format!("install {context} signal observation"))
    }

    async fn recv(&mut self) -> ExitCode {
        tokio::select! {
            biased;
            _ = self.interrupt.recv() => OutcomeClass::Interrupted.exit_code(),
            _ = self.terminate.recv() => OutcomeClass::Terminated.exit_code(),
        }
    }
}

fn execute_read_only_with_signals(
    context: &'static str,
    operation: impl FnOnce(&OperationControl<()>) -> CommandResult + Send + 'static,
) -> CommandResult {
    execute_mutation_with_signals(context, (), operation, |signal, _| Ok(signal))
}

fn execute_cancellable_with_signals(
    context: &'static str,
    operation: impl FnOnce(&Cancellation) -> CommandResult + Send + 'static,
) -> CommandResult {
    run_blocking_signal_runtime(context, async move {
        let mut signals = ProcessSignals::install(context)?;
        let cancellation = Cancellation::new();
        let operation_cancellation = cancellation.clone();
        let mut running = tokio::task::spawn_blocking(move || operation(&operation_cancellation));
        tokio::select! {
            biased;
            signal = signals.recv() => {
                cancellation.cancel();
                let result = finish_read_only_operation(context, running.await);
                match result {
                    Ok(ExitCode::Interrupted) => Ok(signal),
                    result => result,
                }
            }
            result = &mut running => finish_read_only_operation(context, result),
        }
    })
}

fn execute_cancellable_mutation_with_signals(
    context: &'static str,
    operation: impl FnOnce(&crate::api::HttpCancellation, &OperationControl<()>) -> CommandResult
    + Send
    + 'static,
    interrupt_operation: impl FnOnce() -> bool + 'static,
    incomplete_signal: impl FnOnce(ExitCode, bool) -> CommandResult + 'static,
) -> CommandResult {
    run_blocking_signal_runtime(context, async move {
        let mut signals = ProcessSignals::install(context)?;
        let cancellation = crate::api::HttpCancellation::new();
        let control = Arc::new(OperationControl::new(()));
        let operation_cancellation = cancellation.clone();
        let operation_control = Arc::clone(&control);
        let mut running = tokio::task::spawn_blocking(move || {
            operation(&operation_cancellation, &operation_control)
        });
        tokio::select! {
            biased;
            signal = signals.recv() => match control.claim_signal() {
                Some(_) => {
                    cancellation.cancel();
                    let commitment_unknown = interrupt_operation();
                    finish_read_only_operation(context, running.await)?;
                    incomplete_signal(signal, commitment_unknown)
                }
                None => finish_read_only_operation(context, running.await),
            },
            result = &mut running => finish_read_only_operation(context, result),
        }
    })
}

fn execute_bounded_mutation_with_signals(
    context: &'static str,
    operation: impl FnOnce(&OperationControl<()>) -> CommandResult + Send + 'static,
) -> CommandResult {
    execute_cancellable_mutation_with_signals(
        context,
        move |_, control| operation(control),
        || false,
        |signal, _| Ok(signal),
    )
}

// A terminal result and a local stop compete for one output claim so timeout/signal
// races cannot emit two machine documents or replace a terminal result after it wins.
const OBSERVATION_ACTIVE: u8 = 0;
const OBSERVATION_COMPLETING: u8 = 1;
const OBSERVATION_STOPPED: u8 = 2;

trait ObservationControl {
    fn is_stopped(&self) -> bool;
}

struct BlockingObservationControl {
    state: AtomicU8,
}

impl BlockingObservationControl {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(OBSERVATION_ACTIVE),
        }
    }

    fn is_stopped(&self) -> bool {
        self.state.load(Ordering::Acquire) == OBSERVATION_STOPPED
    }

    fn begin_completion(&self) -> bool {
        self.state
            .compare_exchange(
                OBSERVATION_ACTIVE,
                OBSERVATION_COMPLETING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn stop(&self) -> bool {
        self.state
            .compare_exchange(
                OBSERVATION_ACTIVE,
                OBSERVATION_STOPPED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

impl ObservationControl for BlockingObservationControl {
    fn is_stopped(&self) -> bool {
        self.is_stopped()
    }
}

struct ObservationTimeout {
    duration: Option<Duration>,
}

impl ObservationTimeout {
    async fn wait(self) {
        match self.duration {
            Some(duration) => crate::timing::async_sleep(duration).await,
            None => std::future::pending().await,
        }
    }
}

struct DeferredObservationTimeoutStart {
    sender: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl DeferredObservationTimeoutStart {
    fn start(&self) {
        if let Some(sender) = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = sender.send(());
        }
    }
}

struct DeferredObservationTimeout {
    duration: Option<Duration>,
    started: tokio::sync::oneshot::Receiver<()>,
}

impl DeferredObservationTimeout {
    async fn wait(self) {
        let Some(duration) = self.duration else {
            return std::future::pending().await;
        };
        if self.started.await.is_err() {
            return std::future::pending().await;
        }
        crate::timing::async_sleep(duration).await;
    }
}

const OBSERVATION_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAXIMUM_CONSECUTIVE_OBSERVATION_FAILURES: usize = 2;

trait ObservationClock {
    fn now(&self) -> Instant;
    fn sleep(&self, duration: Duration);
}

struct SystemObservationClock;

impl ObservationClock for SystemObservationClock {
    fn now(&self) -> Instant {
        crate::timing::monotonic_now()
    }

    fn sleep(&self, duration: Duration) {
        crate::timing::sleep(duration);
    }
}

#[cfg(test)]
mod observation_test_support {
    use std::cell::{Cell, RefCell};
    use std::time::{Duration, Instant};

    pub(super) struct ControlledObservationClock {
        now: Cell<Instant>,
        sleeps: RefCell<Vec<Duration>>,
    }

    impl ControlledObservationClock {
        pub(super) fn new(now: Instant) -> Self {
            Self {
                now: Cell::new(now),
                sleeps: RefCell::new(Vec::new()),
            }
        }

        pub(super) fn assert_timeout_schedule(self) {
            assert_eq!(
                self.sleeps.into_inner(),
                vec![
                    Duration::from_secs(2),
                    Duration::from_secs(2),
                    Duration::from_secs(1)
                ]
            );
        }

        pub(super) fn assert_single_poll(self) {
            assert_eq!(
                self.sleeps.into_inner(),
                vec![super::OBSERVATION_POLL_INTERVAL]
            );
        }

        pub(super) fn into_sleeps(self) -> Vec<Duration> {
            self.sleeps.into_inner()
        }
    }

    impl super::ObservationClock for ControlledObservationClock {
        fn now(&self) -> Instant {
            self.now.get()
        }

        fn sleep(&self, duration: Duration) {
            self.sleeps.borrow_mut().push(duration);
            self.now.set(self.now.get() + duration);
        }
    }
}

enum TerminalObservation<T, S> {
    Terminal { resource: Box<T>, state: S },
    TimedOut,
    Stopped,
}

fn wait_for_terminal_observation<T, S, E>(
    mut observe: impl FnMut() -> Result<T, E>,
    terminal_state: impl Fn(&T) -> Option<S>,
    retryable_failure: impl Fn(&E) -> bool,
    timeout: Option<Duration>,
    control: &impl ObservationControl,
    clock: &impl ObservationClock,
) -> Result<TerminalObservation<T, S>, E> {
    let started_at = clock.now();
    let mut consecutive_failures = 0;
    loop {
        if control.is_stopped() {
            return Ok(TerminalObservation::Stopped);
        }
        if remaining_observation_wait(timeout, started_at, clock.now()).is_none() {
            return Ok(TerminalObservation::TimedOut);
        }

        match observe() {
            Ok(resource) => {
                consecutive_failures = 0;
                if let Some(state) = terminal_state(&resource) {
                    return Ok(TerminalObservation::Terminal {
                        resource: Box::new(resource),
                        state,
                    });
                }
            }
            Err(failure)
                if retryable_failure(&failure)
                    && consecutive_failures + 1 < MAXIMUM_CONSECUTIVE_OBSERVATION_FAILURES =>
            {
                consecutive_failures += 1;
            }
            Err(failure) => return Err(failure),
        }

        if control.is_stopped() {
            return Ok(TerminalObservation::Stopped);
        }
        let Some(remaining) = remaining_observation_wait(timeout, started_at, clock.now()) else {
            return Ok(TerminalObservation::TimedOut);
        };
        clock.sleep(OBSERVATION_POLL_INTERVAL.min(remaining));
    }
}

fn remaining_observation_wait(
    timeout: Option<Duration>,
    started_at: Instant,
    now: Instant,
) -> Option<Duration> {
    match timeout {
        Some(timeout) => timeout
            .checked_sub(now.saturating_duration_since(started_at))
            .filter(|remaining| !remaining.is_zero()),
        None => Some(OBSERVATION_POLL_INTERVAL),
    }
}

fn parse_wait_timeout(value: &str) -> Result<Duration, String> {
    let (quantity, milliseconds) = if let Some(quantity) = value.strip_suffix("ms") {
        (quantity, 1)
    } else if let Some(quantity) = value.strip_suffix('s') {
        (quantity, 1_000)
    } else if let Some(quantity) = value.strip_suffix('m') {
        (quantity, 60_000)
    } else if let Some(quantity) = value.strip_suffix('h') {
        (quantity, 3_600_000)
    } else {
        (value, 1_000)
    };
    let quantity = quantity
        .parse::<u64>()
        .map_err(|_| "duration must be a positive integer followed by ms, s, m, or h".to_owned())?;
    let total_milliseconds = quantity
        .checked_mul(milliseconds)
        .filter(|duration| *duration > 0)
        .ok_or_else(|| {
            "duration must be a positive integer followed by ms, s, m, or h".to_owned()
        })?;
    Ok(Duration::from_millis(total_milliseconds))
}

fn blocking_signal_runtime(context: &str) -> anyhow::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .with_context(|| format!("start {context} runtime"))
}

fn run_blocking_signal_runtime(
    context: &str,
    operation: impl std::future::Future<Output = CommandResult>,
) -> CommandResult {
    let runtime = blocking_signal_runtime(context)?;
    let result = runtime.block_on(operation);
    runtime.shutdown_timeout(Duration::ZERO);
    result
}

fn spawn_controlled_blocking<C>(
    control: Arc<C>,
    operation: impl FnOnce(&C) -> CommandResult + Send + 'static,
) -> tokio::task::JoinHandle<CommandResult>
where
    C: Send + Sync + 'static,
{
    tokio::task::spawn_blocking(move || operation(&control))
}

async fn finish_stopped_operation(
    context: &str,
    running: &mut tokio::task::JoinHandle<CommandResult>,
    stopped: Option<CommandResult>,
) -> CommandResult {
    match stopped {
        Some(result) => result,
        None => finish_read_only_operation(context, running.await),
    }
}

fn execute_observation_with_signals_and_timeout(
    context: &'static str,
    timeout: Option<Duration>,
    operation: impl FnOnce(&BlockingObservationControl) -> CommandResult + Send + 'static,
    timed_out: impl FnOnce() -> CommandResult + 'static,
) -> CommandResult {
    run_blocking_signal_runtime(context, async move {
        let mut signals = ProcessSignals::install(context)?;
        let control = Arc::new(BlockingObservationControl::new());
        let mut running = spawn_controlled_blocking(Arc::clone(&control), operation);
        tokio::select! {
            biased;
            signal = signals.recv() => {
                let stopped = control.stop().then(|| Ok(signal));
                finish_stopped_operation(context, &mut running, stopped).await
            }
            () = (ObservationTimeout { duration: timeout }).wait() => {
                let stopped = control.stop().then(timed_out);
                finish_stopped_operation(context, &mut running, stopped).await
            }
            result = &mut running => finish_read_only_operation(context, result),
        }
    })
}

// Dispatch, recovery, and output ownership are linearized by one state lock. Mutations claim
// Completion before rendering so a signal cannot add a second receipt. A bounded one-time-secret
// mutation may claim Completion at dispatch and retain it through delivery. Read-only commands
// enter ReadOnlyOutput instead: a signal may abandon a blocked writer until output finishes and
// claims Completion. The lock is released before requests, joins, rendering, and callbacks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationOwner {
    Active,
    ReadOnlyOutput,
    Completion,
    Signal,
}

struct OperationState<R> {
    owner: OperationOwner,
    dispatched: bool,
    recovery: R,
}

struct OperationControl<R> {
    // A derived cooperative-stop notification for CPU-bound helpers. It is set only while
    // transitioning the authoritative state to Signal and never grants dispatch/output authority.
    cooperative_stop: AtomicBool,
    state: Mutex<OperationState<R>>,
}

impl<R> ObservationControl for OperationControl<R> {
    fn is_stopped(&self) -> bool {
        self.is_cancelled()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SignalSnapshot<R> {
    dispatched: bool,
    recovery: R,
}

fn report_dispatched_signal<R>(
    signal: ExitCode,
    snapshot: SignalSnapshot<R>,
    report: impl FnOnce(R) -> CommandResult,
) -> CommandResult {
    if snapshot.dispatched {
        report(snapshot.recovery)
    } else {
        Ok(signal)
    }
}

impl<R> OperationControl<R> {
    fn new(recovery: R) -> Self {
        Self {
            cooperative_stop: AtomicBool::new(false),
            state: Mutex::new(OperationState {
                owner: OperationOwner::Active,
                dispatched: false,
                recovery,
            }),
        }
    }

    fn cancellation(&self) -> &AtomicBool {
        &self.cooperative_stop
    }

    fn is_cancelled(&self) -> bool {
        self.cooperative_stop.load(Ordering::Acquire)
    }

    fn lock_owned_state(
        &self,
        expected_owner: OperationOwner,
    ) -> Option<std::sync::MutexGuard<'_, OperationState<R>>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.owner == expected_owner).then_some(state)
    }

    fn begin_dispatch(&self) -> bool {
        let Some(mut state) = self.lock_owned_state(OperationOwner::Active) else {
            return false;
        };
        state.dispatched = true;
        true
    }

    fn begin_bounded_dispatch(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.owner {
            OperationOwner::Active => {
                state.owner = OperationOwner::Completion;
                state.dispatched = true;
                true
            }
            OperationOwner::Completion if state.dispatched => true,
            OperationOwner::ReadOnlyOutput
            | OperationOwner::Completion
            | OperationOwner::Signal => false,
        }
    }

    fn dispatched(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .dispatched
    }

    fn begin_dispatch_with_recovery(&self, recovery: R) -> bool {
        let Some(mut state) = self.lock_owned_state(OperationOwner::Active) else {
            return false;
        };
        state.recovery = recovery;
        state.dispatched = true;
        true
    }

    fn update_recovery(&self, recovery: R) -> bool {
        let Some(mut state) = self.lock_owned_state(OperationOwner::Active) else {
            return false;
        };
        state.recovery = recovery;
        true
    }

    fn recovery(&self) -> R
    where
        R: Clone,
    {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovery
            .clone()
    }

    fn transition_owner(&self, from: OperationOwner, to: OperationOwner) -> bool {
        let Some(mut state) = self.lock_owned_state(from) else {
            return false;
        };
        state.owner = to;
        true
    }

    fn begin_completion(&self) -> bool {
        self.transition_owner(OperationOwner::Active, OperationOwner::Completion)
    }

    fn begin_read_only_output(&self) -> bool {
        self.transition_owner(OperationOwner::Active, OperationOwner::ReadOnlyOutput)
    }

    fn finish_read_only_output(&self) -> bool {
        self.transition_owner(OperationOwner::ReadOnlyOutput, OperationOwner::Completion)
    }

    fn claim_signal(&self) -> Option<SignalSnapshot<R>>
    where
        R: Clone,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            state.owner,
            OperationOwner::Active | OperationOwner::ReadOnlyOutput
        ) {
            return None;
        }
        state.owner = OperationOwner::Signal;
        self.cooperative_stop.store(true, Ordering::Release);
        Some(SignalSnapshot {
            dispatched: state.dispatched,
            recovery: state.recovery.clone(),
        })
    }
}

fn complete_operation<R>(
    control: &OperationControl<R>,
    output: impl FnOnce() -> CommandResult,
) -> CommandResult {
    if control.begin_completion() {
        output()
    } else {
        Ok(ExitCode::GeneralFailure)
    }
}

fn complete_read_only_output<R>(
    control: &OperationControl<R>,
    output: impl FnOnce() -> CommandResult,
) -> CommandResult {
    if !control.begin_read_only_output() {
        return Ok(ExitCode::GeneralFailure);
    }
    let result = output();
    if control.finish_read_only_output() {
        result
    } else {
        Ok(ExitCode::GeneralFailure)
    }
}

fn execute_mutation_with_signals<R>(
    context: &'static str,
    recovery: R,
    operation: impl FnOnce(&OperationControl<R>) -> CommandResult + Send + 'static,
    incomplete_signal: impl FnOnce(ExitCode, SignalSnapshot<R>) -> CommandResult + 'static,
) -> CommandResult
where
    R: Clone + Send + 'static,
{
    execute_mutation_with_signals_and_deferred_timeout(
        context,
        recovery,
        None,
        move |control, _| operation(control),
        incomplete_signal,
        |_| Ok(ExitCode::GeneralFailure),
    )
}

fn execute_mutation_with_signals_and_deferred_timeout<R>(
    context: &'static str,
    recovery: R,
    timeout: Option<Duration>,
    operation: impl FnOnce(&OperationControl<R>, &DeferredObservationTimeoutStart) -> CommandResult
    + Send
    + 'static,
    incomplete_signal: impl FnOnce(ExitCode, SignalSnapshot<R>) -> CommandResult + 'static,
    timed_out: impl FnOnce(SignalSnapshot<R>) -> CommandResult + 'static,
) -> CommandResult
where
    R: Clone + Send + 'static,
{
    run_blocking_signal_runtime(context, async move {
        let mut signals = ProcessSignals::install(context)?;
        let control = Arc::new(OperationControl::new(recovery));
        let (timeout_sender, timeout_started) = tokio::sync::oneshot::channel();
        let timeout_start = Arc::new(DeferredObservationTimeoutStart {
            sender: Mutex::new(Some(timeout_sender)),
        });
        let operation_timeout_start = Arc::clone(&timeout_start);
        let mut running = spawn_controlled_blocking(Arc::clone(&control), move |control| {
            operation(control, &operation_timeout_start)
        });
        tokio::select! {
            biased;
            signal = signals.recv() => {
                let stopped = control
                    .claim_signal()
                    .map(|snapshot| incomplete_signal(signal, snapshot));
                finish_stopped_operation(context, &mut running, stopped).await
            }
            () = (DeferredObservationTimeout {
                duration: timeout,
                started: timeout_started,
            }).wait() => {
                let stopped = control.claim_signal().map(timed_out);
                finish_stopped_operation(context, &mut running, stopped).await
            }
            result = &mut running => finish_read_only_operation(context, result),
        }
    })
}

fn finish_read_only_operation(
    context: &str,
    result: Result<CommandResult, tokio::task::JoinError>,
) -> CommandResult {
    result.with_context(|| format!("complete {context} operation"))?
}

struct HumanApiOutcomeAdapters<O, E> {
    unauthenticated: fn() -> O,
    unreachable: fn(crate::api::UnreachableCategory) -> O,
    operation_error: fn(E) -> anyhow::Error,
}

fn human_session_client(transport_policy: HttpTransportPolicy) -> anyhow::Result<HttpClient> {
    HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context("prepare human session networking")
}

struct PrincipalApiContext<'a> {
    client: &'a HttpClient,
    deployment: &'a Deployment,
    authentication: &'a PrincipalAuthenticationArgs,
    session_context: &'static str,
}

fn principal_api_context<'a>(
    client: &'a HttpClient,
    deployment: &'a Deployment,
    authentication: &'a PrincipalAuthenticationArgs,
    session_context: &'static str,
) -> PrincipalApiContext<'a> {
    PrincipalApiContext {
        client,
        deployment,
        authentication,
        session_context,
    }
}

fn execute_selected_api_operation<T, E>(
    context: PrincipalApiContext<'_>,
    operation: impl FnMut(&str) -> anyhow::Result<Result<T, E>>,
    credential_rejected: impl Fn(&E) -> bool,
    unauthenticated: impl Fn() -> E,
    unreachable: impl Fn(UnreachableCategory) -> E,
) -> anyhow::Result<Result<T, E>> {
    execute_selected_api_operation_retrying_result(
        context,
        operation,
        |operation| operation.as_ref().is_err_and(&credential_rejected),
        unauthenticated,
        unreachable,
    )
}

fn execute_selected_api_operation_retrying_result<T, E>(
    context: PrincipalApiContext<'_>,
    mut operation: impl FnMut(&str) -> anyhow::Result<Result<T, E>>,
    credential_rejected: impl Fn(&Result<T, E>) -> bool,
    unauthenticated: impl Fn() -> E,
    unreachable: impl Fn(UnreachableCategory) -> E,
) -> anyhow::Result<Result<T, E>> {
    if let Some(api_key) = context.authentication.service_api_key()? {
        operation(api_key.expose())
    } else {
        execute_required_api_operation_retrying_result(
            context.client,
            context.deployment,
            operation,
            credential_rejected,
            unauthenticated,
            unreachable,
            context.session_context,
        )
    }
}

fn execute_required_api_operation_retrying_result<T, E>(
    client: &HttpClient,
    deployment: &Deployment,
    mut operation: impl FnMut(&str) -> anyhow::Result<Result<T, E>>,
    credential_rejected: impl Fn(&Result<T, E>) -> bool,
    unauthenticated: impl Fn() -> E,
    unreachable: impl Fn(UnreachableCategory) -> E,
    session_context: &'static str,
) -> anyhow::Result<Result<T, E>> {
    match session::execute_required(
        client,
        deployment,
        |access_token| operation(access_token.expose()),
        |result| result.as_ref().is_ok_and(&credential_rejected),
    ) {
        Ok(RequiredOperation::Unauthenticated) => Ok(Err(unauthenticated())),
        Ok(RequiredOperation::Completed(result)) => result,
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok(Err(unreachable(category))),
            None => Err(anyhow!(error).context(session_context)),
        },
    }
}

fn execute_human_api_operation<O, E>(
    client: &crate::api::HttpClient,
    deployment: &Deployment,
    mut operation: impl FnMut(&str) -> Result<O, E>,
    credential_rejected: impl Fn(&Result<O, E>) -> bool,
    adapters: HumanApiOutcomeAdapters<O, E>,
    api_context: String,
) -> anyhow::Result<O> {
    match session::execute_required(
        client,
        deployment,
        |access_token| operation(access_token.expose()),
        credential_rejected,
    ) {
        Ok(RequiredOperation::Unauthenticated) => Ok((adapters.unauthenticated)()),
        Ok(RequiredOperation::Completed(result)) => result
            .map_err(adapters.operation_error)
            .context(api_context),
        Err(error) => match error.unreachable_category() {
            Some(category) => Ok((adapters.unreachable)(category)),
            None => Err(anyhow!(error).context("acquire human session")),
        },
    }
}

trait HumanCredentialOutcome: Sized {
    type Error: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static;

    fn unauthenticated() -> Self;
    fn unreachable(category: UnreachableCategory) -> Self;
    fn is_unauthenticated(&self) -> bool;
    fn credential_rejected(error: &Self::Error) -> bool;
}

fn execute_with_human_credential<O>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    network_context: &'static str,
    api_context: &'static str,
    operation: impl FnMut(&HttpClient, &str, &str) -> Result<O, O::Error>,
) -> anyhow::Result<O>
where
    O: HumanCredentialOutcome,
{
    execute_with_principal_credential(
        deployment,
        transport_policy,
        &PrincipalAuthenticationArgs::default(),
        network_context,
        api_context,
        operation,
    )
}

fn execute_with_principal_credential<O>(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: &PrincipalAuthenticationArgs,
    network_context: &'static str,
    api_context: &'static str,
    mut operation: impl FnMut(&HttpClient, &str, &str) -> Result<O, O::Error>,
) -> anyhow::Result<O>
where
    O: HumanCredentialOutcome,
{
    let client = HttpClient::new(transport_policy)
        .map_err(|error| anyhow!(error))
        .context(network_context)?;
    if let Some(api_key) = authentication.service_api_key()? {
        return operation(
            &client,
            deployment.fingerprint().api_url(),
            api_key.expose(),
        )
        .map_err(|error| anyhow!(error))
        .with_context(|| format!("{api_context} {}", deployment.fingerprint().api_url()));
    }
    execute_human_api_operation(
        &client,
        deployment,
        |access_token| operation(&client, deployment.fingerprint().api_url(), access_token),
        |result| {
            result.as_ref().is_ok_and(O::is_unauthenticated)
                || result.as_ref().is_err_and(O::credential_rejected)
        },
        HumanApiOutcomeAdapters {
            unauthenticated: O::unauthenticated,
            unreachable: O::unreachable,
            operation_error: |error: O::Error| anyhow!(error),
        },
        format!("{api_context} {}", deployment.fingerprint().api_url()),
    )
}

fn execute_deployment_leaf<T>(
    command: T,
    command_path: &[&str],
    error_context: &'static str,
    execute: impl FnOnce(T, &Deployment) -> anyhow::Result<ExitCode>,
) -> CommandResult {
    execute_deployment_command(
        Some(command),
        command_path,
        error_context,
        |command, deployment| execute(command, deployment).map_err(Into::into),
    )
}

fn execute_deployment_command<T>(
    command: Option<T>,
    command_path: &[&str],
    error_context: &'static str,
    execute: impl FnOnce(T, &Deployment) -> CommandResult,
) -> CommandResult {
    let Some(command) = command else {
        return print_help(command_path);
    };
    let deployment = Deployment::load()
        .map_err(|error| anyhow!(error))
        .context(error_context)?;
    execute(command, &deployment)
}

fn print_help(command_path: &[&str]) -> CommandResult {
    let mut root = Cli::command();
    root.build();
    let mut command = &mut root;

    for name in command_path {
        let Some(subcommand) = command.find_subcommand_mut(name) else {
            return Err(anyhow!("command help metadata is unavailable for {name}").into());
        };
        command = subcommand;
    }

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    command
        .write_help(&mut stdout)
        .context("failed to write command help")?;
    Ok(ExitCode::Success)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::Path;
    use std::sync::{Arc, Barrier, Mutex};

    use clap::CommandFactory;
    use serde_json::Value;

    use super::{Cli, parse, unreachable_outcome_class};
    use crate::api::UnreachableCategory;
    use crate::exit_code::{ExitCode, OutcomeClass};

    #[test]
    fn completion_and_cancellation_each_win_one_controlled_output_race() {
        for signal_wins in [true, false] {
            let control = Arc::new(super::OperationControl::new(()));
            let at_boundary = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            let documents = Arc::new(Mutex::new(Vec::new()));

            let worker_control = Arc::clone(&control);
            let worker_boundary = Arc::clone(&at_boundary);
            let worker_release = Arc::clone(&release);
            let worker_documents = Arc::clone(&documents);
            let worker = std::thread::spawn(move || {
                if signal_wins {
                    worker_boundary.wait();
                    worker_release.wait();
                }
                super::complete_operation(&worker_control, || {
                    if !signal_wins {
                        worker_boundary.wait();
                        worker_release.wait();
                    }
                    worker_documents.lock().unwrap().push(
                        serde_json::to_vec(&serde_json::json!({"outcome": "completed"})).unwrap(),
                    );
                    Ok(ExitCode::Success)
                })
                .unwrap_or_else(|failure| panic!("{}", failure.error()))
            });

            at_boundary.wait();
            let exit = if control.claim_signal().is_some() {
                documents.lock().unwrap().push(
                    serde_json::to_vec(&serde_json::json!({"outcome": "interrupted"})).unwrap(),
                );
                ExitCode::Interrupted
            } else {
                ExitCode::Success
            };
            release.wait();
            let worker_exit = worker.join().unwrap();

            let documents = documents.lock().unwrap();
            assert_eq!(documents.len(), 1);
            let document: Value = serde_json::from_slice(&documents[0]).unwrap();
            if signal_wins {
                assert_eq!(exit, ExitCode::Interrupted);
                assert_eq!(worker_exit, ExitCode::GeneralFailure);
                assert_eq!(document["outcome"], "interrupted");
            } else {
                assert_eq!(exit, ExitCode::Success);
                assert_eq!(worker_exit, ExitCode::Success);
                assert_eq!(document["outcome"], "completed");
            }
        }
    }

    #[test]
    fn bounded_dispatch_and_signal_have_one_ordered_owner() {
        let signal_first = super::OperationControl::new(());
        assert!(signal_first.claim_signal().is_some());
        assert!(!signal_first.begin_bounded_dispatch());
        assert!(!signal_first.dispatched());

        let dispatch_first = super::OperationControl::new(());
        assert!(dispatch_first.begin_bounded_dispatch());
        assert!(dispatch_first.begin_bounded_dispatch());
        assert!(dispatch_first.dispatched());
        assert!(dispatch_first.claim_signal().is_none());
    }

    #[test]
    fn controlled_completion_preserves_output_failures() {
        let mutation = super::OperationControl::new(());
        let result = super::complete_operation(&mutation, || {
            mutation.recovery();
            Err(anyhow::anyhow!("fixture mutation output failure").into())
        });
        assert!(result.is_err());
        assert!(mutation.claim_signal().is_none());

        let read_only = super::OperationControl::new(());
        let result = super::complete_read_only_output(&read_only, || {
            Err(anyhow::anyhow!("fixture read-only output failure").into())
        });
        assert!(result.is_err());
        assert!(read_only.claim_signal().is_none());
    }

    #[test]
    #[ignore = "launched only as the nested assignment workflow fixture"]
    fn nested_workflow_fixture_process() {
        let workspace = std::env::current_dir().unwrap();
        let round = workspace.join("delivery-rounds/0001");
        let execution = round.join("workspace");
        let run = round.join("run");
        fs::create_dir_all(&execution).unwrap();
        let arguments = vec![
            OsString::from("scherzo-cloud"),
            OsString::from("workflow"),
            OsString::from("run"),
            OsString::from("--source-root"),
            workspace.clone().into_os_string(),
            OsString::from("--execution-root"),
            execution.into_os_string(),
            OsString::from("--run-dir"),
            run.clone().into_os_string(),
            OsString::from("--json"),
            OsString::from("--color"),
            OsString::from("never"),
            workspace.join("nested.yaml").into_os_string(),
        ];
        let outcome = match parse(arguments).unwrap().execute() {
            Ok(outcome) => outcome,
            Err(failure) => panic!("nested workflow fixture failed: {}", failure.error()),
        };
        assert_eq!(outcome, ExitCode::Success);

        let result_root = run.join("attempts/000001/result");
        let result: Value =
            serde_json::from_slice(&fs::read(result_root.join("result.json")).unwrap()).unwrap();
        assert_eq!(result["outcome"], "succeeded");
        let export = result["exports"]["portable"]["path"].as_str().unwrap();
        let bytes = fs::read(result_root.join(export)).unwrap();
        assert_eq!(bytes, b"nested portable result");
        fs::write(round.join("nested-result.txt"), bytes).unwrap();

        let retained =
            run.join(".private/workflow-retained/.inputs-retained/view-retained/values/payload");
        assert_eq!(
            fs::symlink_metadata(&retained)
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o400
        );
        for directory in [
            retained.parent().unwrap(),
            retained.parent().unwrap().parent().unwrap(),
        ] {
            assert_eq!(
                fs::symlink_metadata(directory)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o7777,
                0o500
            );
        }
    }

    fn collect_command_paths(command: &clap::Command, prefix: &str, paths: &mut Vec<String>) {
        assert!(
            !command.is_allow_external_subcommands_set(),
            "customer command {prefix:?} accepts external subcommands"
        );
        for child in command
            .get_subcommands()
            .filter(|child| child.get_name() != "help")
        {
            for name in std::iter::once(child.get_name()).chain(child.get_all_aliases()) {
                let path = if prefix.is_empty() {
                    name.to_owned()
                } else {
                    format!("{prefix} {name}")
                };
                paths.push(path.clone());
                collect_command_paths(child, &path, paths);
            }
        }
    }

    fn customer_command_paths() -> Vec<String> {
        let mut paths = Vec::new();
        collect_command_paths(&Cli::command(), "", &mut paths);
        paths.sort();
        paths.dedup();
        paths
    }

    fn help_snapshot_paths() -> Vec<String> {
        let snapshot_directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/cmd/help");
        let mut paths = snapshot_directory
            .read_dir()
            .expect("help snapshot directory should exist")
            .filter_map(|entry| {
                let path = entry
                    .expect("help snapshot entry should be readable")
                    .path();
                path.extension()
                    .is_some_and(|extension| extension == "trycmd")
                    .then_some(path)
            })
            .map(|path| {
                let snapshot =
                    fs::read_to_string(path).expect("help snapshot should be readable as UTF-8");
                snapshot
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("$ scherzo-cloud")
                            .and_then(|invocation| invocation.strip_suffix(" --help"))
                    })
                    .expect("help snapshot should declare its command invocation")
                    .trim()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    #[test]
    fn every_unreachable_category_uses_the_shared_outcome_table() {
        for category in [
            UnreachableCategory::Dns,
            UnreachableCategory::Timeout,
            UnreachableCategory::Connection,
            UnreachableCategory::Tls,
            UnreachableCategory::Server,
        ] {
            assert_eq!(
                unreachable_outcome_class(category),
                OutcomeClass::Unreachable
            );
        }
        assert_eq!(
            unreachable_outcome_class(UnreachableCategory::RateLimited),
            OutcomeClass::RateLimited
        );
    }

    #[test]
    fn every_customer_command_has_one_help_snapshot() {
        let mut command_paths = vec![String::new()];
        command_paths.extend(customer_command_paths());

        assert_eq!(help_snapshot_paths(), command_paths);
    }

    #[test]
    fn service_authentication_is_explicit_and_human_only_leaves_reject_it() {
        assert!(
            parse([
                "scherzo-cloud",
                "service-principal",
                "credential",
                "list",
                "--service-api-key-file",
                "service.key",
            ])
            .is_ok()
        );
        assert!(
            parse([
                "scherzo-cloud",
                "organization",
                "show",
                "example",
                "--service-api-key-file",
                "service.key",
            ])
            .is_ok()
        );
        assert!(
            parse([
                "scherzo-cloud",
                "service-principal",
                "create",
                "--display-name",
                "Build agent",
                "--api-key-file",
                "new.key",
                "--service-api-key-file",
                "service.key",
            ])
            .is_err()
        );
        assert!(
            parse([
                "scherzo-cloud",
                "organization",
                "deletion",
                "request",
                "example",
                "--yes",
                "--service-api-key-file",
                "service.key",
            ])
            .is_err()
        );
    }

    #[test]
    fn customer_command_surface_is_exact_and_has_no_operator_entrypoint() {
        let actual = customer_command_paths();
        let expected = [
            "account",
            "account deletion",
            "account deletion cancel",
            "account deletion request",
            "account signup",
            "account update",
            "artifact",
            "artifact download",
            "artifact list",
            "artifact validate",
            "auth",
            "auth identities",
            "auth identities link",
            "auth identities list",
            "auth identities remove",
            "auth login",
            "auth logout",
            "auth status",
            "github",
            "github installation",
            "github installation disconnect",
            "github installation list",
            "github repository",
            "github repository list",
            "github setup",
            "github setup begin",
            "github setup complete",
            "invitation",
            "invitation accept",
            "invitation decline",
            "invitation list",
            "invitation preview",
            "organization",
            "organization audit",
            "organization audit list",
            "organization create",
            "organization deletion",
            "organization deletion cancel",
            "organization deletion request",
            "organization invitations",
            "organization invitations issue",
            "organization invitations list",
            "organization invitations revoke",
            "organization leave",
            "organization list",
            "organization members",
            "organization members history",
            "organization members list",
            "organization members remove",
            "organization members update",
            "organization show",
            "organization update",
            "project",
            "project create",
            "project list",
            "project rename",
            "project repository",
            "project repository detach",
            "project repository installation",
            "project repository installation list",
            "project repository list",
            "project repository set",
            "project repository show",
            "project repository update",
            "project runner-pool",
            "project runner-pool remove",
            "project runner-pool set",
            "project show",
            "publication",
            "publication create",
            "publication list",
            "publication show",
            "run",
            "run create",
            "run input-set",
            "run input-set create",
            "run input-set delete",
            "run input-set seal",
            "run input-set show",
            "run input-set upload",
            "run inputs",
            "run inputs delete",
            "run inputs download",
            "run inputs show",
            "run show",
            "run wait",
            "runner",
            "runner activation",
            "runner activation create",
            "runner activation list",
            "runner activation revoke",
            "runner create",
            "runner credential",
            "runner credential list",
            "runner credential retire",
            "runner credential revoke",
            "runner delete",
            "runner disable",
            "runner doctor",
            "runner drain",
            "runner enable",
            "runner enroll",
            "runner list",
            "runner move",
            "runner pool",
            "runner pool create",
            "runner pool delete",
            "runner pool list",
            "runner pool rename",
            "runner pool show",
            "runner rename",
            "runner serve",
            "runner show",
            "runner status",
            "service-principal",
            "service-principal create",
            "service-principal credential",
            "service-principal credential issue",
            "service-principal credential list",
            "service-principal credential revoke",
            "version",
            "workflow",
            "workflow reference",
            "workflow retry",
            "workflow run",
            "workflow schema",
            "workflow status",
            "workflow validate",
            "workflow view",
        ];

        assert_eq!(actual, expected);
    }
}
