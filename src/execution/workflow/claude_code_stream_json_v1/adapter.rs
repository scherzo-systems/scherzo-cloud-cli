use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::future::{Future, pending};
use std::io::{self, Read as _, Write as _};
use std::num::NonZeroU64;
use std::ops::Add as _;
use std::os::fd::OwnedFd;
use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rustix::fs::{AtFlags, FileType, statat, symlinkat, unlinkat};
use rustix::process::Pid;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::process::ChildStdout;
use tokio::sync::{mpsc, oneshot};

use super::{
    ClaudeCodeStreamJsonV1Parser, ClaudeCodeStreamJsonV1ProtocolLimits, CompletedResultExchange,
    FIXED_INVOCATION_ENVIRONMENT, initial_user_text_frame, normal_mode_arguments,
    result_mode_arguments, user_content_frame,
};
use crate::execution::claude_code::CLAUDE_CODE_STREAM_JSON_V1_VERSION;
use crate::execution::workflow::admission::{CancellationReason, CancellationSource};
use crate::execution::workflow::agent::{
    AgentAdapter, AgentCompatibilityProfile, AgentFailureCause, AgentInputKind, AgentInvocation,
    AgentLifecycleMilestone, AgentObservation, AgentObservationSink, AgentOutcome,
    AgentProcessDirective, AgentStartCallback, AgentTerminalCallback, AgentValueKind,
    AgentValueMode, OrderedAgentObservationSink, PositiveDuration, StagedAgentAttachment,
    check_agent_input_bound, failed_agent_outcome, finish_agent_diagnostic_capture,
};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::execution::workflow::child_guard::StoppedChildGuard;
use crate::execution::workflow::claude_code::ClaudeCodeConfig;
use crate::execution::workflow::coordinator::CoordinatorClock;
use crate::execution::workflow::diagnostic::StepDiagnosticLog;
use crate::execution::workflow::observation::ExecutionObserver;
use crate::execution::workflow::private_staging::open_directory_path;
// Both native adapters use the same containment and validator primitives, but their
// protocol state machines decide independently when those primitives gain authority.
// jscpd:ignore-start
use crate::execution::workflow::process_group::{
    ProcessGuardRegistration, interrupt_process_group, process_group_is_quiescent,
    reap_process_group_children, terminate_authenticated_process_group, terminate_process_group,
};
use crate::execution::workflow::result_validation::{
    AuthoritativeResultValidator, ProcessResultValidationWorker, ResultValidationDecision,
    ResultValidationOutcome, ResultValidationWorker,
};
// jscpd:ignore-end

const SYSTEM_PROMPT_FILE_PREFIX: &str = "claude-code-system-prompt-";
const READ_BUFFER_BYTES: usize = 8 * 1024;
pub(super) const PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(10);
const AMBIGUOUS_CANDIDATE_FEEDBACK: &str =
    "Result rejected: submit exactly one standalone structured result candidate.\n";

pub(crate) struct ClaudeCodeStreamJsonV1Adapter<
    Clock,
    Observer,
    Worker = ProcessResultValidationWorker,
> {
    diagnostics: StepDiagnosticLog,
    maximum_diagnostic_stream_bytes: NonZeroU64,
    clock: Clock,
    observer: Observer,
    validation_worker: Worker,
}

// Both concrete adapters own the same generic worker plumbing, but sharing their
// constructors or clone implementation would erase the closed native adapter types.
// jscpd:ignore-start
impl<Clock, Observer>
    ClaudeCodeStreamJsonV1Adapter<Clock, Observer, ProcessResultValidationWorker>
{
    pub(crate) fn new(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
    ) -> io::Result<Self> {
        Ok(Self {
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            validation_worker: ProcessResultValidationWorker::for_current_executable()?,
        })
    }
}

impl<Clock, Observer, Worker> ClaudeCodeStreamJsonV1Adapter<Clock, Observer, Worker> {
    #[cfg(test)]
    pub(super) fn with_validation_worker(
        diagnostics: StepDiagnosticLog,
        maximum_diagnostic_stream_bytes: NonZeroU64,
        clock: Clock,
        observer: Observer,
        validation_worker: Worker,
    ) -> Self {
        Self {
            diagnostics,
            maximum_diagnostic_stream_bytes,
            clock,
            observer,
            validation_worker,
        }
    }
}

impl<Clock, Observer, Worker> Clone for ClaudeCodeStreamJsonV1Adapter<Clock, Observer, Worker>
where
    Clock: Clone,
    Observer: Clone,
    Worker: Clone,
{
    fn clone(&self) -> Self {
        Self {
            diagnostics: self.diagnostics.clone(),
            maximum_diagnostic_stream_bytes: self.maximum_diagnostic_stream_bytes,
            clock: self.clock.clone(),
            observer: self.observer.clone(),
            validation_worker: self.validation_worker.clone(),
        }
    }
}

// jscpd:ignore-end

// The trait boilerplate matches the Pi adapter, while each implementation keeps its
// profile-specific lifecycle and result transport statically typed.
// jscpd:ignore-start
impl<Clock, Observer, Worker, Sink> AgentAdapter<Sink>
    for ClaudeCodeStreamJsonV1Adapter<Clock, Observer, Worker>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type NativeConfiguration = ClaudeCodeConfig;
    type ProtocolLimits = ClaudeCodeStreamJsonV1ProtocolLimits;

    async fn invoke(
        &self,
        invocation: AgentInvocation<Self::NativeConfiguration, Self::ProtocolLimits, Sink>,
        started: AgentStartCallback,
        terminal: AgentTerminalCallback,
    ) {
        let cancellation = invocation.cancellation().clone();
        let outcome = self.invoke_inner(invocation, &started).await;
        let outcome = cancellation
            .cancellation_reason()
            .map_or(outcome, |reason| AgentOutcome::Cancelled { reason });
        let _ = terminal.report(outcome);
    }
}
// jscpd:ignore-end

// The concrete generic bounds are intentionally repeated per closed adapter; a shared
// erased implementation would weaken exhaustive dispatch.
// jscpd:ignore-start
impl<Clock, Observer, Worker> ClaudeCodeStreamJsonV1Adapter<Clock, Observer, Worker>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
{
    async fn invoke_inner<Sink>(
        &self,
        mut invocation: AgentInvocation<
            ClaudeCodeConfig,
            ClaudeCodeStreamJsonV1ProtocolLimits,
            Sink,
        >,
        started: &AgentStartCallback,
    ) -> AgentOutcome
    where
        Sink: AgentObservationSink,
    {
        // jscpd:ignore-end
        // Claude's init acknowledgement and stream input make this a distinct startup
        // transition from Pi's session header and extension preparation.
        // jscpd:ignore-start
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }
        let plan = match prepare_launch(&invocation) {
            Ok(plan) => plan,
            Err(cause) => return failed_agent_outcome(cause),
        };
        // jscpd:ignore-end
        // The shared validator is wired into a native exchange here, while Pi wires it
        // into an injected socket bridge; combining those lifecycles would hide authority.
        // jscpd:ignore-start
        let validator = match invocation.value_mode() {
            AgentValueMode::Result { schema, .. } => Some(AuthoritativeResultValidator::new(
                schema.clone(),
                invocation.limits().maximum_result_bytes(),
                invocation
                    .limits()
                    .maximum_result_rejection_feedback_bytes(),
                invocation.limits().result_validation_deadline(),
                self.clock.clone(),
                self.validation_worker.clone(),
            )),
            AgentValueMode::None | AgentValueMode::Response { .. } => None,
        };
        let Some(process_directives) = invocation.take_process_directives() else {
            return failed_agent_outcome(AgentFailureCause::HarnessStartFailed);
        };
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }
        // jscpd:ignore-end

        let (process, standard_error) = match launch_process(&invocation, &plan).await {
            Ok(process) => process,
            Err(cause) => return failed_agent_outcome(cause),
        };
        // Diagnostic drain is tied to Claude's stream driver lifetime rather than Pi's
        // result bridge and native-session settlement.
        // jscpd:ignore-start
        let diagnostic = self.diagnostics.start_standard_error_capture(
            invocation.identity().step().to_owned(),
            invocation.identity().invocation(),
            self.maximum_diagnostic_stream_bytes,
            standard_error,
            self.observer.clone(),
        );
        // jscpd:ignore-end
        let parser = ClaudeCodeStreamJsonV1Parser::profile(
            Arc::clone(&plan.expected_cwd),
            Arc::from(invocation.adapter().native_configuration().model.as_str()),
            Arc::clone(&plan.session_id),
            invocation.value_mode().kind(),
            invocation.limits().maximum_response_bytes(),
        );
        let outcome = drive_process(
            &invocation,
            started,
            process,
            parser,
            process_directives,
            &plan.input,
            validator.as_ref(),
            self.clock.clone(),
            invocation.limits().result_settlement_grace(),
        )
        .await;
        invocation
            .diagnostic_session()
            .retain_protocol_rejection_from(&outcome);
        finish_agent_diagnostic_capture(diagnostic, &outcome).await;
        outcome
    }
}

pub(super) struct ClaudeCodeStreamJsonV1LaunchPlan {
    arguments: Vec<OsString>,
    expected_cwd: Arc<str>,
    session_id: Arc<str>,
    input: Vec<u8>,
    _native_session_bridge: ClaudeCodeNativeSessionBridge,
    _system_prompt_file: tempfile::NamedTempFile,
}

impl ClaudeCodeStreamJsonV1LaunchPlan {
    #[cfg(test)]
    pub(super) fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    #[cfg(test)]
    pub(super) fn input(&self) -> &[u8] {
        &self.input
    }

    #[cfg(test)]
    pub(super) fn session_id(&self) -> &str {
        &self.session_id
    }

    #[cfg(test)]
    pub(super) fn system_prompt_file(&self) -> &std::path::Path {
        self._system_prompt_file.path()
    }
}

pub(super) fn prepare_launch<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
) -> Result<ClaudeCodeStreamJsonV1LaunchPlan, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    check_agent_input_bound(
        invocation.prompt().system_prompt(),
        invocation.limits().maximum_system_prompt_bytes(),
        AgentInputKind::SystemPrompt,
    )?;
    check_agent_input_bound(
        invocation.prompt().message(),
        invocation.limits().maximum_message_bytes(),
        AgentInputKind::Message,
    )?;
    if invocation.adapter().profile() != AgentCompatibilityProfile::ClaudeCodeStreamJsonV1
        || invocation.adapter().version() != CLAUDE_CODE_STREAM_JSON_V1_VERSION
        || !invocation.adapter().executable().is_absolute()
        || invocation
            .diagnostic_session()
            .verify_claude_code_native_session_path_binding()
            .is_err()
    {
        return Err(AgentFailureCause::HarnessStartFailed);
    }

    // Claude correlates this path in system/init and stages a native prompt file; Pi's
    // corresponding preparation uses a persisted session and injected input extension.
    // jscpd:ignore-start
    let expected_cwd_path = invocation
        .process()
        .protocol_cwd()
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let session_id = Arc::from(
        invocation
            .diagnostic_session()
            .claude_code_native_session_id()
            .ok_or(AgentFailureCause::HarnessStartFailed)?,
    );
    let native_session_bridge =
        ClaudeCodeNativeSessionBridge::prepare(invocation, &expected_cwd_path, &session_id)?;
    let expected_cwd = expected_cwd_path
        .to_str()
        .map(Arc::from)
        .ok_or(AgentFailureCause::HarnessStartFailed)?;
    let mut system_prompt_file = tempfile::Builder::new()
        .prefix(SYSTEM_PROMPT_FILE_PREFIX)
        .tempfile_in(invocation.staging().result_endpoint_directory())
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    system_prompt_file
        .write_all(invocation.prompt().system_prompt().as_bytes())
        .and_then(|()| system_prompt_file.flush())
        .and_then(|()| {
            system_prompt_file
                .as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o400))
        })
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    // jscpd:ignore-end

    let configuration = invocation.adapter().native_configuration();
    let arguments = if invocation.value_mode().kind() == AgentValueKind::Result {
        result_mode_arguments(
            &configuration.model,
            configuration.effort.as_str(),
            &session_id,
            system_prompt_file.path(),
        )
    } else {
        normal_mode_arguments(
            &configuration.model,
            configuration.effort.as_str(),
            &session_id,
            system_prompt_file.path(),
        )
    };
    let input = initial_user_frame(invocation)?;
    Ok(ClaudeCodeStreamJsonV1LaunchPlan {
        arguments,
        expected_cwd,
        session_id,
        input,
        _native_session_bridge: native_session_bridge,
        _system_prompt_file: system_prompt_file,
    })
}

struct ClaudeCodeNativeSessionBridge {
    ambient_project_directory: OwnedFd,
    links: Vec<OwnedAmbientSessionLink>,
}

struct OwnedAmbientSessionLink {
    name: OsString,
    device: libc::dev_t,
    inode: libc::ino_t,
}

impl ClaudeCodeNativeSessionBridge {
    fn prepare<Sink>(
        invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
        expected_cwd: &Path,
        session_id: &str,
    ) -> Result<Self, AgentFailureCause>
    where
        Sink: AgentObservationSink,
    {
        invocation
            .diagnostic_session()
            .verify_claude_code_native_session_path_binding()
            .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
        let transcript = invocation
            .diagnostic_session()
            .claude_code_native_transcript_path()
            .ok_or(AgentFailureCause::HarnessStartFailed)?;
        let resources = invocation
            .diagnostic_session()
            .claude_code_native_resources_directory()
            .ok_or(AgentFailureCause::HarnessStartFailed)?;
        if !transcript.is_absolute() || !resources.is_absolute() {
            return Err(AgentFailureCause::HarnessStartFailed);
        }

        let config = claude_code_config_directory(invocation, expected_cwd)?;
        let project = config.join("projects").join(native_project_slug(
            expected_cwd
                .to_str()
                .ok_or(AgentFailureCause::HarnessStartFailed)?,
        ));
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder
            .create(&project)
            .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
        let project =
            fs::canonicalize(project).map_err(|_| AgentFailureCause::HarnessStartFailed)?;
        let ambient_project_directory =
            open_directory_path(&project).map_err(|_| AgentFailureCause::HarnessStartFailed)?;
        let mut bridge = Self {
            ambient_project_directory,
            links: Vec::with_capacity(2),
        };
        bridge.create_link(&transcript, OsString::from(format!("{session_id}.jsonl")))?;
        bridge.create_link(&resources, OsString::from(session_id))?;
        Ok(bridge)
    }

    fn create_link(&mut self, target: &Path, name: OsString) -> Result<(), AgentFailureCause> {
        symlinkat(target, &self.ambient_project_directory, &name)
            .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
        let metadata = statat(
            &self.ambient_project_directory,
            &name,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
        if FileType::from_raw_mode(metadata.st_mode) != FileType::Symlink {
            return Err(AgentFailureCause::HarnessStartFailed);
        }
        self.links.push(OwnedAmbientSessionLink {
            name,
            device: metadata.st_dev,
            inode: metadata.st_ino,
        });
        Ok(())
    }
}

impl Drop for ClaudeCodeNativeSessionBridge {
    fn drop(&mut self) {
        for link in self.links.iter().rev() {
            let Ok(metadata) = statat(
                &self.ambient_project_directory,
                &link.name,
                AtFlags::SYMLINK_NOFOLLOW,
            ) else {
                continue;
            };
            if FileType::from_raw_mode(metadata.st_mode) == FileType::Symlink
                && metadata.st_dev == link.device
                && metadata.st_ino == link.inode
            {
                let _ = unlinkat(
                    &self.ambient_project_directory,
                    &link.name,
                    AtFlags::empty(),
                );
            }
        }
    }
}

fn claude_code_config_directory<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    expected_cwd: &Path,
) -> Result<PathBuf, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    let environment = invocation.process().environment().variables();
    let configured = environment
        .get(std::ffi::OsStr::new("CLAUDE_CONFIG_DIR"))
        .map(PathBuf::from);
    let directory = if let Some(configured) = configured {
        configured
    } else {
        let home = environment
            .get(std::ffi::OsStr::new("HOME"))
            .map(PathBuf::from)
            .ok_or(AgentFailureCause::HarnessStartFailed)?;
        home.join(".claude")
    };
    if directory.is_absolute() {
        Ok(directory)
    } else {
        Ok(expected_cwd.join(directory))
    }
}

pub(super) fn native_project_slug(cwd: &str) -> String {
    const MAXIMUM_SLUG_CODE_UNITS: usize = 200;
    let code_units = cwd.encode_utf16().collect::<Vec<_>>();
    let mut slug = String::with_capacity(code_units.len());
    for code_unit in &code_units {
        if let Ok(ascii) = u8::try_from(*code_unit)
            && ascii.is_ascii_alphanumeric()
        {
            slug.push(char::from(ascii));
        } else {
            slug.push('-');
        }
    }
    if code_units.len() <= MAXIMUM_SLUG_CODE_UNITS {
        return slug;
    }

    let mut hash = 0_i32;
    for code_unit in code_units {
        hash = hash.wrapping_mul(31).wrapping_add(i32::from(code_unit));
    }
    format!(
        "{}-{}",
        &slug[..MAXIMUM_SLUG_CODE_UNITS],
        base36(hash.unsigned_abs())
    )
}

fn base36(mut value: u32) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".to_owned();
    }
    let mut reversed = Vec::new();
    while value > 0 {
        let index = usize::try_from(value % 36).unwrap_or_default();
        reversed.push(char::from(DIGITS[index]));
        value /= 36;
    }
    reversed.iter().rev().collect()
}

fn initial_user_frame<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
) -> Result<Vec<u8>, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    if invocation.attachments().is_empty() {
        return initial_user_text_frame(invocation.prompt().message())
            .map_err(|_| AgentFailureCause::HarnessStartFailed);
    }

    if invocation.attachments().len() > invocation.limits().maximum_attachments().get() {
        return Err(AgentFailureCause::HarnessStartFailed);
    }
    let mut content = Vec::new();
    content
        .try_reserve_exact(invocation.attachments().len().saturating_add(1))
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    content.push(json!({
        "type": "text",
        "text": invocation.prompt().message(),
    }));
    let mut total_bytes = 0_u64;
    for (index, attachment) in invocation.attachments().iter().enumerate() {
        let expected_identity = format!("{index:06}");
        let identity = attachment
            .path()
            .file_name()
            .and_then(|identity| identity.to_str())
            .filter(|identity| *identity == expected_identity)
            .ok_or(AgentFailureCause::HarnessStartFailed)?;
        if !attachment.path().is_absolute() {
            return Err(AgentFailureCause::HarnessStartFailed);
        }
        let metadata = fs::symlink_metadata(attachment.path())
            .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
        if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o377 != 0 {
            return Err(AgentFailureCause::HarnessStartFailed);
        }
        total_bytes = total_bytes
            .checked_add(metadata.len())
            .filter(|total| *total <= invocation.limits().maximum_attachment_bytes().get())
            .ok_or(AgentFailureCause::HarnessStartFailed)?;
        content.push(attachment_content_block(
            attachment,
            identity,
            metadata.len(),
        )?);
    }
    user_content_frame(content).map_err(|_| AgentFailureCause::HarnessStartFailed)
}

fn attachment_content_block(
    attachment: &StagedAgentAttachment,
    identity: &str,
    expected_bytes: u64,
) -> Result<Value, AgentFailureCause> {
    let media_type = attachment.media_type();
    let base_media_type = media_type
        .split_once(';')
        .map_or(media_type, |(base, _)| base)
        .trim();
    let text_media = (base_media_type.len() > "text/".len()
        && base_media_type[.."text/".len()].eq_ignore_ascii_case("text/"))
        || base_media_type.eq_ignore_ascii_case("application/json");
    let native_media = if media_type.eq_ignore_ascii_case("image/png") {
        Some(("image", "image/png"))
    } else if media_type.eq_ignore_ascii_case("application/pdf") {
        Some(("document", "application/pdf"))
    } else {
        None
    };

    if text_media || native_media.is_some() {
        let bytes = read_staged_attachment(attachment, expected_bytes)?;
        if text_media && let Ok(text) = std::str::from_utf8(&bytes) {
            return Ok(json!({
                "type": "text",
                "text": format!(
                    "Scherzo attachment {identity} ({media_type}) follows:\n{text}"
                ),
            }));
        }
        if let Some((block_type, native_media_type)) = native_media {
            return Ok(json!({
                "type": block_type,
                "source": {
                    "type": "base64",
                    "media_type": native_media_type,
                    "data": BASE64.encode(bytes),
                },
            }));
        }
    }

    let sealed_path = attachment
        .path()
        .to_str()
        .ok_or(AgentFailureCause::HarnessStartFailed)?;
    Ok(json!({
        "type": "text",
        "text": format!(
            "Scherzo attachment {identity} has media type {media_type} and is available to runner tools at {sealed_path}."
        ),
    }))
}

fn read_staged_attachment(
    attachment: &StagedAgentAttachment,
    expected_bytes: u64,
) -> Result<Vec<u8>, AgentFailureCause> {
    let capacity =
        usize::try_from(expected_bytes).map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let mut file =
        fs::File::open(attachment.path()).map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    file.read_to_end(&mut bytes)
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    if u64::try_from(bytes.len()) != Ok(expected_bytes) {
        return Err(AgentFailureCause::HarnessStartFailed);
    }
    Ok(bytes)
}

struct LaunchedClaudeCodeProcess {
    child: ClaudeCodeChild,
    process_group: Pid,
    standard_input: UnixStream,
    standard_output: ChildStdout,
}

struct ClaudeCodeChild {
    child: StoppedChildGuard,
    registration: ProcessGuardRegistration,
}

impl ClaudeCodeChild {
    fn force_process_group(&self, _process_group: Pid) {
        let _ = terminate_authenticated_process_group(self.child.identity());
    }

    async fn wait(&mut self) -> Result<ExitStatus, ()> {
        let status = self.child.wait().await.map_err(|_| ())?;
        self.mark_quiesced()?;
        Ok(status)
    }

    async fn force_stop(&mut self, process_group: Pid) -> Result<(), ()> {
        self.force_process_group(process_group);
        self.child.force_stop().await.map_err(|_| ())?;
        self.mark_quiesced()
    }

    fn mark_quiesced(&mut self) -> Result<(), ()> {
        self.registration.mark_quiesced()
    }
}

async fn launch_process<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    plan: &ClaudeCodeStreamJsonV1LaunchPlan,
) -> Result<(LaunchedClaudeCodeProcess, tokio::process::ChildStderr), AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    // Claude always uses an invocation guard because native tools may create new sessions that
    // an ordinary process-group boundary cannot contain. Registration remains optional.
    // jscpd:ignore-start
    let environment = invocation_environment(invocation);
    let (mut child, standard_input) = StoppedChildGuard::spawn_with_stdin(
        invocation.adapter().executable(),
        &plan.arguments,
        &environment,
        |command| {
            invocation
                .process()
                .bind_command(command)
                .map_err(|_| io::Error::other("agent working directory is unavailable"))
        },
    )
    .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    let process_group = child.identity().process_group();
    let (Some(standard_output), Some(standard_error)) = (child.take_stdout(), child.take_stderr())
    else {
        let _ = child.force_stop().await;
        return Err(AgentFailureCause::HarnessStartFailed);
    };
    let mut registration = match invocation.process_guards().register(
        invocation.identity().step(),
        invocation.identity().invocation().transition_sequence.get(),
        child.identity(),
    ) {
        Ok(registration) => registration,
        Err(()) => {
            let _ = child.force_stop().await;
            return Err(AgentFailureCause::HarnessStartFailed);
        }
    };
    if release_guarded_claude_code(invocation.diagnostic_session(), || {
        child.continue_execution().map_err(|_| ())?;
        registration.mark_released()
    })
    .is_err()
    {
        let _ = child.force_stop().await;
        let _ = registration.mark_quiesced();
        return Err(AgentFailureCause::HarnessStartFailed);
    }

    Ok((
        LaunchedClaudeCodeProcess {
            child: ClaudeCodeChild {
                child,
                registration,
            },
            process_group,
            standard_input,
            standard_output,
        },
        standard_error,
    ))
    // jscpd:ignore-end
}

fn release_guarded_claude_code(
    diagnostic_session: &AgentDiagnosticSession,
    release: impl FnOnce() -> Result<(), ()>,
) -> Result<(), AgentFailureCause> {
    diagnostic_session
        .verify_claude_code_native_session_path_binding()
        .map_err(|_| AgentFailureCause::HarnessStartFailed)?;
    release().map_err(|()| AgentFailureCause::HarnessStartFailed)
}

fn invocation_environment<Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
) -> Vec<(OsString, OsString)>
where
    Sink: AgentObservationSink,
{
    let mut environment = invocation
        .process()
        .environment()
        .variables()
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    environment.remove(OsStr::new("CLAUDE_CODE_PROJECT_DIR_NAME"));
    for (name, value) in FIXED_INVOCATION_ENVIRONMENT {
        environment.insert(OsString::from(name), OsString::from(value));
    }
    environment.into_iter().collect()
}

type SettlementDeadlineWait = Pin<Box<dyn Future<Output = ()> + Send>>;

#[expect(
    clippy::too_many_arguments,
    reason = "the driver receives each admitted result boundary explicitly"
)]
async fn drive_process<Clock, Worker, Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    started: &AgentStartCallback,
    process: LaunchedClaudeCodeProcess,
    mut parser: ClaudeCodeStreamJsonV1Parser,
    process_directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    input: &[u8],
    validator: Option<&AuthoritativeResultValidator<Clock, Worker>>,
    mut clock: Clock,
    settlement_grace: PositiveDuration,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    let LaunchedClaudeCodeProcess {
        mut child,
        process_group,
        standard_input,
        mut standard_output,
    } = process;
    let cancellation_source = invocation.cancellation().clone();
    let (stop_supervisor, supervisor_shutdown) = oneshot::channel();
    let process_supervisor = tokio::spawn(supervise_process_group(
        process_group,
        cancellation_source.clone(),
        process_directives,
        supervisor_shutdown,
    ));
    let mut standard_input = Some(standard_input);
    let mut parser_enabled = true;
    let mut failure = None;
    let mut cancelled = None;
    match initialize_standard_input(
        &mut standard_input,
        input,
        invocation.value_mode().kind() != AgentValueKind::Result,
        &cancellation_source,
    )
    .await
    {
        InitialInputProgress::Ready => {}
        InitialInputProgress::Cancelled(reason) => {
            cancelled = Some(reason);
            parser_enabled = false;
        }
        InitialInputProgress::Failed => {
            failure = Some(AgentFailureCause::HarnessStartFailed);
            parser_enabled = false;
            child.force_process_group(process_group);
        }
    }

    let cancellation = cancellation_source.wait_for_cancellation();
    tokio::pin!(cancellation);
    let mut buffer = [0_u8; READ_BUFFER_BYTES];
    let mut standard_output_closed = false;
    let mut process_completion: Option<ExitStatus> = None;
    let mut process_group_quiescent = false;
    let mut process_group_termination_requested = failure.is_some();
    let mut wait_failed = false;
    let mut settlement_deadline: Option<SettlementDeadlineWait> = None;
    let mut process_group_probe_clock = clock.clone();

    // Claude's result closes an exchange without closing the process; retain a separate
    // driver loop even where stream-drain mechanics resemble Pi's terminal loop.
    // jscpd:ignore-start
    while !standard_output_closed
        || (process_completion.is_none() && !wait_failed)
        || !process_group_quiescent
    {
        let probe_process_group = standard_output_closed
            && (process_completion.is_some() || wait_failed)
            && !process_group_quiescent;
        tokio::select! {
            biased;
            reason = &mut cancellation, if cancelled.is_none() => {
                cancelled = Some(reason);
                parser_enabled = false;
                settlement_deadline = None;
                standard_input.take();
            }
            read = standard_output.read(&mut buffer), if !standard_output_closed => {
                match read {
                    Ok(0) => standard_output_closed = true,
                    // jscpd:ignore-end
                    Ok(read) if parser_enabled => {
                        let mut observations = Vec::new();
                        let parsed = parser.push_stdout(&buffer[..read], |observation| {
                            observations.push(observation);
                        });
                        if let Some(reason) = cancellation_source.cancellation_reason() {
                            cancelled = Some(reason);
                            parser_enabled = false;
                            settlement_deadline = None;
                            standard_input.take();
                        }
                        if parser_enabled {
                            match emit_observations(
                                invocation.observations(),
                                started,
                                observations,
                                &cancellation_source,
                            ).await {
                                ObservationProgress::Completed => {}
                                ObservationProgress::Cancelled(reason) => {
                                    cancelled = Some(reason);
                                    parser_enabled = false;
                                    settlement_deadline = None;
                                    standard_input.take();
                                }
                                ObservationProgress::Failed => {
                                    failure = Some(AgentFailureCause::HarnessProtocolFailed);
                                    parser_enabled = false;
                                    child.force_process_group(process_group);
                                    process_group_termination_requested = true;
                                }
                            }
                        }
                        // Parser failure selects Claude's exchange phase even though process
                        // termination uses the same three assignments as Pi.
                        // jscpd:ignore-start
                        if parser_enabled
                            && let Err(cause) = parsed
                        {
                            failure = Some(cause);
                            parser_enabled = false;
                            child.force_process_group(process_group);
                            process_group_termination_requested = true;
                        }
                        // jscpd:ignore-end
                        if parser_enabled
                            && let Some(exchange) = parser.take_completed_result_exchange()
                        {
                            match handle_result_exchange(
                                exchange,
                                invocation,
                                validator,
                                &mut parser,
                                &mut standard_input,
                            ).await {
                                ResultExchangeProgress::Continue => {}
                                ResultExchangeProgress::Accepted => {
                                    let deadline = clock.now().add(settlement_grace.get());
                                    let deadline_clock = clock.clone();
                                    settlement_deadline = Some(Box::pin(async move {
                                        deadline_clock.wait_until(deadline).await;
                                    }));
                                    match emit_observations(
                                        invocation.observations(),
                                        started,
                                        vec![AgentObservation::Lifecycle {
                                            milestone: AgentLifecycleMilestone::HarnessCompleted,
                                        }],
                                        &cancellation_source,
                                    ).await {
                                        ObservationProgress::Completed => {}
                                        ObservationProgress::Cancelled(reason) => {
                                            cancelled = Some(reason);
                                            parser_enabled = false;
                                            settlement_deadline = None;
                                            standard_input.take();
                                        }
                                        ObservationProgress::Failed => {
                                            failure = Some(AgentFailureCause::HarnessProtocolFailed);
                                            parser_enabled = false;
                                            child.force_process_group(process_group);
                                            process_group_termination_requested = true;
                                        }
                                    }
                                }
                                ResultExchangeProgress::Failed(cause) => {
                                    failure = Some(cause);
                                    parser_enabled = false;
                                    child.force_process_group(process_group);
                                    process_group_termination_requested = true;
                                }
                                ResultExchangeProgress::Cancelled(reason) => {
                                    cancelled = Some(reason);
                                    parser_enabled = false;
                                    settlement_deadline = None;
                                    standard_input.take();
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    // Stream, wait, and deadline failures share OS mechanics with Pi but use
                    // Claude's initialization and exchange phase for typed failure authority.
                    // jscpd:ignore-start
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        standard_output_closed = true;
                        if parser_enabled {
                            if let Some(reason) = cancellation_source.cancellation_reason() {
                                cancelled = Some(reason);
                            } else {
                                failure = Some(parser.protocol_failure());
                                child.force_process_group(process_group);
                                process_group_termination_requested = true;
                            }
                            parser_enabled = false;
                        }
                    }
                }
            }
            waited = child.wait(), if process_completion.is_none() && !wait_failed => {
                match waited {
                    Ok(status) => process_completion = Some(status),
                    Err(_) => {
                        wait_failed = true;
                        parser_enabled = false;
                        if cancelled.is_none() {
                            failure.get_or_insert(parser.protocol_failure());
                        }
                        child.force_process_group(process_group);
                        process_group_termination_requested = true;
                    }
                }
            }
            () = wait_for_optional_deadline(&mut settlement_deadline),
                if settlement_deadline.is_some() =>
            {
                settlement_deadline = None;
                if let Some(reason) = cancellation_source.cancellation_reason() {
                    cancelled = Some(reason);
                } else {
                    failure = Some(AgentFailureCause::ResultSettlementFailed);
                }
                parser_enabled = false;
                child.force_process_group(process_group);
                process_group_termination_requested = true;
            }
            // jscpd:ignore-end
            () = wait_for_process_group_probe(&mut process_group_probe_clock),
                if probe_process_group => {}
        }

        // Claude settlement permits terminal drain events that Pi's native settled event does
        // not, so this otherwise shared group probe remains in the Claude driver.
        // jscpd:ignore-start
        if standard_output_closed
            && (process_completion.is_some() || wait_failed)
            && !process_group_quiescent
        {
            reap_process_group_children(process_group);
            if process_group_is_quiescent(process_group) {
                process_group_quiescent = true;
                settlement_deadline = None;
            } else if settlement_deadline.is_none() && !process_group_termination_requested {
                if cancelled.is_none() {
                    failure.get_or_insert(AgentFailureCause::HarnessProtocolFailed);
                }
                parser_enabled = false;
                child.force_process_group(process_group);
                process_group_termination_requested = true;
            }
        }
        // jscpd:ignore-end
    }

    // Final cleanup precedes Claude's parser finish and provisional-value decision; sharing
    // this block with Pi would erase its distinct native completion value.
    // jscpd:ignore-start
    if !process_group_quiescent {
        child.force_process_group(process_group);
    }
    if process_completion.is_none() {
        process_completion = child.wait().await.ok();
        if process_completion.is_none() {
            let _ = child.force_stop(process_group).await;
        }
    }
    let _ = stop_supervisor.send(());
    let supervisor_quiesced = process_supervisor.await.is_ok();

    if cancelled.is_none() {
        cancelled = cancellation_source.cancellation_reason();
    }
    if let Some(reason) = cancelled {
        return AgentOutcome::Cancelled { reason };
    }
    if let Some(cause) = failure {
        return AgentOutcome::Failed(parser.agent_failure(cause));
    }
    if wait_failed || !supervisor_quiesced {
        return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
    }
    // jscpd:ignore-end
    match emit_observations(
        invocation.observations(),
        started,
        vec![AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::HarnessQuiescent,
        }],
        &cancellation_source,
    )
    .await
    {
        ObservationProgress::Completed => {}
        ObservationProgress::Cancelled(reason) => return AgentOutcome::Cancelled { reason },
        ObservationProgress::Failed => {
            return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
        }
    }
    let Some(status) = process_completion else {
        return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
    };
    parser.finish(status.success())
}

enum InitialInputProgress {
    Ready,
    Cancelled(CancellationReason),
    Failed,
}

enum WriteProgress {
    Completed,
    Cancelled(CancellationReason),
    Failed,
}

async fn write_with_cancellation(
    input: &mut UnixStream,
    bytes: &[u8],
    cancellation: &CancellationSource,
) -> WriteProgress {
    let write = input.write_all(bytes);
    tokio::pin!(write);
    let cancelled = cancellation.wait_for_cancellation();
    tokio::pin!(cancelled);
    tokio::select! {
        biased;
        reason = &mut cancelled => WriteProgress::Cancelled(reason),
        result = &mut write => {
            if result.is_ok() {
                WriteProgress::Completed
            } else if let Some(reason) = cancellation.cancellation_reason() {
                WriteProgress::Cancelled(reason)
            } else {
                WriteProgress::Failed
            }
        }
    }
}

async fn initialize_standard_input(
    standard_input: &mut Option<UnixStream>,
    bytes: &[u8],
    close_after_write: bool,
    cancellation: &CancellationSource,
) -> InitialInputProgress {
    if let Some(reason) = cancellation.cancellation_reason() {
        standard_input.take();
        return InitialInputProgress::Cancelled(reason);
    }
    let Some(input) = standard_input.as_mut() else {
        return InitialInputProgress::Failed;
    };
    match write_with_cancellation(input, bytes, cancellation).await {
        WriteProgress::Completed if !close_after_write => InitialInputProgress::Ready,
        WriteProgress::Completed => {
            let Some(mut input) = standard_input.take() else {
                return InitialInputProgress::Failed;
            };
            let shutdown = input.shutdown();
            tokio::pin!(shutdown);
            let cancelled = cancellation.wait_for_cancellation();
            tokio::pin!(cancelled);
            tokio::select! {
                biased;
                reason = &mut cancelled => InitialInputProgress::Cancelled(reason),
                result = &mut shutdown => {
                    // NotConnected: see close_standard_input. A harness that
                    // exited after reading its prompt has already observed
                    // end of input.
                    if result.is_ok()
                        || result.is_err_and(|error| {
                            error.kind() == io::ErrorKind::NotConnected
                        })
                    {
                        InitialInputProgress::Ready
                    } else if let Some(reason) = cancellation.cancellation_reason() {
                        InitialInputProgress::Cancelled(reason)
                    } else {
                        InitialInputProgress::Failed
                    }
                }
            }
        }
        WriteProgress::Cancelled(reason) => {
            standard_input.take();
            InitialInputProgress::Cancelled(reason)
        }
        WriteProgress::Failed => {
            standard_input.take();
            InitialInputProgress::Failed
        }
    }
}

enum ObservationProgress {
    Completed,
    Cancelled(CancellationReason),
    Failed,
}

async fn emit_observations<Sink: AgentObservationSink>(
    sink: &OrderedAgentObservationSink<Sink>,
    started: &AgentStartCallback,
    observations: Vec<AgentObservation>,
    cancellation: &CancellationSource,
) -> ObservationProgress {
    for observation in observations {
        if let Some(reason) = cancellation.cancellation_reason() {
            return ObservationProgress::Cancelled(reason);
        }
        let reports_start = matches!(
            observation,
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessStarted,
            }
        );
        if reports_start && started.report().is_err() {
            return cancellation
                .cancellation_reason()
                .map_or(ObservationProgress::Failed, ObservationProgress::Cancelled);
        }
        let progress = emit_observation(sink, observation, cancellation).await;
        if !matches!(progress, ObservationProgress::Completed) {
            return progress;
        }
    }
    ObservationProgress::Completed
}

async fn emit_observation<Sink: AgentObservationSink>(
    sink: &OrderedAgentObservationSink<Sink>,
    observation: AgentObservation,
    cancellation: &CancellationSource,
) -> ObservationProgress {
    let emitted = sink.emit(observation);
    tokio::pin!(emitted);
    let cancelled = cancellation.wait_for_cancellation();
    tokio::pin!(cancelled);
    tokio::select! {
        biased;
        reason = &mut cancelled => ObservationProgress::Cancelled(reason),
        result = &mut emitted => {
            if result.is_ok() {
                ObservationProgress::Completed
            } else {
                ObservationProgress::Failed
            }
        }
    }
}

// Claude's supervisor has no Pi result-bridge settlement channel; keeping this smaller
// closed loop profile-local makes cancellation and input closure authority explicit.
// jscpd:ignore-start
async fn supervise_process_group(
    process_group: Pid,
    cancellation: CancellationSource,
    mut directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    mut shutdown: oneshot::Receiver<()>,
) {
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
                if !interrupted {
                    interrupt_process_group(process_group);
                    interrupted = true;
                }
            }
            directive = directives.recv(), if directives_open => {
                match directive {
                    Some(AgentProcessDirective::Interrupt) if !interrupted => {
                        interrupt_process_group(process_group);
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
        }
    }
}
// jscpd:ignore-end

enum ResultExchangeProgress {
    Continue,
    Accepted,
    Failed(AgentFailureCause),
    Cancelled(crate::execution::workflow::admission::CancellationReason),
}

async fn handle_result_exchange<Clock, Worker, Sink>(
    exchange: CompletedResultExchange,
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    validator: Option<&AuthoritativeResultValidator<Clock, Worker>>,
    parser: &mut ClaudeCodeStreamJsonV1Parser,
    standard_input: &mut Option<UnixStream>,
) -> ResultExchangeProgress
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    match exchange {
        CompletedResultExchange::Candidate(candidate) => {
            let Some(validator) = validator else {
                return ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed);
            };
            match validator
                .validate(candidate, invocation.cancellation())
                .await
            {
                ResultValidationOutcome::Cancelled { reason } => {
                    ResultExchangeProgress::Cancelled(reason)
                }
                ResultValidationOutcome::Decided(ResultValidationDecision::Valid(result)) => {
                    if parser.accept_result(result).is_err()
                        || close_standard_input(standard_input).await.is_err()
                    {
                        ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed)
                    } else {
                        ResultExchangeProgress::Accepted
                    }
                }
                ResultValidationOutcome::Decided(ResultValidationDecision::Rejected {
                    feedback,
                }) => reject_and_continue(invocation, parser, standard_input, feedback).await,
                ResultValidationOutcome::Decided(ResultValidationDecision::Fatal(fatal)) => {
                    ResultExchangeProgress::Failed(AgentFailureCause::from(fatal))
                }
            }
        }
        CompletedResultExchange::AmbiguousCandidate => {
            let feedback = bounded_feedback(
                AMBIGUOUS_CANDIDATE_FEEDBACK,
                invocation
                    .limits()
                    .maximum_result_rejection_feedback_bytes()
                    .get(),
            );
            reject_and_continue(invocation, parser, standard_input, feedback).await
        }
        CompletedResultExchange::MissingCandidate | CompletedResultExchange::NativeFailure => {
            if close_standard_input(standard_input).await.is_err() {
                ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed)
            } else {
                ResultExchangeProgress::Continue
            }
        }
    }
}

async fn reject_and_continue<Sink: AgentObservationSink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    parser: &mut ClaudeCodeStreamJsonV1Parser,
    standard_input: &mut Option<UnixStream>,
    feedback: Arc<str>,
) -> ResultExchangeProgress {
    match emit_observation(
        invocation.observations(),
        AgentObservation::ValueRejected {
            kind: AgentValueKind::Result,
            feedback: Arc::clone(&feedback),
        },
        invocation.cancellation(),
    )
    .await
    {
        ObservationProgress::Completed => {}
        ObservationProgress::Cancelled(reason) => {
            standard_input.take();
            return ResultExchangeProgress::Cancelled(reason);
        }
        ObservationProgress::Failed => {
            return ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed);
        }
    }
    if parser.reject_result_candidate().is_err() || parser.begin_exchange().is_err() {
        return ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed);
    }
    let frame = match initial_user_text_frame(&feedback) {
        Ok(frame) => frame,
        Err(_) => {
            return ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed);
        }
    };
    let Some(input) = standard_input.as_mut() else {
        return ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed);
    };
    match write_with_cancellation(input, &frame, invocation.cancellation()).await {
        WriteProgress::Completed => ResultExchangeProgress::Continue,
        WriteProgress::Cancelled(reason) => {
            standard_input.take();
            ResultExchangeProgress::Cancelled(reason)
        }
        WriteProgress::Failed => {
            ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed)
        }
    }
}

async fn close_standard_input(standard_input: &mut Option<UnixStream>) -> Result<(), ()> {
    let Some(mut input) = standard_input.take() else {
        return Ok(());
    };
    match input.shutdown().await {
        Ok(()) => Ok(()),
        // A write-side close only guarantees the harness observes end of
        // input. When the harness already closed its side of the socket pair,
        // macOS reports NotConnected where Linux reports success; the peer has
        // necessarily observed end of input either way.
        Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
        Err(_) => Err(()),
    }
}

fn bounded_feedback(feedback: &str, maximum_bytes: u64) -> Arc<str> {
    let maximum_bytes = usize::try_from(maximum_bytes).unwrap_or(usize::MAX);
    let mut end = feedback.len().min(maximum_bytes);
    while !feedback.is_char_boundary(end) {
        end -= 1;
    }
    Arc::from(&feedback[..end])
}

async fn wait_for_optional_deadline(wait: &mut Option<SettlementDeadlineWait>) {
    match wait {
        Some(wait) => wait.await,
        None => pending().await,
    }
}

async fn wait_for_process_group_probe<Clock: CoordinatorClock>(clock: &mut Clock) {
    let deadline = clock.now().add(PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL);
    clock.clone().wait_until(deadline).await;
}
