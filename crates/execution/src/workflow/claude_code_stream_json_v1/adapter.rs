use std::ffi::{OsStr, OsString};
use std::fs;
use std::future::{Future, pending};
use std::io::{self, Write as _};
use std::ops::Add as _;
use std::os::fd::OwnedFd;
use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rustix::fs::{AtFlags, FileType, statat, symlinkat, unlinkat};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt as _;
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use super::{
    ClaudeCodeStreamJsonV1Parser, ClaudeCodeStreamJsonV1ProtocolLimits,
    ClaudeCodeStreamJsonV1RejectionReason, CompletedResultExchange, FIXED_INVOCATION_ENVIRONMENT,
    initial_user_text_frame, normal_mode_arguments, result_mode_arguments, user_content_frame,
};
use crate::claude_code::compatibility_profile_for_version;
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{
    AgentCompatibilityProfile, AgentDiagnosticLevel, AgentFailureCause, AgentInvocation,
    AgentLifecycleMilestone, AgentObservation, AgentObservationSink, AgentOutcome,
    AgentProcessDirective, AgentStartCallback, AgentValueKind, OrderedAgentObservationSink,
    PositiveDuration, StagedAgentAttachment, failed_agent_outcome, finish_agent_diagnostic_capture,
};
use crate::workflow::agent_process_driver::{
    self, StdioProcess, WriteDeadline, close_standard_input,
};
use crate::workflow::claude_code::ClaudeCodeConfig;
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::observation::ExecutionObserver;
use crate::workflow::private_staging::open_directory_path;
// Both native adapters use the same containment and validator primitives, but their
// protocol state machines decide independently when those primitives gain authority.
use crate::workflow::result_validation::{
    AuthoritativeResultValidator, ProcessResultValidationWorker, ResultValidationDecision,
    ResultValidationOutcome, ResultValidationWorker,
};

const SYSTEM_PROMPT_FILE_PREFIX: &str = "claude-code-system-prompt-";
pub(super) const MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub(super) const STANDARD_INPUT_WRITE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);
const AMBIGUOUS_CANDIDATE_FEEDBACK: &str =
    "Result rejected: submit exactly one standalone structured result candidate.\n";
pub(super) const NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC: &str =
    "Claude Code native transcript capture missing";

#[derive(Clone, Default)]
pub(crate) struct ClaudeProfile;
pub(crate) type ClaudeCodeStreamJsonV1Adapter<
    Clock,
    Observer,
    Worker = ProcessResultValidationWorker,
> = agent_process_driver::AdapterCore<Clock, Observer, Worker, ClaudeProfile>;

impl<Clock, Observer, Worker, Sink>
    agent_process_driver::NativeProcessProfile<Clock, Observer, Worker, Sink> for ClaudeProfile
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type Configuration = ClaudeCodeConfig;
    type Limits = ClaudeCodeStreamJsonV1ProtocolLimits;
    const NAME: &'static str = "claude_code_stream_json_v1";

    async fn invoke(
        adapter: &agent_process_driver::AdapterCore<Clock, Observer, Worker, Self>,
        invocation: AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
        started: &AgentStartCallback,
    ) -> AgentOutcome {
        adapter.invoke_inner(invocation, started).await
    }
}

// The concrete generic bounds are intentionally repeated per closed adapter; a shared
// erased implementation would weaken exhaustive dispatch.
impl<Clock, Observer, Worker>
    agent_process_driver::AdapterCore<Clock, Observer, Worker, ClaudeProfile>
where
    Clock: CoordinatorClock,
    Observer: ExecutionObserver<Clock::Instant>,
    Worker: ResultValidationWorker,
{
    async fn invoke_inner<Sink>(
        &self,
        invocation: AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
        started: &AgentStartCallback,
    ) -> AgentOutcome
    where
        Sink: AgentObservationSink,
    {
        // Claude's init acknowledgement and stream input make this a distinct startup
        // transition from Pi's session header and extension preparation.
        if let Some(reason) = invocation.cancellation().cancellation_reason() {
            return AgentOutcome::Cancelled { reason };
        }
        let (invocation, plan) = match self
            .prepare_invocation(
                invocation,
                prepare_launch,
                AgentFailureCause::start_failure("launch preparation", "unavailable"),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        // The shared validator is wired into a native exchange here, while Pi wires it
        // into an injected socket bridge; combining those lifecycles would hide authority.
        let validator = self.result_validator(&invocation);

        let (invocation, plan, process, standard_error, process_directives) = match self
            .launch_stdio(
                invocation,
                plan,
                AgentFailureCause::start_failure("launch preparation", "unavailable"),
            )
            .await
        {
            Ok(launched) => launched,
            Err(outcome) => return outcome,
        };
        // Diagnostic drain is tied to Claude's stream driver lifetime rather than Pi's
        // result bridge and native-session settlement.
        let diagnostic = self.start_diagnostic(&invocation, standard_error);
        let parser = ClaudeCodeStreamJsonV1Parser::profile(
            Arc::clone(&plan.expected_cwd),
            Arc::from(invocation.adapter().native_configuration().model.as_str()),
            Arc::clone(&plan.session_id),
            Arc::from(invocation.adapter().version()),
            invocation.value_mode().kind(),
            invocation.limits().maximum_response_bytes(),
        );
        let outcome = drive_process(
            &invocation,
            started,
            process,
            parser,
            process_directives,
            ClaudeDriverInput {
                bytes: &plan.input,
                validator: validator.as_ref(),
                clock: self.clock.clone(),
                settlement_grace: invocation.limits().result_settlement_grace(),
            },
        )
        .await;
        if !plan.native_session_bridge.transcript_capture_verified() {
            let _ = invocation
                .observations()
                .emit(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Warning,
                    message: Arc::from(format!(
                        "{NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC}: no transcript was written through the temporary history link"
                    )),
                })
                .await;
        }
        finish_agent_diagnostic_capture(invocation.diagnostic_session(), diagnostic, &outcome)
            .await;
        outcome
    }
}

pub(super) struct ClaudeCodeStreamJsonV1LaunchPlan {
    arguments: Vec<OsString>,
    expected_cwd: Arc<str>,
    session_id: Arc<str>,
    input: Vec<u8>,
    native_session_bridge: ClaudeCodeNativeSessionBridge,
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
    agent_process_driver::check_prompt_bounds(invocation)?;
    agent_process_driver::require_native_profile(
        invocation,
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
        compatibility_profile_for_version(invocation.adapter().version()).is_some(),
        || AgentFailureCause::start_failure("launch preparation", "unavailable"),
    )?;
    agent_process_driver::verify_session_binding(
        invocation
            .diagnostic_session()
            .verify_claude_code_native_session_path_binding(),
        "claude diagnostic session binding",
    )?;

    // Claude correlates this path in system/init and stages a native prompt file; Pi's
    // corresponding preparation uses a persisted session and injected input extension.
    let expected_cwd_path = invocation
        .process()
        .protocol_cwd()
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?;
    let session_id = Arc::from(
        invocation
            .diagnostic_session()
            .claude_code_native_session_id()
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?,
    );
    let native_session_bridge =
        ClaudeCodeNativeSessionBridge::prepare(invocation, &expected_cwd_path, &session_id)?;
    let expected_cwd =
        expected_cwd_path
            .to_str()
            .map(Arc::from)
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
    let mut system_prompt_file = tempfile::Builder::new()
        .prefix(SYSTEM_PROMPT_FILE_PREFIX)
        .tempfile_in(invocation.staging().result_endpoint_directory())
        .map_err(|error| AgentFailureCause::start_failure("claude prompt temp file", error))?;
    system_prompt_file
        .write_all(invocation.prompt().system_prompt().as_bytes())
        .and_then(|()| system_prompt_file.flush())
        .and_then(|()| {
            system_prompt_file
                .as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o400))
        })
        .map_err(|error| AgentFailureCause::start_failure("claude prompt write", error))?;

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
        native_session_bridge,
        _system_prompt_file: system_prompt_file,
    })
}

struct ClaudeCodeNativeSessionBridge {
    ambient_project_directory: OwnedFd,
    transcript_target: PathBuf,
    transcript_link_name: OsString,
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
            .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?;
        let transcript = invocation
            .diagnostic_session()
            .claude_code_native_transcript_path()
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
        let resources = invocation
            .diagnostic_session()
            .claude_code_native_resources_directory()
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
        if !transcript.is_absolute() || !resources.is_absolute() {
            return Err(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ));
        }

        if !matches!(
            fs::symlink_metadata(&transcript),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        ) {
            return Err(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ));
        }

        let config = claude_code_config_directory(invocation, expected_cwd)?;
        let project =
            config
                .join("projects")
                .join(native_project_slug(expected_cwd.to_str().ok_or(
                    AgentFailureCause::start_failure("launch preparation", "unavailable"),
                )?));
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder
            .create(&project)
            .map_err(|error| AgentFailureCause::start_failure("claude project directory", error))?;
        let project = fs::canonicalize(project).map_err(|error| {
            AgentFailureCause::start_failure("claude project canonicalization", error)
        })?;
        let ambient_project_directory = open_directory_path(&project)
            .map_err(|error| AgentFailureCause::start_failure("claude project open", error))?;
        let transcript_link_name = OsString::from(format!("{session_id}.jsonl"));
        let mut bridge = Self {
            ambient_project_directory,
            transcript_target: transcript.clone(),
            transcript_link_name: transcript_link_name.clone(),
            links: Vec::with_capacity(2),
        };
        bridge.create_link(&transcript, transcript_link_name)?;
        bridge.create_link(&resources, OsString::from(session_id))?;
        Ok(bridge)
    }

    fn transcript_capture_verified(&self) -> bool {
        let Some(link) = self
            .links
            .iter()
            .find(|link| link.name == self.transcript_link_name)
        else {
            return false;
        };
        self.owns_link(link)
            && fs::metadata(&self.transcript_target)
                .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
    }

    fn owns_link(&self, link: &OwnedAmbientSessionLink) -> bool {
        statat(
            &self.ambient_project_directory,
            &link.name,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .is_ok_and(|metadata| {
            FileType::from_raw_mode(metadata.st_mode) == FileType::Symlink
                && metadata.st_dev == link.device
                && metadata.st_ino == link.inode
        })
    }

    fn create_link(&mut self, target: &Path, name: OsString) -> Result<(), AgentFailureCause> {
        symlinkat(target, &self.ambient_project_directory, &name)
            .map_err(|error| AgentFailureCause::start_failure("claude session symlink", error))?;
        let metadata = statat(
            &self.ambient_project_directory,
            &name,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|error| AgentFailureCause::start_failure("claude session link stat", error))?;
        if FileType::from_raw_mode(metadata.st_mode) != FileType::Symlink {
            return Err(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ));
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
            if self.owns_link(link) {
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
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
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
            .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"));
    }

    let validated = agent_process_driver::validate_staged_attachments(
        invocation.attachments(),
        invocation.staging().result_endpoint_directory(),
        invocation.limits().maximum_attachments().get(),
        invocation.limits().maximum_attachment_bytes().get(),
        |_| AgentFailureCause::start_failure("launch preparation", "unavailable"),
    )?;

    let mut content = Vec::new();
    content
        .try_reserve_exact(validated.len().saturating_add(1))
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?;
    content.push(json!({
        "type": "text",
        "text": invocation.prompt().message(),
    }));
    for (attachment, identity, _) in &validated {
        content.push(attachment_reference_content_block(attachment, identity)?);
    }

    let empty_frame_bytes = user_content_frame(Vec::new())
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?
        .len();
    let mut block_bytes = content
        .iter()
        .map(serialized_content_block_bytes)
        .collect::<Result<Vec<_>, _>>()?;
    let mut frame_bytes = block_bytes
        .iter()
        .try_fold(empty_frame_bytes, |total, bytes| total.checked_add(*bytes))
        .and_then(|total| total.checked_add(content.len().saturating_sub(1)))
        .ok_or(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable",
        ))?;
    if frame_bytes > MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES {
        return Err(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable",
        ));
    }

    for (index, (attachment, identity, expected_bytes)) in validated.iter().enumerate() {
        let content_index = index.saturating_add(1);
        let reference_bytes =
            block_bytes
                .get(content_index)
                .copied()
                .ok_or(AgentFailureCause::start_failure(
                    "launch preparation",
                    "unavailable",
                ))?;
        let frame_without_reference =
            frame_bytes
                .checked_sub(reference_bytes)
                .ok_or(AgentFailureCause::start_failure(
                    "launch preparation",
                    "unavailable",
                ))?;
        let replacement_budget = MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES
            .checked_sub(frame_without_reference)
            .ok_or(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))?;
        if *expected_bytes > u64::try_from(replacement_budget).unwrap_or(u64::MAX) {
            continue;
        }
        let Some(inline) = attachment_inline_content_block(attachment, identity, *expected_bytes)?
        else {
            continue;
        };
        let inline_bytes = serialized_content_block_bytes(&inline)?;
        let candidate_frame_bytes = frame_without_reference.checked_add(inline_bytes).ok_or(
            AgentFailureCause::start_failure("launch preparation", "unavailable"),
        )?;
        if candidate_frame_bytes <= MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES {
            let content_slot =
                content
                    .get_mut(content_index)
                    .ok_or(AgentFailureCause::start_failure(
                        "launch preparation",
                        "unavailable",
                    ))?;
            *content_slot = inline;
            let block_bytes_slot =
                block_bytes
                    .get_mut(content_index)
                    .ok_or(AgentFailureCause::start_failure(
                        "launch preparation",
                        "unavailable",
                    ))?;
            *block_bytes_slot = inline_bytes;
            frame_bytes = candidate_frame_bytes;
        }
    }

    let frame = user_content_frame(content)
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))?;
    if frame.len() > MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES {
        return Err(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable",
        ));
    }
    Ok(frame)
}

fn serialized_content_block_bytes(block: &Value) -> Result<usize, AgentFailureCause> {
    serde_json::to_vec(block)
        .map(|bytes| bytes.len())
        .map_err(|_| AgentFailureCause::start_failure("launch preparation", "unavailable"))
}

fn attachment_inline_content_block(
    attachment: &StagedAgentAttachment,
    identity: &str,
    expected_bytes: u64,
) -> Result<Option<Value>, AgentFailureCause> {
    let media_type = attachment.media_type();
    let (_, text_media) = agent_process_driver::attachment_media_type(media_type);
    let native_media = if media_type.eq_ignore_ascii_case("image/png") {
        Some(("image", "image/png"))
    } else if media_type.eq_ignore_ascii_case("application/pdf") {
        Some(("document", "application/pdf"))
    } else {
        None
    };
    if !text_media && native_media.is_none() {
        return Ok(None);
    }

    let bytes = read_staged_attachment(attachment, expected_bytes)?;
    if text_media && let Ok(text) = std::str::from_utf8(&bytes) {
        return Ok(Some(agent_process_driver::attachment_text_content(
            identity, media_type, text,
        )));
    }
    Ok(native_media.map(|(block_type, native_media_type)| {
        json!({
            "type": block_type,
            "source": {
                "type": "base64",
                "media_type": native_media_type,
                "data": BASE64.encode(bytes),
            },
        })
    }))
}

fn attachment_reference_content_block(
    attachment: &StagedAgentAttachment,
    identity: &str,
) -> Result<Value, AgentFailureCause> {
    agent_process_driver::staged_attachment_reference(attachment, identity, || {
        AgentFailureCause::start_failure("launch preparation", "unavailable")
    })
}

fn read_staged_attachment(
    attachment: &StagedAgentAttachment,
    expected_bytes: u64,
) -> Result<Vec<u8>, AgentFailureCause> {
    agent_process_driver::read_staged_attachment(
        attachment,
        expected_bytes,
        |error| AgentFailureCause::start_failure("attachment capacity", error),
        |error| AgentFailureCause::start_failure("attachment read", error),
        |error| AgentFailureCause::start_failure("attachment read", error),
        |len| {
            AgentFailureCause::start_failure(
                "attachment length",
                format!("expected {expected_bytes} bytes, read {len}"),
            )
        },
    )
}

type LaunchedClaudeCodeProcess = StdioProcess;

impl<Sink: AgentObservationSink>
    agent_process_driver::StdioLaunchPlan<
        ClaudeCodeConfig,
        ClaudeCodeStreamJsonV1ProtocolLimits,
        Sink,
    > for ClaudeCodeStreamJsonV1LaunchPlan
{
    fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
    fn environment(
        &self,
        invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    ) -> Vec<(OsString, OsString)> {
        let mut environment = agent_process_driver::invocation_environment(invocation);
        environment.remove(OsStr::new("CLAUDE_CODE_PROJECT_DIR_NAME"));
        for (name, value) in FIXED_INVOCATION_ENVIRONMENT {
            environment.insert(OsString::from(name), OsString::from(value));
        }
        environment.into_iter().collect()
    }
    fn verify_binding(
        &self,
        invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    ) -> Result<(), AgentFailureCause> {
        agent_process_driver::verify_session_binding(
            invocation
                .diagnostic_session()
                .verify_claude_code_native_session_path_binding(),
            "claude diagnostic session binding",
        )
    }
    fn spawn_stage(&self) -> &'static str {
        "claude process spawn"
    }
    fn release_stage(&self) -> &'static str {
        "claude process release"
    }
    fn guard_failure(&self) -> AgentFailureCause {
        AgentFailureCause::start_failure("launch preparation", "unavailable")
    }
}

type SettlementDeadlineWait = Pin<Box<dyn Future<Output = ()> + Send>>;

enum ClaudeExtra {
    Initial(InitialInputProgress),
    SettlementExpired,
}

struct ClaudeProtocol<'a, Clock, Worker, Sink> {
    invocation: &'a AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    started: &'a AgentStartCallback,
    parser: ClaudeCodeStreamJsonV1Parser,
    validator: Option<&'a AuthoritativeResultValidator<Clock, Worker>>,
    standard_input: Option<UnixStream>,
    initial_input: Pin<Box<dyn Future<Output = InitialInputProgress> + Send + 'a>>,
    initial_input_pending: bool,
    settlement_deadline: Option<SettlementDeadlineWait>,
    settlement_grace: PositiveDuration,
    failure: Option<AgentFailureCause>,
}

impl<Clock, Worker, Sink> agent_process_driver::Protocol<Clock>
    for ClaudeProtocol<'_, Clock, Worker, Sink>
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    type Extra = ClaudeExtra;

    fn extra_enabled(&self, _state: &agent_process_driver::State<Clock>) -> bool {
        self.initial_input_pending || self.settlement_deadline.is_some()
    }

    async fn extra(&mut self) -> ClaudeExtra {
        tokio::select! {
            biased;
            progress = &mut self.initial_input, if self.initial_input_pending => ClaudeExtra::Initial(progress),
            () = wait_for_optional_deadline(&mut self.settlement_deadline), if self.settlement_deadline.is_some() => ClaudeExtra::SettlementExpired,
        }
    }

    async fn on_extra(
        &mut self,
        event: ClaudeExtra,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        match event {
            ClaudeExtra::Initial(progress) => {
                self.initial_input_pending = false;
                match progress {
                    InitialInputProgress::Ready(input) => self.standard_input = input,
                    InitialInputProgress::Cancelled(reason) => {
                        state.cancelled.get_or_insert(reason);
                        state.parser_enabled = false;
                    }
                    InitialInputProgress::Failed(reason) => {
                        if state.cancelled.is_none() && self.failure.is_none() {
                            self.failure = Some(self.parser.fail_initial_input(reason));
                        }
                        state.parser_enabled = false;
                        state.force_group();
                    }
                }
            }
            ClaudeExtra::SettlementExpired => {
                self.settlement_deadline = None;
                if let Some(reason) = state.cancellation.cancellation_reason() {
                    state.cancelled = Some(reason);
                } else {
                    self.failure = Some(AgentFailureCause::ResultSettlementFailed);
                }
                state.parser_enabled = false;
                state.force_group();
            }
        }
    }

    async fn on_cancel(
        &mut self,
        _reason: CancellationReason,
        state: &mut agent_process_driver::State<Clock>,
    ) {
        state.parser_enabled = false;
        self.settlement_deadline = None;
        self.standard_input.take();
    }

    async fn on_stdout(&mut self, bytes: &[u8], state: &mut agent_process_driver::State<Clock>) {
        let (parsed, observations) = agent_process_driver::collect_stdout_observations(|emit| {
            self.parser.push_stdout(bytes, emit)
        });
        if let Some(reason) = state.cancellation.cancellation_reason() {
            state.cancelled = Some(reason);
            state.parser_enabled = false;
            self.settlement_deadline = None;
            self.standard_input.take();
        }
        if state.parser_enabled {
            match emit_observations(
                self.invocation.observations(),
                self.started,
                observations,
                &state.cancellation,
            )
            .await
            {
                ObservationProgress::Completed => {}
                ObservationProgress::Cancelled(reason) => {
                    state.cancelled = Some(reason);
                    state.parser_enabled = false;
                    self.settlement_deadline = None;
                    self.standard_input.take();
                }
                ObservationProgress::Failed => {
                    self.failure = Some(AgentFailureCause::HarnessProtocolFailed);
                    state.parser_enabled = false;
                    state.force_group();
                }
            }
        }
        if state.parser_enabled
            && let Err(cause) = parsed
        {
            self.failure = Some(cause);
            state.parser_enabled = false;
            state.force_group();
        }
        if state.parser_enabled
            && let Some(exchange) = self.parser.take_completed_result_exchange()
        {
            match handle_result_exchange(
                exchange,
                self.invocation,
                self.validator,
                &mut self.parser,
                &mut self.standard_input,
                &mut state.clock,
            )
            .await
            {
                ResultExchangeProgress::Continue => {}
                ResultExchangeProgress::Accepted => {
                    let deadline = state.clock.now().add(self.settlement_grace.get());
                    let deadline_clock = state.clock.clone();
                    self.settlement_deadline = Some(Box::pin(async move {
                        deadline_clock.wait_until(deadline).await;
                    }));
                    match emit_observations(
                        self.invocation.observations(),
                        self.started,
                        vec![AgentObservation::Lifecycle {
                            milestone: AgentLifecycleMilestone::HarnessCompleted,
                        }],
                        &state.cancellation,
                    )
                    .await
                    {
                        ObservationProgress::Completed => {}
                        ObservationProgress::Cancelled(reason) => {
                            state.cancelled = Some(reason);
                            state.parser_enabled = false;
                            self.settlement_deadline = None;
                            self.standard_input.take();
                        }
                        ObservationProgress::Failed => {
                            self.failure = Some(AgentFailureCause::HarnessProtocolFailed);
                            state.parser_enabled = false;
                            state.force_group();
                        }
                    }
                }
                ResultExchangeProgress::Failed(cause) => {
                    self.failure = Some(cause);
                    state.parser_enabled = false;
                    state.force_group();
                }
                ResultExchangeProgress::Cancelled(reason) => {
                    state.cancelled = Some(reason);
                    state.parser_enabled = false;
                    self.settlement_deadline = None;
                    self.standard_input.take();
                }
            }
        }
    }

    fn classify_read_failure(&mut self) {
        self.failure = Some(self.parser.protocol_failure());
    }
    fn cancellation_precedes_read_failure(&self) -> bool {
        true
    }

    async fn on_wait_error(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if state.cancelled.is_none() {
            self.failure.get_or_insert(self.parser.protocol_failure());
        }
        state.force_group();
    }

    fn needs_group(&self, state: &agent_process_driver::State<Clock>) -> bool {
        self.initial_input_pending || !state.group_quiescent
    }

    fn probe_group(&self, state: &agent_process_driver::State<Clock>) -> bool {
        state.output_closed
            && (state.completion.is_some() || state.wait_failed)
            && !state.group_quiescent
    }

    fn on_group_quiescent(&mut self, _state: &mut agent_process_driver::State<Clock>) {
        self.settlement_deadline = None;
    }

    fn on_group_live(&mut self, state: &mut agent_process_driver::State<Clock>) {
        if self.settlement_deadline.is_none() && !state.termination_requested {
            if state.cancelled.is_none() {
                self.failure
                    .get_or_insert(AgentFailureCause::HarnessProtocolFailed);
            }
            state.parser_enabled = false;
            state.force_group();
        }
    }

    async fn finish(
        self,
        mut state: agent_process_driver::State<Clock>,
        supervisor_quiesced: bool,
    ) -> AgentOutcome {
        if state.cancelled.is_none() {
            state.cancelled = state.cancellation.cancellation_reason();
        }
        if let Some(reason) = state.cancelled {
            return AgentOutcome::Cancelled { reason };
        }
        if let Some(cause) = self.failure {
            return AgentOutcome::Failed(self.parser.agent_failure(cause));
        }
        if state.wait_failed || !supervisor_quiesced {
            return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
        }
        match emit_observations(
            self.invocation.observations(),
            self.started,
            vec![AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessQuiescent,
            }],
            &state.cancellation,
        )
        .await
        {
            ObservationProgress::Completed => {}
            ObservationProgress::Cancelled(reason) => return AgentOutcome::Cancelled { reason },
            ObservationProgress::Failed => {
                return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
            }
        }
        let Some(status) = state.completion else {
            return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
        };
        self.parser.finish(status.success())
    }
}

struct ClaudeDriverInput<'a, Clock, Worker> {
    bytes: &'a [u8],
    validator: Option<&'a AuthoritativeResultValidator<Clock, Worker>>,
    clock: Clock,
    settlement_grace: PositiveDuration,
}

async fn drive_process<Clock, Worker, Sink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    started: &AgentStartCallback,
    process: LaunchedClaudeCodeProcess,
    parser: ClaudeCodeStreamJsonV1Parser,
    process_directives: mpsc::UnboundedReceiver<AgentProcessDirective>,
    driver_input: ClaudeDriverInput<'_, Clock, Worker>,
) -> AgentOutcome
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
    Sink: AgentObservationSink,
{
    let ClaudeDriverInput {
        bytes,
        validator,
        clock,
        settlement_grace,
    } = driver_input;
    let (standard_input, output) = process.split();
    let cancellation = invocation.cancellation().clone();
    let initial_cancellation = cancellation.clone();
    let close_after_write = invocation.value_mode().kind() != AgentValueKind::Result;
    let initial_clock = clock.clone();
    let initial_input = Box::pin(async move {
        initialize_standard_input(
            standard_input,
            bytes,
            close_after_write,
            &initial_cancellation,
            initial_clock,
        )
        .await
    });
    let protocol = ClaudeProtocol {
        invocation,
        started,
        parser,
        validator,
        initial_input,
        initial_input_pending: true,
        standard_input: None,
        settlement_deadline: None,
        settlement_grace,
        failure: None,
    };
    agent_process_driver::drive_signalled(
        output,
        cancellation,
        clock,
        protocol,
        process_directives,
        None,
    )
    .await
}

enum InitialInputProgress {
    Ready(Option<UnixStream>),
    Cancelled(CancellationReason),
    Failed(ClaudeCodeStreamJsonV1RejectionReason),
}

enum WriteProgress {
    Completed,
    Cancelled(CancellationReason),
    Failed,
    TimedOut,
}

async fn write_with_cancellation<Clock: CoordinatorClock>(
    input: &mut UnixStream,
    bytes: &[u8],
    cancellation: &CancellationSource,
    mut clock: Clock,
) -> WriteProgress {
    let cancelled = cancellation.wait_for_cancellation();
    tokio::pin!(cancelled);
    tokio::select! {
        biased;
        reason = &mut cancelled => WriteProgress::Cancelled(reason),
        result = agent_process_driver::write_until(&mut clock, STANDARD_INPUT_WRITE_TIMEOUT, input.write_all(bytes)) => {
            if let Some(reason) = cancellation.cancellation_reason() {
                return WriteProgress::Cancelled(reason);
            }
            match result {
                Ok(()) => WriteProgress::Completed,
                Err(WriteDeadline::Failed(_)) => WriteProgress::Failed,
                Err(WriteDeadline::TimedOut) => WriteProgress::TimedOut,
            }
        }
    }
}

async fn initialize_standard_input<Clock: CoordinatorClock>(
    mut standard_input: UnixStream,
    bytes: &[u8],
    close_after_write: bool,
    cancellation: &CancellationSource,
    clock: Clock,
) -> InitialInputProgress {
    if let Some(reason) = cancellation.cancellation_reason() {
        return InitialInputProgress::Cancelled(reason);
    }
    match write_with_cancellation(&mut standard_input, bytes, cancellation, clock).await {
        WriteProgress::Completed if !close_after_write => {
            InitialInputProgress::Ready(Some(standard_input))
        }
        WriteProgress::Completed => {
            let shutdown = standard_input.shutdown();
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
                        InitialInputProgress::Ready(None)
                    } else if let Some(reason) = cancellation.cancellation_reason() {
                        InitialInputProgress::Cancelled(reason)
                    } else {
                        InitialInputProgress::Failed(
                            ClaudeCodeStreamJsonV1RejectionReason::StandardInputCloseFailed,
                        )
                    }
                }
            }
        }
        WriteProgress::Cancelled(reason) => InitialInputProgress::Cancelled(reason),
        WriteProgress::Failed => InitialInputProgress::Failed(
            ClaudeCodeStreamJsonV1RejectionReason::StandardInputWriteFailed,
        ),
        WriteProgress::TimedOut => InitialInputProgress::Failed(
            ClaudeCodeStreamJsonV1RejectionReason::StandardInputWriteTimedOut,
        ),
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

enum ResultExchangeProgress {
    Continue,
    Accepted,
    Failed(AgentFailureCause),
    Cancelled(crate::workflow::admission::CancellationReason),
}

async fn handle_result_exchange<Clock, Worker, Sink>(
    exchange: CompletedResultExchange,
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    validator: Option<&AuthoritativeResultValidator<Clock, Worker>>,
    parser: &mut ClaudeCodeStreamJsonV1Parser,
    standard_input: &mut Option<UnixStream>,
    clock: &mut Clock,
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
                }) => {
                    reject_and_continue(invocation, parser, standard_input, feedback, clock).await
                }
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
            reject_and_continue(invocation, parser, standard_input, feedback, clock).await
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

async fn reject_and_continue<Clock: CoordinatorClock, Sink: AgentObservationSink>(
    invocation: &AgentInvocation<ClaudeCodeConfig, ClaudeCodeStreamJsonV1ProtocolLimits, Sink>,
    parser: &mut ClaudeCodeStreamJsonV1Parser,
    standard_input: &mut Option<UnixStream>,
    feedback: Arc<str>,
    clock: &mut Clock,
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
    match write_with_cancellation(input, &frame, invocation.cancellation(), clock.clone()).await {
        WriteProgress::Completed => ResultExchangeProgress::Continue,
        WriteProgress::Cancelled(reason) => {
            standard_input.take();
            ResultExchangeProgress::Cancelled(reason)
        }
        WriteProgress::Failed | WriteProgress::TimedOut => {
            ResultExchangeProgress::Failed(AgentFailureCause::HarnessProtocolFailed)
        }
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
