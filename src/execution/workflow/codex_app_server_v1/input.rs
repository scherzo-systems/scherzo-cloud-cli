use std::fs::{self, File};
use std::io::Read as _;
use std::os::unix::fs::PermissionsExt as _;

use serde_json::{Value, json};

use crate::execution::workflow::agent::{
    AgentFailureCause, AgentInvocation, AgentObservationSink, StagedAgentAttachment,
};
use crate::execution::workflow::codex::CodexConfig;

use super::CodexAppServerV1ProtocolLimits;

pub(super) fn initial_turn_input<Sink>(
    invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
) -> Result<Vec<Value>, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    if invocation.attachments().len() > invocation.limits().maximum_attachments().get() {
        return Err(AgentFailureCause::HarnessSetupFailed {
            stage: crate::execution::workflow::agent::AgentHarnessSetupStage::ExecutableLaunch,
        });
    }

    let attachment_root = invocation
        .staging()
        .result_endpoint_directory()
        .parent()
        .map(|parent| parent.join("attachments"))
        .ok_or_else(launch_failure)?;
    let mut input = Vec::new();
    input
        .try_reserve_exact(invocation.attachments().len().saturating_add(1))
        .map_err(|_| launch_failure())?;
    input.push(json!({
        "type": "text",
        "text": invocation.prompt().message(),
    }));

    let mut total_bytes = 0_u64;
    for (index, attachment) in invocation.attachments().iter().enumerate() {
        let identity = format!("{index:06}");
        if !attachment.path().is_absolute()
            || attachment.path().parent() != Some(attachment_root.as_path())
            || attachment.path().file_name().and_then(|name| name.to_str())
                != Some(identity.as_str())
        {
            return Err(launch_failure());
        }
        let metadata = fs::symlink_metadata(attachment.path()).map_err(|_| launch_failure())?;
        if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o377 != 0 {
            return Err(launch_failure());
        }
        total_bytes = total_bytes
            .checked_add(metadata.len())
            .filter(|total| *total <= invocation.limits().maximum_attachment_bytes().get())
            .ok_or_else(launch_failure)?;
        input.push(attachment_input(attachment, &identity, metadata.len())?);
    }
    Ok(input)
}

// Codex keeps its exact native input union and PDF/JPEG policy profile-private;
// sharing Claude's similarly worded wrappers would couple distinct transports.
// jscpd:ignore-start
fn attachment_input(
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

    if text_media {
        let bytes = read_staged_attachment(attachment, expected_bytes)?;
        if let Ok(text) = std::str::from_utf8(&bytes) {
            return Ok(json!({
                "type": "text",
                "text": format!(
                    "Scherzo attachment {identity} ({media_type}) follows:\n{text}"
                ),
            }));
        }
    }

    if base_media_type.eq_ignore_ascii_case("image/png")
        || base_media_type.eq_ignore_ascii_case("image/jpeg")
    {
        let path = attachment.path().to_str().ok_or_else(launch_failure)?;
        return Ok(json!({
            "type": "localImage",
            "path": path,
        }));
    }

    let sealed_path = attachment.path().to_str().ok_or_else(launch_failure)?;
    Ok(json!({
        "type": "text",
        "text": format!(
            "Scherzo attachment {identity} has media type {media_type} and is available to runner tools at {sealed_path}."
        ),
    }))
}
// jscpd:ignore-end

fn read_staged_attachment(
    attachment: &StagedAgentAttachment,
    expected_bytes: u64,
) -> Result<Vec<u8>, AgentFailureCause> {
    let capacity = usize::try_from(expected_bytes).map_err(|_| launch_failure())?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| launch_failure())?;
    let mut file = File::open(attachment.path()).map_err(|_| launch_failure())?;
    file.read_to_end(&mut bytes).map_err(|_| launch_failure())?;
    if u64::try_from(bytes.len()) != Ok(expected_bytes) {
        return Err(launch_failure());
    }
    Ok(bytes)
}

fn launch_failure() -> AgentFailureCause {
    AgentFailureCause::HarnessSetupFailed {
        stage: crate::execution::workflow::agent::AgentHarnessSetupStage::ExecutableLaunch,
    }
}
