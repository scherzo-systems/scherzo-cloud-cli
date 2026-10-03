use serde_json::{Value, json};

use crate::workflow::agent::{
    AgentFailureCause, AgentInvocation, AgentObservationSink, StagedAgentAttachment,
};
use crate::workflow::codex::CodexConfig;

use super::CodexAppServerV1ProtocolLimits;

pub(super) fn initial_turn_input<Sink>(
    invocation: &AgentInvocation<CodexConfig, CodexAppServerV1ProtocolLimits, Sink>,
) -> Result<Vec<Value>, AgentFailureCause>
where
    Sink: AgentObservationSink,
{
    let validated = crate::workflow::agent_process_driver::validate_staged_attachments(
        invocation.attachments(),
        invocation.staging().result_endpoint_directory(),
        invocation.limits().maximum_attachments().get(),
        invocation.limits().maximum_attachment_bytes().get(),
        |error| {
            error.map_or_else(launch_failure, |error| {
                AgentFailureCause::start_failure("codex attachment stat", error)
            })
        },
    )?;
    let mut input = Vec::new();
    input
        .try_reserve_exact(invocation.attachments().len().saturating_add(1))
        .map_err(|_| launch_failure())?;
    input.push(json!({
        "type": "text",
        "text": invocation.prompt().message(),
    }));

    for (attachment, identity, expected_bytes) in validated {
        input.push(attachment_input(attachment, &identity, expected_bytes)?);
    }
    Ok(input)
}

// Codex keeps its exact native input union and PDF/JPEG policy profile-private;
// sharing Claude's similarly worded wrappers would couple distinct transports.
fn attachment_input(
    attachment: &StagedAgentAttachment,
    identity: &str,
    expected_bytes: u64,
) -> Result<Value, AgentFailureCause> {
    let media_type = attachment.media_type();
    let (base_media_type, text_media) =
        crate::workflow::agent_process_driver::attachment_media_type(media_type);

    if text_media {
        let bytes = read_staged_attachment(attachment, expected_bytes)?;
        if let Ok(text) = std::str::from_utf8(&bytes) {
            return Ok(
                crate::workflow::agent_process_driver::attachment_text_content(
                    identity, media_type, text,
                ),
            );
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

    crate::workflow::agent_process_driver::staged_attachment_reference(
        attachment,
        identity,
        launch_failure,
    )
}

fn read_staged_attachment(
    attachment: &StagedAgentAttachment,
    expected_bytes: u64,
) -> Result<Vec<u8>, AgentFailureCause> {
    crate::workflow::agent_process_driver::read_staged_attachment(
        attachment,
        expected_bytes,
        |_| launch_failure(),
        |error| AgentFailureCause::start_failure("codex attachment open", error),
        |error| AgentFailureCause::start_failure("codex attachment read", error),
        |_| launch_failure(),
    )
}

fn launch_failure() -> AgentFailureCause {
    AgentFailureCause::HarnessSetupFailed {
        stage: crate::workflow::agent::AgentHarnessSetupStage::ExecutableLaunch,
    }
}
