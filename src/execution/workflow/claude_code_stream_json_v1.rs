use std::ffi::OsString;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use serde_json::{Map, Value, json};

use super::agent::{AgentFailureCause, AgentHarnessFailureDetail};
use crate::execution::claude_code::CLAUDE_CODE_STREAM_JSON_V1_VERSION;
const MAXIMUM_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const PERMISSION_MODE: &str = "bypassPermissions";

pub(crate) const FIXED_INVOCATION_ENVIRONMENT: [(&str, &str); 5] = [
    ("DISABLE_UPDATES", "1"),
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ("CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL", "1"),
    ("CLAUDE_CODE_DISABLE_AUTO_MEMORY", "1"),
    ("CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS", "1"),
];

// Keep each closed profile's limits beside its native parser so future profile changes
// cannot silently alter another harness's admission contract.
// jscpd:ignore-start
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClaudeCodeStreamJsonV1ProtocolLimits {
    maximum_frame_bytes: NonZeroU64,
}

impl ClaudeCodeStreamJsonV1ProtocolLimits {
    pub(crate) const fn profile() -> Self {
        let Some(maximum_frame_bytes) = NonZeroU64::new(MAXIMUM_FRAME_BYTES) else {
            unreachable!();
        };
        Self {
            maximum_frame_bytes,
        }
    }
    // jscpd:ignore-end

    #[cfg(test)]
    const fn with_maximum_frame_bytes(maximum_frame_bytes: NonZeroU64) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub(crate) const fn maximum_frame_bytes(self) -> NonZeroU64 {
        self.maximum_frame_bytes
    }
}

pub(crate) fn normal_mode_arguments(
    model: &str,
    effort: &str,
    system_prompt_file: &Path,
) -> Vec<OsString> {
    [
        OsString::from("-p"),
        OsString::from("--input-format"),
        OsString::from("stream-json"),
        OsString::from("--output-format"),
        OsString::from("stream-json"),
        OsString::from("--verbose"),
        OsString::from("--include-partial-messages"),
        OsString::from("--forward-subagent-text"),
        OsString::from("--no-session-persistence"),
        OsString::from("--permission-mode"),
        OsString::from(PERMISSION_MODE),
        OsString::from("--setting-sources"),
        OsString::from("user,project,local"),
        OsString::from("--model"),
        OsString::from(model),
        OsString::from("--effort"),
        OsString::from(effort),
        OsString::from("--append-system-prompt-file"),
        system_prompt_file.as_os_str().to_owned(),
    ]
    .into()
}

pub(crate) fn initial_user_text_frame(message: &str) -> Result<Vec<u8>, serde_json::Error> {
    let mut frame = serde_json::to_vec(&json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{
                "type": "text",
                "text": message,
            }],
        },
    }))?;
    frame.push(b'\n');
    Ok(frame)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClaudeCodeStreamJsonV1Completion {
    session_id: Arc<str>,
    completed_exchanges: u64,
}

impl ClaudeCodeStreamJsonV1Completion {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) const fn completed_exchanges(&self) -> u64 {
        self.completed_exchanges
    }
}

pub(crate) struct ClaudeCodeStreamJsonV1Parser {
    expected_cwd: Arc<str>,
    expected_model: Arc<str>,
    limits: ClaudeCodeStreamJsonV1ProtocolLimits,
    frame: Vec<u8>,
    session_id: Option<Arc<str>>,
    exchange_initialized: bool,
    exchange_active: bool,
    completed_exchanges: u64,
    failure: Option<AgentFailureCause>,
}

impl ClaudeCodeStreamJsonV1Parser {
    pub(crate) fn profile(expected_cwd: Arc<str>, expected_model: Arc<str>) -> Self {
        Self::new(
            expected_cwd,
            expected_model,
            ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
        )
    }

    fn new(
        expected_cwd: Arc<str>,
        expected_model: Arc<str>,
        limits: ClaudeCodeStreamJsonV1ProtocolLimits,
    ) -> Self {
        Self {
            expected_cwd,
            expected_model,
            limits,
            frame: Vec::new(),
            session_id: None,
            exchange_initialized: false,
            exchange_active: true,
            completed_exchanges: 0,
            failure: None,
        }
    }

    /// Begins the next serialized user exchange after the preceding result was rejected.
    /// The same process and session remain authoritative for the invocation.
    pub(crate) fn begin_exchange(&mut self) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        if self.session_id.is_none() || self.exchange_active || self.exchange_initialized {
            return self.fail_protocol();
        }
        self.exchange_active = true;
        Ok(())
    }

    /// Consumes arbitrary stdout chunks while retaining at most one bounded JSONL frame.
    pub(crate) fn push_stdout(&mut self, bytes: &[u8]) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }

        // Pi and Claude intentionally retain independent native state machines; sharing
        // this small byte loop would couple their profile-specific failure transitions.
        // jscpd:ignore-start
        for &byte in bytes {
            if byte == b'\n' {
                let frame = std::mem::take(&mut self.frame);
                if let Err(failure) = self.parse_frame(&frame) {
                    self.failure = Some(failure.clone());
                    return Err(failure);
                }
                continue;
            }

            let retained = u64::try_from(self.frame.len()).unwrap_or(u64::MAX);
            if retained >= self.limits.maximum_frame_bytes().get() {
                return self.fail_protocol();
            }
            self.frame.push(byte);
        }
        // jscpd:ignore-end
        Ok(())
    }

    pub(crate) fn finish(
        self,
        exit_success: bool,
    ) -> Result<ClaudeCodeStreamJsonV1Completion, AgentFailureCause> {
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        if !self.frame.is_empty() || self.exchange_active || self.exchange_initialized {
            return Err(self.protocol_failure());
        }
        let Some(session_id) = self.session_id else {
            return Err(AgentFailureCause::HarnessStartFailed);
        };
        if self.completed_exchanges == 0 {
            return Err(AgentFailureCause::HarnessProtocolFailed);
        }
        if !exit_success {
            return Err(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            });
        }
        Ok(ClaudeCodeStreamJsonV1Completion {
            session_id,
            completed_exchanges: self.completed_exchanges,
        })
    }

    fn parse_frame(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        // This profile owns decoding failure classification because Claude's init boundary
        // differs from Pi's session-header and agent-start boundaries.
        // jscpd:ignore-start
        let frame_bytes = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        if frame_bytes > self.limits.maximum_frame_bytes().get() {
            return Err(self.protocol_failure());
        }
        let value = serde_json::from_slice::<Value>(frame).map_err(|_| self.protocol_failure())?;
        let object = value.as_object().ok_or_else(|| self.protocol_failure())?;
        // jscpd:ignore-end

        if self.session_id.is_none() {
            return self.parse_initialization(object);
        }
        if !self.exchange_active {
            return Err(self.protocol_failure());
        }

        match required_string(object, "type") {
            Some("system") if required_string(object, "subtype") == Some("init") => {
                self.parse_initialization(object)
            }
            Some("result") => self.parse_result(object),
            Some(_) if self.exchange_initialized => self.require_matching_session(object),
            _ => Err(self.protocol_failure()),
        }
    }

    fn parse_initialization(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let session_id = required_string(object, "session_id");
        if !self.exchange_active
            || self.exchange_initialized
            || required_string(object, "type") != Some("system")
            || required_string(object, "subtype") != Some("init")
            || required_string(object, "claude_code_version")
                != Some(CLAUDE_CODE_STREAM_JSON_V1_VERSION)
            || required_string(object, "cwd") != Some(self.expected_cwd.as_ref())
            || required_string(object, "model") != Some(self.expected_model.as_ref())
            || required_string(object, "permissionMode") != Some(PERMISSION_MODE)
            || !session_id.is_some_and(valid_session_id)
        {
            return Err(self.protocol_failure());
        }

        match (&self.session_id, session_id) {
            (None, Some(session_id)) => self.session_id = Some(Arc::from(session_id)),
            (Some(expected), Some(session_id)) if expected.as_ref() == session_id => {}
            _ => return Err(self.protocol_failure()),
        }
        self.exchange_initialized = true;
        Ok(())
    }

    fn parse_result(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !self.exchange_initialized
            || required_string(object, "subtype") != Some("success")
            || required_bool(object, "is_error") != Some(false)
            || required_string(object, "terminal_reason") != Some("completed")
            || required_string(object, "result").is_none()
        {
            return Err(self.protocol_failure());
        }
        self.require_matching_session(object)?;
        self.exchange_initialized = false;
        self.exchange_active = false;
        self.completed_exchanges = self
            .completed_exchanges
            .checked_add(1)
            .ok_or_else(|| self.protocol_failure())?;
        Ok(())
    }

    fn require_matching_session(
        &self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if required_string(object, "session_id")
            == self.session_id.as_ref().map(AsRef::<str>::as_ref)
        {
            Ok(())
        } else {
            Err(self.protocol_failure())
        }
    }

    // Claude initialization, rather than Pi's session-plus-agent-start sequence, owns the
    // start-to-protocol failure transition; keep that authority in this parser.
    // jscpd:ignore-start
    fn protocol_failure(&self) -> AgentFailureCause {
        if self.session_id.is_some() {
            AgentFailureCause::HarnessProtocolFailed
        } else {
            AgentFailureCause::HarnessStartFailed
        }
    }

    fn fail_protocol<T>(&mut self) -> Result<T, AgentFailureCause> {
        let failure = self.protocol_failure();
        self.failure = Some(failure.clone());
        Err(failure)
    }
    // jscpd:ignore-end
}

// These accessors and native identity checks stay profile-local because their callers
// assign different lifecycle authority and failure timing to superficially similar fields.
// jscpd:ignore-start
fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key)?.as_str()
}

fn required_bool(object: &Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key)?.as_bool()
}

fn valid_session_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes.get(index) == Some(&b'-'))
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
        && bytes
            .iter()
            .any(|byte| byte.is_ascii_hexdigit() && *byte != b'0')
}
// jscpd:ignore-end

#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
