use std::collections::BTreeMap;

use super::{CheckDescriptor, DoctorCheck, Outcome, compatible_harness_outcome};
use crate::execution::claude_code::{
    CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION, CLAUDE_CODE_STREAM_JSON_V1_SUPPORTED_RANGE,
    ClaudeCodeIncompatibility, ClaudeCodeInstallationFailure, ClaudeCodeProbe,
    discover_and_validate_claude_code_installation,
};

pub(super) struct ClaudeCodeCheck;

impl DoctorCheck for ClaudeCodeCheck {
    fn descriptor(&self) -> CheckDescriptor {
        CheckDescriptor {
            id: "execution.harness.claude-code-stream-json-v1",
            title: "ClaudeCodeStreamJsonV1 installation",
            default: false,
        }
    }

    // Doctor keeps a harness-local adapter so Pi and Claude reports stay independently selectable.
    // jscpd:ignore-start
    fn run(&self) -> Outcome {
        match discover_and_validate_claude_code_installation() {
            Ok(installation) => compatible_harness_outcome(
                "Claude Code",
                installation.version().as_str(),
                installation.profile().as_str(),
                installation
                    .capabilities()
                    .required()
                    .iter()
                    .map(|capability| capability.as_str()),
                installation.executable(),
                compatibility_details(),
            ),
            Err(failure) => failure_outcome(failure),
        }
    }
    // jscpd:ignore-end
}

fn failure_outcome(failure: ClaudeCodeInstallationFailure) -> Outcome {
    match failure {
        ClaudeCodeInstallationFailure::Missing => Outcome::fail(
            "missing_claude_code_installation",
            "Claude Code was not found in inherited PATH; install a stable release in the supported range.",
            compatibility_details(),
        ),
        ClaudeCodeInstallationFailure::Unexecutable => Outcome::fail(
            "unexecutable_claude_code_installation",
            "Claude Code selected from inherited PATH could not complete its validation probes.",
            compatibility_details(),
        ),
        ClaudeCodeInstallationFailure::Malformed(probe) => {
            let (code, message) = match probe {
                ClaudeCodeProbe::Version => (
                    "malformed_claude_code_version",
                    "Claude Code selected from inherited PATH returned a malformed version.",
                ),
                ClaudeCodeProbe::Capabilities => (
                    "malformed_claude_code_capabilities",
                    "Claude Code selected from inherited PATH returned malformed capability help.",
                ),
            };
            let mut details = compatibility_details();
            details.insert("probe".to_owned(), probe.as_str().to_owned());
            Outcome::fail(code, message, details)
        }
        ClaudeCodeInstallationFailure::Unsupported(ClaudeCodeIncompatibility::Version(version)) => {
            let mut details = compatibility_details();
            details.insert("version".to_owned(), version);
            Outcome::fail(
                "unsupported_claude_code_version",
                "The Claude Code version selected from inherited PATH is outside the supported stable release range.",
                details,
            )
        }
        ClaudeCodeInstallationFailure::Unsupported(ClaudeCodeIncompatibility::Capability(
            capability,
        )) => {
            let mut details = compatibility_details();
            details.insert("capability".to_owned(), capability.as_str().to_owned());
            Outcome::fail(
                "unsupported_claude_code_capability",
                "Claude Code selected from inherited PATH does not provide every capability required by ClaudeCodeStreamJsonV1.",
                details,
            )
        }
    }
}

fn compatibility_details() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "qualificationVersion".to_owned(),
            CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION.to_owned(),
        ),
        (
            "supportedRange".to_owned(),
            CLAUDE_CODE_STREAM_JSON_V1_SUPPORTED_RANGE.to_owned(),
        ),
    ])
}
