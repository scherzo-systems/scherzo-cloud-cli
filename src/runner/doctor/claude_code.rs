use std::collections::BTreeMap;

use super::{CheckDescriptor, DoctorCheck, Outcome, compatible_harness_outcome};
use crate::execution::claude_code::{
    CLAUDE_CODE_STREAM_JSON_V1_VERSION, ClaudeCodeIncompatibility, ClaudeCodeInstallationFailure,
    ClaudeCodeProbe, discover_and_validate_claude_code_installation,
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
                expected_version_details(),
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
            "Claude Code was not found in inherited PATH; install the exact supported release.",
            expected_version_details(),
        ),
        ClaudeCodeInstallationFailure::Unexecutable => Outcome::fail(
            "unexecutable_claude_code_installation",
            "Claude Code selected from inherited PATH could not complete its validation probes.",
            expected_version_details(),
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
            Outcome::fail(
                code,
                message,
                BTreeMap::from([
                    (
                        "expectedVersion".to_owned(),
                        CLAUDE_CODE_STREAM_JSON_V1_VERSION.to_owned(),
                    ),
                    ("probe".to_owned(), probe.as_str().to_owned()),
                ]),
            )
        }
        ClaudeCodeInstallationFailure::Unsupported(ClaudeCodeIncompatibility::Version(version)) => {
            Outcome::fail(
                "unsupported_claude_code_version",
                "The Claude Code version selected from inherited PATH is not the exact supported release.",
                BTreeMap::from([
                    (
                        "expectedVersion".to_owned(),
                        CLAUDE_CODE_STREAM_JSON_V1_VERSION.to_owned(),
                    ),
                    ("version".to_owned(), version),
                ]),
            )
        }
        ClaudeCodeInstallationFailure::Unsupported(ClaudeCodeIncompatibility::Capability(
            capability,
        )) => Outcome::fail(
            "unsupported_claude_code_capability",
            "Claude Code selected from inherited PATH does not provide every capability required by ClaudeCodeStreamJsonV1.",
            BTreeMap::from([
                ("capability".to_owned(), capability.as_str().to_owned()),
                (
                    "expectedVersion".to_owned(),
                    CLAUDE_CODE_STREAM_JSON_V1_VERSION.to_owned(),
                ),
            ]),
        ),
    }
}

fn expected_version_details() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "expectedVersion".to_owned(),
        CLAUDE_CODE_STREAM_JSON_V1_VERSION.to_owned(),
    )])
}
