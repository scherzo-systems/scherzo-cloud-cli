use std::collections::BTreeMap;

use super::{CheckDescriptor, DoctorCheck, Outcome, compatible_harness_outcome};
use crate::execution::codex::{
    CODEX_APP_SERVER_V1_QUALIFICATION_VERSION, CODEX_APP_SERVER_V1_SUPPORTED_RANGE,
    CodexIncompatibility, CodexInstallationFailure, CodexProbe,
    discover_and_validate_codex_installation,
};

pub(super) struct CodexCheck;

impl DoctorCheck for CodexCheck {
    fn descriptor(&self) -> CheckDescriptor {
        CheckDescriptor {
            id: "execution.harness.codex-app-server-v1",
            title: "CodexAppServerV1 installation",
            default: false,
        }
    }

    // Doctor keeps a profile-local adapter so Codex remains independently selectable.
    // jscpd:ignore-start
    fn run(&self) -> Outcome {
        match discover_and_validate_codex_installation() {
            Ok(installation) => compatible_harness_outcome(
                "Codex CLI",
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

fn failure_outcome(failure: CodexInstallationFailure) -> Outcome {
    let mut details = compatibility_details();
    if let Some(identity) = failure.identity() {
        details.insert("profile".to_owned(), identity.profile().as_str().to_owned());
        details.insert("version".to_owned(), identity.version().as_str().to_owned());
        if let Some(executable) = identity.executable().to_str() {
            details.insert("executablePath".to_owned(), executable.to_owned());
        }
    }

    match failure {
        CodexInstallationFailure::Missing => Outcome::fail(
            "missing_codex_installation",
            "Codex CLI was not found in inherited PATH; install a stable release in the supported range.",
            details,
        ),
        CodexInstallationFailure::Unexecutable { .. } => Outcome::fail(
            "unexecutable_codex_installation",
            "Codex CLI selected from inherited PATH could not complete its validation probes.",
            details,
        ),
        CodexInstallationFailure::Malformed { probe, .. } => {
            let (code, message) = match probe {
                CodexProbe::Version => (
                    "malformed_codex_version",
                    "Codex CLI selected from inherited PATH returned a malformed stable version.",
                ),
                CodexProbe::AppServerSchema => (
                    "malformed_codex_app_server_schema",
                    "Codex CLI selected from inherited PATH returned malformed App Server schemas.",
                ),
            };
            details.insert("probe".to_owned(), probe.as_str().to_owned());
            Outcome::fail(code, message, details)
        }
        CodexInstallationFailure::Unsupported {
            incompatibility: CodexIncompatibility::Version(version),
            ..
        } => {
            details.insert("version".to_owned(), version);
            Outcome::fail(
                "unsupported_codex_version",
                "The Codex CLI version selected from inherited PATH is not in the supported stable release line.",
                details,
            )
        }
        CodexInstallationFailure::Unsupported {
            incompatibility: CodexIncompatibility::Capability(capability),
            ..
        } => {
            details.insert("capability".to_owned(), capability.as_str().to_owned());
            Outcome::fail(
                "unsupported_codex_capability",
                "Codex CLI selected from inherited PATH does not provide a capability required by CodexAppServerV1.",
                details,
            )
        }
    }
}

fn compatibility_details() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "qualificationVersion".to_owned(),
            CODEX_APP_SERVER_V1_QUALIFICATION_VERSION.to_owned(),
        ),
        (
            "supportedRange".to_owned(),
            CODEX_APP_SERVER_V1_SUPPORTED_RANGE.to_owned(),
        ),
    ])
}
