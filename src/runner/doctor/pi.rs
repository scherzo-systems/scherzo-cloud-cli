use std::collections::BTreeMap;

use super::{
    CheckDescriptor, DoctorCheck, Outcome, capability_failure_details, compatible_harness_outcome,
};
use crate::execution::pi::{
    PI_JSON_V1_QUALIFICATION_VERSION, PI_JSON_V1_SUPPORTED_RANGE, PiCompatibilityProfile,
    PiIncompatibility, PiInstallationFailure, PiProbe, discover_and_validate_pi_installation,
};

pub(super) struct PiCheck;

impl DoctorCheck for PiCheck {
    fn descriptor(&self) -> CheckDescriptor {
        CheckDescriptor {
            id: "execution.harness.pi-json-v1",
            title: "PiJsonV1 installation",
            default: false,
        }
    }

    fn run(&self) -> Outcome {
        match discover_and_validate_pi_installation() {
            Ok(installation) => compatible_harness_outcome(
                "Pi",
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
}

fn failure_outcome(failure: PiInstallationFailure) -> Outcome {
    match failure {
        PiInstallationFailure::Missing => Outcome::fail(
            "missing_pi_installation",
            "Pi was not found in inherited PATH; install a stable Pi release in the supported range.",
            compatibility_details(),
        ),
        PiInstallationFailure::Unexecutable { version } => {
            let mut details = compatibility_details();
            if let Some(version) = version {
                details.insert("version".to_owned(), version);
            }
            Outcome::fail(
                "unexecutable_pi_installation",
                "Pi selected from inherited PATH could not complete its validation probes.",
                details,
            )
        }
        PiInstallationFailure::Malformed { probe, version } => {
            let (code, message) = match probe {
                PiProbe::Version => (
                    "malformed_pi_version",
                    "Pi selected from inherited PATH returned a malformed version.",
                ),
                PiProbe::Capabilities => (
                    "malformed_pi_capabilities",
                    "Pi selected from inherited PATH returned malformed capability help.",
                ),
            };
            let mut details = compatibility_details();
            details.insert("probe".to_owned(), probe.as_str().to_owned());
            if let Some(version) = version {
                details.insert("version".to_owned(), version);
            }
            Outcome::fail(code, message, details)
        }
        PiInstallationFailure::Unsupported(PiIncompatibility::Version(version)) => {
            let mut details = compatibility_details();
            details.insert("version".to_owned(), version);
            Outcome::fail(
                "unsupported_pi_version",
                "The Pi version selected from inherited PATH is not supported; install a stable Pi release in the supported range.",
                details,
            )
        }
        PiInstallationFailure::Unsupported(PiIncompatibility::Capability {
            capability,
            version,
        }) => {
            let details =
                capability_failure_details(compatibility_details(), capability.as_str(), version);
            Outcome::fail(
                "unsupported_pi_capability",
                "Pi selected from inherited PATH does not provide every capability required by PiJsonV1.",
                details,
            )
        }
    }
}

fn compatibility_details() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "profile".to_owned(),
            PiCompatibilityProfile::PiJsonV1.as_str().to_owned(),
        ),
        (
            "qualificationVersion".to_owned(),
            PI_JSON_V1_QUALIFICATION_VERSION.to_owned(),
        ),
        (
            "supportedRange".to_owned(),
            PI_JSON_V1_SUPPORTED_RANGE.to_owned(),
        ),
    ])
}
