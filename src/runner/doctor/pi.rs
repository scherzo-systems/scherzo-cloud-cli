use std::collections::BTreeMap;

use super::{CheckDescriptor, DoctorCheck, Outcome, Status};
use crate::execution::pi::{
    PI_JSON_V1_SUPPORTED_RANGE, PiIncompatibility, PiInstallationFailure, PiProbe,
    discover_and_validate_pi_installation,
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
            Ok(installation) => {
                let mut details = BTreeMap::from([
                    (
                        "capabilities".to_owned(),
                        installation
                            .capabilities()
                            .required()
                            .iter()
                            .map(|capability| capability.as_str())
                            .collect::<Vec<_>>()
                            .join(","),
                    ),
                    (
                        "profile".to_owned(),
                        installation.profile().as_str().to_owned(),
                    ),
                    (
                        "supportedRange".to_owned(),
                        PI_JSON_V1_SUPPORTED_RANGE.to_owned(),
                    ),
                    (
                        "version".to_owned(),
                        installation.version().as_str().to_owned(),
                    ),
                ]);
                if let Some(executable) = installation.executable().to_str() {
                    details.insert("executablePath".to_owned(), executable.to_owned());
                }
                Outcome {
                    status: Status::Pass,
                    code: "ok",
                    message: format!(
                        "Pi {} is compatible with {}.",
                        installation.version().as_str(),
                        installation.profile().as_str()
                    ),
                    details,
                }
            }
            Err(failure) => failure_outcome(failure),
        }
    }
}

fn failure_outcome(failure: PiInstallationFailure) -> Outcome {
    match failure {
        PiInstallationFailure::Missing => Outcome::fail(
            "missing_pi_installation",
            "Pi was not found in inherited PATH; install a stable Pi release in the supported range.",
            supported_range_details(),
        ),
        PiInstallationFailure::Unexecutable => Outcome::fail(
            "unexecutable_pi_installation",
            "Pi selected from inherited PATH could not complete its validation probes.",
            supported_range_details(),
        ),
        PiInstallationFailure::Malformed(probe) => {
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
            Outcome::fail(
                code,
                message,
                BTreeMap::from([
                    (
                        "supportedRange".to_owned(),
                        PI_JSON_V1_SUPPORTED_RANGE.to_owned(),
                    ),
                    ("probe".to_owned(), probe.as_str().to_owned()),
                ]),
            )
        }
        PiInstallationFailure::Unsupported(PiIncompatibility::Version(version)) => Outcome::fail(
            "unsupported_pi_version",
            "The Pi version selected from inherited PATH is not supported; install a stable Pi release in the supported range.",
            BTreeMap::from([
                (
                    "supportedRange".to_owned(),
                    PI_JSON_V1_SUPPORTED_RANGE.to_owned(),
                ),
                ("version".to_owned(), version),
            ]),
        ),
        PiInstallationFailure::Unsupported(PiIncompatibility::Capability(capability)) => {
            Outcome::fail(
                "unsupported_pi_capability",
                "Pi selected from inherited PATH does not provide every capability required by PiJsonV1.",
                BTreeMap::from([
                    (
                        "supportedRange".to_owned(),
                        PI_JSON_V1_SUPPORTED_RANGE.to_owned(),
                    ),
                    ("capability".to_owned(), capability.as_str().to_owned()),
                ]),
            )
        }
    }
}

fn supported_range_details() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "supportedRange".to_owned(),
        PI_JSON_V1_SUPPORTED_RANGE.to_owned(),
    )])
}
