use std::collections::BTreeMap;
use std::path::PathBuf;

use super::{CheckDescriptor, DoctorCheck, Outcome, Status};
use crate::execution::pi::{
    PI_JSON_V1_SUPPORTED_RANGE, PiIncompatibility, PiInstallationFailure, PiProbe,
    validate_pi_installation,
};

pub(super) struct PiCheck {
    configured_executable: Option<PathBuf>,
}

impl PiCheck {
    pub(super) fn new(configured_executable: Option<PathBuf>) -> Self {
        Self {
            configured_executable,
        }
    }
}

impl DoctorCheck for PiCheck {
    fn descriptor(&self) -> CheckDescriptor {
        CheckDescriptor {
            id: "execution.harness.pi-json-v1",
            title: "PiJsonV1 installation",
            default: self.configured_executable.is_some(),
        }
    }

    fn run(&self) -> Outcome {
        let Some(configured_executable) = &self.configured_executable else {
            return Outcome::fail(
                "pi_not_configured",
                "Configure --pi-executable to validate PiJsonV1 support.",
                BTreeMap::new(),
            );
        };

        match validate_pi_installation(configured_executable) {
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
            "The configured Pi executable was not found; install a stable Pi release in the supported range or correct --pi-executable.",
            supported_range_details(),
        ),
        PiInstallationFailure::Unexecutable => Outcome::fail(
            "unexecutable_pi_installation",
            "The configured Pi executable could not complete its validation probes.",
            supported_range_details(),
        ),
        PiInstallationFailure::Malformed(probe) => {
            let (code, message) = match probe {
                PiProbe::Version => (
                    "malformed_pi_version",
                    "The configured Pi executable returned a malformed version.",
                ),
                PiProbe::Capabilities => (
                    "malformed_pi_capabilities",
                    "The configured Pi executable returned malformed capability help.",
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
            "The configured Pi version is not supported; install a stable Pi release in the supported range.",
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
                "The configured Pi executable does not provide every capability required by PiJsonV1.",
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
