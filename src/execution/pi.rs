use super::harness_installation::{
    ExecutableValidationFailure, HarnessInstallationProfile, ProbeIsolation, StableVersion,
    ValidatedInstallationParts, discover_and_validate_installation, parse_probe_line,
    parse_probe_text, validate_installation_with as validate_shared_installation,
    validate_selected_installation,
};
use crate::process::{CommandOutput, CommandRunner, SystemCommandRunner};
use std::ffi::OsStr;
use std::fmt;
use std::path::{Path, PathBuf};

pub(crate) const PI_JSON_V1_SUPPORTED_RANGE: &str = ">=0.84.2 <0.85.0";
pub(crate) const PI_JSON_V1_QUALIFICATION_VERSION: &str = "0.84.2";
const PI_JSON_V1_MINIMUM_VERSION: (u64, u64, u64) = (0, 84, 2);
const PI_JSON_V1_MAXIMUM_VERSION: (u64, u64, u64) = (0, 85, 0);
const CAPABILITY_PROBE_ARGUMENTS: [&str; 7] = [
    "--no-approve",
    "--no-extensions",
    "--no-skills",
    "--no-prompt-templates",
    "--no-themes",
    "--no-context-files",
    "--help",
];

const REQUIRED_CAPABILITIES: [PiCapability; 5] = [
    PiCapability::JsonEventStream,
    PiCapability::CustomSessionDirectory,
    PiCapability::ExtensionLoading,
    PiCapability::SystemPromptAppend,
    PiCapability::InvocationScopedProjectTrust,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PiCompatibilityProfile {
    PiJsonV1,
}

impl PiCompatibilityProfile {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PiJsonV1 => "PiJsonV1",
        }
    }
}

pub(crate) type PiVersion = StableVersion;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PiCapability {
    JsonEventStream,
    CustomSessionDirectory,
    ExtensionLoading,
    SystemPromptAppend,
    InvocationScopedProjectTrust,
}

impl PiCapability {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::JsonEventStream => "json_event_stream",
            Self::CustomSessionDirectory => "custom_session_directory",
            Self::ExtensionLoading => "extension_loading",
            Self::SystemPromptAppend => "system_prompt_append",
            Self::InvocationScopedProjectTrust => "invocation_scoped_project_trust",
        }
    }

    fn is_advertised_by(self, help: &str) -> bool {
        help.lines().map(str::trim_start).any(|line| match self {
            Self::JsonEventStream => line.starts_with("--mode <mode>") && line.contains("json"),
            Self::CustomSessionDirectory => line.starts_with("--session-dir <dir> "),
            Self::ExtensionLoading => line.starts_with("--extension, -e <path> "),
            Self::SystemPromptAppend => line.starts_with("--append-system-prompt <text> "),
            Self::InvocationScopedProjectTrust => line.starts_with("--approve, -a "),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PiJsonV1Capabilities {
    required: [PiCapability; REQUIRED_CAPABILITIES.len()],
}

impl PiJsonV1Capabilities {
    pub(crate) fn required(&self) -> &[PiCapability] {
        &self.required
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedPiInstallation {
    executable: PathBuf,
    version: PiVersion,
    profile: PiCompatibilityProfile,
    capabilities: PiJsonV1Capabilities,
}

impl ValidatedPiInstallation {
    #[cfg(test)]
    pub(crate) fn fixture(executable: PathBuf) -> Self {
        Self {
            executable,
            version: PiVersion::parse(PI_JSON_V1_QUALIFICATION_VERSION).unwrap(),
            profile: PiCompatibilityProfile::PiJsonV1,
            capabilities: PiJsonV1Capabilities {
                required: REQUIRED_CAPABILITIES,
            },
        }
    }

    pub(crate) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(crate) const fn version(&self) -> &PiVersion {
        &self.version
    }

    pub(crate) const fn profile(&self) -> PiCompatibilityProfile {
        self.profile
    }

    pub(crate) const fn capabilities(&self) -> &PiJsonV1Capabilities {
        &self.capabilities
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PiProbe {
    Version,
    Capabilities,
}

impl PiProbe {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::Capabilities => "capabilities",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PiIncompatibility {
    Version(String),
    Capability {
        capability: PiCapability,
        version: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PiInstallationFailure {
    Missing,
    Unexecutable {
        version: Option<String>,
    },
    Malformed {
        probe: PiProbe,
        version: Option<String>,
    },
    Unsupported(PiIncompatibility),
}

impl fmt::Display for PiInstallationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => write!(
                formatter,
                "Pi was not found in inherited PATH; install a stable Pi release in range {PI_JSON_V1_SUPPORTED_RANGE}"
            ),
            Self::Unexecutable { .. } => formatter.write_str(
                "Pi selected from inherited PATH could not complete its validation probes",
            ),
            Self::Malformed {
                probe: PiProbe::Version,
                ..
            } => {
                formatter.write_str("Pi selected from inherited PATH returned a malformed version")
            }
            Self::Malformed {
                probe: PiProbe::Capabilities,
                ..
            } => formatter
                .write_str("Pi selected from inherited PATH returned malformed capability help"),
            Self::Unsupported(PiIncompatibility::Version(version)) => write!(
                formatter,
                "Pi version {version} selected from inherited PATH is unsupported; install a stable Pi release in range {PI_JSON_V1_SUPPORTED_RANGE}"
            ),
            Self::Unsupported(PiIncompatibility::Capability { capability, .. }) => write!(
                formatter,
                "Pi selected from inherited PATH lacks the required {} capability",
                capability.as_str()
            ),
        }
    }
}

impl std::error::Error for PiInstallationFailure {}

struct PiInstallationProfile;

impl HarnessInstallationProfile for PiInstallationProfile {
    type Version = PiVersion;
    type CompatibilityProfile = PiCompatibilityProfile;
    type Capabilities = PiJsonV1Capabilities;
    type Installation = ValidatedPiInstallation;
    type Failure = PiInstallationFailure;

    const EXECUTABLE_NAME: &'static str = "pi";
    const CAPABILITY_PROBE_ARGUMENTS: &'static [&'static str] = &CAPABILITY_PROBE_ARGUMENTS;

    fn probe_isolation(search_path: &OsStr) -> Result<ProbeIsolation, Self::Failure> {
        let mut isolation =
            ProbeIsolation::create("scherzo-pi-validation-", &["agent"], search_path)
                .map_err(|()| Self::unexecutable_failure())?;
        isolation.add_directory_environment("PI_CODING_AGENT_DIR", "agent");
        isolation.add_literal_environment("PI_OFFLINE", "1");
        isolation.add_literal_environment("PI_SKIP_VERSION_CHECK", "1");
        isolation.add_literal_environment("PI_TELEMETRY", "0");
        Ok(isolation)
    }

    fn parse_version_output(output: &CommandOutput) -> Result<Self::Version, Self::Failure> {
        parse_version_output(output)
    }

    fn compatibility_profile(version: &Self::Version) -> Option<Self::CompatibilityProfile> {
        compatibility_profile(version)
    }

    fn unsupported_version(version: &Self::Version) -> Self::Failure {
        PiInstallationFailure::Unsupported(PiIncompatibility::Version(version.as_str().to_owned()))
    }

    fn validate_capability_output(
        output: &CommandOutput,
        _isolation: &ProbeIsolation,
        _executable: &Path,
        version: &Self::Version,
        _profile: &Self::CompatibilityProfile,
    ) -> Result<Self::Capabilities, Self::Failure> {
        validate_capability_output(output, version)?;
        Ok(PiJsonV1Capabilities {
            required: REQUIRED_CAPABILITIES,
        })
    }

    fn capability_probe_failure(
        _executable: &Path,
        version: &Self::Version,
        _profile: &Self::CompatibilityProfile,
    ) -> Self::Failure {
        PiInstallationFailure::Unexecutable {
            version: Some(version.as_str().to_owned()),
        }
    }

    fn installation(
        parts: ValidatedInstallationParts<
            Self::Version,
            Self::CompatibilityProfile,
            Self::Capabilities,
        >,
    ) -> Self::Installation {
        let (executable, version, profile, capabilities) = parts.into_parts();
        ValidatedPiInstallation {
            executable,
            version,
            profile,
            capabilities,
        }
    }

    fn executable_failure(failure: ExecutableValidationFailure) -> Self::Failure {
        match failure {
            ExecutableValidationFailure::Missing => PiInstallationFailure::Missing,
            ExecutableValidationFailure::Unexecutable => Self::unexecutable_failure(),
        }
    }

    fn unexecutable_failure() -> Self::Failure {
        PiInstallationFailure::Unexecutable { version: None }
    }
}

pub(crate) fn discover_and_validate_pi_installation()
-> Result<ValidatedPiInstallation, PiInstallationFailure> {
    discover_and_validate_installation::<PiInstallationProfile>(&SystemCommandRunner)
}

pub(crate) fn validate_pi_installation(
    selected_executable: &Path,
) -> Result<ValidatedPiInstallation, PiInstallationFailure> {
    validate_selected_installation::<PiInstallationProfile>(
        selected_executable,
        &SystemCommandRunner,
    )
}

fn validate_pi_installation_with(
    selected_executable: &Path,
    search_path: &OsStr,
    runner: &dyn CommandRunner,
) -> Result<ValidatedPiInstallation, PiInstallationFailure> {
    validate_shared_installation::<PiInstallationProfile>(selected_executable, search_path, runner)
}

fn parse_version_output(output: &CommandOutput) -> Result<PiVersion, PiInstallationFailure> {
    let malformed = || PiInstallationFailure::Malformed {
        probe: PiProbe::Version,
        version: None,
    };
    let version = parse_probe_line(output).ok_or_else(malformed)?;
    PiVersion::parse(version).ok_or_else(malformed)
}

fn compatibility_profile(version: &PiVersion) -> Option<PiCompatibilityProfile> {
    (version.numeric() >= PI_JSON_V1_MINIMUM_VERSION
        && version.numeric() < PI_JSON_V1_MAXIMUM_VERSION)
        .then_some(PiCompatibilityProfile::PiJsonV1)
}

pub(crate) fn compatibility_profile_for_version(observed: &str) -> Option<PiCompatibilityProfile> {
    compatibility_profile(&PiVersion::parse(observed)?)
}

fn validate_capability_output(
    output: &CommandOutput,
    version: &PiVersion,
) -> Result<(), PiInstallationFailure> {
    let malformed = || PiInstallationFailure::Malformed {
        probe: PiProbe::Capabilities,
        version: Some(version.as_str().to_owned()),
    };
    let help = parse_probe_text(output).ok_or_else(malformed)?;
    if !help.starts_with("pi ")
        || !help
            .lines()
            .any(|line| line.trim() == "pi [options] [@files...] [messages...]")
    {
        return Err(malformed());
    }
    if let Some(capability) = REQUIRED_CAPABILITIES
        .into_iter()
        .find(|capability| !capability.is_advertised_by(help))
    {
        return Err(PiInstallationFailure::Unsupported(
            PiIncompatibility::Capability {
                capability,
                version: version.as_str().to_owned(),
            },
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
