use std::ffi::OsStr;
use std::fmt;
use std::path::{Path, PathBuf};

// This harness consumes the shared installation lifecycle but owns all profile policy below.
// jscpd:ignore-start
use super::harness_installation::{
    ExecutableValidationFailure, HarnessInstallationProfile, ProbeIsolation,
    ValidatedInstallationParts, discover_and_validate_installation, parse_numeric_component,
    parse_probe_line, parse_probe_text, validate_installation_with as validate_shared_installation,
    validate_selected_installation,
};
use crate::process::{CommandOutput, CommandRunner, SystemCommandRunner};
// jscpd:ignore-end

pub(crate) const CLAUDE_CODE_STREAM_JSON_V1_VERSION: &str = "2.1.234";
const CAPABILITY_PROBE_ARGUMENTS: [&str; 1] = ["--help"];

const REQUIRED_CAPABILITIES: [ClaudeCodeCapability; 13] = [
    ClaudeCodeCapability::PrintMode,
    ClaudeCodeCapability::StreamJsonInput,
    ClaudeCodeCapability::StreamJsonOutput,
    ClaudeCodeCapability::Verbose,
    ClaudeCodeCapability::PartialMessages,
    ClaudeCodeCapability::ForwardSubagentText,
    ClaudeCodeCapability::SessionId,
    ClaudeCodeCapability::PermissionMode,
    ClaudeCodeCapability::SettingSources,
    ClaudeCodeCapability::Model,
    ClaudeCodeCapability::Effort,
    ClaudeCodeCapability::AppendSystemPromptFile,
    ClaudeCodeCapability::JsonSchema,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClaudeCodeCompatibilityProfile {
    ClaudeCodeStreamJsonV1,
}

impl ClaudeCodeCompatibilityProfile {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCodeStreamJsonV1 => "ClaudeCodeStreamJsonV1",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClaudeCodeVersion(Box<str>);

impl ClaudeCodeVersion {
    fn parse(observed: &str) -> Option<Self> {
        let version = observed.strip_suffix(" (Claude Code)")?;
        let mut components = version.split('.');
        for _ in 0..3 {
            parse_numeric_component(components.next()?)?;
        }
        if components.next().is_some() {
            return None;
        }
        Some(Self(version.into()))
    }

    #[cfg(test)]
    fn exact() -> Self {
        Self(CLAUDE_CODE_STREAM_JSON_V1_VERSION.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClaudeCodeCapability {
    PrintMode,
    StreamJsonInput,
    StreamJsonOutput,
    Verbose,
    PartialMessages,
    ForwardSubagentText,
    SessionId,
    PermissionMode,
    SettingSources,
    Model,
    Effort,
    AppendSystemPromptFile,
    JsonSchema,
}

impl ClaudeCodeCapability {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PrintMode => "print_mode",
            Self::StreamJsonInput => "stream_json_input",
            Self::StreamJsonOutput => "stream_json_output",
            Self::Verbose => "verbose",
            Self::PartialMessages => "partial_messages",
            Self::ForwardSubagentText => "forward_subagent_text",
            Self::SessionId => "session_id",
            Self::PermissionMode => "permission_mode",
            Self::SettingSources => "setting_sources",
            Self::Model => "model",
            Self::Effort => "effort",
            Self::AppendSystemPromptFile => "append_system_prompt_file",
            Self::JsonSchema => "json_schema",
        }
    }

    fn is_advertised_by(self, help: &str) -> bool {
        match self {
            Self::PrintMode => has_option(help, "-p, --print"),
            Self::StreamJsonInput => has_option(help, "--input-format <format>"),
            Self::StreamJsonOutput => has_option(help, "--output-format <format>"),
            Self::Verbose => has_option(help, "--verbose"),
            Self::PartialMessages => has_option(help, "--include-partial-messages"),
            Self::ForwardSubagentText => has_option(help, "--forward-subagent-text"),
            Self::SessionId => has_option(help, "--session-id <uuid>"),
            Self::PermissionMode => has_option(help, "--permission-mode <mode>"),
            Self::SettingSources => has_option(help, "--setting-sources <sources>"),
            Self::Model => has_option(help, "--model <model>"),
            Self::Effort => has_option(help, "--effort <level>"),
            Self::AppendSystemPromptFile => help.contains("--append-system-prompt[-file]"),
            Self::JsonSchema => has_option(help, "--json-schema <schema>"),
        }
    }
}

fn has_option(help: &str, prefix: &str) -> bool {
    help.lines()
        .map(str::trim_start)
        .any(|line| line.starts_with(prefix))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClaudeCodeStreamJsonV1Capabilities {
    required: [ClaudeCodeCapability; REQUIRED_CAPABILITIES.len()],
}

impl ClaudeCodeStreamJsonV1Capabilities {
    pub(crate) fn required(&self) -> &[ClaudeCodeCapability] {
        &self.required
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedClaudeCodeInstallation {
    executable: PathBuf,
    version: ClaudeCodeVersion,
    profile: ClaudeCodeCompatibilityProfile,
    capabilities: ClaudeCodeStreamJsonV1Capabilities,
}

impl ValidatedClaudeCodeInstallation {
    #[cfg(test)]
    pub(crate) fn fixture(executable: PathBuf) -> Self {
        Self {
            executable,
            version: ClaudeCodeVersion::exact(),
            profile: ClaudeCodeCompatibilityProfile::ClaudeCodeStreamJsonV1,
            capabilities: ClaudeCodeStreamJsonV1Capabilities {
                required: REQUIRED_CAPABILITIES,
            },
        }
    }

    pub(crate) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(crate) fn version(&self) -> &ClaudeCodeVersion {
        &self.version
    }

    pub(crate) const fn profile(&self) -> ClaudeCodeCompatibilityProfile {
        self.profile
    }

    pub(crate) const fn capabilities(&self) -> &ClaudeCodeStreamJsonV1Capabilities {
        &self.capabilities
    }
}

// Probe labels remain harness-specific because they are part of Claude diagnostics.
// jscpd:ignore-start
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClaudeCodeProbe {
    Version,
    Capabilities,
}

impl ClaudeCodeProbe {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::Capabilities => "capabilities",
        }
    }
}
// jscpd:ignore-end

// Claude diagnostics stay harness-specific so adding Pi failure policy cannot alter Claude codes.
// jscpd:ignore-start
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClaudeCodeIncompatibility {
    Version(String),
    Capability(ClaudeCodeCapability),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClaudeCodeInstallationFailure {
    Missing,
    Unexecutable,
    Malformed(ClaudeCodeProbe),
    Unsupported(ClaudeCodeIncompatibility),
}

impl fmt::Display for ClaudeCodeInstallationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => write!(
                formatter,
                "Claude Code was not found in inherited PATH; install exact Claude Code {CLAUDE_CODE_STREAM_JSON_V1_VERSION}"
            ),
            Self::Unexecutable => formatter.write_str(
                "Claude Code selected from inherited PATH could not complete its validation probes",
            ),
            Self::Malformed(ClaudeCodeProbe::Version) => formatter
                .write_str("Claude Code selected from inherited PATH returned a malformed version"),
            Self::Malformed(ClaudeCodeProbe::Capabilities) => formatter.write_str(
                "Claude Code selected from inherited PATH returned malformed capability help",
            ),
            Self::Unsupported(ClaudeCodeIncompatibility::Version(version)) => write!(
                formatter,
                "Claude Code version {version} selected from inherited PATH is unsupported; install exact Claude Code {CLAUDE_CODE_STREAM_JSON_V1_VERSION}"
            ),
            Self::Unsupported(ClaudeCodeIncompatibility::Capability(capability)) => write!(
                formatter,
                "Claude Code selected from inherited PATH lacks the required {} capability",
                capability.as_str()
            ),
        }
    }
}

impl std::error::Error for ClaudeCodeInstallationFailure {}
// jscpd:ignore-end

struct ClaudeCodeInstallationProfile;

impl HarnessInstallationProfile for ClaudeCodeInstallationProfile {
    type Version = ClaudeCodeVersion;
    type CompatibilityProfile = ClaudeCodeCompatibilityProfile;
    type Capabilities = ClaudeCodeStreamJsonV1Capabilities;
    type Installation = ValidatedClaudeCodeInstallation;
    type Failure = ClaudeCodeInstallationFailure;

    const EXECUTABLE_NAME: &'static str = "claude";
    const CAPABILITY_PROBE_ARGUMENTS: &'static [&'static str] = &CAPABILITY_PROBE_ARGUMENTS;

    fn probe_isolation(search_path: &OsStr) -> Result<ProbeIsolation, Self::Failure> {
        let mut isolation = ProbeIsolation::create(
            "scherzo-claude-code-validation-",
            &["claude-config"],
            search_path,
        )
        .map_err(|()| Self::unexecutable_failure())?;
        isolation.add_directory_environment("CLAUDE_CONFIG_DIR", "claude-config");
        for name in [
            "DISABLE_UPDATES",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
            "CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL",
            "CLAUDE_CODE_DISABLE_AUTO_MEMORY",
            "CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS",
        ] {
            isolation.add_literal_environment(name, "1");
        }
        Ok(isolation)
    }

    // Each profile owns these policy mappings despite the shared probe lifecycle.
    // jscpd:ignore-start
    fn parse_version_output(output: &CommandOutput) -> Result<Self::Version, Self::Failure> {
        parse_version_output(output)
    }

    fn compatibility_profile(version: &Self::Version) -> Option<Self::CompatibilityProfile> {
        compatibility_profile(version)
    }

    fn unsupported_version(version: &Self::Version) -> Self::Failure {
        ClaudeCodeInstallationFailure::Unsupported(ClaudeCodeIncompatibility::Version(
            version.as_str().to_owned(),
        ))
    }

    fn validate_capability_output(
        output: &CommandOutput,
        _isolation: &ProbeIsolation,
        _executable: &Path,
        _version: &Self::Version,
        _profile: &Self::CompatibilityProfile,
    ) -> Result<Self::Capabilities, Self::Failure> {
        validate_capability_output(output)?;
        Ok(ClaudeCodeStreamJsonV1Capabilities {
            required: REQUIRED_CAPABILITIES,
        })
    }

    fn installation(
        parts: ValidatedInstallationParts<
            Self::Version,
            Self::CompatibilityProfile,
            Self::Capabilities,
        >,
    ) -> Self::Installation {
        let (executable, version, profile, capabilities) = parts.into_parts();
        ValidatedClaudeCodeInstallation {
            executable,
            version,
            profile,
            capabilities,
        }
    }

    fn executable_failure(failure: ExecutableValidationFailure) -> Self::Failure {
        match failure {
            ExecutableValidationFailure::Missing => ClaudeCodeInstallationFailure::Missing,
            ExecutableValidationFailure::Unexecutable => {
                ClaudeCodeInstallationFailure::Unexecutable
            }
        }
    }

    fn unexecutable_failure() -> Self::Failure {
        ClaudeCodeInstallationFailure::Unexecutable
    }
    // jscpd:ignore-end
}

pub(crate) fn discover_and_validate_claude_code_installation()
-> Result<ValidatedClaudeCodeInstallation, ClaudeCodeInstallationFailure> {
    discover_and_validate_installation::<ClaudeCodeInstallationProfile>(&SystemCommandRunner)
}

pub(crate) fn validate_claude_code_installation(
    selected_executable: &Path,
) -> Result<ValidatedClaudeCodeInstallation, ClaudeCodeInstallationFailure> {
    validate_selected_installation::<ClaudeCodeInstallationProfile>(
        selected_executable,
        &SystemCommandRunner,
    )
}

fn validate_claude_code_installation_with(
    selected_executable: &Path,
    search_path: &OsStr,
    runner: &dyn CommandRunner,
) -> Result<ValidatedClaudeCodeInstallation, ClaudeCodeInstallationFailure> {
    validate_shared_installation::<ClaudeCodeInstallationProfile>(
        selected_executable,
        search_path,
        runner,
    )
}

fn parse_version_output(
    output: &CommandOutput,
) -> Result<ClaudeCodeVersion, ClaudeCodeInstallationFailure> {
    let observed = parse_probe_line(output).ok_or(ClaudeCodeInstallationFailure::Malformed(
        ClaudeCodeProbe::Version,
    ))?;
    ClaudeCodeVersion::parse(observed).ok_or(ClaudeCodeInstallationFailure::Malformed(
        ClaudeCodeProbe::Version,
    ))
}

fn compatibility_profile(version: &ClaudeCodeVersion) -> Option<ClaudeCodeCompatibilityProfile> {
    (version.as_str() == CLAUDE_CODE_STREAM_JSON_V1_VERSION)
        .then_some(ClaudeCodeCompatibilityProfile::ClaudeCodeStreamJsonV1)
}

fn validate_capability_output(output: &CommandOutput) -> Result<(), ClaudeCodeInstallationFailure> {
    let help = parse_probe_text(output).ok_or(ClaudeCodeInstallationFailure::Malformed(
        ClaudeCodeProbe::Capabilities,
    ))?;
    if !help.starts_with("Usage: claude [options] [command] [prompt]\n") {
        return Err(ClaudeCodeInstallationFailure::Malformed(
            ClaudeCodeProbe::Capabilities,
        ));
    }
    if let Some(capability) = REQUIRED_CAPABILITIES
        .into_iter()
        .find(|capability| !capability.is_advertised_by(help))
    {
        return Err(ClaudeCodeInstallationFailure::Unsupported(
            ClaudeCodeIncompatibility::Capability(capability),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
