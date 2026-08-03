use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::process::{CommandOutput, CommandRequest, CommandRunner, SystemCommandRunner};

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const MAXIMUM_VERSION_OUTPUT_BYTES: usize = 128;
const MAXIMUM_CAPABILITY_OUTPUT_BYTES: usize = 16 * 1024;
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
    PiCapability::EphemeralSession,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PiVersion {
    V0_82_1,
}

impl PiVersion {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::V0_82_1 => "0.82.1",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PiCapability {
    JsonEventStream,
    EphemeralSession,
    ExtensionLoading,
    SystemPromptAppend,
    InvocationScopedProjectTrust,
}

impl PiCapability {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::JsonEventStream => "json_event_stream",
            Self::EphemeralSession => "ephemeral_session",
            Self::ExtensionLoading => "extension_loading",
            Self::SystemPromptAppend => "system_prompt_append",
            Self::InvocationScopedProjectTrust => "invocation_scoped_project_trust",
        }
    }

    fn is_advertised_by(self, help: &str) -> bool {
        help.lines().map(str::trim_start).any(|line| match self {
            Self::JsonEventStream => line.starts_with("--mode <mode>") && line.contains("json"),
            Self::EphemeralSession => line.starts_with("--no-session "),
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
            version: PiVersion::V0_82_1,
            profile: PiCompatibilityProfile::PiJsonV1,
            capabilities: PiJsonV1Capabilities {
                required: REQUIRED_CAPABILITIES,
            },
        }
    }

    pub(crate) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(crate) const fn version(&self) -> PiVersion {
        self.version
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
    Capability(PiCapability),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PiInstallationFailure {
    Missing,
    Unexecutable,
    Malformed(PiProbe),
    Unsupported(PiIncompatibility),
}

impl fmt::Display for PiInstallationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => formatter.write_str(
                "configured Pi executable was not found; install Pi 0.82.1 or correct --pi-executable",
            ),
            Self::Unexecutable => formatter.write_str(
                "configured Pi executable could not complete its validation probes",
            ),
            Self::Malformed(PiProbe::Version) => {
                formatter.write_str("configured Pi executable returned a malformed version")
            }
            Self::Malformed(PiProbe::Capabilities) => formatter.write_str(
                "configured Pi executable returned malformed capability help",
            ),
            Self::Unsupported(PiIncompatibility::Version(version)) => write!(
                formatter,
                "configured Pi version {version} is unsupported; install Pi 0.82.1 exactly"
            ),
            Self::Unsupported(PiIncompatibility::Capability(capability)) => write!(
                formatter,
                "configured Pi executable lacks the required {} capability",
                capability.as_str()
            ),
        }
    }
}

impl std::error::Error for PiInstallationFailure {}

pub(crate) fn validate_pi_installation(
    configured_executable: &Path,
) -> Result<ValidatedPiInstallation, PiInstallationFailure> {
    validate_pi_installation_with(configured_executable, &SystemCommandRunner)
}

fn validate_pi_installation_with(
    configured_executable: &Path,
    runner: &dyn CommandRunner,
) -> Result<ValidatedPiInstallation, PiInstallationFailure> {
    let executable = normalize_executable(configured_executable)?;
    let isolation = PiProbeIsolation::create()?;
    let validation = (|| {
        let version_output = run_probe(
            runner,
            &executable,
            &["--version"],
            MAXIMUM_VERSION_OUTPUT_BYTES,
            &isolation,
        )?;
        let version_text = parse_version_output(&version_output)?;
        let version = compatible_version(&version_text).ok_or(
            PiInstallationFailure::Unsupported(PiIncompatibility::Version(version_text)),
        )?;

        let capability_output = run_probe(
            runner,
            &executable,
            &CAPABILITY_PROBE_ARGUMENTS,
            MAXIMUM_CAPABILITY_OUTPUT_BYTES,
            &isolation,
        )?;
        validate_capability_output(&capability_output)?;

        Ok(ValidatedPiInstallation {
            executable,
            version,
            profile: PiCompatibilityProfile::PiJsonV1,
            capabilities: PiJsonV1Capabilities {
                required: REQUIRED_CAPABILITIES,
            },
        })
    })();
    isolation.close()?;
    validation
}

fn normalize_executable(configured: &Path) -> Result<PathBuf, PiInstallationFailure> {
    let executable = fs::canonicalize(configured).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => PiInstallationFailure::Missing,
        _ => PiInstallationFailure::Unexecutable,
    })?;
    let metadata = fs::metadata(&executable).map_err(|_| PiInstallationFailure::Unexecutable)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(PiInstallationFailure::Unexecutable);
    }
    Ok(executable)
}

struct PiProbeIsolation {
    _temporary: tempfile::TempDir,
    current_directory: PathBuf,
    home: PathBuf,
    agent_directory: PathBuf,
    xdg_config_home: PathBuf,
    xdg_cache_home: PathBuf,
    xdg_data_home: PathBuf,
    xdg_state_home: PathBuf,
}

impl PiProbeIsolation {
    fn create() -> Result<Self, PiInstallationFailure> {
        let temporary = tempfile::Builder::new()
            .prefix("scherzo-pi-validation-")
            .tempdir()
            .map_err(|_| PiInstallationFailure::Unexecutable)?;
        let root = temporary.path().to_owned();
        let isolation = Self {
            current_directory: root.join("project"),
            home: root.join("home"),
            agent_directory: root.join("agent"),
            xdg_config_home: root.join("config"),
            xdg_cache_home: root.join("cache"),
            xdg_data_home: root.join("data"),
            xdg_state_home: root.join("state"),
            _temporary: temporary,
        };
        for directory in [
            &isolation.current_directory,
            &isolation.home,
            &isolation.agent_directory,
            &isolation.xdg_config_home,
            &isolation.xdg_cache_home,
            &isolation.xdg_data_home,
            &isolation.xdg_state_home,
        ] {
            fs::create_dir(directory).map_err(|_| PiInstallationFailure::Unexecutable)?;
        }
        Ok(isolation)
    }

    fn close(self) -> Result<(), PiInstallationFailure> {
        self._temporary
            .close()
            .map_err(|_| PiInstallationFailure::Unexecutable)
    }

    fn environment(&self) -> [(&OsStr, &OsStr); 11] {
        [
            (OsStr::new("HOME"), self.home.as_os_str()),
            (
                OsStr::new("PI_CODING_AGENT_DIR"),
                self.agent_directory.as_os_str(),
            ),
            (
                OsStr::new("XDG_CONFIG_HOME"),
                self.xdg_config_home.as_os_str(),
            ),
            (
                OsStr::new("XDG_CACHE_HOME"),
                self.xdg_cache_home.as_os_str(),
            ),
            (OsStr::new("XDG_DATA_HOME"), self.xdg_data_home.as_os_str()),
            (
                OsStr::new("XDG_STATE_HOME"),
                self.xdg_state_home.as_os_str(),
            ),
            (OsStr::new("PI_OFFLINE"), OsStr::new("1")),
            (OsStr::new("PI_SKIP_VERSION_CHECK"), OsStr::new("1")),
            (OsStr::new("PI_TELEMETRY"), OsStr::new("0")),
            (OsStr::new("FORCE_COLOR"), OsStr::new("0")),
            (OsStr::new("NO_COLOR"), OsStr::new("1")),
        ]
    }
}

fn run_probe(
    runner: &dyn CommandRunner,
    executable: &Path,
    args: &[&str],
    maximum_stdout_bytes: usize,
    isolation: &PiProbeIsolation,
) -> Result<CommandOutput, PiInstallationFailure> {
    let environment = isolation.environment();
    let output = runner
        .run(CommandRequest {
            program: executable,
            args,
            timeout: PROBE_TIMEOUT,
            maximum_stdout_bytes,
            clear_environment: true,
            environment: &environment,
            current_directory: Some(&isolation.current_directory),
        })
        .map_err(|_| PiInstallationFailure::Unexecutable)?;
    if !output.success {
        return Err(PiInstallationFailure::Unexecutable);
    }
    Ok(output)
}

fn parse_version_output(output: &CommandOutput) -> Result<String, PiInstallationFailure> {
    if output.truncated {
        return Err(PiInstallationFailure::Malformed(PiProbe::Version));
    }
    let text = std::str::from_utf8(&output.stdout)
        .map_err(|_| PiInstallationFailure::Malformed(PiProbe::Version))?;
    let version = text
        .strip_suffix('\n')
        .ok_or(PiInstallationFailure::Malformed(PiProbe::Version))?;
    let mut components = version.split('.');
    if components.clone().count() != 3
        || components.any(|component| {
            component.is_empty()
                || !component.bytes().all(|byte| byte.is_ascii_digit())
                || component.parse::<u64>().is_err()
        })
    {
        return Err(PiInstallationFailure::Malformed(PiProbe::Version));
    }
    Ok(version.to_owned())
}

fn compatible_version(version: &str) -> Option<PiVersion> {
    match version.as_bytes() {
        b"0.82.1" => Some(PiVersion::V0_82_1),
        _ => None,
    }
}

fn validate_capability_output(output: &CommandOutput) -> Result<(), PiInstallationFailure> {
    if output.truncated {
        return Err(PiInstallationFailure::Malformed(PiProbe::Capabilities));
    }
    let help = std::str::from_utf8(&output.stdout)
        .map_err(|_| PiInstallationFailure::Malformed(PiProbe::Capabilities))?;
    if !help.starts_with("pi ")
        || !help
            .lines()
            .any(|line| line.trim() == "pi [options] [@files...] [messages...]")
    {
        return Err(PiInstallationFailure::Malformed(PiProbe::Capabilities));
    }
    if let Some(capability) = REQUIRED_CAPABILITIES
        .into_iter()
        .find(|capability| !capability.is_advertised_by(help))
    {
        return Err(PiInstallationFailure::Unsupported(
            PiIncompatibility::Capability(capability),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::CommandProbeError;
    use std::sync::Mutex;

    const COMPLETE_HELP: &str = "pi - fixture\nUsage:\n  pi [options] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --no-session Do not save session\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n  --approve, -a Trust project files for this run\n";

    struct FakeRunner {
        invocations: Mutex<Vec<Vec<String>>>,
        version: CommandOutput,
        capabilities: CommandOutput,
    }

    impl CommandRunner for FakeRunner {
        fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError> {
            assert!(command.program.is_absolute());
            assert_eq!(command.timeout, Duration::from_secs(5));
            assert!(command.clear_environment);
            assert_isolated(command.environment, command.current_directory);
            self.invocations.lock().unwrap().push(
                command
                    .args
                    .iter()
                    .map(|argument| (*argument).to_owned())
                    .collect(),
            );
            match command.args {
                ["--version"] => {
                    assert_eq!(command.maximum_stdout_bytes, 128);
                    Ok(self.version.clone())
                }
                args if args == CAPABILITY_PROBE_ARGUMENTS => {
                    assert_eq!(command.maximum_stdout_bytes, 16 * 1024);
                    Ok(self.capabilities.clone())
                }
                _ => Err(CommandProbeError::Spawn),
            }
        }
    }

    fn assert_isolated(environment: &[(&OsStr, &OsStr)], current_directory: Option<&Path>) {
        let current_directory = current_directory.unwrap();
        let root = current_directory.parent().unwrap();
        assert_eq!(current_directory, root.join("project"));
        assert_eq!(environment.len(), 11);
        for (name, expected) in [
            ("HOME", root.join("home")),
            ("PI_CODING_AGENT_DIR", root.join("agent")),
            ("XDG_CONFIG_HOME", root.join("config")),
            ("XDG_CACHE_HOME", root.join("cache")),
            ("XDG_DATA_HOME", root.join("data")),
            ("XDG_STATE_HOME", root.join("state")),
        ] {
            assert_eq!(
                environment
                    .iter()
                    .find(|(candidate, _)| *candidate == OsStr::new(name))
                    .map(|(_, value)| *value),
                Some(expected.as_os_str())
            );
        }
        for (name, expected) in [
            ("PI_OFFLINE", "1"),
            ("PI_SKIP_VERSION_CHECK", "1"),
            ("PI_TELEMETRY", "0"),
            ("FORCE_COLOR", "0"),
            ("NO_COLOR", "1"),
        ] {
            assert_eq!(
                environment
                    .iter()
                    .find(|(candidate, _)| *candidate == OsStr::new(name))
                    .map(|(_, value)| *value),
                Some(OsStr::new(expected))
            );
        }
    }

    fn output(bytes: &[u8]) -> CommandOutput {
        CommandOutput {
            success: true,
            stdout: bytes.to_vec(),
            truncated: false,
        }
    }

    #[test]
    fn closed_compatibility_table_constructs_the_complete_validated_value() {
        let executable = std::env::current_exe().unwrap();
        let runner = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(b"0.82.1\n"),
            capabilities: output(COMPLETE_HELP.as_bytes()),
        };

        let installation = validate_pi_installation_with(&executable, &runner).unwrap();

        assert_eq!(
            installation.executable(),
            fs::canonicalize(executable).unwrap()
        );
        assert_eq!(installation.version().as_str(), "0.82.1");
        assert_eq!(installation.profile().as_str(), "PiJsonV1");
        assert_eq!(
            installation.capabilities().required(),
            REQUIRED_CAPABILITIES
        );
        assert_eq!(
            *runner.invocations.lock().unwrap(),
            [
                vec!["--version".to_owned()],
                CAPABILITY_PROBE_ARGUMENTS.map(str::to_owned).to_vec(),
            ]
        );
    }

    #[test]
    fn nearby_versions_and_missing_capabilities_do_not_construct_an_installation() {
        let executable = std::env::current_exe().unwrap();
        let unsupported_version = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(b"0.82.2\n"),
            capabilities: output(COMPLETE_HELP.as_bytes()),
        };
        assert_eq!(
            validate_pi_installation_with(&executable, &unsupported_version),
            Err(PiInstallationFailure::Unsupported(
                PiIncompatibility::Version("0.82.2".to_owned())
            ))
        );
        assert_eq!(
            *unsupported_version.invocations.lock().unwrap(),
            [vec!["--version".to_owned()]]
        );

        let missing_trust = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(b"0.82.1\n"),
            capabilities: output(
                COMPLETE_HELP
                    .replace("--approve, -a", "--permit, -p")
                    .as_bytes(),
            ),
        };
        assert_eq!(
            validate_pi_installation_with(&executable, &missing_trust),
            Err(PiInstallationFailure::Unsupported(
                PiIncompatibility::Capability(PiCapability::InvocationScopedProjectTrust)
            ))
        );
    }
}
