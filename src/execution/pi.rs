use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rustix::fs::{Access, AtFlags, CWD, accessat};

use crate::process::{CommandOutput, CommandRequest, CommandRunner, SystemCommandRunner};

const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const MAXIMUM_VERSION_OUTPUT_BYTES: usize = 4 * 1024;
const MAXIMUM_CAPABILITY_OUTPUT_BYTES: usize = 64 * 1024;
pub(crate) const PI_JSON_V1_SUPPORTED_RANGE: &str = ">=0.83.0 <0.84.0";
pub(crate) const PI_JSON_V1_QUALIFICATION_VERSION: &str = "0.83.0";
const PI_JSON_V1_MINIMUM_VERSION: (u64, u64, u64) = (0, 83, 0);
const PI_JSON_V1_MAXIMUM_VERSION: (u64, u64, u64) = (0, 84, 0);
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct PiVersion {
    major: u64,
    minor: u64,
    patch: u64,
    observed: Box<str>,
}

impl PiVersion {
    fn parse(observed: String) -> Option<Self> {
        let mut components = observed.split('.');
        let major = parse_numeric_component(components.next()?)?;
        let minor = parse_numeric_component(components.next()?)?;
        let patch = parse_numeric_component(components.next()?)?;
        if components.next().is_some() {
            return None;
        }
        Some(Self {
            major,
            minor,
            patch,
            observed: observed.into_boxed_str(),
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.observed
    }

    const fn numeric(&self) -> (u64, u64, u64) {
        (self.major, self.minor, self.patch)
    }
}

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
            version: PiVersion::parse(PI_JSON_V1_QUALIFICATION_VERSION.to_owned()).unwrap(),
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
            Self::Missing => write!(
                formatter,
                "Pi was not found in inherited PATH; install a stable Pi release in range {PI_JSON_V1_SUPPORTED_RANGE}"
            ),
            Self::Unexecutable => formatter.write_str(
                "Pi selected from inherited PATH could not complete its validation probes",
            ),
            Self::Malformed(PiProbe::Version) => {
                formatter.write_str("Pi selected from inherited PATH returned a malformed version")
            }
            Self::Malformed(PiProbe::Capabilities) => formatter
                .write_str("Pi selected from inherited PATH returned malformed capability help"),
            Self::Unsupported(PiIncompatibility::Version(version)) => write!(
                formatter,
                "Pi version {version} selected from inherited PATH is unsupported; install a stable Pi release in range {PI_JSON_V1_SUPPORTED_RANGE}"
            ),
            Self::Unsupported(PiIncompatibility::Capability(capability)) => write!(
                formatter,
                "Pi selected from inherited PATH lacks the required {} capability",
                capability.as_str()
            ),
        }
    }
}

impl std::error::Error for PiInstallationFailure {}

pub(crate) fn discover_and_validate_pi_installation()
-> Result<ValidatedPiInstallation, PiInstallationFailure> {
    let search_path = std::env::var_os("PATH").ok_or(PiInstallationFailure::Missing)?;
    let executable = discover_executable(OsStr::new("pi"), Some(&search_path))
        .ok_or(PiInstallationFailure::Missing)?;
    validate_pi_installation_with(&executable, &search_path, &SystemCommandRunner)
}

pub(crate) fn validate_pi_installation(
    selected_executable: &Path,
) -> Result<ValidatedPiInstallation, PiInstallationFailure> {
    let search_path = std::env::var_os("PATH").unwrap_or_default();
    validate_pi_installation_with(selected_executable, &search_path, &SystemCommandRunner)
}

fn discover_executable(name: &OsStr, search_path: Option<&OsStr>) -> Option<PathBuf> {
    std::env::split_paths(search_path?)
        .map(|directory| directory.join(name))
        .find(|candidate| is_executable_file(candidate))
}

fn is_executable_file(candidate: &Path) -> bool {
    fs::metadata(candidate).is_ok_and(|metadata| metadata.is_file())
        && accessat(CWD, candidate, Access::EXEC_OK, AtFlags::EACCESS).is_ok()
}

fn validate_pi_installation_with(
    selected_executable: &Path,
    search_path: &OsStr,
    runner: &dyn CommandRunner,
) -> Result<ValidatedPiInstallation, PiInstallationFailure> {
    let executable = normalize_executable(selected_executable)?;
    let isolation = PiProbeIsolation::create(search_path)?;
    let validation = (|| {
        let version_output = run_probe(
            runner,
            &executable,
            &["--version"],
            MAXIMUM_VERSION_OUTPUT_BYTES,
            &isolation,
        )?;
        let version = parse_version_output(&version_output)?;
        let profile = compatibility_profile(&version).ok_or_else(|| {
            PiInstallationFailure::Unsupported(PiIncompatibility::Version(
                version.as_str().to_owned(),
            ))
        })?;

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
            profile,
            capabilities: PiJsonV1Capabilities {
                required: REQUIRED_CAPABILITIES,
            },
        })
    })();
    isolation.close()?;
    validation
}

fn normalize_executable(selected: &Path) -> Result<PathBuf, PiInstallationFailure> {
    let executable = fs::canonicalize(selected).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => PiInstallationFailure::Missing,
        _ => PiInstallationFailure::Unexecutable,
    })?;
    if !is_executable_file(&executable) {
        return Err(PiInstallationFailure::Unexecutable);
    }
    Ok(executable)
}

struct PiProbeIsolation {
    _temporary: tempfile::TempDir,
    search_path: OsString,
    current_directory: PathBuf,
    home: PathBuf,
    agent_directory: PathBuf,
    xdg_config_home: PathBuf,
    xdg_cache_home: PathBuf,
    xdg_data_home: PathBuf,
    xdg_state_home: PathBuf,
}

impl PiProbeIsolation {
    fn create(search_path: &OsStr) -> Result<Self, PiInstallationFailure> {
        let temporary = tempfile::Builder::new()
            .prefix("scherzo-pi-validation-")
            .tempdir()
            .map_err(|_| PiInstallationFailure::Unexecutable)?;
        let root = temporary.path().to_owned();
        let isolation = Self {
            search_path: search_path.to_owned(),
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

    fn environment(&self) -> [(&OsStr, &OsStr); 12] {
        [
            (OsStr::new("PATH"), self.search_path.as_os_str()),
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

fn parse_version_output(output: &CommandOutput) -> Result<PiVersion, PiInstallationFailure> {
    if output.truncated {
        return Err(PiInstallationFailure::Malformed(PiProbe::Version));
    }
    let text = std::str::from_utf8(&output.stdout)
        .map_err(|_| PiInstallationFailure::Malformed(PiProbe::Version))?;
    let version = text
        .strip_suffix('\n')
        .ok_or(PiInstallationFailure::Malformed(PiProbe::Version))?;
    PiVersion::parse(version.to_owned()).ok_or(PiInstallationFailure::Malformed(PiProbe::Version))
}

fn parse_numeric_component(component: &str) -> Option<u64> {
    if component.is_empty()
        || !component.bytes().all(|byte| byte.is_ascii_digit())
        || (component.len() > 1 && component.starts_with('0'))
    {
        return None;
    }
    component.parse().ok()
}

fn compatibility_profile(version: &PiVersion) -> Option<PiCompatibilityProfile> {
    (version.numeric() >= PI_JSON_V1_MINIMUM_VERSION
        && version.numeric() < PI_JSON_V1_MAXIMUM_VERSION)
        .then_some(PiCompatibilityProfile::PiJsonV1)
}

pub(crate) fn compatibility_profile_for_version(observed: &str) -> Option<PiCompatibilityProfile> {
    compatibility_profile(&PiVersion::parse(observed.to_owned())?)
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

    const COMPLETE_HELP: &str = "pi - fixture\nUsage:\n  pi [options] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --session-dir <dir> Directory for session storage and lookup\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n  --approve, -a Trust project files for this run\n";

    struct FakeRunner {
        invocations: Mutex<Vec<Vec<String>>>,
        version: CommandOutput,
        capabilities: CommandOutput,
    }

    impl CommandRunner for FakeRunner {
        fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError> {
            assert!(command.program.is_absolute());
            assert_eq!(command.timeout, Duration::from_secs(30));
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
                    assert_eq!(command.maximum_stdout_bytes, 4 * 1024);
                    Ok(self.version.clone())
                }
                args if args == CAPABILITY_PROBE_ARGUMENTS => {
                    assert_eq!(command.maximum_stdout_bytes, 64 * 1024);
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
        assert_eq!(environment.len(), 12);
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| *candidate == OsStr::new("PATH"))
                .map(|(_, value)| *value),
            Some(OsStr::new("/controlled/bin"))
        );
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
    fn bounded_compatibility_policy_constructs_the_complete_validated_value() {
        let executable = std::env::current_exe().unwrap();
        let runner = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(b"0.83.7\n"),
            capabilities: output(COMPLETE_HELP.as_bytes()),
        };

        let installation =
            validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner)
                .unwrap();

        assert_eq!(
            installation.executable(),
            fs::canonicalize(executable).unwrap()
        );
        assert_eq!(installation.version().as_str(), "0.83.7");
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
    fn version_range_and_capabilities_are_both_required() {
        let executable = std::env::current_exe().unwrap();
        for unsupported in ["0.82.1", "0.84.0"] {
            let runner = FakeRunner {
                invocations: Mutex::new(Vec::new()),
                version: output(format!("{unsupported}\n").as_bytes()),
                capabilities: output(COMPLETE_HELP.as_bytes()),
            };
            assert_eq!(
                validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner,),
                Err(PiInstallationFailure::Unsupported(
                    PiIncompatibility::Version(unsupported.to_owned())
                ))
            );
            assert_eq!(
                *runner.invocations.lock().unwrap(),
                [vec!["--version".to_owned()]]
            );
        }

        for malformed in ["0.83.0-rc.1", "0.083.0", "0.83"] {
            let runner = FakeRunner {
                invocations: Mutex::new(Vec::new()),
                version: output(format!("{malformed}\n").as_bytes()),
                capabilities: output(COMPLETE_HELP.as_bytes()),
            };
            assert_eq!(
                validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner,),
                Err(PiInstallationFailure::Malformed(PiProbe::Version))
            );
        }

        let missing_session_directory = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(b"0.83.0\n"),
            capabilities: output(
                COMPLETE_HELP
                    .replace("--session-dir <dir>", "--session-root <dir>")
                    .as_bytes(),
            ),
        };
        assert_eq!(
            validate_pi_installation_with(
                &executable,
                OsStr::new("/controlled/bin"),
                &missing_session_directory,
            ),
            Err(PiInstallationFailure::Unsupported(
                PiIncompatibility::Capability(PiCapability::CustomSessionDirectory)
            ))
        );

        let missing_trust = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(b"0.83.0\n"),
            capabilities: output(
                COMPLETE_HELP
                    .replace("--approve, -a", "--permit, -p")
                    .as_bytes(),
            ),
        };
        assert_eq!(
            validate_pi_installation_with(
                &executable,
                OsStr::new("/controlled/bin"),
                &missing_trust,
            ),
            Err(PiInstallationFailure::Unsupported(
                PiIncompatibility::Capability(PiCapability::InvocationScopedProjectTrust)
            ))
        );
    }
}
