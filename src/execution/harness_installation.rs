use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rustix::fs::{Access, AtFlags, CWD, accessat};

use crate::process::{CommandOutput, CommandRunner};

const MAXIMUM_VERSION_OUTPUT_BYTES: usize = 4 * 1024;
const MAXIMUM_CAPABILITY_OUTPUT_BYTES: usize = 64 * 1024;
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutableValidationFailure {
    Missing,
    Unexecutable,
}

pub(crate) trait HarnessInstallationProfile {
    type Version;
    type CompatibilityProfile;
    type Capabilities;
    type Installation;
    type Failure;

    const EXECUTABLE_NAME: &'static str;
    const CAPABILITY_PROBE_ARGUMENTS: &'static [&'static str];

    fn probe_isolation(search_path: &OsStr) -> Result<ProbeIsolation, Self::Failure>;
    fn parse_version_output(output: &CommandOutput) -> Result<Self::Version, Self::Failure>;
    fn compatibility_profile(version: &Self::Version) -> Option<Self::CompatibilityProfile>;
    fn unsupported_version(version: &Self::Version) -> Self::Failure;
    fn validate_capability_output(
        output: &CommandOutput,
        isolation: &ProbeIsolation,
        executable: &Path,
        version: &Self::Version,
        profile: &Self::CompatibilityProfile,
    ) -> Result<Self::Capabilities, Self::Failure>;
    fn capability_probe_failure(
        _executable: &Path,
        _version: &Self::Version,
        _profile: &Self::CompatibilityProfile,
    ) -> Self::Failure {
        Self::unexecutable_failure()
    }
    fn installation(
        parts: ValidatedInstallationParts<
            Self::Version,
            Self::CompatibilityProfile,
            Self::Capabilities,
        >,
    ) -> Self::Installation;
    fn executable_failure(failure: ExecutableValidationFailure) -> Self::Failure;
    fn unexecutable_failure() -> Self::Failure;
}

pub(crate) struct ValidatedInstallationParts<Version, Profile, Capabilities> {
    executable: PathBuf,
    version: Version,
    profile: Profile,
    capabilities: Capabilities,
}

impl<Version, Profile, Capabilities> ValidatedInstallationParts<Version, Profile, Capabilities> {
    pub(crate) fn into_parts(self) -> (PathBuf, Version, Profile, Capabilities) {
        (
            self.executable,
            self.version,
            self.profile,
            self.capabilities,
        )
    }
}

pub(crate) fn discover_and_validate_installation<Profile: HarnessInstallationProfile>(
    runner: &dyn CommandRunner,
) -> Result<Profile::Installation, Profile::Failure> {
    let search_path = std::env::var_os("PATH")
        .ok_or_else(|| Profile::executable_failure(ExecutableValidationFailure::Missing))?;
    let executable = discover_executable(OsStr::new(Profile::EXECUTABLE_NAME), Some(&search_path))
        .ok_or_else(|| Profile::executable_failure(ExecutableValidationFailure::Missing))?;
    validate_installation_with::<Profile>(&executable, &search_path, runner)
}

pub(crate) fn validate_selected_installation<Profile: HarnessInstallationProfile>(
    selected_executable: &Path,
    runner: &dyn CommandRunner,
) -> Result<Profile::Installation, Profile::Failure> {
    let search_path = std::env::var_os("PATH").unwrap_or_default();
    validate_installation_with::<Profile>(selected_executable, &search_path, runner)
}

pub(crate) fn validate_installation_with<Profile: HarnessInstallationProfile>(
    selected_executable: &Path,
    search_path: &OsStr,
    runner: &dyn CommandRunner,
) -> Result<Profile::Installation, Profile::Failure> {
    let executable =
        normalize_executable(selected_executable).map_err(Profile::executable_failure)?;
    let isolation = Profile::probe_isolation(search_path)?;
    let validation = (|| {
        let version_output = isolation
            .run(
                runner,
                &executable,
                &["--version"],
                MAXIMUM_VERSION_OUTPUT_BYTES,
            )
            .map_err(|()| Profile::unexecutable_failure())?;
        let version = Profile::parse_version_output(&version_output)?;
        let profile = Profile::compatibility_profile(&version)
            .ok_or_else(|| Profile::unsupported_version(&version))?;
        let capability_output = isolation
            .run(
                runner,
                &executable,
                Profile::CAPABILITY_PROBE_ARGUMENTS,
                MAXIMUM_CAPABILITY_OUTPUT_BYTES,
            )
            .map_err(|()| Profile::capability_probe_failure(&executable, &version, &profile))?;
        let capabilities = Profile::validate_capability_output(
            &capability_output,
            &isolation,
            &executable,
            &version,
            &profile,
        )?;

        Ok(Profile::installation(ValidatedInstallationParts {
            executable,
            version,
            profile,
            capabilities,
        }))
    })();
    isolation
        .close()
        .map_err(|()| Profile::unexecutable_failure())?;
    validation
}

pub(crate) struct ProbeIsolation {
    filesystem: ProbeFilesystem,
    current_directory: PathBuf,
    environment: Vec<(OsString, OsString)>,
}

impl ProbeIsolation {
    pub(crate) fn create(
        prefix: &str,
        extra_directories: &[&str],
        search_path: &OsStr,
    ) -> Result<Self, ()> {
        let filesystem = ProbeFilesystem::create(prefix, extra_directories)?;
        let current_directory = filesystem.directory("project");
        let mut isolation = Self {
            environment: vec![
                (OsString::from("PATH"), search_path.to_owned()),
                (
                    OsString::from("HOME"),
                    filesystem.directory("home").into_os_string(),
                ),
                (
                    OsString::from("XDG_CONFIG_HOME"),
                    filesystem.directory("config").into_os_string(),
                ),
                (
                    OsString::from("XDG_CACHE_HOME"),
                    filesystem.directory("cache").into_os_string(),
                ),
                (
                    OsString::from("XDG_DATA_HOME"),
                    filesystem.directory("data").into_os_string(),
                ),
                (
                    OsString::from("XDG_STATE_HOME"),
                    filesystem.directory("state").into_os_string(),
                ),
            ],
            filesystem,
            current_directory,
        };
        isolation.add_literal_environment("FORCE_COLOR", "0");
        isolation.add_literal_environment("NO_COLOR", "1");
        Ok(isolation)
    }

    pub(crate) fn add_directory_environment(&mut self, name: &str, directory: &str) {
        self.environment.push((
            OsString::from(name),
            self.filesystem.directory(directory).into_os_string(),
        ));
    }

    pub(crate) fn directory(&self, name: &str) -> PathBuf {
        self.filesystem.directory(name)
    }

    pub(crate) fn add_literal_environment(&mut self, name: &str, value: &str) {
        self.environment
            .push((OsString::from(name), OsString::from(value)));
    }

    fn run(
        &self,
        runner: &dyn CommandRunner,
        executable: &Path,
        args: &[&str],
        maximum_stdout_bytes: usize,
    ) -> Result<CommandOutput, ()> {
        let environment = self
            .environment
            .iter()
            .map(|(name, value)| (name.as_os_str(), value.as_os_str()))
            .collect::<Vec<_>>();
        run_probe(
            runner,
            executable,
            args,
            maximum_stdout_bytes,
            &environment,
            &self.current_directory,
        )
    }

    fn close(self) -> Result<(), ()> {
        self.filesystem.close()
    }
}

pub(crate) fn parse_probe_text(output: &CommandOutput) -> Option<&str> {
    if output.truncated {
        return None;
    }
    std::str::from_utf8(&output.stdout).ok()
}

pub(crate) fn parse_probe_line(output: &CommandOutput) -> Option<&str> {
    parse_probe_text(output)?.strip_suffix('\n')
}

pub(crate) fn parse_numeric_component(component: &str) -> Option<u64> {
    if component.is_empty()
        || !component.bytes().all(|byte| byte.is_ascii_digit())
        || (component.len() > 1 && component.starts_with('0'))
    {
        return None;
    }
    component.parse().ok()
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct StableVersion {
    major: u64,
    minor: u64,
    patch: u64,
    observed: Box<str>,
}

impl StableVersion {
    pub(crate) fn parse(observed: &str) -> Option<Self> {
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
            observed: observed.into(),
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.observed
    }

    pub(crate) const fn numeric(&self) -> (u64, u64, u64) {
        (self.major, self.minor, self.patch)
    }
}

fn discover_executable(name: &OsStr, search_path: Option<&OsStr>) -> Option<PathBuf> {
    std::env::split_paths(search_path?)
        .map(|directory| directory.join(name))
        .find(|candidate| is_executable_file(candidate))
}

fn normalize_executable(selected: &Path) -> Result<PathBuf, ExecutableValidationFailure> {
    let executable = fs::canonicalize(selected).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => {
            ExecutableValidationFailure::Missing
        }
        _ => ExecutableValidationFailure::Unexecutable,
    })?;
    if !is_executable_file(&executable) {
        return Err(ExecutableValidationFailure::Unexecutable);
    }
    Ok(executable)
}

fn is_executable_file(candidate: &Path) -> bool {
    fs::metadata(candidate).is_ok_and(|metadata| metadata.is_file())
        && accessat(CWD, candidate, Access::EXEC_OK, AtFlags::EACCESS).is_ok()
}

struct ProbeFilesystem {
    temporary: tempfile::TempDir,
    root: PathBuf,
}

impl ProbeFilesystem {
    fn create(prefix: &str, extra_directories: &[&str]) -> Result<Self, ()> {
        let temporary = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir()
            .map_err(|_| ())?;
        let root = temporary.path().to_owned();
        let filesystem = Self { temporary, root };
        for directory in ["project", "home", "config", "cache", "data", "state"]
            .into_iter()
            .chain(extra_directories.iter().copied())
        {
            fs::create_dir(filesystem.directory(directory)).map_err(|_| ())?;
        }
        Ok(filesystem)
    }

    fn directory(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn close(self) -> Result<(), ()> {
        self.temporary.close().map_err(|_| ())
    }
}

fn run_probe(
    runner: &dyn CommandRunner,
    executable: &Path,
    args: &[&str],
    maximum_stdout_bytes: usize,
    environment: &[(&OsStr, &OsStr)],
    current_directory: &Path,
) -> Result<CommandOutput, ()> {
    let output = runner
        .run(crate::process::CommandRequest {
            program: executable,
            args,
            timeout: PROBE_TIMEOUT,
            maximum_stdout_bytes,
            clear_environment: true,
            environment,
            current_directory: Some(current_directory),
        })
        .map_err(|_| ())?;
    output.success.then_some(output).ok_or(())
}
