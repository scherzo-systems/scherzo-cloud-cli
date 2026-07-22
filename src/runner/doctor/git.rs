use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use super::process::{CommandProbeError, CommandRunner, SystemCommandRunner};
use super::{CheckDescriptor, DoctorCheck, Outcome, Status};

const MINIMUM_VERSION: GitVersion = GitVersion(0, 0, 1);
const SYSTEM_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct GitCheck {
    runner: Arc<dyn CommandRunner>,
    timeout: Duration,
}

impl GitCheck {
    pub(super) fn system() -> Self {
        Self {
            runner: Arc::new(SystemCommandRunner),
            timeout: SYSTEM_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_runner(runner: Arc<dyn CommandRunner>, timeout: Duration) -> Self {
        Self { runner, timeout }
    }
}

impl DoctorCheck for GitCheck {
    fn descriptor(&self) -> CheckDescriptor {
        CheckDescriptor {
            id: "environment.command.git",
            title: "Git",
            default: true,
        }
    }

    fn run(&self) -> Outcome {
        match self.runner.run("git", &["--version"], self.timeout) {
            Ok(output) if !output.success => fail(
                "command_failed",
                "git --version exited unsuccessfully.",
                BTreeMap::new(),
            ),
            Ok(output) => match parse_version(&output.stdout, output.truncated) {
                Ok(version) if version >= MINIMUM_VERSION => pass(version),
                Ok(version) => old_version(version),
                Err(()) => fail(
                    "invalid_version_output",
                    "git --version did not return a recognized version.",
                    BTreeMap::new(),
                ),
            },
            Err(CommandProbeError::CommandNotFound) => fail(
                "command_not_found",
                "Git was not found on PATH.",
                BTreeMap::new(),
            ),
            Err(CommandProbeError::Timeout) => fail(
                "command_timed_out",
                "Git version inspection timed out.",
                BTreeMap::new(),
            ),
            Err(
                CommandProbeError::Spawn | CommandProbeError::Wait | CommandProbeError::PipeRead,
            ) => fail(
                "command_probe_failed",
                "Git could not be inspected.",
                BTreeMap::new(),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GitVersion(u64, u64, u64);

impl fmt::Display for GitVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.0, self.1, self.2)
    }
}

fn parse_version(output: &[u8], truncated: bool) -> Result<GitVersion, ()> {
    if truncated {
        return Err(());
    }
    let output = std::str::from_utf8(output).map_err(|_| ())?;
    let version_token = output.strip_prefix("git version ").ok_or(())?;
    let version_token = version_token.split_whitespace().next().ok_or(())?;
    let mut components = version_token.split('.');
    let major = parse_component(components.next())?;
    let minor = parse_component(components.next())?;
    let patch = parse_component(components.next())?;
    Ok(GitVersion(major, minor, patch))
}

fn parse_component(component: Option<&str>) -> Result<u64, ()> {
    let component = component.ok_or(())?;
    if component.is_empty() || !component.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(());
    }
    component.parse().map_err(|_| ())
}

fn pass(version: GitVersion) -> Outcome {
    let mut details = version_details(version);
    details.insert("minimumVersion".to_owned(), MINIMUM_VERSION.to_string());
    Outcome {
        status: Status::Pass,
        code: "ok",
        message: format!("Git {version} is available (minimum {MINIMUM_VERSION})."),
        details,
    }
}

fn old_version(version: GitVersion) -> Outcome {
    Outcome {
        status: Status::Fail,
        code: "version_too_old",
        message: format!("Git {version} is older than the minimum {MINIMUM_VERSION}."),
        details: version_details(version),
    }
}

fn version_details(version: GitVersion) -> BTreeMap<String, String> {
    let mut details = BTreeMap::new();
    details.insert("version".to_owned(), version.to_string());
    details.insert("minimumVersion".to_owned(), MINIMUM_VERSION.to_string());
    details
}

fn fail(code: &'static str, message: &'static str, details: BTreeMap<String, String>) -> Outcome {
    Outcome {
        status: Status::Fail,
        code,
        message: message.to_owned(),
        details,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{GitCheck, GitVersion, MINIMUM_VERSION, parse_version};
    use crate::runner::doctor::process::{CommandOutput, CommandProbeError, CommandRunner};
    use crate::runner::doctor::{DoctorCheck, Outcome, Status};

    #[derive(Clone)]
    struct FakeRunner {
        result: Result<CommandOutput, CommandProbeError>,
    }

    impl CommandRunner for FakeRunner {
        fn run(
            &self,
            program: &str,
            args: &[&str],
            _timeout: Duration,
        ) -> Result<CommandOutput, CommandProbeError> {
            assert_eq!(program, "git");
            assert_eq!(args, ["--version"]);
            self.result.clone()
        }
    }

    fn check(result: Result<CommandOutput, CommandProbeError>) -> Outcome {
        GitCheck::with_runner(Arc::new(FakeRunner { result }), Duration::from_millis(1)).run()
    }

    fn output(success: bool, stdout: &[u8], truncated: bool) -> CommandOutput {
        CommandOutput {
            success,
            stdout: stdout.to_vec(),
            truncated,
        }
    }

    fn assert_no_raw_output(outcome: &Outcome, sentinel: &str) {
        assert!(!outcome.message.contains(sentinel));
        assert!(
            outcome
                .details
                .values()
                .all(|value| !value.contains(sentinel))
        );
    }

    #[test]
    fn parses_standard_apple_and_windows_git_versions() {
        for (output, expected) in [
            (b"git version 2.54.0\n".as_slice(), GitVersion(2, 54, 0)),
            (
                b"git version 2.39.5 (Apple Git-154)\n".as_slice(),
                GitVersion(2, 39, 5),
            ),
            (
                b"git version 2.47.1.windows.1\n".as_slice(),
                GitVersion(2, 47, 1),
            ),
        ] {
            assert_eq!(parse_version(output, false), Ok(expected));
        }
    }

    #[test]
    fn rejects_malformed_git_versions() {
        for (output, truncated) in [
            (b"".as_slice(), false),
            (b"version 2.54.0\n".as_slice(), false),
            (b"git version 2.54\n".as_slice(), false),
            (b"git version two.54.0\n".as_slice(), false),
            (b"git version 18446744073709551616.54.0\n".as_slice(), false),
            (b"git version 2.54.0\n".as_slice(), true),
        ] {
            assert_eq!(parse_version(output, truncated), Err(()));
        }
    }

    #[test]
    fn git_check_maps_probe_and_version_outcomes_without_raw_output() {
        let sentinel = "unique-raw-command-output";
        let cases = [
            (
                Ok(output(
                    true,
                    b"git version 2.54.0 unique-raw-command-output\n",
                    false,
                )),
                Status::Pass,
                "ok",
            ),
            (
                Ok(output(
                    true,
                    b"git version 0.0.0 unique-raw-command-output\n",
                    false,
                )),
                Status::Fail,
                "version_too_old",
            ),
            (
                Err(CommandProbeError::CommandNotFound),
                Status::Fail,
                "command_not_found",
            ),
            (
                Err(CommandProbeError::Timeout),
                Status::Fail,
                "command_timed_out",
            ),
            (
                Ok(output(false, sentinel.as_bytes(), false)),
                Status::Fail,
                "command_failed",
            ),
            (
                Ok(output(true, &[0xff, 0xfe], false)),
                Status::Fail,
                "invalid_version_output",
            ),
            (
                Ok(output(true, sentinel.as_bytes(), false)),
                Status::Fail,
                "invalid_version_output",
            ),
            (
                Ok(output(true, b"git version 2.54.0", true)),
                Status::Fail,
                "invalid_version_output",
            ),
            (
                Err(CommandProbeError::Spawn),
                Status::Fail,
                "command_probe_failed",
            ),
            (
                Err(CommandProbeError::Wait),
                Status::Fail,
                "command_probe_failed",
            ),
            (
                Err(CommandProbeError::PipeRead),
                Status::Fail,
                "command_probe_failed",
            ),
        ];

        for (result, status, code) in cases {
            let outcome = check(result);
            assert_eq!(outcome.status, status);
            assert_eq!(outcome.code, code);
            assert_no_raw_output(&outcome, sentinel);
        }
    }

    #[test]
    fn git_check_accepts_the_minimum_version() {
        let outcome = check(Ok(output(
            true,
            format!("git version {MINIMUM_VERSION}\n").as_bytes(),
            false,
        )));

        assert_eq!(outcome.status, Status::Pass);
        assert_eq!(outcome.details["version"], "0.0.1");
        assert_eq!(outcome.details["minimumVersion"], "0.0.1");
    }

    #[test]
    fn old_version_details_are_normalized() {
        let outcome = check(Ok(output(true, b"git version 0.0.0.vendor\n", false)));

        assert_eq!(outcome.code, "version_too_old");
        assert_eq!(
            outcome.details,
            BTreeMap::from([
                ("minimumVersion".to_owned(), "0.0.1".to_owned()),
                ("version".to_owned(), "0.0.0".to_owned()),
            ])
        );
    }
}
