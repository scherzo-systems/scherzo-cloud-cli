use super::*;
use crate::process::{CommandProbeError, CommandRequest};
use std::fs;
use std::sync::Mutex;
use std::time::Duration;

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
        version: output(b"0.84.7\n"),
        capabilities: output(COMPLETE_HELP.as_bytes()),
    };

    let installation =
        validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &runner).unwrap();

    assert_eq!(
        installation.executable(),
        fs::canonicalize(executable).unwrap()
    );
    assert_eq!(installation.version().as_str(), "0.84.7");
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
    for unsupported in ["0.83.0", "0.84.1", "0.85.0"] {
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

    for malformed in ["0.84.2-rc.1", "0.084.2", "0.84"] {
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
        version: output(b"0.84.2\n"),
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
        version: output(b"0.84.2\n"),
        capabilities: output(
            COMPLETE_HELP
                .replace("--approve, -a", "--permit, -p")
                .as_bytes(),
        ),
    };
    assert_eq!(
        validate_pi_installation_with(&executable, OsStr::new("/controlled/bin"), &missing_trust,),
        Err(PiInstallationFailure::Unsupported(
            PiIncompatibility::Capability(PiCapability::InvocationScopedProjectTrust)
        ))
    );
}
