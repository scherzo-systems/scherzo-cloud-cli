use super::*;
use crate::process::{CommandProbeError, CommandRequest};
use std::fs;
use std::sync::Mutex;
use std::time::Duration;

const COMPLETE_HELP: &str = "Usage: claude [options] [command] [prompt]\nOptions:\n  -p, --print Print response\n  --input-format <format> Input format: stream-json\n  --output-format <format> Output format: stream-json\n  --verbose Verbose mode\n  --include-partial-messages Include chunks\n  --forward-subagent-text Forward text\n  --session-id <uuid> Use session\n  --permission-mode <mode> Permission mode\n  --setting-sources <sources> Setting sources\n  --model <model> Model\n  --effort <level> Effort\n  --bare Context via --append-system-prompt[-file]\n  --json-schema <schema> Schema\n";

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
            ["--help"] => {
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
    assert_eq!(environment.len(), 14);
    assert_eq!(
        environment
            .iter()
            .find(|(name, _)| *name == OsStr::new("PATH"))
            .map(|(_, value)| *value),
        Some(OsStr::new("/controlled/bin"))
    );
    for name in [
        "HOME",
        "CLAUDE_CONFIG_DIR",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
    ] {
        assert!(
            environment
                .iter()
                .find(|(candidate, _)| *candidate == OsStr::new(name))
                .is_some_and(|(_, value)| Path::new(value).starts_with(root))
        );
    }
    for name in [
        "DISABLE_UPDATES",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
        "CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL",
        "CLAUDE_CODE_DISABLE_AUTO_MEMORY",
        "CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS",
    ] {
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| *candidate == OsStr::new(name))
                .map(|(_, value)| *value),
            Some(OsStr::new("1"))
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
fn stable_releases_in_range_with_every_capability_construct_pinned_installations() {
    let executable = std::env::current_exe().unwrap();
    for version in ["2.1.234", "2.1.235"] {
        let runner = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(format!("{version} (Claude Code)\n").as_bytes()),
            capabilities: output(COMPLETE_HELP.as_bytes()),
        };

        let installation = validate_claude_code_installation_with(
            &executable,
            OsStr::new("/controlled/bin"),
            &runner,
        )
        .unwrap();

        assert_eq!(
            installation.executable(),
            fs::canonicalize(&executable).unwrap()
        );
        assert_eq!(installation.version().as_str(), version);
        assert_eq!(
            installation.profile(),
            ClaudeCodeCompatibilityProfile::ClaudeCodeStreamJsonV1
        );
        assert_eq!(
            installation.capabilities().required(),
            REQUIRED_CAPABILITIES
        );
        assert_eq!(
            *runner.invocations.lock().unwrap(),
            [vec!["--version".to_owned()], vec!["--help".to_owned()]]
        );
    }
}

#[test]
fn malformed_incompatible_and_missing_capability_outputs_are_distinct() {
    let executable = std::env::current_exe().unwrap();
    for malformed in [
        "not-a-version",
        "2.1.234",
        "2.1.234-rc.1 (Claude Code)",
        "2.1.234+build (Claude Code)",
        "02.1.234 (Claude Code)",
    ] {
        let runner = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(format!("{malformed}\n").as_bytes()),
            capabilities: output(COMPLETE_HELP.as_bytes()),
        };
        assert_eq!(
            validate_claude_code_installation_with(
                &executable,
                OsStr::new("/controlled/bin"),
                &runner,
            ),
            Err(ClaudeCodeInstallationFailure::Malformed(
                ClaudeCodeProbe::Version
            ))
        );
    }

    for version in ["2.1.222", "2.1.233", "2.2.0", "3.0.0"] {
        let incompatible = FakeRunner {
            invocations: Mutex::new(Vec::new()),
            version: output(format!("{version} (Claude Code)\n").as_bytes()),
            capabilities: output(COMPLETE_HELP.as_bytes()),
        };
        assert_eq!(
            validate_claude_code_installation_with(
                &executable,
                OsStr::new("/controlled/bin"),
                &incompatible,
            ),
            Err(ClaudeCodeInstallationFailure::Unsupported(
                ClaudeCodeIncompatibility::Version(version.to_owned())
            ))
        );
        assert_eq!(
            *incompatible.invocations.lock().unwrap(),
            [vec!["--version".to_owned()]]
        );
    }

    let missing_schema = FakeRunner {
        invocations: Mutex::new(Vec::new()),
        version: output(b"2.1.234 (Claude Code)\n"),
        capabilities: output(
            COMPLETE_HELP
                .replace("--json-schema", "--schema")
                .as_bytes(),
        ),
    };
    assert_eq!(
        validate_claude_code_installation_with(
            &executable,
            OsStr::new("/controlled/bin"),
            &missing_schema,
        ),
        Err(ClaudeCodeInstallationFailure::Unsupported(
            ClaudeCodeIncompatibility::Capability(ClaudeCodeCapability::JsonSchema)
        ))
    );
}
