use std::path::PathBuf;

use anyhow::Context;
use clap::Args;

use crate::execution::claude_code::{
    ClaudeCodeInstallationFailure, ValidatedClaudeCodeInstallation,
    discover_and_validate_claude_code_installation,
};
use crate::execution::pi::{
    PiInstallationFailure, ValidatedPiInstallation, discover_and_validate_pi_installation,
};
use crate::exit_code::ExitCode;
use crate::runner::service::Config;

pub(super) const ABOUT: &str = "Connect to Scherzo Cloud and serve run assignments";

#[derive(Debug, Args)]
pub(super) struct Command {
    /// Read the closed runner operator configuration and enrolled state.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let (pi_installation, claude_code_installation) = discover_harness_installations_with(
            discover_and_validate_pi_installation,
            discover_and_validate_claude_code_installation,
        );
        let config_path = super::operator_config_path(&self.config)?;
        let config = Config::load(&config_path).with_context(|| {
            format!(
                "load runner operator configuration {}",
                config_path.display()
            )
        })?;
        let config =
            configure_harness_installations(config, pi_installation, claude_code_installation);
        crate::runner::service::run(config).context("serve enrolled runner assignments")?;
        Ok(ExitCode::Success)
    }
}

fn discover_harness_installations_with<PiDiscovery, ClaudeCodeDiscovery>(
    discover_pi: PiDiscovery,
    discover_claude_code: ClaudeCodeDiscovery,
) -> (
    Option<ValidatedPiInstallation>,
    Option<ValidatedClaudeCodeInstallation>,
)
where
    PiDiscovery: FnOnce() -> Result<ValidatedPiInstallation, PiInstallationFailure>,
    ClaudeCodeDiscovery:
        FnOnce() -> Result<ValidatedClaudeCodeInstallation, ClaudeCodeInstallationFailure>,
{
    (discover_pi().ok(), discover_claude_code().ok())
}

fn configure_harness_installations(
    config: Config,
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
) -> Config {
    let config = match pi_installation {
        Some(installation) => config.with_pi_installation(installation),
        None => config,
    };
    match claude_code_installation {
        Some(installation) => config.with_claude_code_installation(installation),
        None => config,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::path::PathBuf;

    use super::*;
    use crate::execution::claude_code::ClaudeCodeIncompatibility;
    use crate::runner::credential::test_credential;

    #[test]
    fn startup_retains_each_available_harness_snapshot_independently() {
        for (pi_available, claude_code_available) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let pi_calls = Cell::new(0);
            let claude_code_calls = Cell::new(0);
            let pi = ValidatedPiInstallation::fixture(PathBuf::from("/validated/pi"));
            let claude_code =
                ValidatedClaudeCodeInstallation::fixture(PathBuf::from("/validated/claude"));
            let config = Config::fixture(
                "ws://127.0.0.1:8081/v1/runner/connect",
                test_credential(),
                true,
            )
            .unwrap();

            let (pi_installation, claude_code_installation) = discover_harness_installations_with(
                || {
                    pi_calls.set(pi_calls.get() + 1);
                    pi_available
                        .then(|| pi.clone())
                        .ok_or(PiInstallationFailure::Missing)
                },
                || {
                    claude_code_calls.set(claude_code_calls.get() + 1);
                    claude_code_available
                        .then(|| claude_code.clone())
                        .ok_or(ClaudeCodeInstallationFailure::Missing)
                },
            );
            let config =
                configure_harness_installations(config, pi_installation, claude_code_installation);

            assert_eq!(pi_calls.get(), 1);
            assert_eq!(claude_code_calls.get(), 1);
            assert_eq!(config.pi_installation(), pi_available.then_some(&pi));
            assert_eq!(
                config.claude_code_installation(),
                claude_code_available.then_some(&claude_code),
            );
        }
    }

    #[test]
    fn incompatible_claude_code_does_not_remove_compatible_pi() {
        let pi = ValidatedPiInstallation::fixture(PathBuf::from("/validated/pi"));
        let config = Config::fixture(
            "ws://127.0.0.1:8081/v1/runner/connect",
            test_credential(),
            true,
        )
        .unwrap();

        let (pi_installation, claude_code_installation) = discover_harness_installations_with(
            || Ok(pi.clone()),
            || {
                Err(ClaudeCodeInstallationFailure::Unsupported(
                    ClaudeCodeIncompatibility::Version("2.1.221".to_owned()),
                ))
            },
        );
        let config =
            configure_harness_installations(config, pi_installation, claude_code_installation);

        assert_eq!(config.pi_installation(), Some(&pi));
        assert_eq!(config.claude_code_installation(), None);
    }
}
