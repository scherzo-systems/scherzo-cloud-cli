use std::path::PathBuf;

use anyhow::Context;
use clap::Args;

use crate::execution::claude_code::{
    ClaudeCodeInstallationFailure, ValidatedClaudeCodeInstallation,
    discover_and_validate_claude_code_installation,
};
use crate::execution::codex::{
    CodexInstallationFailure, ValidatedCodexInstallation, discover_and_validate_codex_installation,
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
        let (pi_installation, claude_code_installation, codex_installation) =
            discover_harness_installations_with(
                discover_and_validate_pi_installation,
                discover_and_validate_claude_code_installation,
                discover_and_validate_codex_installation,
            );
        let config_path = super::operator_config_path(&self.config)?;
        let config = Config::load(&config_path).with_context(|| {
            format!(
                "load runner operator configuration {}",
                config_path.display()
            )
        })?;
        let config = configure_harness_installations(
            config,
            pi_installation,
            claude_code_installation,
            codex_installation,
        );
        match crate::runner::service::run(config) {
            Ok(()) => Ok(ExitCode::Success),
            Err(error) => {
                let exit_code = service_exit_code(&error);
                Err(super::super::CommandFailure::with_exit_code(
                    anyhow::Error::new(error).context("serve enrolled runner assignments"),
                    exit_code,
                ))
            }
        }
    }
}

const fn service_exit_code(error: &crate::runner::service::ServiceError) -> ExitCode {
    if error.requires_operator_recovery() {
        ExitCode::RunnerRecoveryRequired
    } else {
        ExitCode::GeneralFailure
    }
}

fn discover_harness_installations_with<PiDiscovery, ClaudeCodeDiscovery, CodexDiscovery>(
    discover_pi: PiDiscovery,
    discover_claude_code: ClaudeCodeDiscovery,
    discover_codex: CodexDiscovery,
) -> (
    Option<ValidatedPiInstallation>,
    Option<ValidatedClaudeCodeInstallation>,
    Option<ValidatedCodexInstallation>,
)
where
    PiDiscovery: FnOnce() -> Result<ValidatedPiInstallation, PiInstallationFailure>,
    ClaudeCodeDiscovery:
        FnOnce() -> Result<ValidatedClaudeCodeInstallation, ClaudeCodeInstallationFailure>,
    CodexDiscovery: FnOnce() -> Result<ValidatedCodexInstallation, CodexInstallationFailure>,
{
    (
        discover_pi().ok(),
        discover_claude_code().ok(),
        discover_codex().ok(),
    )
}

fn configure_harness_installations(
    config: Config,
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
    codex_installation: Option<ValidatedCodexInstallation>,
) -> Config {
    let config = match pi_installation {
        Some(installation) => config.with_pi_installation(installation),
        None => config,
    };
    let config = match claude_code_installation {
        Some(installation) => config.with_claude_code_installation(installation),
        None => config,
    };
    match codex_installation {
        Some(installation) => config.with_codex_installation(installation),
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
    use crate::runner::service::ConfigFixture;

    #[test]
    fn startup_retains_each_available_harness_snapshot_independently() {
        for (pi_available, claude_code_available, codex_available) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
            (true, true, false),
            (true, false, true),
            (false, true, true),
            (true, true, true),
        ] {
            let pi_calls = Cell::new(0);
            let claude_code_calls = Cell::new(0);
            let codex_calls = Cell::new(0);
            let pi = ValidatedPiInstallation::fixture(PathBuf::from("/validated/pi"));
            let claude_code =
                ValidatedClaudeCodeInstallation::fixture(PathBuf::from("/validated/claude"));
            let codex = ValidatedCodexInstallation::fixture(PathBuf::from("/validated/codex"));
            let fixture = ConfigFixture::new(
                "ws://127.0.0.1:8081/v1/runner/connect",
                test_credential(),
                true,
            )
            .unwrap();
            let config = fixture.cloned_config();

            let (pi_installation, claude_code_installation, codex_installation) =
                discover_harness_installations_with(
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
                    || {
                        codex_calls.set(codex_calls.get() + 1);
                        codex_available
                            .then(|| codex.clone())
                            .ok_or(CodexInstallationFailure::Missing)
                    },
                );
            let config = configure_harness_installations(
                config,
                pi_installation,
                claude_code_installation,
                codex_installation,
            );

            assert_eq!(pi_calls.get(), 1);
            assert_eq!(claude_code_calls.get(), 1);
            assert_eq!(codex_calls.get(), 1);
            assert_eq!(config.pi_installation(), pi_available.then_some(&pi));
            assert_eq!(
                config.claude_code_installation(),
                claude_code_available.then_some(&claude_code),
            );
            assert_eq!(
                config.codex_installation(),
                codex_available.then_some(&codex),
            );
        }
    }

    #[test]
    fn recovery_required_service_failures_have_a_supervisor_stopping_exit_code() {
        for error in [
            crate::runner::service::ServiceError::WorkRootInUse,
            crate::runner::service::ServiceError::WorkRootIsolation,
            crate::runner::service::ServiceError::WorkRootRecovery,
            crate::runner::service::ServiceError::WorkspaceCleanupFailed,
        ] {
            assert_eq!(service_exit_code(&error), ExitCode::RunnerRecoveryRequired);
        }
        assert_eq!(
            service_exit_code(&crate::runner::service::ServiceError::BuildRuntime),
            ExitCode::GeneralFailure
        );
    }

    #[test]
    fn incompatible_claude_code_and_missing_codex_do_not_remove_compatible_pi() {
        let pi = ValidatedPiInstallation::fixture(PathBuf::from("/validated/pi"));
        let fixture = ConfigFixture::new(
            "ws://127.0.0.1:8081/v1/runner/connect",
            test_credential(),
            true,
        )
        .unwrap();
        let config = fixture.cloned_config();

        let (pi_installation, claude_code_installation, codex_installation) =
            discover_harness_installations_with(
                || Ok(pi.clone()),
                || {
                    Err(ClaudeCodeInstallationFailure::Unsupported(
                        ClaudeCodeIncompatibility::Version("2.1.221".to_owned()),
                    ))
                },
                || Err(CodexInstallationFailure::Missing),
            );
        let config = configure_harness_installations(
            config,
            pi_installation,
            claude_code_installation,
            codex_installation,
        );

        assert_eq!(config.pi_installation(), Some(&pi));
        assert_eq!(config.claude_code_installation(), None);
        assert_eq!(config.codex_installation(), None);
    }
}
