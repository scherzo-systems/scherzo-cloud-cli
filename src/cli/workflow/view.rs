use std::num::NonZeroU64;
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;

use anyhow::{Context, anyhow};
use clap::Args;

use crate::execution::workflow::archived_attempt::{
    ArchivedAttemptIneligibilityReason, ArchivedAttemptLoadError,
    ArchivedAttemptOperationalErrorCode, load_local_archived_attempt,
};
use crate::execution::workflow::presentation::{
    ColorChoice, PresentationConfig, PresentationFailure, PresentationMode,
    RequestedPresentationMode, TerminalCapabilities,
};
use crate::execution::workflow::presentation_feed::normalize_terminal_scalar;
use crate::execution::workflow::terminal_host::archived::{
    ArchivedTerminalHostExit, ArchivedWorkflowTerminalHost,
};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "View a published local workflow attempt";
pub(super) const AFTER_HELP: &str = "Attempt selection:
  When --attempt is omitted, the command selects the current attempt from a stable run
  snapshot before opening the interactive view.";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(
        long,
        value_name = "NUMBER",
        value_parser = parse_attempt,
        help = "Attempt number to view (defaults to the current attempt)"
    )]
    attempt: Option<NonZeroU64>,

    #[arg(
        long,
        value_enum,
        value_name = "WHEN",
        default_value_t = super::ColorArgument::Auto,
        help = "Select renderer color behavior"
    )]
    color: super::ColorArgument,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::execute_with_abandonable_runtime("view", self.execute_async())
    }

    async fn execute_async(self) -> super::super::CommandResult {
        let (mut interrupt, mut terminate) = super::observe_workflow_signals("view")?;

        let config = viewer_config(self.color, TerminalCapabilities::detect());
        if config.mode() != PresentationMode::Tui {
            return Err(anyhow!(
                "workflow view requires terminal stdin, terminal stdout, and a usable TERM\n\nUse workflow status for non-interactive output:\n  scherzo-cloud workflow status <RUN_DIR>"
            )
            .into());
        }

        let requested = self.run.run_dir;
        let load_path = requested.clone();
        let selected_attempt = self.attempt;
        let mut load = tokio::task::spawn_blocking(move || {
            load_local_archived_attempt(&load_path, selected_attempt)
        });
        let attempt = tokio::select! {
            biased;
            exit = first_view_signal(&mut interrupt, &mut terminate) => {
                return Ok(viewer_exit_code(exit));
            }
            result = &mut load => {
                match result {
                    Ok(Ok(attempt)) => attempt,
                    Ok(Err(error)) => return Err(load_error(&requested, error).into()),
                    Err(error) => {
                        return Err(anyhow::Error::new(error)
                            .context("load workflow view archive")
                            .into());
                    }
                }
            }
        };

        let host = ArchivedWorkflowTerminalHost::start(attempt, config.color_enabled())
            .map_err(anyhow::Error::new)
            .context("start workflow view terminal")?;
        let request = host.exit_request();
        let mut waiting = Box::pin(host.wait());
        let result = tokio::select! {
            biased;
            exit = first_view_signal(&mut interrupt, &mut terminate) => {
                request.request(exit);
                waiting.await
            }
            result = &mut waiting => result,
        };
        match result {
            Ok(exit) => Ok(viewer_exit_code(exit)),
            Err(failure) => Err(terminal_failure(&failure).into()),
        }
    }
}

fn parse_attempt(value: &str) -> Result<NonZeroU64, String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("attempt must be a positive decimal integer".to_owned());
    }
    value
        .parse::<u64>()
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| "attempt must be a positive decimal integer".to_owned())
}

fn viewer_config(
    color: super::ColorArgument,
    capabilities: TerminalCapabilities,
) -> PresentationConfig {
    PresentationConfig {
        requested_mode: RequestedPresentationMode::Automatic,
        color: match color {
            super::ColorArgument::Auto => ColorChoice::Auto,
            super::ColorArgument::Always => ColorChoice::Always,
            super::ColorArgument::Never => ColorChoice::Never,
        },
        capabilities,
        standard_input_reserved: false,
    }
}

async fn first_view_signal(
    interrupt: &mut tokio::signal::unix::Signal,
    terminate: &mut tokio::signal::unix::Signal,
) -> ArchivedTerminalHostExit {
    tokio::select! {
        biased;
        _ = interrupt.recv() => ArchivedTerminalHostExit::Interrupted,
        _ = terminate.recv() => ArchivedTerminalHostExit::Terminated,
    }
}

fn viewer_exit_code(exit: ArchivedTerminalHostExit) -> ExitCode {
    match exit {
        ArchivedTerminalHostExit::Quit => ExitCode::Success,
        ArchivedTerminalHostExit::Interrupted => ExitCode::Interrupted,
        ArchivedTerminalHostExit::Terminated => ExitCode::Terminated,
    }
}

fn load_error(requested: &Path, error: ArchivedAttemptLoadError) -> anyhow::Error {
    match error {
        ArchivedAttemptLoadError::Operational(error) => {
            let run_directory = error.run_directory.as_deref().unwrap_or(requested);
            anyhow!(operational_error_code(error.code)).context(format!(
                "workflow view cannot load run {}",
                safe_path(run_directory)
            ))
        }
        ArchivedAttemptLoadError::Ineligible(error) => anyhow!(ineligibility_reason(error.reason))
            .context(format!(
                "workflow view cannot open run {} attempt {}",
                safe_path(&error.run_directory),
                error.attempt_number
            )),
    }
}

fn operational_error_code(code: ArchivedAttemptOperationalErrorCode) -> &'static str {
    match code {
        ArchivedAttemptOperationalErrorCode::RunDirectoryUnavailable => "run_directory_unavailable",
        ArchivedAttemptOperationalErrorCode::RunDirectoryInvalid => "run_directory_invalid",
        ArchivedAttemptOperationalErrorCode::LockQueryFailed => "lock_query_failed",
        ArchivedAttemptOperationalErrorCode::StatusSnapshotUnstable => "status_snapshot_unstable",
        ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable => {
            "published_result_unavailable"
        }
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid => "published_result_invalid",
        ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid => "retained_workflow_invalid",
    }
}

fn ineligibility_reason(reason: ArchivedAttemptIneligibilityReason) -> &'static str {
    match reason {
        ArchivedAttemptIneligibilityReason::Unknown => "attempt_unknown",
        ArchivedAttemptIneligibilityReason::Nonterminal => "attempt_nonterminal",
        ArchivedAttemptIneligibilityReason::Interrupted => "attempt_interrupted",
        ArchivedAttemptIneligibilityReason::Rejected => "attempt_rejected",
        ArchivedAttemptIneligibilityReason::PublicationFailed => "attempt_publication_failed",
        ArchivedAttemptIneligibilityReason::Unpublished => "attempt_unpublished",
    }
}

fn terminal_failure(failure: &PresentationFailure) -> anyhow::Error {
    failure.error_kind.map_or_else(
        || anyhow!("workflow view terminal failure: {:?}", failure.operation),
        |kind| {
            anyhow!(
                "workflow view terminal failure: {:?} ({kind:?})",
                failure.operation
            )
        },
    )
}

fn safe_path(path: &Path) -> String {
    normalize_terminal_scalar(path.as_os_str().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::parse_attempt;

    #[test]
    fn attempt_parser_accepts_only_positive_unsigned_decimal_values() {
        assert_eq!(parse_attempt("0002").unwrap().get(), 2);
        for invalid in ["", "0", "+1", "-1", "1.0", "18446744073709551616"] {
            assert!(parse_attempt(invalid).is_err(), "accepted {invalid:?}");
        }
    }
}
