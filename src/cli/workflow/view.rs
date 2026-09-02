use std::num::NonZeroU64;
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;

use anyhow::{Context, anyhow};
use clap::Args;

use crate::execution::workflow::archived_attempt::{
    ArchivedAttemptLoadError, load_local_archived_attempt,
};
use crate::execution::workflow::archived_presentation::{
    ArchivedViewOutput, ineligibility_code, operational_error_code,
};
use crate::execution::workflow::presentation::{
    PresentationConfig, PresentationFailure, PresentationMode, TerminalCapabilities,
};
use crate::execution::workflow::presentation_feed::normalize_terminal_scalar;
use crate::execution::workflow::terminal_host::archived::{
    ArchivedTerminalHostExit, ArchivedWorkflowTerminalHost,
};
use crate::exit_code::{ExitCode, OutcomeClass};

pub(super) const ABOUT: &str = "View a published local workflow attempt";
pub(super) const AFTER_HELP: &str = "Presentation mode:
  Without an explicit mode, an eligible terminal opens the interactive viewer. Every
  other stream arrangement receives a frozen plain summary.

Attempt selection:
  When --attempt is omitted, the command selects the current attempt from a stable run
  snapshot before opening the selected view.";

// View keeps its attempt selector beside its leaf-specific presentation surface; sharing
// this clap shape with execution commands would incorrectly share their inputs.
// jscpd:ignore-start
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

    #[command(flatten)]
    presentation: super::PresentationOptions,
}
// jscpd:ignore-end

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::execute_with_abandonable_runtime("view", self.execute_async())
    }

    async fn execute_async(self) -> super::super::CommandResult {
        let (mut interrupt, mut terminate) = super::observe_workflow_signals("view")?;

        let config = viewer_config(&self.presentation, TerminalCapabilities::detect());
        let mode = config.mode();
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
                    Ok(Err(error)) if mode == PresentationMode::Json => {
                        return write_noninteractive(
                            ArchivedViewOutput::JsonError(error),
                            &mut interrupt,
                            &mut terminate,
                        ).await;
                    }
                    Ok(Err(error)) => return Err(load_error(&requested, error).into()),
                    Err(error) => {
                        return Err(anyhow::Error::new(error)
                            .context("load workflow view archive")
                            .into());
                    }
                }
            }
        };

        if mode == PresentationMode::Plain {
            return write_noninteractive(
                ArchivedViewOutput::Plain {
                    attempt: Box::new(attempt.projection),
                    color: config.color_enabled(),
                },
                &mut interrupt,
                &mut terminate,
            )
            .await;
        }
        if mode == PresentationMode::Json {
            return write_noninteractive(
                ArchivedViewOutput::JsonSuccess(Box::new(attempt)),
                &mut interrupt,
                &mut terminate,
            )
            .await;
        }

        let host = ArchivedWorkflowTerminalHost::start(attempt.projection, config.color_enabled())
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
    presentation: &super::PresentationOptions,
    capabilities: TerminalCapabilities,
) -> PresentationConfig {
    super::run::presentation_config_with(presentation, false, capabilities)
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
        ArchivedTerminalHostExit::Quit => OutcomeClass::Success,
        ArchivedTerminalHostExit::Interrupted => OutcomeClass::Interrupted,
        ArchivedTerminalHostExit::Terminated => OutcomeClass::Terminated,
    }
    .exit_code()
}

async fn write_noninteractive(
    output: ArchivedViewOutput,
    interrupt: &mut tokio::signal::unix::Signal,
    terminate: &mut tokio::signal::unix::Signal,
) -> super::super::CommandResult {
    let mut job = tokio::task::spawn_blocking(move || output.write_stdout());
    tokio::select! {
        biased;
        result = &mut job => match result {
            Ok(Ok(exit)) => Ok(exit),
            Ok(Err(error)) => Err(anyhow::Error::new(error).into()),
            Err(error) => Err(anyhow::Error::new(error)
                .context("produce workflow view output")
                .into()),
        },
        exit = first_view_signal(interrupt, terminate) => Ok(viewer_exit_code(exit)),
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
        ArchivedAttemptLoadError::Ineligible(error) => anyhow!(ineligibility_code(error.reason))
            .context(format!(
                "workflow view cannot open run {} attempt {}",
                safe_path(&error.run_directory),
                error.attempt_number
            )),
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
