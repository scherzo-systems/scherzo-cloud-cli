use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
use serde_json::Value;

use crate::execution::workflow::local_run::{
    LocalRecoveryStatus, LocalRetryEligibility, LocalRunStatusSnapshot, LocalStatusError,
    LocalStatusResult, RetryIneligibilityReason, read_local_run_status,
};
use crate::execution::workflow::presentation::{
    ColorChoice, PresentationConfig, RequestedPresentationMode, TerminalCapabilities,
    styled_terminal_text as styled,
};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Show local workflow run status";

const COMMAND: &str = "scherzo-cloud workflow status";
const STYLE_ACTIVE: &str = "38;2;137;180;250";
const STYLE_SUCCESS: &str = "38;2;166;227;161";
const STYLE_FAILURE: &str = "38;2;243;139;168";
const STYLE_BLOCKED: &str = "38;2;250;179;135";

// Status composes read-only run identity and presentation options without execution inputs.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[command(flatten)]
    presentation: super::PresentationOptions,
}
// jscpd:ignore-end

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_read_only_with_signals(
            "workflow status",
            move |cancelled, completed| self.execute_blocking(cancelled, completed),
        )
    }

    fn execute_blocking(
        &self,
        cancelled: &AtomicBool,
        completed: &AtomicBool,
    ) -> super::super::CommandResult {
        debug_assert!(!(self.presentation.plain && self.presentation.json));
        let snapshot = read_local_run_status(&self.run.run_dir);
        if cancelled.load(Ordering::Acquire) {
            return Ok(ExitCode::GeneralFailure);
        }
        let exit = if self.presentation.json {
            render_json(snapshot).context("write workflow status output")?
        } else {
            let snapshot = snapshot
                .map_err(|error| anyhow!(error.code.message()))
                .with_context(|| format!("inspect workflow run {}", self.run.run_dir.display()))?;
            let color = self.plain_color_enabled();
            render_plain(&snapshot, color).context("write workflow status output")?
        };
        completed.store(true, Ordering::Release);
        Ok(exit)
    }

    fn plain_color_enabled(&self) -> bool {
        PresentationConfig {
            requested_mode: RequestedPresentationMode::Plain,
            color: match self.presentation.color {
                super::ColorArgument::Auto => ColorChoice::Auto,
                super::ColorArgument::Always => ColorChoice::Always,
                super::ColorArgument::Never => ColorChoice::Never,
            },
            capabilities: TerminalCapabilities::detect(),
            standard_input_reserved: false,
        }
        .color_enabled()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusSuccess<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    run_directory: &'a str,
    run: &'a Value,
    state: &'a Value,
    recovery: RecoveryOutput<'a>,
    retry: RetryOutput,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum RecoveryOutput<'a> {
    Active,
    Settled,
    Abandoned,
    OwnershipUnproven {
        #[serde(rename = "guardIds")]
        guard_ids: &'a [String],
        reason: &'static str,
    },
}

impl<'a> From<&'a LocalRecoveryStatus> for RecoveryOutput<'a> {
    fn from(recovery: &'a LocalRecoveryStatus) -> Self {
        match recovery {
            LocalRecoveryStatus::Active => Self::Active,
            LocalRecoveryStatus::Settled => Self::Settled,
            LocalRecoveryStatus::Abandoned => Self::Abandoned,
            LocalRecoveryStatus::OwnershipUnproven { guard_ids, reason } => {
                Self::OwnershipUnproven {
                    guard_ids,
                    reason: reason.as_str(),
                }
            }
        }
    }
}

#[derive(Serialize)]
struct RetryOutput {
    eligible: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

impl From<LocalRetryEligibility> for RetryOutput {
    fn from(retry: LocalRetryEligibility) -> Self {
        match retry {
            LocalRetryEligibility::Eligible => Self {
                eligible: true,
                reason: None,
            },
            LocalRetryEligibility::Ineligible(reason) => Self {
                eligible: false,
                reason: Some(reason.as_str()),
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusErrorOutput<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_directory: Option<&'a str>,
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: &'static str,
}

fn render_json(snapshot: Result<LocalRunStatusSnapshot, LocalStatusError>) -> io::Result<ExitCode> {
    match snapshot {
        Ok(snapshot) => {
            let run_directory = snapshot
                .run_directory
                .to_str()
                .ok_or_else(|| io::Error::other("normalized run path is not UTF-8"))?;
            write_json(&StatusSuccess {
                schema_version: 1,
                command: COMMAND,
                outcome: "status",
                exit_status: ExitCode::Success.as_u8(),
                run_directory,
                run: &snapshot.run,
                state: &snapshot.state,
                recovery: RecoveryOutput::from(&snapshot.recovery),
                retry: snapshot.retry.into(),
            })?;
            Ok(ExitCode::Success)
        }
        Err(error) => {
            write_json(&StatusErrorOutput {
                schema_version: 1,
                command: COMMAND,
                outcome: "error",
                exit_status: ExitCode::GeneralFailure.as_u8(),
                run_directory: error
                    .run_directory
                    .as_deref()
                    .and_then(std::path::Path::to_str),
                error: ErrorDetail {
                    code: error.code.as_str(),
                    message: error.code.message(),
                },
            })?;
            Ok(ExitCode::GeneralFailure)
        }
    }
}

fn write_json(value: &impl Serialize) -> io::Result<()> {
    super::super::write_pretty_json(value)
}

fn render_plain(snapshot: &LocalRunStatusSnapshot, color: bool) -> io::Result<ExitCode> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_plain_snapshot(&mut stdout, snapshot, color)?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

fn write_plain_snapshot(
    writer: &mut impl Write,
    snapshot: &LocalRunStatusSnapshot,
    color: bool,
) -> io::Result<()> {
    writeln!(writer, "run: {}", snapshot.run_directory.display())?;
    writeln!(writer, "attempt: {}", snapshot.current_attempt_number)?;
    writeln!(
        writer,
        "state: {}",
        styled_state(snapshot.current_attempt_state, color)
    )?;
    writeln!(
        writer,
        "result: {}",
        styled_result(&snapshot.current_result, color)
    )?;
    writeln!(
        writer,
        "recovery: {}",
        styled_recovery(&snapshot.recovery, color)
    )?;
    writeln!(writer, "retry: {}", styled_retry(snapshot.retry, color))?;
    if let LocalRecoveryStatus::OwnershipUnproven { guard_ids, reason } = &snapshot.recovery {
        writeln!(writer, "ownership reason: {}", reason.as_str())?;
        writeln!(writer, "remedy: {}", reason.remedy())?;
        writeln!(writer, "guard ids: {}", guard_ids.join(", "))?;
    }
    writeln!(writer)?;
    writeln!(writer, "history:")?;
    for attempt in &snapshot.attempts {
        writeln!(
            writer,
            "  {}  {}  {}  {}",
            attempt.attempt_number,
            attempt.trigger,
            styled_state(attempt.state, color),
            styled_result(&attempt.result, color)
        )?;
    }
    Ok(())
}

fn styled_state(state: &str, color: bool) -> String {
    let shown = if state == "workflow_failed" {
        "failed"
    } else {
        state
    };
    let style = match state {
        "created" | "running" | "cancelling" => STYLE_ACTIVE,
        "succeeded" => STYLE_SUCCESS,
        "workflow_failed" | "interrupted" | "rejected" => STYLE_FAILURE,
        "cancelled" => STYLE_BLOCKED,
        _ => STYLE_FAILURE,
    };
    styled(shown, style, color)
}

fn styled_result(result: &LocalStatusResult, color: bool) -> String {
    match result {
        LocalStatusResult::NotPublished { reason } => {
            styled(&format!("not_published ({reason})"), STYLE_BLOCKED, color)
        }
        LocalStatusResult::Published { relative_directory } => format!(
            "{} ({relative_directory})",
            styled("published", STYLE_SUCCESS, color)
        ),
        LocalStatusResult::PublicationFailed { phase } => styled(
            &format!("publication_failed ({phase})"),
            STYLE_FAILURE,
            color,
        ),
    }
}

fn styled_recovery(recovery: &LocalRecoveryStatus, color: bool) -> String {
    let style = match recovery {
        LocalRecoveryStatus::Active => STYLE_ACTIVE,
        LocalRecoveryStatus::Settled => STYLE_SUCCESS,
        LocalRecoveryStatus::Abandoned => STYLE_BLOCKED,
        LocalRecoveryStatus::OwnershipUnproven { .. } => STYLE_FAILURE,
    };
    styled(recovery.as_str(), style, color)
}

fn styled_retry(retry: LocalRetryEligibility, color: bool) -> String {
    match retry {
        LocalRetryEligibility::Eligible => styled("eligible", STYLE_SUCCESS, color),
        LocalRetryEligibility::Ineligible(reason) => styled(
            &format!("ineligible ({})", reason.as_str()),
            retry_reason_style(reason),
            color,
        ),
    }
}

const fn retry_reason_style(reason: RetryIneligibilityReason) -> &'static str {
    match reason {
        RetryIneligibilityReason::RunLocked => STYLE_ACTIVE,
        RetryIneligibilityReason::LatestAttemptSucceeded => STYLE_SUCCESS,
        RetryIneligibilityReason::LatestAttemptRejected
        | RetryIneligibilityReason::OwnershipUnproven => STYLE_FAILURE,
    }
}
