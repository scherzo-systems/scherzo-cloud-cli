use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use crate::runner::control_protocol::{AssignmentCounts, Operation, Response, StatusSnapshot};

pub(super) const ABOUT: &str = "Show live Runner Serve status";

#[derive(Debug, Args)]
pub(super) struct Command {
    /// Read the control socket path from the closed runner operator configuration.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let config_path = super::operator_config_path(&self.config)?;
        let socket_path = crate::runner::enrollment::load_control_socket_path(&config_path)
            .with_context(|| {
                format!(
                    "load runner operator configuration {}",
                    config_path.display()
                )
            })?;
        let response = crate::runner::control_client::request(&socket_path, Operation::Status)
            .map_err(|error| {
                super::super::CommandFailure::with_exit_code(
                    anyhow::Error::new(error),
                    ExitCode::Unavailable,
                )
            })?;
        let Response::Status(status) = response else {
            return Err(super::super::CommandFailure::with_exit_code(
                anyhow::anyhow!("Runner Serve is not reachable"),
                ExitCode::Unavailable,
            ));
        };
        write_status(&mut io::stdout().lock(), &status).map_err(anyhow::Error::new)?;
        Ok(ExitCode::Success)
    }
}

fn write_status(output: &mut impl Write, status: &StatusSnapshot) -> io::Result<()> {
    writeln!(
        output,
        "process:        {}",
        enum_json(status.process_state)
    )?;
    writeln!(output, "boot:           {}", status.boot_id)?;
    writeln!(
        output,
        "uptime:         {}",
        format_uptime(status.uptime_milliseconds)
    )?;
    writeln!(
        output,
        "connection:     {}",
        enum_json(status.connection_state)
    )?;
    if let Some(last_connected_at) = &status.last_connected_at {
        writeln!(output, "last connected: {last_connected_at}")?;
    }
    if let Some(credential_id) = &status.current_credential_id {
        writeln!(output, "credential:     {credential_id}")?;
    }
    if let Some(credential_id) = &status.pending_credential_id {
        writeln!(output, "pending:        {credential_id}")?;
    }
    writeln!(
        output,
        "assignments:    {}",
        format_assignments(status.assignment_counts)
    )?;
    if let Some(failure) = status.last_connection_failure {
        writeln!(output, "last failure:   {}", enum_json(failure))?;
    }
    Ok(())
}

fn enum_json(value: impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn format_uptime(milliseconds: u64) -> String {
    let seconds = milliseconds / 1_000;
    let days = seconds / 86_400;
    let hours = seconds % 86_400 / 3_600;
    let minutes = seconds % 3_600 / 60;
    let remaining_seconds = seconds % 60;
    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {remaining_seconds}s")
    } else {
        format!("{remaining_seconds}s")
    }
}

fn format_assignments(counts: AssignmentCounts) -> String {
    let total = counts.total().unwrap_or(u64::MAX);
    let details = [
        ("preparing", counts.preparing),
        ("accepted", counts.accepted),
        ("running", counts.running),
        ("finishing", counts.finishing),
        ("reporting", counts.reporting),
    ]
    .into_iter()
    .filter(|(_, count)| *count != 0)
    .map(|(state, count)| format!("{state}: {count}"))
    .collect::<Vec<_>>();
    if details.is_empty() {
        format!("{total} total")
    } else {
        format!("{total} total ({})", details.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::control_protocol::{ConnectionState, ProcessState};

    #[test]
    fn renders_idle_and_nonzero_assignment_counts() {
        assert_eq!(format_assignments(AssignmentCounts::default()), "0 total");
        assert_eq!(
            format_assignments(AssignmentCounts {
                running: 1,
                ..AssignmentCounts::default()
            }),
            "1 total (running: 1)"
        );
    }

    #[test]
    fn renders_the_closed_live_snapshot_without_absent_optional_fields() {
        let status = StatusSnapshot {
            process_state: ProcessState::Running,
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            uptime_milliseconds: 273_600_000,
            connection_state: ConnectionState::Connected,
            last_connected_at: Some("2026-08-06T19:00:00Z".to_owned()),
            current_credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            pending_credential_id: None,
            assignment_counts: AssignmentCounts {
                running: 1,
                ..AssignmentCounts::default()
            },
            last_connection_failure: None,
        };
        let mut output = Vec::new();
        write_status(&mut output, &status).unwrap();
        let expected = [
            "process:        running\n",
            "boot:           rbt_01k0z6r1w8f4jy2m7q9v3x5abc\n",
            "uptime:         3d 4h\n",
            "connection:     connected\n",
            "last connected: 2026-08-06T19:00:00Z\n",
            "credential:     ",
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
            "\nassignments:    1 total (running: 1)\n",
        ]
        .concat();
        assert_eq!(String::from_utf8(output).unwrap(), expected);
    }
}
