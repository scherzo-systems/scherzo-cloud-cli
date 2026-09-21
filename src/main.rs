#![cfg_attr(
    test,
    allow(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used,
        reason = "unit tests use Cargo-provided fixture paths and panic shortcuts"
    )
)]
mod build_info;
mod cli;
mod error;
mod exit_code;
mod human_auth;
mod idempotency;
mod runner;
mod service_auth;

use std::env;

use crate::exit_code::ExitCode;

fn main() -> ExitCode {
    if scherzo_cloud_execution::child_guard_worker_requested() {
        return scherzo_cloud_execution::run_child_guard_worker().into();
    }
    if scherzo_cloud_execution::result_validation_worker_requested() {
        return scherzo_cloud_execution::run_result_validation_worker().into();
    }
    if runner::service::workflow_git_helper_requested() {
        return if runner::service::run_workflow_git_helper() {
            ExitCode::Success
        } else {
            ExitCode::GeneralFailure
        };
    }

    match cli::parse(env::args_os()) {
        Ok(command) => match command.execute() {
            Ok(exit_code) => exit_code,
            Err(failure) => error::render(failure.error(), failure.exit_code()),
        },
        Err(error) => {
            let exit_code = if error.use_stderr() {
                ExitCode::UsageError
            } else {
                ExitCode::Success
            };

            if let Err(write_error) = error.print() {
                let failure =
                    anyhow::Error::new(write_error).context("failed to write command output");
                return crate::error::render(&failure, ExitCode::GeneralFailure);
            }

            exit_code
        }
    }
}
