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
mod api;
mod build_info;
mod cli;
mod error;
#[allow(
    dead_code,
    reason = "validation resolves execution fields that later runtime components will consume"
)]
mod execution;
mod exit_code;
mod human_auth;
mod process;
mod public_id;
mod runner;
mod runner_protocol;
mod timing;
mod tls;

use std::env;

use crate::exit_code::ExitCode;

fn main() -> ExitCode {
    if execution::workflow::child_guard::internal_worker_requested() {
        return execution::workflow::child_guard::run_internal_worker();
    }
    if execution::workflow::result_validation::internal_worker_requested() {
        return execution::workflow::result_validation::run_internal_worker();
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
