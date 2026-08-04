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
#[allow(
    dead_code,
    reason = "validation resolves execution fields that later runtime components will consume"
)]
mod execution;
mod human_auth;
mod process;
mod runner;
mod runner_protocol;
mod timing;
mod tls;

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    if execution::workflow::result_validation::internal_worker_requested() {
        return execution::workflow::result_validation::run_internal_worker();
    }

    match cli::parse(env::args_os()) {
        Ok(command) => command.execute(),
        Err(error) => {
            let exit_code = error.exit_code();

            if let Err(write_error) = error.print() {
                eprintln!("Error: failed to write command output: {write_error}");
                return ExitCode::FAILURE;
            }

            to_exit_code(exit_code)
        }
    }
}

fn to_exit_code(code: i32) -> ExitCode {
    match u8::try_from(code) {
        Ok(code) => ExitCode::from(code),
        Err(_) => ExitCode::FAILURE,
    }
}
