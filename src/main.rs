#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used,
        reason = "unit tests use panic shortcuts to express assertion failures"
    )
)]
#![allow(
    clippy::disallowed_methods,
    reason = "runner service timing raises this restriction within its own module"
)]

mod api;
mod build_info;
mod cli;
#[allow(
    dead_code,
    reason = "workflow decoding is an internal execution boundary with no CLI surface yet"
)]
mod execution;
mod human_auth;
mod runner;
mod runner_protocol;
mod tls;

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
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
