mod api;
mod build_info;
mod cli;
mod human_auth;

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
