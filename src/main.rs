mod cli;

use std::env;
use std::process::ExitCode;

use cli::Command;

fn main() -> ExitCode {
    match cli::parse(env::args().skip(1)) {
        Ok(Command::RootHelp) => {
            println!("{}", cli::ROOT_HELP);
            ExitCode::SUCCESS
        }
        Ok(Command::Version) => {
            println!("scherzo-cloud {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Ok(Command::RunnerHelp) => {
            println!("{}", cli::RUNNER_HELP);
            ExitCode::SUCCESS
        }
        Ok(Command::RunnerServeHelp) => {
            println!("{}", cli::RUNNER_SERVE_HELP);
            ExitCode::SUCCESS
        }
        Ok(Command::RunnerServe) => {
            eprintln!("Error: scherzo-cloud runner serve is not implemented yet");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("Error: {}\n\n{}", error.message, cli::ROOT_HELP);
            ExitCode::from(2)
        }
    }
}
