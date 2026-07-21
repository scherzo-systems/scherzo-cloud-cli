pub const ROOT_HELP: &str = "Scherzo Cloud CLI

Usage: scherzo-cloud <COMMAND>

Commands:
  version  Print version information
  runner   Run and manage the Scherzo Cloud runner

Options:
  -h, --help     Print help
  -V, --version  Print version";

pub const RUNNER_HELP: &str = "Run and manage the Scherzo Cloud runner

Usage: scherzo-cloud runner <COMMAND>

Commands:
  serve  Connect to Scherzo Cloud and serve run assignments

Options:
  -h, --help  Print help";

pub const RUNNER_SERVE_HELP: &str = "Connect to Scherzo Cloud and serve run assignments

Usage: scherzo-cloud runner serve

Options:
  -h, --help  Print help";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    RootHelp,
    Version,
    RunnerHelp,
    RunnerServe,
    RunnerServeHelp,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ParseError {
    pub message: String,
}

pub fn parse<I, S>(args: I) -> Result<Command, ParseError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();

    match args.as_slice() {
        [] => Ok(Command::RootHelp),
        [arg] if is_help(arg) => Ok(Command::RootHelp),
        [arg] if is_version(arg) || arg == "version" => Ok(Command::Version),
        [runner] if runner == "runner" => Ok(Command::RunnerHelp),
        [runner, arg] if runner == "runner" && is_help(arg) => Ok(Command::RunnerHelp),
        [runner, serve] if runner == "runner" && serve == "serve" => Ok(Command::RunnerServe),
        [runner, serve, arg] if runner == "runner" && serve == "serve" && is_help(arg) => {
            Ok(Command::RunnerServeHelp)
        }
        [arg, ..] => Err(ParseError {
            message: format!("unrecognized command or option: {arg}"),
        }),
    }
}

fn is_help(arg: &str) -> bool {
    matches!(arg, "-h" | "--help")
}

fn is_version(arg: &str) -> bool {
    matches!(arg, "-V" | "--version")
}

#[cfg(test)]
mod tests {
    use super::{Command, ParseError, parse};

    #[test]
    fn no_arguments_select_root_help() {
        assert_eq!(parse(Vec::<String>::new()), Ok(Command::RootHelp));
    }

    #[test]
    fn version_flag_selects_version() {
        assert_eq!(parse(["--version"]), Ok(Command::Version));
    }

    #[test]
    fn version_command_selects_version() {
        assert_eq!(parse(["version"]), Ok(Command::Version));
    }

    #[test]
    fn runner_serve_selects_runner_service() {
        assert_eq!(parse(["runner", "serve"]), Ok(Command::RunnerServe));
    }

    #[test]
    fn unknown_command_is_rejected() {
        assert_eq!(
            parse(["unknown"]),
            Err(ParseError {
                message: "unrecognized command or option: unknown".to_owned(),
            })
        );
    }
}
