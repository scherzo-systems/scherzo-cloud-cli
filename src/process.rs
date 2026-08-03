use std::env;
use std::ffi::OsStr;
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use rustix::process::{Pid, Signal, kill_process_group};

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) trait CommandRunner: Send + Sync {
    fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError>;
}

#[derive(Clone, Copy)]
pub(crate) struct CommandRequest<'a> {
    pub(crate) program: &'a Path,
    pub(crate) args: &'a [&'a str],
    pub(crate) timeout: Duration,
    pub(crate) maximum_stdout_bytes: usize,
    pub(crate) clear_environment: bool,
    pub(crate) environment: &'a [(&'a OsStr, &'a OsStr)],
    pub(crate) current_directory: Option<&'a Path>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandOutput {
    pub(crate) success: bool,
    pub(crate) stdout: Vec<u8>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandProbeError {
    CommandNotFound,
    Spawn,
    Timeout,
    Wait,
    PipeRead,
}

pub(crate) struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError> {
        let executable = resolve_program(command.program)?;
        let mut process = Command::new(executable);
        process
            .args(command.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        if command.clear_environment {
            process.env_clear();
        }
        process.envs(command.environment.iter().copied());
        if let Some(current_directory) = command.current_directory {
            process.current_dir(current_directory);
        }
        let mut child = process.spawn().map_err(|error| match error.kind() {
            io::ErrorKind::NotFound => CommandProbeError::CommandNotFound,
            _ => CommandProbeError::Spawn,
        })?;
        let process_group = i32::try_from(child.id()).ok().and_then(Pid::from_raw);

        let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
            terminate(&mut child, process_group);
            return Err(CommandProbeError::PipeRead);
        };
        let stdout_thread =
            thread::spawn(move || drain_stdout(stdout, command.maximum_stdout_bytes));
        let stderr_thread = thread::spawn(move || drain(stderr));
        let started = crate::timing::monotonic_now();

        let success = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status.success(),
                Ok(None) if crate::timing::elapsed(started) >= command.timeout => {
                    terminate(&mut child, process_group);
                    let _ = join_readers(stdout_thread, stderr_thread);
                    return Err(CommandProbeError::Timeout);
                }
                Ok(None) => crate::timing::sleep(WAIT_POLL_INTERVAL),
                Err(_) => {
                    terminate(&mut child, process_group);
                    let _ = join_readers(stdout_thread, stderr_thread);
                    return Err(CommandProbeError::Wait);
                }
            }
        };

        terminate_process_group(process_group);
        let (stdout, truncated) = join_readers(stdout_thread, stderr_thread)?;
        Ok(CommandOutput {
            success,
            stdout,
            truncated,
        })
    }
}

fn resolve_program(program: &Path) -> Result<PathBuf, CommandProbeError> {
    if !matches!(
        program.components().collect::<Vec<_>>().as_slice(),
        [Component::Normal(_)]
    ) {
        return Ok(program.to_owned());
    }

    // Resolve bare names ourselves so command classification does not depend
    // on platform-specific spawn-time PATH lookup.
    let search_path = env::var_os("PATH").ok_or(CommandProbeError::CommandNotFound)?;
    let mut inaccessible_candidate = false;
    for directory in env::split_paths(&search_path) {
        let candidate = directory.join(program);
        match candidate.metadata() {
            Ok(metadata) if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 => {
                return Ok(candidate);
            }
            Ok(_) => inaccessible_candidate = true,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) => {}
            Err(_) => inaccessible_candidate = true,
        }
    }

    if inaccessible_candidate {
        Err(CommandProbeError::Spawn)
    } else {
        Err(CommandProbeError::CommandNotFound)
    }
}

fn terminate(child: &mut std::process::Child, process_group: Option<Pid>) {
    terminate_process_group(process_group);
    let _ = child.kill();
    let _ = child.wait();
}

fn terminate_process_group(process_group: Option<Pid>) {
    if let Some(process_group) = process_group {
        let _ = kill_process_group(process_group, Signal::KILL);
    }
}

fn join_readers(
    stdout_thread: thread::JoinHandle<io::Result<(Vec<u8>, bool)>>,
    stderr_thread: thread::JoinHandle<io::Result<()>>,
) -> Result<(Vec<u8>, bool), CommandProbeError> {
    let stdout = stdout_thread
        .join()
        .map_err(|_| CommandProbeError::PipeRead)?
        .map_err(|_| CommandProbeError::PipeRead)?;
    stderr_thread
        .join()
        .map_err(|_| CommandProbeError::PipeRead)?
        .map_err(|_| CommandProbeError::PipeRead)?;
    Ok(stdout)
}

fn drain_stdout(mut reader: impl Read, maximum_bytes: usize) -> io::Result<(Vec<u8>, bool)> {
    let mut retained = Vec::with_capacity(maximum_bytes);
    let mut buffer = [0_u8; 4096];
    let mut truncated = false;

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok((retained, truncated));
        }

        let available = maximum_bytes.saturating_sub(retained.len());
        let retained_bytes = available.min(read);
        retained.extend_from_slice(&buffer[..retained_bytes]);
        truncated |= retained_bytes < read;
    }
}

fn drain(mut reader: impl Read) -> io::Result<()> {
    let mut buffer = [0_u8; 4096];
    while reader.read(&mut buffer)? != 0 {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use super::{CommandProbeError, CommandRequest, CommandRunner, SystemCommandRunner};

    const FIXTURE_STDOUT_LIMIT: usize = 8 * 1024;

    #[cfg(unix)]
    #[test]
    fn timed_out_command_terminates_pipe_holding_descendants() {
        let runner = SystemCommandRunner;

        let result = runner.run(CommandRequest {
            program: Path::new("/bin/sh"),
            args: &["-c", "(while :; do :; done) & while :; do :; done"],
            timeout: Duration::from_millis(50),
            maximum_stdout_bytes: FIXTURE_STDOUT_LIMIT,
            clear_environment: false,
            environment: &[],
            current_directory: None,
        });

        assert_eq!(result, Err(CommandProbeError::Timeout));
    }

    #[cfg(unix)]
    #[test]
    fn excessive_standard_output_is_drained_and_truncated_at_the_callers_limit() {
        let runner = SystemCommandRunner;
        let output = runner
            .run(CommandRequest {
                program: Path::new("/bin/sh"),
                args: &[
                    "-c",
                    "i=0; while [ \"$i\" -le 8192 ]; do printf x; i=$((i + 1)); done",
                ],
                timeout: Duration::from_secs(1),
                maximum_stdout_bytes: FIXTURE_STDOUT_LIMIT,
                clear_environment: false,
                environment: &[],
                current_directory: None,
            })
            .unwrap();

        assert!(output.success);
        assert_eq!(output.stdout.len(), FIXTURE_STDOUT_LIMIT);
        assert!(output.truncated);
    }
}
