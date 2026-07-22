use std::io::{self, Read};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const MAX_STDOUT_BYTES: usize = 8 * 1024;
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) trait CommandRunner: Send + Sync {
    fn run(
        &self,
        program: &str,
        args: &[&str],
        timeout: Duration,
    ) -> Result<CommandOutput, CommandProbeError>;
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
    fn run(
        &self,
        program: &str,
        args: &[&str],
        timeout: Duration,
    ) -> Result<CommandOutput, CommandProbeError> {
        let mut child = Command::new(program)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| match error.kind() {
                io::ErrorKind::NotFound => CommandProbeError::CommandNotFound,
                _ => CommandProbeError::Spawn,
            })?;

        let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
            terminate(&mut child);
            return Err(CommandProbeError::PipeRead);
        };
        let stdout_thread = thread::spawn(move || drain_stdout(stdout));
        let stderr_thread = thread::spawn(move || drain(stderr));
        let started = Instant::now();

        let success = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status.success(),
                Ok(None) if started.elapsed() >= timeout => {
                    terminate(&mut child);
                    let _ = join_readers(stdout_thread, stderr_thread);
                    return Err(CommandProbeError::Timeout);
                }
                Ok(None) => thread::sleep(WAIT_POLL_INTERVAL),
                Err(_) => {
                    terminate(&mut child);
                    let _ = join_readers(stdout_thread, stderr_thread);
                    return Err(CommandProbeError::Wait);
                }
            }
        };

        let (stdout, truncated) = join_readers(stdout_thread, stderr_thread)?;
        Ok(CommandOutput {
            success,
            stdout,
            truncated,
        })
    }
}

fn terminate(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
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

fn drain_stdout(mut reader: impl Read) -> io::Result<(Vec<u8>, bool)> {
    let mut retained = Vec::with_capacity(MAX_STDOUT_BYTES);
    let mut buffer = [0_u8; 4096];
    let mut truncated = false;

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok((retained, truncated));
        }

        let available = MAX_STDOUT_BYTES.saturating_sub(retained.len());
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
    use std::time::{Duration, Instant};

    use super::{CommandProbeError, CommandRunner, MAX_STDOUT_BYTES, SystemCommandRunner};

    #[cfg(unix)]
    #[test]
    fn timed_out_command_is_killed_and_reaped() {
        let runner = SystemCommandRunner;
        let started = Instant::now();

        let result = runner.run(
            "/bin/sh",
            &["-c", "while :; do :; done"],
            Duration::from_millis(50),
        );

        assert_eq!(result, Err(CommandProbeError::Timeout));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[cfg(unix)]
    #[test]
    fn excessive_standard_output_is_drained_and_truncated() {
        let runner = SystemCommandRunner;
        let output = runner
            .run(
                "/bin/sh",
                &[
                    "-c",
                    "i=0; while [ \"$i\" -le 8192 ]; do printf x; i=$((i + 1)); done",
                ],
                Duration::from_secs(1),
            )
            .unwrap();

        assert!(output.success);
        assert_eq!(output.stdout.len(), MAX_STDOUT_BYTES);
        assert!(output.truncated);
    }
}
