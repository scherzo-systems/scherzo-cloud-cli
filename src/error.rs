use std::io::{self, Write};

use crate::exit_code::ExitCode;

/// Render the one human-facing operational diagnostic shape for the CLI.
///
/// Output failures cannot change the registered failure class: recursively
/// attempting another diagnostic would only repeat the same write failure.
pub(crate) fn render(error: &anyhow::Error, exit_code: ExitCode) -> ExitCode {
    let standard_error = io::stderr();
    let mut standard_error = standard_error.lock();
    render_to(&mut standard_error, error, exit_code)
}

fn render_to(writer: &mut impl Write, error: &anyhow::Error, exit_code: ExitCode) -> ExitCode {
    let _ = writeln!(writer, "Error: {error:#}").and_then(|()| writer.flush());
    exit_code
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailedWriter;

    impl Write for FailedWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("synthetic diagnostic write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("synthetic diagnostic flush failure"))
        }
    }

    #[test]
    fn diagnostic_write_failure_preserves_the_registered_exit_code() {
        let error = anyhow::anyhow!("usage failure");

        let exit_code = render_to(&mut FailedWriter, &error, ExitCode::UsageError);

        assert_eq!(exit_code, ExitCode::UsageError);
    }
}
