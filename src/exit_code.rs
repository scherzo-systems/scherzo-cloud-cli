use std::process::{ExitCode as ProcessExitCode, Termination};

/// Registry of process exit codes emitted by the CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ExitCode {
    Success = 0,
    GeneralFailure = 1,
    UsageError = 2,
    AuthenticationRequired = 3,
    Unavailable = 4,
    Interrupted = 130,
    Terminated = 143,
}

impl ExitCode {
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }

    pub(crate) const fn as_u16(self) -> u16 {
        self as u16
    }

    pub(crate) const fn from_u16(code: u16) -> Option<Self> {
        match code {
            0 => Some(Self::Success),
            1 => Some(Self::GeneralFailure),
            2 => Some(Self::UsageError),
            3 => Some(Self::AuthenticationRequired),
            4 => Some(Self::Unavailable),
            130 => Some(Self::Interrupted),
            143 => Some(Self::Terminated),
            _ => None,
        }
    }
}

impl From<ExitCode> for ProcessExitCode {
    fn from(exit_code: ExitCode) -> Self {
        Self::from(exit_code.as_u8())
    }
}

impl Termination for ExitCode {
    fn report(self) -> ProcessExitCode {
        self.into()
    }
}
