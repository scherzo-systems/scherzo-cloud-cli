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

/// Command-independent classes of CLI outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutcomeClass {
    Success,
    GeneralFailure,
    Unauthenticated,
    Forbidden,
    Unreachable,
    RateLimited,
    Protocol,
    Interrupted,
    Terminated,
}

impl OutcomeClass {
    /// The single outcome-class to process-exit-code table.
    pub(crate) const fn exit_code(self) -> ExitCode {
        match self {
            Self::Success => ExitCode::Success,
            Self::GeneralFailure | Self::Forbidden | Self::Protocol => ExitCode::GeneralFailure,
            Self::Unauthenticated => ExitCode::AuthenticationRequired,
            Self::Unreachable | Self::RateLimited => ExitCode::Unavailable,
            Self::Interrupted => ExitCode::Interrupted,
            Self::Terminated => ExitCode::Terminated,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{ExitCode, OutcomeClass};

    #[test]
    fn outcome_classes_map_to_registered_exit_codes() {
        let cases = [
            (OutcomeClass::Success, ExitCode::Success),
            (OutcomeClass::GeneralFailure, ExitCode::GeneralFailure),
            (
                OutcomeClass::Unauthenticated,
                ExitCode::AuthenticationRequired,
            ),
            (OutcomeClass::Forbidden, ExitCode::GeneralFailure),
            (OutcomeClass::Unreachable, ExitCode::Unavailable),
            (OutcomeClass::RateLimited, ExitCode::Unavailable),
            (OutcomeClass::Protocol, ExitCode::GeneralFailure),
            (OutcomeClass::Interrupted, ExitCode::Interrupted),
            (OutcomeClass::Terminated, ExitCode::Terminated),
        ];

        for (outcome, expected) in cases {
            assert_eq!(outcome.exit_code(), expected);
        }
    }
}
