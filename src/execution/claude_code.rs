use std::path::{Path, PathBuf};

pub(crate) const CLAUDE_CODE_STREAM_JSON_V1_VERSION: &str = "2.1.222";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClaudeCodeCompatibilityProfile {
    ClaudeCodeStreamJsonV1,
}

impl ClaudeCodeCompatibilityProfile {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCodeStreamJsonV1 => "ClaudeCodeStreamJsonV1",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClaudeCodeVersion(Box<str>);

impl ClaudeCodeVersion {
    #[cfg(test)]
    fn exact() -> Self {
        Self(CLAUDE_CODE_STREAM_JSON_V1_VERSION.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedClaudeCodeInstallation {
    executable: PathBuf,
    version: ClaudeCodeVersion,
    profile: ClaudeCodeCompatibilityProfile,
}

impl ValidatedClaudeCodeInstallation {
    #[cfg(test)]
    pub(crate) fn fixture(executable: PathBuf) -> Self {
        Self {
            executable,
            version: ClaudeCodeVersion::exact(),
            profile: ClaudeCodeCompatibilityProfile::ClaudeCodeStreamJsonV1,
        }
    }

    pub(crate) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(crate) fn version(&self) -> &ClaudeCodeVersion {
        &self.version
    }

    pub(crate) const fn profile(&self) -> ClaudeCodeCompatibilityProfile {
        self.profile
    }
}
