use std::sync::Arc;

use serde_json::Value;

use super::artifact::{CapturedArtifact, CapturedGitBranch, StagedCarrier};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CapturedValue {
    Text(Arc<str>),
    Json(Arc<Value>),
    File(CapturedArtifact),
    GitBranch(CapturedGitBranch),
}

impl CapturedValue {
    pub(crate) fn file(value: CapturedArtifact) -> Self {
        Self::File(value)
    }

    pub(crate) fn git_branch(value: CapturedGitBranch) -> Self {
        Self::GitBranch(value)
    }

    pub(crate) fn as_file(&self) -> Option<&CapturedArtifact> {
        match self {
            Self::File(file) => Some(file),
            Self::Text(_) | Self::Json(_) | Self::GitBranch(_) => None,
        }
    }

    pub(crate) fn into_file(self) -> Option<CapturedArtifact> {
        match self {
            Self::File(file) => Some(file),
            Self::Text(_) | Self::Json(_) | Self::GitBranch(_) => None,
        }
    }

    pub(crate) fn as_git_branch(&self) -> Option<&CapturedGitBranch> {
        match self {
            Self::GitBranch(branch) => Some(branch),
            Self::Text(_) | Self::Json(_) | Self::File(_) => None,
        }
    }

    pub(crate) fn carrier(&self) -> Option<&StagedCarrier> {
        match self {
            Self::File(file) => Some(file.carrier()),
            Self::GitBranch(branch) => branch.carrier().map(|carrier| carrier.staged()),
            Self::Text(_) | Self::Json(_) => None,
        }
    }
}
