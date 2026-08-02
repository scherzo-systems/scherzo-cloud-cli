use std::sync::Arc;

use serde_json::Value;

use super::artifact::CapturedArtifact;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CapturedValue {
    Text(Arc<str>),
    Json(Arc<Value>),
    File(CapturedArtifact),
}

impl CapturedValue {
    pub(crate) fn file(value: CapturedArtifact) -> Self {
        Self::File(value)
    }

    pub(crate) fn as_file(&self) -> Option<&CapturedArtifact> {
        match self {
            Self::File(file) => Some(file),
            Self::Text(_) | Self::Json(_) => None,
        }
    }
}
