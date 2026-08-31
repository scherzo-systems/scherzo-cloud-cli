use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use serde_json::Value;

use super::artifact::{CaptureLease, CapturedArtifact, CapturedGitBranch, StagedCarrier};
use super::canonical_json;
use super::result_validation::RetainedJsonSchema;
use super::validated::WorkflowValueType;

#[derive(Clone)]
pub(crate) struct CapturedText {
    inner: Arc<CapturedTextInner>,
}

struct CapturedTextInner {
    value: Arc<str>,
    carrier: Arc<[u8]>,
    capture_lease: Option<CaptureLease>,
}

impl CapturedText {
    pub(crate) fn new(value: Arc<str>) -> Self {
        Self {
            inner: Arc::new(CapturedTextInner {
                carrier: Arc::from(value.as_bytes()),
                value,
                capture_lease: None,
            }),
        }
    }

    pub(crate) fn carrier(&self) -> &[u8] {
        &self.inner.carrier
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.inner.value
    }

    pub(crate) const fn value_type(&self) -> WorkflowValueType {
        WorkflowValueType::Text
    }

    pub(super) fn from_bounded_carrier(
        carrier: Arc<[u8]>,
        capture_lease: CaptureLease,
    ) -> Result<Self, SemanticCarrierError> {
        Self::from_carrier(carrier, Some(capture_lease))
    }

    fn from_carrier(
        carrier: Arc<[u8]>,
        capture_lease: Option<CaptureLease>,
    ) -> Result<Self, SemanticCarrierError> {
        let value = std::str::from_utf8(&carrier)
            .map(Arc::<str>::from)
            .map_err(|_| SemanticCarrierError::InvalidTextEncoding)?;
        Ok(Self {
            inner: Arc::new(CapturedTextInner {
                value,
                carrier,
                capture_lease,
            }),
        })
    }

    fn private_capture_carrier(&self) -> Option<&StagedCarrier> {
        self.inner.capture_lease.as_ref().map(CaptureLease::carrier)
    }
}

impl AsRef<str> for CapturedText {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for CapturedText {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Debug for CapturedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CapturedText")
            .field(&self.as_str())
            .finish()
    }
}

impl PartialEq for CapturedText {
    fn eq(&self, other: &Self) -> bool {
        self.carrier() == other.carrier()
    }
}

impl Eq for CapturedText {}

#[derive(Clone)]
pub(crate) struct CapturedJson {
    inner: Arc<CapturedJsonInner>,
}

struct CapturedJsonInner {
    value: Arc<Value>,
    carrier: Arc<[u8]>,
    schema: RetainedJsonSchema,
    capture_lease: Option<CaptureLease>,
}

impl CapturedJson {
    pub(super) fn from_validated(
        value: Arc<Value>,
        carrier: Arc<[u8]>,
        schema: RetainedJsonSchema,
    ) -> Self {
        Self {
            inner: Arc::new(CapturedJsonInner {
                value,
                carrier,
                schema,
                capture_lease: None,
            }),
        }
    }

    pub(super) fn from_bounded_carrier(
        value: Arc<Value>,
        carrier: Arc<[u8]>,
        schema: RetainedJsonSchema,
        capture_lease: CaptureLease,
    ) -> Result<Self, SemanticCarrierError> {
        if !canonical_carrier_matches(&value, &carrier) {
            return Err(SemanticCarrierError::InvalidCanonicalJson);
        }
        Ok(Self {
            inner: Arc::new(CapturedJsonInner {
                value,
                carrier,
                schema,
                capture_lease: Some(capture_lease),
            }),
        })
    }

    pub(crate) fn value(&self) -> &Value {
        &self.inner.value
    }

    pub(crate) fn carrier(&self) -> &[u8] {
        &self.inner.carrier
    }

    pub(crate) fn canonical_json(&self) -> &[u8] {
        self.carrier()
    }

    pub(crate) fn schema(&self) -> &RetainedJsonSchema {
        &self.inner.schema
    }

    pub(crate) const fn value_type(&self) -> WorkflowValueType {
        WorkflowValueType::Json
    }

    fn private_capture_carrier(&self) -> Option<&StagedCarrier> {
        self.inner.capture_lease.as_ref().map(CaptureLease::carrier)
    }

    #[cfg(test)]
    pub(crate) fn fixture(value: Arc<Value>) -> Self {
        let schema_document = Arc::new(serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema"
        }));
        let schema_bytes =
            Arc::from(br#"{"$schema":"https://json-schema.org/draft/2020-12/schema"}"#.as_slice());
        let schema = RetainedJsonSchema::compile(schema_bytes, schema_document).unwrap();
        let carrier = canonical_json::to_bounded_bytes(&value, u64::MAX).unwrap();
        Self::from_validated(value, carrier, schema)
    }
}

impl AsRef<Value> for CapturedJson {
    fn as_ref(&self) -> &Value {
        self.value()
    }
}

impl Deref for CapturedJson {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl fmt::Debug for CapturedJson {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapturedJson")
            .field("value", &self.value())
            .field("carrier", &self.carrier())
            .finish_non_exhaustive()
    }
}

impl PartialEq for CapturedJson {
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value() && self.carrier() == other.carrier()
    }
}

impl Eq for CapturedJson {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SemanticCarrierError {
    InvalidTextEncoding,
    InvalidCanonicalJson,
}

fn canonical_carrier_matches(value: &Value, carrier: &[u8]) -> bool {
    let Ok(maximum_bytes) = u64::try_from(carrier.len()) else {
        return false;
    };
    canonical_json::to_bounded_bytes(value, maximum_bytes)
        .is_ok_and(|canonical| canonical.as_ref() == carrier)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CapturedValue {
    Text(CapturedText),
    Json(CapturedJson),
    File(CapturedArtifact),
    GitBranch(CapturedGitBranch),
}

impl CapturedValue {
    pub(crate) fn text(value: Arc<str>) -> Self {
        Self::Text(CapturedText::new(value))
    }

    pub(crate) fn json(value: CapturedJson) -> Self {
        Self::Json(value)
    }

    #[cfg(test)]
    pub(crate) fn json_fixture(value: Arc<Value>) -> Self {
        Self::Json(CapturedJson::fixture(value))
    }

    pub(crate) fn file(value: CapturedArtifact) -> Self {
        Self::File(value)
    }

    pub(crate) fn git_branch(value: CapturedGitBranch) -> Self {
        Self::GitBranch(value)
    }

    pub(crate) const fn value_type(&self) -> WorkflowValueType {
        match self {
            Self::Text(value) => value.value_type(),
            Self::Json(value) => value.value_type(),
            Self::File(_) => WorkflowValueType::File,
            Self::GitBranch(_) => WorkflowValueType::GitBranch,
        }
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

    pub(super) fn private_capture_carrier(&self) -> Option<&StagedCarrier> {
        match self {
            Self::Text(text) => text.private_capture_carrier(),
            Self::Json(json) => json.private_capture_carrier(),
            Self::File(file) => Some(file.carrier()),
            Self::GitBranch(branch) => branch.carrier().map(|carrier| carrier.staged()),
        }
    }
}
