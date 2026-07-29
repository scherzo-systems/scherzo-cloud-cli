pub(crate) mod document;
pub(crate) mod resolution;
mod schema;
mod strict_yaml;
pub(crate) mod validated;
pub(crate) mod validation;

use std::fmt;
use std::sync::OnceLock;

use jsonschema::Validator;
use serde_json::Value;

use document::WorkflowDocument;

const STRUCTURAL_SCHEMA: &str = include_str!("workflow-v1.schema.json");
pub(crate) const MAX_DECODE_DIAGNOSTIC_BYTES: usize = 96;

static STRUCTURAL_VALIDATOR: OnceLock<Result<Validator, ()>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodeFailureKind {
    MalformedYaml,
    ForbiddenYaml,
    StructuralContract,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodeFailure {
    kind: DecodeFailureKind,
    diagnostic: &'static str,
}

impl DecodeFailure {
    pub(crate) fn kind(self) -> DecodeFailureKind {
        self.kind
    }

    pub(crate) fn diagnostic(self) -> &'static str {
        self.diagnostic
    }

    fn malformed_yaml() -> Self {
        Self {
            kind: DecodeFailureKind::MalformedYaml,
            diagnostic: "workflow document is not well-formed YAML",
        }
    }

    fn forbidden_yaml() -> Self {
        Self {
            kind: DecodeFailureKind::ForbiddenYaml,
            diagnostic: "workflow document uses a forbidden YAML feature or scalar",
        }
    }

    fn structural_contract() -> Self {
        Self {
            kind: DecodeFailureKind::StructuralContract,
            diagnostic: "workflow document violates the Workflow V1 structural contract",
        }
    }
}

impl fmt::Display for DecodeFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.diagnostic)
    }
}

impl std::error::Error for DecodeFailure {}

pub(crate) fn decode(bytes: &[u8]) -> Result<WorkflowDocument, DecodeFailure> {
    let value = strict_yaml::parse(bytes)?;
    let validator = structural_validator().ok_or_else(DecodeFailure::structural_contract)?;
    if !validator.is_valid(&value) {
        return Err(DecodeFailure::structural_contract());
    }

    let dto = serde_json::from_value::<schema::WorkflowDto>(value)
        .map_err(|_| DecodeFailure::structural_contract())?;
    dto.into_document()
        .ok_or_else(DecodeFailure::structural_contract)
}

fn structural_validator() -> Option<&'static Validator> {
    STRUCTURAL_VALIDATOR
        .get_or_init(|| {
            let schema = serde_json::from_str::<Value>(STRUCTURAL_SCHEMA).map_err(|_| ())?;
            jsonschema::draft202012::new(&schema).map_err(|_| ())
        })
        .as_ref()
        .ok()
}

#[cfg(test)]
mod tests;
