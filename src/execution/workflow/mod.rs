pub(crate) mod admission;
pub(crate) mod agent;
pub(crate) mod agent_input;
pub(crate) mod artifact;
mod canonical_json;
pub(crate) mod coordinator;
pub(crate) mod diagnostic;
pub(crate) mod document;
pub(crate) mod execution;
mod execution_root;
pub(crate) mod input;
pub(crate) mod observation;
mod pi;
pub(crate) mod pi_json_v1;
pub(crate) mod presentation;
mod private_staging;
pub(crate) mod publication;
pub(crate) mod rejection;
pub(crate) mod resolution;
pub(crate) mod result_validation;
pub(crate) mod runtime;
mod schema;
pub(crate) mod step_runtime;
mod strict_yaml;
#[cfg(test)]
mod test_support;
pub(crate) mod validated;
pub(crate) mod validation;
pub(crate) mod value;

use std::fmt;
use std::sync::OnceLock;

use jsonschema::Validator;
use serde_json::Value;

use document::WorkflowDocument;

const STRUCTURAL_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/schemas/workflow-v1.schema.json"
));
pub(crate) const MAX_DECODE_DIAGNOSTIC_BYTES: usize = 96;

static STRUCTURAL_VALIDATOR: OnceLock<Result<Validator, ()>> = OnceLock::new();
static MEDIA_TYPE_VALIDATOR: OnceLock<Result<Validator, ()>> = OnceLock::new();

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
    let parsed = strict_yaml::parse(bytes)?;
    let validator = structural_validator().ok_or_else(DecodeFailure::structural_contract)?;
    if !validator.is_valid(&parsed.value) {
        return Err(DecodeFailure::structural_contract());
    }

    let dto = serde_json::from_value::<schema::WorkflowDto>(parsed.value)
        .map_err(|_| DecodeFailure::structural_contract())?;
    dto.into_document(parsed.step_order)
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

fn is_valid_media_type(value: &str) -> bool {
    MEDIA_TYPE_VALIDATOR
        .get_or_init(|| {
            let schema = serde_json::from_str::<Value>(STRUCTURAL_SCHEMA).map_err(|_| ())?;
            let media_type_schema = schema.pointer("/$defs/MediaType").ok_or(())?;
            jsonschema::draft202012::new(media_type_schema).map_err(|_| ())
        })
        .as_ref()
        .is_ok_and(|validator| validator.is_valid(&Value::String(value.to_owned())))
}

#[cfg(test)]
mod tests;
