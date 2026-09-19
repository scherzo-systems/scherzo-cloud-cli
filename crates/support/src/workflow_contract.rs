pub(crate) mod strict_json;

use std::sync::OnceLock;

use jsonschema::Validator;
use serde_json::Value;

const STRUCTURAL_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/workflow-v1.schema.json"
));

static MEDIA_TYPE_VALIDATOR: OnceLock<Result<Validator, ()>> = OnceLock::new();

pub fn is_identifier(value: &str) -> bool {
    value.len() <= 64
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        && value
            .bytes()
            .skip(1)
            .all(|byte| byte.is_ascii_alphanumeric())
}

pub fn is_valid_input_display_name(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        !matches!(value, "" | "." | "..")
            && value.chars().count() <= 255
            && value
                .chars()
                .all(|character| !character.is_control() && character != '/' && character != '\\')
    })
}

pub fn is_valid_media_type(value: &str) -> bool {
    MEDIA_TYPE_VALIDATOR
        .get_or_init(|| {
            let schema = serde_json::from_str::<Value>(STRUCTURAL_SCHEMA).map_err(|_| ())?;
            let media_type_schema = schema.pointer("/$defs/MediaType").ok_or(())?;
            jsonschema::draft202012::new(media_type_schema).map_err(|_| ())
        })
        .as_ref()
        .is_ok_and(|validator| validator.is_valid(&Value::String(value.to_owned())))
}

pub fn is_lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn lowercase_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}
