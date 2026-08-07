use std::path::{Component, Path, PathBuf};

use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

pub(super) fn utc_timestamp(value: OffsetDateTime) -> Result<String, time::error::Format> {
    value.to_offset(UtcOffset::UTC).format(&Rfc3339)
}

pub(super) fn is_canonical_relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && path.components().all(|component| {
            matches!(component, Component::Normal(_)) && component.as_os_str().to_str().is_some()
        })
        && path.components().collect::<PathBuf>().as_os_str() == path.as_os_str()
}

pub(super) fn is_canonical_absolute_path(value: &str) -> bool {
    let path = Path::new(value);
    path.is_absolute()
        && !path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        && path.components().collect::<PathBuf>().as_os_str() == path.as_os_str()
}

pub(super) fn is_identifier(value: &str) -> bool {
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

pub(super) fn is_lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn lowercase_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}
