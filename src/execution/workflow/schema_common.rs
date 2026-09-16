use std::path::{Component, Path, PathBuf};

use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

pub(super) use crate::workflow_contract::{is_identifier, is_lowercase_hex, lowercase_hex};

pub(super) fn utc_timestamp(value: OffsetDateTime) -> Result<String, time::error::Format> {
    value.to_offset(UtcOffset::UTC).format(&Rfc3339)
}

pub(super) fn parse_canonical_utc_timestamp(value: &str) -> Option<OffsetDateTime> {
    if !value.ends_with('Z') {
        return None;
    }
    let parsed = OffsetDateTime::parse(value, &Rfc3339).ok()?;
    (utc_timestamp(parsed).ok()?.as_str() == value).then_some(parsed)
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
