pub(crate) fn valid_typed_id(value: &str, prefix: &str) -> bool {
    let Some(suffix) = value.strip_prefix(prefix) else {
        return false;
    };
    let bytes = suffix.as_bytes();
    bytes.len() == 26
        && matches!(bytes.first(), Some(b'0'..=b'7'))
        && bytes[1..].iter().all(|byte| {
            matches!(
                byte,
                b'0'..=b'9' | b'a'..=b'h' | b'j'..=b'k' | b'm'..=b'n' | b'p'..=b't' | b'v'..=b'z'
            )
        })
}

pub(crate) fn valid_organization_ref(value: &str) -> bool {
    valid_typed_id(value, "org_") || valid_url_safe_name(value)
}

pub(crate) fn valid_url_safe_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}
