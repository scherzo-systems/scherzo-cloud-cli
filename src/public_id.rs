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
