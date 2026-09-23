pub(super) fn valid_secret_syntax(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

pub(super) use scherzo_cloud_support::valid_typed_id;
