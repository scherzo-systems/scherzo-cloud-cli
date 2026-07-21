pub const VERSION: &str = select_value(
    option_env!("SCHERZO_CLOUD_VERSION"),
    env!("CARGO_PKG_VERSION"),
);
pub const BUILD_IDENTITY: &str =
    select_value(option_env!("SCHERZO_CLOUD_BUILD_IDENTITY"), "unknown");

const fn select_value<'a>(injected: Option<&'a str>, fallback: &'a str) -> &'a str {
    match injected {
        Some(value) => value,
        None => fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::select_value;

    #[test]
    fn missing_build_value_uses_the_fallback() {
        assert_eq!(select_value(None, "fallback"), "fallback");
    }

    #[test]
    fn injected_build_value_takes_precedence() {
        assert_eq!(select_value(Some("injected"), "fallback"), "injected");
    }
}
