pub const VERSION: &str = select_version(
    option_env!("SCHERZO_CLOUD_VERSION"),
    env!("CARGO_PKG_VERSION"),
);

const fn select_version<'a>(injected: Option<&'a str>, package: &'a str) -> &'a str {
    match injected {
        Some(version) => version,
        None => package,
    }
}

#[cfg(test)]
mod tests {
    use super::select_version;

    #[test]
    fn package_version_is_the_local_fallback() {
        assert_eq!(select_version(None, "0.1.0"), "0.1.0");
    }

    #[test]
    fn injected_build_version_takes_precedence() {
        assert_eq!(
            select_version(Some("0.1.42+rev-abc123"), "0.1.0"),
            "0.1.42+rev-abc123"
        );
    }
}
