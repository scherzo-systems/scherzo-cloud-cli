use std::sync::OnceLock;

static PROVIDER: OnceLock<()> = OnceLock::new();

pub(crate) fn install_provider() {
    PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
