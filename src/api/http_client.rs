use std::fmt;
use std::future::Future;
use std::io;
use std::sync::OnceLock;
use std::time::Duration;

use reqwest::Client;

static TLS_PROVIDER: OnceLock<()> = OnceLock::new();

pub(crate) struct HttpClient {
    runtime: Option<tokio::runtime::Runtime>,
    client: Client,
}

impl HttpClient {
    pub(crate) fn new() -> Result<Self, HttpClientError> {
        install_tls_provider();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(HttpClientError::BuildRuntime)?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(HttpClientError::BuildClient)?;

        Ok(Self {
            runtime: Some(runtime),
            client,
        })
    }

    pub(crate) fn inner(&self) -> &Client {
        &self.client
    }

    pub(crate) fn run<F>(
        &self,
        timeout: Duration,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future,
    {
        self.runtime
            .as_ref()
            .expect("HTTP runtime should exist until the client is dropped")
            .block_on(async { tokio::time::timeout(timeout, future).await })
    }
}

impl Drop for HttpClient {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::ZERO);
        }
    }
}

#[derive(Debug)]
pub(crate) enum HttpClientError {
    BuildRuntime(io::Error),
    BuildClient(reqwest::Error),
}

impl fmt::Display for HttpClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BuildRuntime(error) => write!(formatter, "build HTTP runtime: {error}"),
            Self::BuildClient(error) => write!(formatter, "build HTTP client: {error}"),
        }
    }
}

fn install_tls_provider() {
    TLS_PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
