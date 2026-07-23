use std::fmt;
use std::future::Future;
use std::io;
use std::time::Duration;

use reqwest::{Client, Url};

use super::http_util;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HttpTransportPolicy {
    HttpsOnly,
    AllowInsecureHttp,
}

impl HttpTransportPolicy {
    pub(crate) fn permits(self, url: &Url) -> bool {
        url.scheme() == "https" || (self == Self::AllowInsecureHttp && url.scheme() == "http")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HttpEndpointError {
    Invalid,
    InsecureHttp,
}

pub(crate) struct HttpClient {
    runtime: Option<tokio::runtime::Runtime>,
    client: Client,
    transport_policy: HttpTransportPolicy,
}

impl HttpClient {
    pub(crate) fn new(transport_policy: HttpTransportPolicy) -> Result<Self, HttpClientError> {
        crate::tls::install_provider();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(HttpClientError::BuildRuntime)?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .https_only(transport_policy == HttpTransportPolicy::HttpsOnly)
            .build()
            .map_err(HttpClientError::BuildClient)?;

        Ok(Self {
            runtime: Some(runtime),
            client,
            transport_policy,
        })
    }

    pub(crate) fn endpoint(&self, base_url: &str, path: &[&str]) -> Result<Url, HttpEndpointError> {
        let endpoint =
            http_util::endpoint(base_url, path).map_err(|()| HttpEndpointError::Invalid)?;
        if self.transport_policy.permits(&endpoint) {
            Ok(endpoint)
        } else if endpoint.scheme() == "http" {
            Err(HttpEndpointError::InsecureHttp)
        } else {
            Err(HttpEndpointError::Invalid)
        }
    }

    pub(crate) fn transport_policy(&self) -> HttpTransportPolicy {
        self.transport_policy
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn https_only_policy_rejects_http_endpoints() {
        let client = HttpClient::new(HttpTransportPolicy::HttpsOnly).unwrap();

        assert_eq!(
            client.endpoint("http://api.fixture.example/base/", &["v1", "me"]),
            Err(HttpEndpointError::InsecureHttp)
        );
        assert!(
            client
                .endpoint("https://api.fixture.example/base/", &["v1", "me"])
                .is_ok()
        );
    }

    #[test]
    fn insecure_http_policy_permits_http_endpoints() {
        let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).unwrap();

        assert!(
            client
                .endpoint("http://api.fixture.example/base/", &["v1", "me"])
                .is_ok()
        );
    }
}
