use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use reqwest::blocking::Client as BlockingClient;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::{CONTENT_LENGTH, HeaderMap};
use reqwest::{Client, Url};
use zeroize::{Zeroize as _, Zeroizing};

use super::generated::apis;
use super::http_util;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpTransportPolicy {
    HttpsOnly,
    AllowInsecureHttp,
}

impl HttpTransportPolicy {
    pub fn permits(self, url: &Url) -> bool {
        url.scheme() == "https" || (self == Self::AllowInsecureHttp && url.scheme() == "http")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpEndpointError {
    Invalid,
    InsecureHttp,
}

#[derive(Clone)]
pub struct HttpCancellation {
    cancelled: Arc<AtomicBool>,
    changed: tokio::sync::watch::Sender<bool>,
}

impl Default for HttpCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpCancellation {
    pub fn new() -> Self {
        let (changed, _) = tokio::sync::watch::channel(false);
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            changed,
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.changed.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        let mut changed = self.changed.subscribe();
        if *changed.borrow() {
            return;
        }
        while changed.changed().await.is_ok() {
            if *changed.borrow() {
                return;
            }
        }
    }
}

pub struct HttpClient {
    runtime: Option<tokio::runtime::Runtime>,
    client: Client,
    transport_policy: HttpTransportPolicy,
}

impl HttpClient {
    pub fn new(transport_policy: HttpTransportPolicy) -> Result<Self, HttpClientError> {
        scherzo_cloud_support::install_provider();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(HttpClientError::BuildRuntime)?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .dns_resolver(categorized_dns_resolver())
            .https_only(transport_policy == HttpTransportPolicy::HttpsOnly)
            .build()
            .map_err(HttpClientError::BuildClient)?;

        Ok(Self {
            runtime: Some(runtime),
            client,
            transport_policy,
        })
    }

    pub fn endpoint(&self, base_url: &str, path: &[&str]) -> Result<Url, HttpEndpointError> {
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

    pub fn transport_policy(&self) -> HttpTransportPolicy {
        self.transport_policy
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "HttpClient is the production boundary for complete-request deadlines"
    )]
    #[expect(
        clippy::expect_used,
        reason = "HttpClient owns its runtime until Drop takes it after all borrows end"
    )]
    pub fn run<F>(
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

    pub(super) fn signed_storage_put(
        &self,
        url: &Url,
        headers: HeaderMap,
        bytes: &[u8],
        timeout: Duration,
    ) -> Result<StatusCode, SignedStorageRequestError> {
        let connector = HttpsConnectorBuilder::new()
            .try_with_platform_verifier()
            .map_err(|_| SignedStorageRequestError::Build)?;
        let connector = match self.transport_policy {
            HttpTransportPolicy::HttpsOnly => connector.https_only(),
            HttpTransportPolicy::AllowInsecureHttp => connector.https_or_http(),
        }
        .enable_http1()
        .build();
        let client: HyperClient<_, Full<Bytes>> =
            HyperClient::builder(TokioExecutor::new()).build(connector);
        let mut request = Request::builder()
            .method(Method::PUT)
            .uri(url.as_str())
            .body(Full::new(Bytes::copy_from_slice(bytes)))
            .map_err(|_| SignedStorageRequestError::InvalidRequest)?;
        *request.headers_mut() = headers;
        match self.run(timeout, client.request(request)) {
            Ok(Ok(response)) => Ok(response.status()),
            Ok(Err(error)) => Err(SignedStorageRequestError::Unreachable(
                super::current_principal::classify_error_chain(&error),
            )),
            Err(_) => Err(SignedStorageRequestError::Unreachable(
                super::UnreachableCategory::Timeout,
            )),
        }
    }

    pub(super) fn signed_storage_get(
        &self,
        url: &Url,
        maximum_bytes: u64,
        timeout: Duration,
        cancellation: &HttpCancellation,
    ) -> Result<SignedStorageDownload, SignedStorageGetError> {
        if !self.transport_policy.permits(url) {
            return Err(SignedStorageGetError::InvalidRequest);
        }
        let request = self.client.get(url.clone());
        match self.run(timeout, async {
            let mut response = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(SignedStorageGetError::Interrupted);
                }
                response = request.send() => response.map_err(|error| {
                    SignedStorageGetError::Unreachable(super::classify_reqwest_error(&error))
                })?,
            };
            let status = response.status();
            if status.is_server_error() {
                return Err(SignedStorageGetError::Server);
            }
            if status != StatusCode::OK {
                return Err(SignedStorageGetError::Rejected);
            }
            let content_length = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            if content_length.is_some_and(|length| length > maximum_bytes) {
                return Err(SignedStorageGetError::TooLarge);
            }
            let mut bytes = Zeroizing::new(Vec::with_capacity(
                content_length
                    .and_then(|length| usize::try_from(length).ok())
                    .unwrap_or(0),
            ));
            loop {
                let chunk = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(SignedStorageGetError::Interrupted);
                    }
                    chunk = response.chunk() => chunk.map_err(|error| {
                        SignedStorageGetError::Unreachable(super::classify_reqwest_error(&error))
                    })?,
                };
                let Some(chunk) = chunk else {
                    break;
                };
                if u64::try_from(bytes.len())
                    .ok()
                    .and_then(|length| length.checked_add(u64::try_from(chunk.len()).ok()?))
                    .is_none_or(|length| length > maximum_bytes)
                {
                    return Err(SignedStorageGetError::TooLarge);
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(SignedStorageDownload {
                status,
                content_length,
                bytes,
            })
        }) {
            Ok(result) => result,
            Err(_) => Err(SignedStorageGetError::Unreachable(
                super::UnreachableCategory::Timeout,
            )),
        }
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
pub(super) enum SignedStorageRequestError {
    Build,
    InvalidRequest,
    Unreachable(super::UnreachableCategory),
}

pub(super) struct SignedStorageDownload {
    pub(super) status: StatusCode,
    pub(super) content_length: Option<u64>,
    pub(super) bytes: Zeroizing<Vec<u8>>,
}

#[derive(Debug)]
pub(super) enum SignedStorageGetError {
    InvalidRequest,
    Unreachable(super::UnreachableCategory),
    Interrupted,
    TooLarge,
    Rejected,
    Server,
}

fn blocking_client_builder(
    transport_policy: HttpTransportPolicy,
    timeout: Duration,
) -> reqwest::blocking::ClientBuilder {
    BlockingClient::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(timeout)
        .dns_resolver(categorized_dns_resolver())
        .https_only(transport_policy == HttpTransportPolicy::HttpsOnly)
}

pub(super) fn generated_configuration(
    api_url: &str,
    access_token: &str,
    transport_policy: HttpTransportPolicy,
    timeout: Duration,
) -> Result<apis::configuration::Configuration, reqwest::Error> {
    scherzo_cloud_support::install_provider();
    let client = blocking_client_builder(transport_policy, timeout).build()?;
    let mut configuration = apis::configuration::Configuration::new();
    configuration.base_path = api_url.trim_end_matches('/').to_owned();
    configuration.bearer_access_token = Some(access_token.to_owned());
    configuration.client = client;
    Ok(configuration)
}

pub(super) fn zeroize_generated_bearer_access_token(
    configuration: &mut apis::configuration::Configuration,
) {
    if let Some(access_token) = &mut configuration.bearer_access_token {
        access_token.zeroize();
    }
}

pub(super) fn categorized_dns_resolver() -> Arc<impl Resolve> {
    Arc::new(CategorizedDnsResolver)
}

struct CategorizedDnsResolver;

impl Resolve for CategorizedDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            tokio::net::lookup_host((host, 0))
                .await
                .map(|addresses| Box::new(addresses) as Addrs)
                .map_err(|source| {
                    Box::new(DnsResolutionError::new(source)) as Box<dyn Error + Send + Sync>
                })
        })
    }
}

#[derive(Debug)]
pub(super) struct DnsResolutionError {
    source: Box<dyn Error + Send + Sync>,
}

impl DnsResolutionError {
    pub(super) fn new(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Box::new(source),
        }
    }
}

impl fmt::Display for DnsResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DNS resolution failed")
    }
}

impl Error for DnsResolutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug)]
pub enum HttpClientError {
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
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "HTTP client unit tests use Rust test assertions and fixture extraction"
)]
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
