use std::fmt;

use url::Url;

use crate::runner::credential::Credential;

pub(crate) struct Config {
    endpoint: Url,
    credential: Credential,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("endpoint", &self.endpoint)
            .field("credential", &self.credential)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ConfigError {
    InvalidGatewayUrl,
    InsecureGatewayUrl,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGatewayUrl => formatter.write_str("invalid runner gateway URL"),
            Self::InsecureGatewayUrl => {
                formatter.write_str("insecure runner gateway URL is not allowed")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    pub(crate) fn new(
        gateway_url: &str,
        credential: Credential,
        allow_insecure_http: bool,
    ) -> Result<Self, ConfigError> {
        let endpoint = Url::parse(gateway_url).map_err(|_| ConfigError::InvalidGatewayUrl)?;
        if endpoint.username() != ""
            || endpoint.password().is_some()
            || endpoint.host_str().is_none()
        {
            return Err(ConfigError::InvalidGatewayUrl);
        }
        match endpoint.scheme() {
            "wss" => {}
            "ws" if allow_insecure_http && is_loopback(&endpoint) => {}
            "ws" => return Err(ConfigError::InsecureGatewayUrl),
            _ => return Err(ConfigError::InvalidGatewayUrl),
        }
        Ok(Self {
            endpoint,
            credential,
        })
    }

    pub(crate) fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    pub(crate) fn credential(&self) -> &Credential {
        &self.credential
    }
}

fn is_loopback(endpoint: &Url) -> bool {
    match endpoint.host_str() {
        Some(host) if host.eq_ignore_ascii_case("localhost") => true,
        Some(host) => host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback()),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ConfigError};
    use crate::runner::credential::test_credential;

    #[test]
    fn permits_wss_and_explicit_loopback_ws_only() {
        assert!(
            Config::new(
                "wss://gateway.example.test/v1/connect",
                test_credential(),
                false
            )
            .is_ok()
        );
        assert!(Config::new("ws://127.0.0.1:8081/v1/connect", test_credential(), true).is_ok());
        assert_eq!(
            Config::new("ws://127.0.0.1:8081/v1/connect", test_credential(), false).unwrap_err(),
            ConfigError::InsecureGatewayUrl,
        );
        assert_eq!(
            Config::new(
                "ws://gateway.example.test/v1/connect",
                test_credential(),
                true
            )
            .unwrap_err(),
            ConfigError::InsecureGatewayUrl,
        );
    }
}
