use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fmt;

use serde::{Deserialize, Serialize};
use url::Url;

const PRODUCTION_API_URL: &str = "https://api.scherzo.dev";
const PRODUCTION_ISSUER: &str = "https://auth.scherzo.dev/";
const PRODUCTION_AUDIENCE: &str = "https://api.scherzo.dev";
const PRODUCTION_CLIENT_ID: &str = "ly5kw9CZ8n0ntuMBeCoSFM4Sdj81tInx";

const API_URL_VARIABLE: &str = "SCHERZO_CLOUD_API_URL";
const ISSUER_VARIABLE: &str = "SCHERZO_CLOUD_AUTH_ISSUER";
const AUDIENCE_VARIABLE: &str = "SCHERZO_CLOUD_AUTH_AUDIENCE";
const CLIENT_ID_VARIABLE: &str = "SCHERZO_CLOUD_AUTH_CLIENT_ID";
const OVERRIDE_VARIABLES: [&str; 4] = [
    API_URL_VARIABLE,
    ISSUER_VARIABLE,
    AUDIENCE_VARIABLE,
    CLIENT_ID_VARIABLE,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Deployment {
    fingerprint: DeploymentFingerprint,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct DeploymentFingerprint {
    api_url: String,
    issuer: String,
    audience: String,
    client_id: String,
}

impl Deployment {
    pub(crate) fn load() -> Result<Self, DeploymentError> {
        Self::load_from(|name| env::var_os(name))
    }

    pub(crate) fn fingerprint(&self) -> &DeploymentFingerprint {
        &self.fingerprint
    }

    #[cfg(test)]
    pub(crate) fn for_test(api_url: String, issuer: String) -> Self {
        Self::from_values(
            api_url,
            issuer,
            "https://api.fixture.example".to_owned(),
            "fixture-public-client".to_owned(),
        )
        .expect("test deployment should be valid")
    }

    fn load_from<F>(lookup: F) -> Result<Self, DeploymentError>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let values = OVERRIDE_VARIABLES.map(lookup);
        let configured_count = values.iter().filter(|value| value.is_some()).count();

        if configured_count == 0 {
            return Self::from_values(
                PRODUCTION_API_URL.to_owned(),
                PRODUCTION_ISSUER.to_owned(),
                PRODUCTION_AUDIENCE.to_owned(),
                PRODUCTION_CLIENT_ID.to_owned(),
            );
        }

        if configured_count != OVERRIDE_VARIABLES.len() {
            return Err(partial_override_error(&values));
        }

        let [Some(api_url), Some(issuer), Some(audience), Some(client_id)] = values else {
            return Err(partial_override_error(&values));
        };

        Self::from_values(
            unicode_value(API_URL_VARIABLE, api_url)?,
            unicode_value(ISSUER_VARIABLE, issuer)?,
            unicode_value(AUDIENCE_VARIABLE, audience)?,
            unicode_value(CLIENT_ID_VARIABLE, client_id)?,
        )
    }

    fn from_values(
        api_url: String,
        issuer: String,
        audience: String,
        client_id: String,
    ) -> Result<Self, DeploymentError> {
        validate_network_url(API_URL_VARIABLE, &api_url)?;
        validate_network_url(ISSUER_VARIABLE, &issuer)?;
        validate_nonempty(AUDIENCE_VARIABLE, &audience)?;
        validate_nonempty(CLIENT_ID_VARIABLE, &client_id)?;

        Ok(Self {
            fingerprint: DeploymentFingerprint::new(api_url, issuer, audience, client_id),
        })
    }
}

impl DeploymentFingerprint {
    pub(crate) fn new(
        api_url: String,
        issuer: String,
        audience: String,
        client_id: String,
    ) -> Self {
        Self {
            api_url,
            issuer,
            audience,
            client_id,
        }
    }

    pub(crate) fn api_url(&self) -> &str {
        &self.api_url
    }

    pub(crate) fn issuer(&self) -> &str {
        &self.issuer
    }

    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }

    pub(crate) fn client_id(&self) -> &str {
        &self.client_id
    }

    #[cfg(test)]
    pub(crate) fn as_tuple(&self) -> (&str, &str, &str, &str) {
        (&self.api_url, &self.issuer, &self.audience, &self.client_id)
    }
}

#[derive(Debug)]
pub(crate) enum DeploymentError {
    PartialOverride {
        missing: Vec<&'static str>,
    },
    NonUnicode {
        variable: &'static str,
    },
    EmptyValue {
        variable: &'static str,
    },
    InvalidUrl {
        variable: &'static str,
        source: url::ParseError,
    },
    InvalidNetworkUrl {
        variable: &'static str,
    },
    UnsupportedScheme {
        variable: &'static str,
        scheme: String,
    },
}

impl fmt::Display for DeploymentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartialOverride { missing } => write!(
                formatter,
                "development deployment overrides must be set together; missing {}",
                missing.join(", ")
            ),
            Self::NonUnicode { variable } => {
                write!(formatter, "{variable} must contain valid Unicode")
            }
            Self::EmptyValue { variable } => write!(formatter, "{variable} must not be empty"),
            Self::InvalidUrl { variable, .. } => {
                write!(formatter, "{variable} must be a valid absolute URL")
            }
            Self::InvalidNetworkUrl { variable } => write!(
                formatter,
                "{variable} must be an absolute network URL without credentials, a query, or a fragment"
            ),
            Self::UnsupportedScheme { variable, scheme } => write!(
                formatter,
                "{variable} uses unsupported URL scheme {scheme}; expected https or http"
            ),
        }
    }
}

impl Error for DeploymentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUrl { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn partial_override_error(values: &[Option<OsString>; 4]) -> DeploymentError {
    let missing = OVERRIDE_VARIABLES
        .iter()
        .zip(values)
        .filter_map(|(name, value)| value.is_none().then_some(*name))
        .collect();

    DeploymentError::PartialOverride { missing }
}

fn unicode_value(variable: &'static str, value: OsString) -> Result<String, DeploymentError> {
    value
        .into_string()
        .map_err(|_| DeploymentError::NonUnicode { variable })
}

fn validate_nonempty(variable: &'static str, value: &str) -> Result<(), DeploymentError> {
    if value.trim().is_empty() {
        Err(DeploymentError::EmptyValue { variable })
    } else {
        Ok(())
    }
}

fn validate_network_url(variable: &'static str, value: &str) -> Result<(), DeploymentError> {
    validate_nonempty(variable, value)?;
    let url =
        Url::parse(value).map_err(|source| DeploymentError::InvalidUrl { variable, source })?;

    if url.cannot_be_a_base()
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DeploymentError::InvalidNetworkUrl { variable });
    }

    match url.scheme() {
        "https" | "http" => Ok(()),
        scheme => Err(DeploymentError::UnsupportedScheme {
            variable,
            scheme: scheme.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn load(values: &[(&str, &str)]) -> Result<Deployment, DeploymentError> {
        let environment: HashMap<&str, &str> = values.iter().copied().collect();
        Deployment::load_from(|name| environment.get(name).map(|value| OsString::from(*value)))
    }

    fn complete_override<'a>(api_url: &'a str, issuer: &'a str) -> [(&'static str, &'a str); 4] {
        [
            (API_URL_VARIABLE, api_url),
            (ISSUER_VARIABLE, issuer),
            (AUDIENCE_VARIABLE, "https://fixture.example/api"),
            (CLIENT_ID_VARIABLE, "fixture-public-client"),
        ]
    }

    #[test]
    fn absent_overrides_select_the_production_deployment() {
        let deployment = load(&[]).expect("production deployment should be valid");

        assert_eq!(
            deployment.fingerprint().as_tuple(),
            (
                PRODUCTION_API_URL,
                PRODUCTION_ISSUER,
                PRODUCTION_AUDIENCE,
                PRODUCTION_CLIENT_ID,
            )
        );
    }

    #[test]
    fn complete_override_is_preserved_as_one_deployment() {
        let values = complete_override(
            "https://api.fixture.example/base/",
            "https://auth.fixture.example/tenant/",
        );
        let deployment = load(&values).expect("complete override should be valid");

        assert_eq!(
            deployment.fingerprint().as_tuple(),
            (
                "https://api.fixture.example/base/",
                "https://auth.fixture.example/tenant/",
                "https://fixture.example/api",
                "fixture-public-client",
            )
        );
    }

    #[test]
    fn every_partial_override_combination_is_rejected() {
        for mask in 1_u8..15 {
            let values: Vec<(&str, &str)> = OVERRIDE_VARIABLES
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << index) != 0)
                .map(|(_, name)| (*name, "configured"))
                .collect();

            let error = load(&values).expect_err("partial override should fail");
            let DeploymentError::PartialOverride { missing } = error else {
                panic!("expected partial-override error");
            };
            let expected_missing: Vec<&str> = OVERRIDE_VARIABLES
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << index) == 0)
                .map(|(_, name)| *name)
                .collect();

            assert_eq!(missing, expected_missing);
        }
    }

    #[test]
    fn http_urls_are_preserved_for_request_time_transport_policy() {
        let values = complete_override(
            "http://api.fixture.example:8080/base/",
            "http://auth.fixture.example:9090/tenant/",
        );
        let deployment = load(&values).expect("HTTP overrides should be structurally valid");

        assert_eq!(deployment.fingerprint().as_tuple().0, values[0].1);
        assert_eq!(deployment.fingerprint().as_tuple().1, values[1].1);
    }

    #[test]
    fn malformed_and_non_network_urls_are_rejected() {
        for api_url in [
            "not a URL",
            "file:///tmp/api",
            "https://user@example.com",
            "https://api.example.com?target=other",
            "https://api.example.com/#fragment",
        ] {
            let values = complete_override(api_url, "https://auth.fixture.example/");

            assert!(load(&values).is_err(), "accepted {api_url}");
        }
    }

    #[test]
    fn empty_audience_and_client_id_are_rejected() {
        for variable in [AUDIENCE_VARIABLE, CLIENT_ID_VARIABLE] {
            let mut values = complete_override(
                "https://api.fixture.example",
                "https://auth.fixture.example/",
            );
            let (_, value) = values
                .iter_mut()
                .find(|(name, _)| *name == variable)
                .expect("variable should be present");
            *value = "";

            let error = load(&values).expect_err("empty override should fail");
            assert!(matches!(
                error,
                DeploymentError::EmptyValue { variable: actual } if actual == variable
            ));
        }
    }
}
