use std::fmt;

use base64::Engine as _;

use super::validation::{valid_secret_syntax, valid_typed_id};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum CredentialError {
    InvalidState,
}

impl fmt::Display for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState => formatter.write_str("enrolled runner credential is invalid"),
        }
    }
}

impl std::error::Error for CredentialError {}

// Credential is runner-only machine authentication material loaded from the
// protected enrolled state. Its Debug output deliberately omits the bearer.
#[derive(Clone)]
pub(crate) struct Credential {
    runner_id: String,
    credential_id: String,
    value: String,
}

impl fmt::Debug for Credential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credential")
            .field("runner_id", &self.runner_id)
            .field("credential_id", &self.credential_id)
            .field("value", &"[redacted]")
            .finish()
    }
}

impl Credential {
    pub(crate) fn from_enrolled_state(
        runner_id: &str,
        credential_id: &str,
        secret: &str,
    ) -> Result<Self, CredentialError> {
        if !valid_typed_id(runner_id, "rnr_")
            || !valid_typed_id(credential_id, "rrc_")
            || !valid_secret_syntax(secret)
            || !base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(secret)
                .is_ok_and(|decoded| decoded.len() == 32)
        {
            return Err(CredentialError::InvalidState);
        }
        Ok(Self {
            runner_id: runner_id.to_owned(),
            credential_id: credential_id.to_owned(),
            value: format!("{credential_id}.{secret}"),
        })
    }

    // Credential access remains on the redacting authentication type rather
    // than sharing enrollment-response accessors that have no secret boundary.
    // jscpd:ignore-start
    pub(crate) fn runner_id(&self) -> &str {
        &self.runner_id
    }

    pub(crate) fn credential_id(&self) -> &str {
        &self.credential_id
    }

    pub(crate) fn bearer_value(&self) -> &str {
        &self.value
    }
    // jscpd:ignore-end
}

#[cfg(test)]
pub(crate) fn test_credential() -> Credential {
    Credential::from_enrolled_state(
        "rnr_01k0z6r1w8f4jy2m7q9v3x5abd",
        "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    )
    .expect("construct test runner credential")
}

#[cfg(test)]
mod tests {
    use super::{Credential, CredentialError};

    #[test]
    fn builds_the_current_enrolled_bearer_without_exposing_it_in_debug() {
        let credential = super::test_credential();
        let bearer = "rrc_01k0z6r1w8f4jy2m7q9v3x5abd.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert_eq!(credential.runner_id(), "rnr_01k0z6r1w8f4jy2m7q9v3x5abd");
        assert_eq!(credential.bearer_value(), bearer);
        assert!(!format!("{credential:?}").contains(bearer));
    }

    #[test]
    fn rejects_inconsistent_enrolled_material() {
        for (runner_id, credential_id, secret) in [
            (
                "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
                "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            ),
            (
                "rnr_01k0z6r1w8f4jy2m7q9v3x5abd",
                "rnr_01k0z6r1w8f4jy2m7q9v3x5abd",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            ),
            (
                "rnr_01k0z6r1w8f4jy2m7q9v3x5abd",
                "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
                "not-a-secret",
            ),
        ] {
            assert_eq!(
                Credential::from_enrolled_state(runner_id, credential_id, secret).unwrap_err(),
                CredentialError::InvalidState,
            );
        }
    }
}
