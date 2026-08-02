use std::fmt;

use crate::api::{
    self, AuthenticatedPrincipal, CurrentPrincipalError, CurrentPrincipalOutcome, HttpClient,
    UnreachableCategory,
};

use super::credentials::{CredentialError, CredentialStore};
use super::deployment::Deployment;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AuthenticationStatus {
    deployment: String,
    state: AuthenticationState,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AuthenticationState {
    Authenticated(AuthenticatedPrincipal),
    SignupRequired {
        actions: Option<Vec<serde_json::Value>>,
    },
    Unauthenticated,
    Unreachable(UnreachableCategory),
}

impl AuthenticationStatus {
    pub(crate) fn deployment(&self) -> &str {
        &self.deployment
    }

    pub(crate) fn state(&self) -> &AuthenticationState {
        &self.state
    }
}

pub(crate) fn check(
    client: &HttpClient,
    deployment: &Deployment,
) -> Result<AuthenticationStatus, StatusError> {
    let store = CredentialStore::from_environment().map_err(StatusError::CredentialStore)?;
    let credential = store
        .selected(deployment.fingerprint(), crate::timing::utc_now())
        .map_err(StatusError::CredentialStore)?;
    let access_token = credential
        .as_ref()
        .map(|credential| credential.access_token());
    let outcome =
        api::get_current_principal(client, deployment.fingerprint().api_url(), access_token);

    let credential_rejected = matches!(&outcome, Ok(CurrentPrincipalOutcome::Unauthenticated))
        || outcome
            .as_ref()
            .is_err_and(|error| error.credential_rejected());
    if credential_rejected && let Some(access_token) = access_token {
        store
            .remove_if_access_token_matches(deployment.fingerprint(), access_token)
            .map_err(StatusError::CredentialStore)?;
    }

    let state = match outcome.map_err(StatusError::PublicApi)? {
        CurrentPrincipalOutcome::Authenticated(authenticated) => {
            AuthenticationState::Authenticated(authenticated)
        }
        CurrentPrincipalOutcome::SignupRequired { actions } => {
            AuthenticationState::SignupRequired { actions }
        }
        CurrentPrincipalOutcome::Unauthenticated => AuthenticationState::Unauthenticated,
        CurrentPrincipalOutcome::Unreachable(category) => {
            AuthenticationState::Unreachable(category)
        }
    };

    Ok(AuthenticationStatus {
        deployment: deployment.fingerprint().api_url().to_owned(),
        state,
    })
}

#[derive(Debug)]
pub(crate) enum StatusError {
    CredentialStore(CredentialError),
    PublicApi(CurrentPrincipalError),
}

impl fmt::Display for StatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialStore(error) => write!(formatter, "access credential store: {error}"),
            Self::PublicApi(error) => write!(formatter, "contact Scherzo Cloud: {error}"),
        }
    }
}
