use std::fmt;

use crate::api::{
    self, AuthenticatedPrincipal, CurrentPrincipalError, CurrentPrincipalOutcome, HttpClient,
    UnreachableCategory,
};

use super::deployment::Deployment;
use super::session::{self, SessionError};

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
    let outcome = match session::execute_optional(
        client,
        deployment,
        |access_token| {
            api::get_current_principal(
                client,
                deployment.fingerprint().api_url(),
                access_token.map(|token| token.expose()),
            )
        },
        |outcome| {
            matches!(outcome, Ok(CurrentPrincipalOutcome::Unauthenticated))
                || outcome
                    .as_ref()
                    .is_err_and(CurrentPrincipalError::credential_rejected)
        },
    ) {
        Ok(outcome) => outcome.map_err(StatusError::PublicApi)?,
        Err(error) => match error.unreachable_category() {
            Some(category) => CurrentPrincipalOutcome::Unreachable(category),
            None => return Err(StatusError::Session(error)),
        },
    };

    let state = match outcome {
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
    Session(SessionError),
    PublicApi(CurrentPrincipalError),
}

impl fmt::Display for StatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => write!(formatter, "acquire human session: {error}"),
            Self::PublicApi(error) => write!(formatter, "contact Scherzo Cloud: {error}"),
        }
    }
}
