use std::fmt;

use reqwest::StatusCode;
use serde::Deserialize;
use time::OffsetDateTime;

use crate::api::{HttpClient, HttpTransportPolicy, UnreachableCategory};

use super::credentials::{CredentialError, CredentialStore, StoredCredential};
use super::deployment::Deployment;
use super::device_authorization::{self, AuthorizationError, AuthorizationLocalError, IssuedToken};

const REFRESH_GRANT_TYPE: &str = "refresh_token";
const TOKEN_PATH: [&str; 2] = ["oauth", "token"];
const REVOCATION_PATH: [&str; 2] = ["oauth", "revoke"];
const MAX_REFRESH_ATTEMPTS: usize = 2;

pub(crate) enum RequiredOperation<T, E> {
    Completed(Result<T, E>),
    Unauthenticated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RevocationState {
    Confirmed,
    Unconfirmed,
    NotApplicable,
}

pub(crate) struct LogoutOutcome {
    credential_removed: bool,
    revocation: RevocationState,
}

impl LogoutOutcome {
    pub(crate) fn credential_removed(&self) -> bool {
        self.credential_removed
    }

    pub(crate) fn revocation(&self) -> RevocationState {
        self.revocation
    }
}

pub(crate) fn execute_optional<T, E>(
    client: &HttpClient,
    deployment: &Deployment,
    mut operation: impl FnMut(Option<&str>) -> Result<T, E>,
    credential_rejected: impl Fn(&Result<T, E>) -> bool,
) -> Result<Result<T, E>, SessionError> {
    let store = CredentialStore::from_environment().map_err(SessionError::CredentialStore)?;
    let Some(credential) = credential_for_use(&store, client, deployment)? else {
        return Ok(operation(None));
    };

    let first = operation(Some(credential.access_token()));
    if !credential_rejected(&first) {
        return Ok(first);
    }

    let Some(credential) =
        refresh_after_rejection(&store, client, deployment, credential.access_token())?
    else {
        return Ok(operation(None));
    };
    let second = operation(Some(credential.access_token()));
    if credential_rejected(&second) {
        remove_rejected_credential(&store, deployment, credential.access_token())?;
    }
    Ok(second)
}

pub(crate) fn execute_required<T, E>(
    client: &HttpClient,
    deployment: &Deployment,
    mut operation: impl FnMut(&str) -> Result<T, E>,
    credential_rejected: impl Fn(&Result<T, E>) -> bool,
) -> Result<RequiredOperation<T, E>, SessionError> {
    let store = CredentialStore::from_environment().map_err(SessionError::CredentialStore)?;
    let Some(credential) = credential_for_use(&store, client, deployment)? else {
        return Ok(RequiredOperation::Unauthenticated);
    };

    let first = operation(credential.access_token());
    if !credential_rejected(&first) {
        return Ok(RequiredOperation::Completed(first));
    }

    let Some(credential) =
        refresh_after_rejection(&store, client, deployment, credential.access_token())?
    else {
        return Ok(RequiredOperation::Unauthenticated);
    };
    let second = operation(credential.access_token());
    if credential_rejected(&second) {
        remove_rejected_credential(&store, deployment, credential.access_token())?;
    }
    Ok(RequiredOperation::Completed(second))
}

pub(crate) fn logout(
    deployment: &Deployment,
    transport_policy: HttpTransportPolicy,
) -> Result<LogoutOutcome, SessionError> {
    let store = CredentialStore::from_environment().map_err(SessionError::CredentialStore)?;
    let _authority = store
        .refresh_authority(deployment.fingerprint())
        .map_err(SessionError::CredentialStore)?;
    let Some(credential) = store
        .take_under_authority(deployment.fingerprint())
        .map_err(SessionError::CredentialStore)?
    else {
        return Ok(LogoutOutcome {
            credential_removed: false,
            revocation: RevocationState::NotApplicable,
        });
    };

    let revocation = HttpClient::new(transport_policy)
        .ok()
        .and_then(|client| revoke(&client, deployment, credential.refresh_token()).ok())
        .map_or(RevocationState::Unconfirmed, |confirmed| {
            if confirmed {
                RevocationState::Confirmed
            } else {
                RevocationState::Unconfirmed
            }
        });

    Ok(LogoutOutcome {
        credential_removed: true,
        revocation,
    })
}

fn credential_for_use(
    store: &CredentialStore,
    client: &HttpClient,
    deployment: &Deployment,
) -> Result<Option<StoredCredential>, SessionError> {
    let credential = store
        .selected(deployment.fingerprint())
        .map_err(SessionError::CredentialStore)?;
    match credential {
        Some(credential) if credential.needs_refresh(crate::timing::utc_now()) => {
            coordinated_refresh(store, client, deployment, RefreshReason::Expiring)
        }
        credential => Ok(credential),
    }
}

fn refresh_after_rejection(
    store: &CredentialStore,
    client: &HttpClient,
    deployment: &Deployment,
    rejected_access_token: &str,
) -> Result<Option<StoredCredential>, SessionError> {
    coordinated_refresh(
        store,
        client,
        deployment,
        RefreshReason::Rejected(rejected_access_token),
    )
}

fn coordinated_refresh(
    store: &CredentialStore,
    client: &HttpClient,
    deployment: &Deployment,
    reason: RefreshReason<'_>,
) -> Result<Option<StoredCredential>, SessionError> {
    let _authority = store
        .refresh_authority(deployment.fingerprint())
        .map_err(SessionError::CredentialStore)?;
    let Some(current) = store
        .selected(deployment.fingerprint())
        .map_err(SessionError::CredentialStore)?
    else {
        return Ok(None);
    };

    let should_refresh = match reason {
        RefreshReason::Expiring => current.needs_refresh(crate::timing::utc_now()),
        RefreshReason::Rejected(rejected) => {
            current.access_token() == rejected || current.needs_refresh(crate::timing::utc_now())
        }
    };
    if !should_refresh {
        return Ok(Some(current));
    }

    let expected_refresh_token = current.refresh_token().to_owned();
    let issued = match exchange_refresh_token(client, deployment, &expected_refresh_token) {
        Ok(issued) => issued,
        Err(RefreshExchangeError::Terminal) => {
            store
                .remove_if_refresh_token_matches_under_authority(
                    deployment.fingerprint(),
                    &expected_refresh_token,
                )
                .map_err(SessionError::CredentialStore)?;
            return Ok(None);
        }
        Err(RefreshExchangeError::Local(error)) => {
            return Err(SessionError::RefreshLocal(error));
        }
        Err(RefreshExchangeError::Unreachable(category)) => {
            return Err(SessionError::RefreshUnreachable(category));
        }
        Err(RefreshExchangeError::Protocol { reason }) => {
            return Err(SessionError::RefreshProtocol { reason });
        }
    };
    let expires_at =
        expiration_after(issued.expires_in()).ok_or(SessionError::RefreshProtocol {
            reason: "the access-token expiration is out of range",
        })?;
    store
        .replace_if_refresh_token_matches(
            deployment.fingerprint(),
            &expected_refresh_token,
            issued.access_token(),
            expires_at,
            issued.refresh_token(),
        )
        .map_err(SessionError::CredentialStore)
}

fn remove_rejected_credential(
    store: &CredentialStore,
    deployment: &Deployment,
    access_token: &str,
) -> Result<(), SessionError> {
    let _authority = store
        .refresh_authority(deployment.fingerprint())
        .map_err(SessionError::CredentialStore)?;
    store
        .remove_if_access_token_matches_under_authority(deployment.fingerprint(), access_token)
        .map_err(SessionError::CredentialStore)?;
    Ok(())
}

fn exchange_refresh_token(
    client: &HttpClient,
    deployment: &Deployment,
    refresh_token: &str,
) -> Result<IssuedToken, RefreshExchangeError> {
    let endpoint = client
        .endpoint(deployment.fingerprint().issuer(), &TOKEN_PATH)
        .map_err(|error| RefreshExchangeError::Local(AuthorizationLocalError::Endpoint(error)))?;
    let fields = [
        ("grant_type", REFRESH_GRANT_TYPE),
        ("refresh_token", refresh_token),
        ("client_id", deployment.fingerprint().client_id()),
    ];

    for attempt in 0..MAX_REFRESH_ATTEMPTS {
        let response = match device_authorization::post_form(client, endpoint.clone(), &fields) {
            Ok(response) => response,
            Err(AuthorizationError::Unreachable(category))
                if attempt + 1 < MAX_REFRESH_ATTEMPTS
                    && matches!(
                        category,
                        UnreachableCategory::Connection | UnreachableCategory::Timeout
                    ) =>
            {
                continue;
            }
            Err(AuthorizationError::Unreachable(category)) => {
                return Err(RefreshExchangeError::Unreachable(category));
            }
            Err(AuthorizationError::Local(error)) => {
                return Err(RefreshExchangeError::Local(error));
            }
            Err(AuthorizationError::Protocol { reason }) => {
                return Err(RefreshExchangeError::Protocol { reason });
            }
        };

        if response.status == StatusCode::OK {
            device_authorization::require_json(&response).map_err(map_protocol_error)?;
            return device_authorization::decode_issued_token(&response.body)
                .map_err(map_protocol_error);
        }
        if response.status.is_redirection() {
            return Err(RefreshExchangeError::Protocol {
                reason: "redirect responses are not permitted",
            });
        }
        if response.status == StatusCode::TOO_MANY_REQUESTS || response.status.is_server_error() {
            let category = if response.status == StatusCode::TOO_MANY_REQUESTS {
                UnreachableCategory::RateLimited
            } else {
                UnreachableCategory::Server
            };
            return Err(RefreshExchangeError::Unreachable(category));
        }
        if response.status.is_client_error() {
            if response.content_type.as_deref() == Some("application/json")
                && serde_json::from_slice::<OAuthErrorResponse>(&response.body)
                    .is_ok_and(|body| body.error == "invalid_grant")
            {
                return Err(RefreshExchangeError::Terminal);
            }
            return Err(RefreshExchangeError::Protocol {
                reason: "the refresh-token error response is invalid",
            });
        }
        return Err(RefreshExchangeError::Protocol {
            reason: "the refresh-token HTTP status is invalid",
        });
    }

    Err(RefreshExchangeError::Protocol {
        reason: "the refresh attempt bound was exhausted",
    })
}

fn revoke(
    client: &HttpClient,
    deployment: &Deployment,
    refresh_token: &str,
) -> Result<bool, AuthorizationError> {
    let endpoint = client
        .endpoint(deployment.fingerprint().issuer(), &REVOCATION_PATH)
        .map_err(|error| AuthorizationError::Local(AuthorizationLocalError::Endpoint(error)))?;
    let fields = [
        ("token", refresh_token),
        ("client_id", deployment.fingerprint().client_id()),
    ];
    let response = device_authorization::post_form(client, endpoint, &fields)?;
    Ok(response.status == StatusCode::OK)
}

fn map_protocol_error(error: AuthorizationError) -> RefreshExchangeError {
    match error {
        AuthorizationError::Local(error) => RefreshExchangeError::Local(error),
        AuthorizationError::Unreachable(category) => RefreshExchangeError::Unreachable(category),
        AuthorizationError::Protocol { reason } => RefreshExchangeError::Protocol { reason },
    }
}

fn expiration_after(duration: std::time::Duration) -> Option<OffsetDateTime> {
    let seconds = i64::try_from(duration.as_secs()).ok()?;
    crate::timing::utc_now().checked_add(time::Duration::seconds(seconds))
}

#[derive(Deserialize)]
struct OAuthErrorResponse {
    error: String,
}

enum RefreshReason<'a> {
    Expiring,
    Rejected(&'a str),
}

enum RefreshExchangeError {
    Terminal,
    Local(AuthorizationLocalError),
    Unreachable(UnreachableCategory),
    Protocol { reason: &'static str },
}

#[derive(Debug)]
pub(crate) enum SessionError {
    CredentialStore(CredentialError),
    RefreshLocal(AuthorizationLocalError),
    RefreshUnreachable(UnreachableCategory),
    RefreshProtocol { reason: &'static str },
}

impl SessionError {
    pub(crate) fn unreachable_category(&self) -> Option<UnreachableCategory> {
        match self {
            Self::RefreshUnreachable(category) => Some(*category),
            _ => None,
        }
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialStore(error) => write!(formatter, "human credential store: {error}"),
            Self::RefreshLocal(error) => write!(formatter, "prepare session refresh: {error}"),
            Self::RefreshUnreachable(category) => write!(
                formatter,
                "authorization server is unreachable during session refresh ({})",
                category.as_str()
            ),
            Self::RefreshProtocol { reason } => write!(
                formatter,
                "session refresh response violates the OAuth contract: {reason}"
            ),
        }
    }
}
