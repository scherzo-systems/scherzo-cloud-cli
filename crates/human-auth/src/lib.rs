#[cfg(test)]
macro_rules! check {
    ($condition:expr $(, $($message:tt)+)?) => {
        anyhow::ensure!($condition $(, $($message)+)?);
    };
}

#[cfg(test)]
macro_rules! check_eq {
    ($left:expr, $right:expr $(,)?) => {{
        let left = &$left;
        let right = &$right;
        anyhow::ensure!(
            left == right,
            "expected equality: left = {left:?}, right = {right:?}"
        );
    }};
}

#[cfg(test)]
macro_rules! check_ne {
    ($left:expr, $right:expr $(,)?) => {{
        let left = &$left;
        let right = &$right;
        anyhow::ensure!(
            left != right,
            "expected inequality: left = {left:?}, right = {right:?}"
        );
    }};
}

mod cancellation;
mod credentials;
mod deployment;
mod device_authorization;
mod device_flow;
mod session;
mod status;
mod token;

pub use cancellation::Cancellation;
pub use credentials::CredentialStore;
pub use deployment::Deployment;
pub use device_authorization::{AuthorizationError, DeviceAuthorization, IssuedToken};
pub use device_flow::{
    ActivationEvent, DeviceFlowError, DeviceFlowOutcome, DeviceFlowPhase, activation_event,
    identity_proof, session as begin_session,
};
pub use session::{
    BoundRequiredOperation, LocalCredentialState, LogoutOutcome, RequiredOperation,
    RequiredOperationWithBinding, RevocationState, SessionBinding, execute_bound_required,
    execute_optional, execute_required, execute_required_until, execute_required_with_binding,
    logout, remove_bound_credential,
};
pub use status::{
    AuthenticationState, AuthenticationStatus, StatusError, check as check_auth_status,
    check_with_service_api_key,
};
pub use token::SecretToken;
