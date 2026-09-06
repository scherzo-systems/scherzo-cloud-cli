use std::time::{Duration, Instant};

use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::cancellation::Cancellation;
use super::deployment::Deployment;
use super::device_authorization::{
    self, AuthorizationError, DeviceAuthorization, IssuedToken, TokenPoll,
};
use super::token::SecretToken;
use crate::api::HttpClient;

const SLOW_DOWN_INCREMENT: Duration = Duration::from_secs(5);

pub(crate) enum DeviceFlowOutcome<T> {
    Issued(T),
    Denied,
    Expired,
    Cancelled,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActivationEvent<'a> {
    schema_version: u8,
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation: Option<&'static str>,
    deployment: &'a str,
    verification_uri: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification_uri_complete: Option<&'a str>,
    user_code: &'a str,
    expires_at: String,
}

pub(crate) fn activation_event<'a>(
    deployment: &'a Deployment,
    authorization: &'a DeviceAuthorization,
    expires_at: OffsetDateTime,
    operation: Option<&'static str>,
) -> anyhow::Result<ActivationEvent<'a>> {
    Ok(ActivationEvent {
        schema_version: 1,
        event: "activation_required",
        operation,
        deployment: deployment.fingerprint().api_url(),
        verification_uri: authorization.verification_uri(),
        verification_uri_complete: authorization.verification_uri_complete(),
        user_code: authorization.user_code(),
        expires_at: expires_at.format(&Rfc3339).map_err(anyhow::Error::new)?,
    })
}

#[derive(Clone, Copy)]
pub(crate) enum DeviceFlowPhase {
    DeviceAuthorization,
    TokenPolling,
}

pub(crate) enum DeviceFlowError {
    Authorization {
        phase: DeviceFlowPhase,
        error: AuthorizationError,
    },
    ExpirationOutOfRange,
    ActivationOutput(anyhow::Error),
}

pub(crate) fn session(
    client: &HttpClient,
    deployment: &Deployment,
    cancellation: &Cancellation,
    activation: impl FnOnce(&DeviceAuthorization, OffsetDateTime) -> anyhow::Result<()>,
) -> Result<DeviceFlowOutcome<IssuedToken>, DeviceFlowError> {
    run(
        client,
        deployment,
        cancellation,
        device_authorization::authorize,
        device_authorization::poll_token,
        activation,
    )
}

pub(crate) fn identity_proof(
    client: &HttpClient,
    deployment: &Deployment,
    cancellation: &Cancellation,
    activation: impl FnOnce(&DeviceAuthorization, OffsetDateTime) -> anyhow::Result<()>,
) -> Result<DeviceFlowOutcome<SecretToken>, DeviceFlowError> {
    run(
        client,
        deployment,
        cancellation,
        device_authorization::authorize_identity_proof,
        device_authorization::poll_identity_proof,
        activation,
    )
}

fn run<T>(
    client: &HttpClient,
    deployment: &Deployment,
    cancellation: &Cancellation,
    authorize: impl FnOnce(&HttpClient, &Deployment) -> Result<DeviceAuthorization, AuthorizationError>,
    poll: impl Fn(&HttpClient, &Deployment, &str) -> Result<TokenPoll<T>, AuthorizationError>,
    activation: impl FnOnce(&DeviceAuthorization, OffsetDateTime) -> anyhow::Result<()>,
) -> Result<DeviceFlowOutcome<T>, DeviceFlowError> {
    if cancellation.is_cancelled() {
        return Ok(DeviceFlowOutcome::Cancelled);
    }
    let authorization =
        authorize(client, deployment).map_err(|error| DeviceFlowError::Authorization {
            phase: DeviceFlowPhase::DeviceAuthorization,
            error,
        })?;
    if cancellation.is_cancelled() {
        return Ok(DeviceFlowOutcome::Cancelled);
    }
    let Some(mut schedule) = PollSchedule::new(
        crate::timing::monotonic_now(),
        authorization.interval(),
        authorization.expires_in(),
    ) else {
        return Err(DeviceFlowError::ExpirationOutOfRange);
    };
    let Some(expires_at) = expiration_after(authorization.expires_in()) else {
        return Err(DeviceFlowError::ExpirationOutOfRange);
    };
    activation(&authorization, expires_at).map_err(DeviceFlowError::ActivationOutput)?;

    loop {
        if cancellation.is_cancelled() {
            return Ok(DeviceFlowOutcome::Cancelled);
        }
        let Some(wait) = schedule.next_wait(crate::timing::monotonic_now()) else {
            return Ok(DeviceFlowOutcome::Expired);
        };
        if cancellation.wait(wait) {
            return Ok(DeviceFlowOutcome::Cancelled);
        }
        if schedule.expired(crate::timing::monotonic_now()) {
            return Ok(DeviceFlowOutcome::Expired);
        }

        let result = poll(client, deployment, authorization.device_code()).map_err(|error| {
            DeviceFlowError::Authorization {
                phase: DeviceFlowPhase::TokenPolling,
                error,
            }
        })?;
        if cancellation.is_cancelled() {
            return Ok(DeviceFlowOutcome::Cancelled);
        }
        if schedule.expired(crate::timing::monotonic_now()) {
            return Ok(DeviceFlowOutcome::Expired);
        }
        match result {
            TokenPoll::Pending => {}
            TokenPoll::SlowDown => schedule.slow_down(),
            TokenPoll::Denied => return Ok(DeviceFlowOutcome::Denied),
            TokenPoll::Expired => return Ok(DeviceFlowOutcome::Expired),
            TokenPoll::Issued(token) => return Ok(DeviceFlowOutcome::Issued(token)),
        }
    }
}

struct PollSchedule {
    interval: Duration,
    deadline: Instant,
}

impl PollSchedule {
    fn new(started_at: Instant, interval: Duration, lifetime: Duration) -> Option<Self> {
        Some(Self {
            interval,
            deadline: started_at.checked_add(lifetime)?,
        })
    }

    fn next_wait(&self, now: Instant) -> Option<Duration> {
        let remaining = self.deadline.checked_duration_since(now)?;
        (!remaining.is_zero()).then_some(self.interval.min(remaining))
    }

    fn expired(&self, now: Instant) -> bool {
        now >= self.deadline
    }

    fn slow_down(&mut self) {
        self.interval = self
            .interval
            .checked_add(SLOW_DOWN_INCREMENT)
            .unwrap_or(Duration::MAX);
    }
}

fn expiration_after(duration: Duration) -> Option<OffsetDateTime> {
    let seconds = i64::try_from(duration.as_secs()).ok()?;
    crate::timing::utc_now().checked_add(time::Duration::seconds(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_schedule_honors_interval_and_slow_down() {
        let start = crate::timing::monotonic_now();
        let mut schedule =
            PollSchedule::new(start, Duration::from_secs(2), Duration::from_secs(30))
                .expect("poll schedule should be representable");

        assert_eq!(schedule.next_wait(start), Some(Duration::from_secs(2)));
        schedule.slow_down();
        assert_eq!(schedule.next_wait(start), Some(Duration::from_secs(7)));
        assert_eq!(
            schedule.next_wait(start + Duration::from_secs(29)),
            Some(Duration::from_secs(1))
        );
        assert!(
            schedule
                .next_wait(start + Duration::from_secs(30))
                .is_none()
        );
    }
}
