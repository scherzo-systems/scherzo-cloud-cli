use std::path::{Path, PathBuf};
use std::sync::mpsc;

use anyhow::{Context, anyhow};
use clap::Args;

use crate::exit_code::OutcomeClass;
use scherzo_cloud_api::{ListIdentitiesOutcome, link_identity, list_identities};
use scherzo_cloud_human_auth::AuthorizationError;
use scherzo_cloud_human_auth::Cancellation;
use scherzo_cloud_human_auth::Deployment;
use scherzo_cloud_human_auth::{DeviceFlowError, DeviceFlowOutcome, DeviceFlowPhase};

use super::{
    BoundHumanSession, OutputOptions, output, with_bound_human_session, with_human_session_binding,
};

pub(super) const ABOUT: &str = "Link another sign-in identity";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        long,
        value_name = "PATH|-",
        help = "Prove a workload identity with a fresh token from a private file, or - for standard input"
    )]
    workload_token_file: Option<PathBuf>,

    #[command(flatten)]
    common: super::super::super::CommonArgs<
        super::super::super::StreamingJson,
        super::super::super::PrincipalAuthenticationArgs,
    >,
}

impl std::ops::Deref for Command {
    type Target = super::super::super::CommonArgs<
        super::super::super::StreamingJson,
        super::super::super::PrincipalAuthenticationArgs,
    >;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> super::super::super::CommandResult {
        let deployment = deployment.clone();
        super::super::super::execute_cancellable_with_signals(
            "identity linking",
            move |cancellation| self.run(&deployment, cancellation),
        )
    }

    fn run(
        self,
        deployment: &Deployment,
        cancellation: &Cancellation,
    ) -> super::super::super::CommandResult {
        if self.authentication.service_api_key_file.is_some() {
            return self.run_service(deployment, cancellation);
        }
        if self.workload_token_file.is_some() {
            return Err(anyhow!("--workload-token-file requires --service-api-key-file").into());
        }
        let options = OutputOptions::new(
            self.json,
            super::super::super::PrincipalAuthenticationArgs::default(),
            self.common.http,
        );
        let client = options.client()?;
        let mut output = output::LinkOutput::new(options.json);

        let (preflight, acting_session) =
            with_human_session_binding(&client, deployment, |access_token| {
                list_identities(
                    &client,
                    deployment.fingerprint().api_url(),
                    access_token,
                    Some(1),
                    None,
                )
            })?;
        let acting_session = match preflight {
            ListIdentitiesOutcome::Listed(_) => acting_session
                .context("retain the acting human session after identity-link preflight")?,
            ListIdentitiesOutcome::Common(common) => {
                return output::write_common(
                    deployment.fingerprint().api_url(),
                    &common,
                    super::super::super::PrincipalAuthenticationKind::HumanSession,
                    options.json,
                )
                .map_err(Into::into);
            }
        };

        let proof = scherzo_cloud_human_auth::identity_proof(
            &client,
            deployment,
            cancellation,
            |authorization, expires_at| output.activation(deployment, authorization, expires_at),
        );
        let proof = match proof {
            Ok(DeviceFlowOutcome::Issued(proof)) => proof,
            Ok(DeviceFlowOutcome::Denied) => {
                return output
                    .browser_failure(
                        deployment.fingerprint().api_url(),
                        "denied",
                        "token_polling",
                        None,
                        "! Identity linking was not authorized.",
                        OutcomeClass::GeneralFailure,
                    )
                    .map_err(Into::into);
            }
            Ok(DeviceFlowOutcome::Expired) => {
                return output
                    .browser_failure(
                        deployment.fingerprint().api_url(),
                        "expired",
                        "token_polling",
                        None,
                        "! The identity-link authorization expired.\n\nStart the linking flow again.",
                        OutcomeClass::GeneralFailure,
                    )
                    .map_err(Into::into);
            }
            Ok(DeviceFlowOutcome::Cancelled) => {
                return output
                    .cancelled(deployment.fingerprint().api_url())
                    .map_err(Into::into);
            }
            Err(error) => {
                return handle_device_flow_error(&mut output, deployment, cancellation, error);
            }
        };

        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate identity-link request identity")?;
        let outcome =
            with_bound_human_session(&client, deployment, &acting_session, |access_token| {
                link_identity(
                    &client,
                    deployment.fingerprint().api_url(),
                    access_token,
                    &idempotency_key,
                    proof.expose(),
                )
            })?;
        match outcome {
            BoundHumanSession::Outcome {
                outcome,
                credential_state,
            } => output
                .api_outcome(
                    deployment.fingerprint().api_url(),
                    &outcome,
                    credential_state,
                    false,
                )
                .map_err(Into::into),
            BoundHumanSession::ActingSessionChanged => output
                .acting_session_changed(deployment.fingerprint().api_url())
                .map_err(Into::into),
        }
    }

    fn run_service(
        self,
        deployment: &Deployment,
        cancellation: &Cancellation,
    ) -> super::super::super::CommandResult {
        let workload_token_file = self.workload_token_file.ok_or_else(|| {
            anyhow!(
                "--workload-token-file is required when --service-api-key-file is used for identity linking"
            )
        })?;
        if self.common.authentication.uses_stdin() && workload_token_file == Path::new("-") {
            return Err(anyhow!(
                "standard input cannot supply both a service API key and a workload identity token"
            )
            .into());
        }
        let mut output = output::LinkOutput::new(self.common.json);
        let authentication = self.common.authentication;
        let Some(api_key) = read_secret_cancellable(cancellation, move || {
            authentication.required_service_api_key()
        })?
        else {
            return output
                .cancelled(deployment.fingerprint().api_url())
                .map_err(Into::into);
        };
        let Some(workload_token) = read_secret_cancellable(cancellation, move || {
            crate::service_auth::read_workload_token(&workload_token_file)
                .context("read workload identity token")
        })?
        else {
            return output
                .cancelled(deployment.fingerprint().api_url())
                .map_err(Into::into);
        };
        let client = scherzo_cloud_api::HttpClient::new(self.common.http.transport_policy())
            .map_err(|error| anyhow!(error))
            .context("prepare identity networking")?;
        let idempotency_key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate identity-link request identity")?;
        // This ownership claim is the authorization boundary for dispatch. The network send
        // cannot be atomic with an OS signal, so a claim that wins preserves bounded completion.
        let Some(outcome) = dispatch_identity_link(cancellation, || {
            link_identity(
                &client,
                deployment.fingerprint().api_url(),
                api_key.expose(),
                &idempotency_key,
                workload_token.expose(),
            )
        }) else {
            return output
                .cancelled(deployment.fingerprint().api_url())
                .map_err(Into::into);
        };
        let outcome = outcome
            .map_err(|error| anyhow!(error))
            .context("contact identity API with service credentials")?;
        output
            .api_outcome(
                deployment.fingerprint().api_url(),
                &outcome,
                scherzo_cloud_human_auth::LocalCredentialState::Retained,
                true,
            )
            .map_err(Into::into)
    }
}

fn dispatch_identity_link<T>(
    cancellation: &Cancellation,
    dispatch: impl FnOnce() -> T,
) -> Option<T> {
    cancellation.claim_bounded_completion().then(dispatch)
}

fn read_secret_cancellable<T: Send + 'static>(
    cancellation: &Cancellation,
    read: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<Option<T>> {
    if cancellation.is_cancelled() {
        return Ok(None);
    }
    let (sender, receiver) = mpsc::sync_channel(1);
    let read_cancellation = cancellation.clone();
    std::thread::Builder::new()
        .name("identity-link-secret-read".to_owned())
        .spawn(move || {
            let _ = sender.send(read());
            read_cancellation.notify_change();
        })
        .context("start cancellable identity-link secret read")?;

    loop {
        let change = cancellation.change_token();
        if cancellation.is_cancelled() {
            return Ok(None);
        }
        match receiver.try_recv() {
            Ok(result) => return result.map(Some),
            Err(mpsc::TryRecvError::Empty) => cancellation.wait_for_change(change),
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err(anyhow!(
                    "identity-link secret reader stopped without a result"
                ));
            }
        }
    }
}

fn handle_device_flow_error(
    output: &mut output::LinkOutput,
    deployment: &Deployment,
    cancellation: &Cancellation,
    error: DeviceFlowError,
) -> super::super::super::CommandResult {
    if cancellation.is_cancelled() {
        return output
            .cancelled(deployment.fingerprint().api_url())
            .map_err(Into::into);
    }
    match error {
        DeviceFlowError::Authorization { phase, error } => {
            handle_authorization_error(output, deployment, phase, error)
        }
        DeviceFlowError::ExpirationOutOfRange => handle_protocol_error(
            output,
            deployment,
            DeviceFlowPhase::DeviceAuthorization,
            anyhow!("the device-authorization expiration is out of range"),
        ),
        DeviceFlowError::ActivationOutput(error) => Err(error.into()),
    }
}

fn handle_authorization_error(
    output: &mut output::LinkOutput,
    deployment: &Deployment,
    phase: DeviceFlowPhase,
    error: AuthorizationError,
) -> super::super::super::CommandResult {
    match error {
        AuthorizationError::Local(error) => Err(anyhow!(error)
            .context(flow_context(deployment, phase))
            .into()),
        AuthorizationError::Unreachable(category) => output
            .browser_failure(
                deployment.fingerprint().api_url(),
                "unreachable",
                phase_name(phase),
                Some(category),
                "! The authorization server is unreachable.\n\nStart the linking flow again when it is available.",
                super::super::super::unreachable_outcome_class(category),
            )
            .map_err(Into::into),
        error @ AuthorizationError::Protocol { .. } => {
            handle_protocol_error(output, deployment, phase, anyhow!(error))
        }
    }
}

fn handle_protocol_error(
    output: &mut output::LinkOutput,
    deployment: &Deployment,
    phase: DeviceFlowPhase,
    error: anyhow::Error,
) -> super::super::super::CommandResult {
    if output.is_json() {
        output
            .browser_failure(
                deployment.fingerprint().api_url(),
                "protocol_error",
                phase_name(phase),
                None,
                "! The identity-link authorization response was invalid.",
                OutcomeClass::Protocol,
            )
            .map_err(Into::into)
    } else {
        Err(super::super::super::CommandFailure::for_outcome(
            error.context(flow_context(deployment, phase)),
            OutcomeClass::Protocol,
        ))
    }
}

const fn phase_name(phase: DeviceFlowPhase) -> &'static str {
    match phase {
        DeviceFlowPhase::DeviceAuthorization => "device_authorization",
        DeviceFlowPhase::TokenPolling => "token_polling",
    }
}

fn flow_context(deployment: &Deployment, phase: DeviceFlowPhase) -> String {
    match phase {
        DeviceFlowPhase::DeviceAuthorization => format!(
            "request identity-link authorization from OAuth issuer {}",
            deployment.fingerprint().issuer()
        ),
        DeviceFlowPhase::TokenPolling => format!(
            "request identity-link proof from OAuth issuer {}",
            deployment.fingerprint().issuer()
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[test]
    fn cancellation_that_wins_prevents_identity_link_dispatch() {
        let cancellation = Cancellation::new();
        let dispatched = Cell::new(false);
        cancellation.cancel();

        let result = dispatch_identity_link(&cancellation, || dispatched.set(true));

        assert!(result.is_none());
        assert!(!dispatched.get());
    }

    #[test]
    fn identity_link_dispatch_that_wins_preserves_its_result() {
        let cancellation = Cancellation::new();
        let signal_cancellation = cancellation.clone();

        let result = dispatch_identity_link(&cancellation, || {
            signal_cancellation.cancel();
            "linked"
        });

        assert_eq!(result, Some("linked"));
        assert!(!cancellation.is_cancelled());
    }

    #[test]
    fn cancellation_stops_a_blocked_secret_read() {
        let cancellation = Cancellation::new();
        let operation_cancellation = cancellation.clone();
        let (started_sender, started_receiver) = mpsc::sync_channel(0);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(0);
        let canceller = std::thread::spawn(move || {
            started_receiver
                .recv()
                .expect("secret read should report readiness");
            operation_cancellation.cancel();
        });

        let result = read_secret_cancellable(&cancellation, move || {
            started_sender
                .send(())
                .expect("secret read readiness should be observed");
            release_receiver
                .recv()
                .expect("test should release the secret reader");
            finished_sender
                .send(())
                .expect("secret read completion should be observed");
            Ok(())
        })
        .expect("cancellable read should complete");

        assert!(result.is_none());
        release_sender
            .send(())
            .expect("blocked secret read should still be present");
        finished_receiver
            .recv()
            .expect("secret reader should finish after release");
        canceller.join().expect("canceller should finish");
    }
}
