use anyhow::{Context, anyhow};
use clap::Args;

use crate::api::{ListIdentitiesOutcome, link_identity, list_identities};
use crate::exit_code::OutcomeClass;
use crate::human_auth::cancellation::Cancellation;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::device_authorization::AuthorizationError;
use crate::human_auth::device_flow::{self, DeviceFlowError, DeviceFlowOutcome, DeviceFlowPhase};

use super::{
    BoundHumanSession, OutputOptions, output, with_bound_human_session, with_human_session_binding,
};

pub(super) const ABOUT: &str = "Link another sign-in identity";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Emit newline-delimited JSON events")]
    json: bool,

    #[command(flatten)]
    http: super::super::super::HttpOptions,
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
        let options = OutputOptions {
            json: self.json,
            http: self.http,
        };
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
                    options.json,
                )
                .map_err(Into::into);
            }
        };

        let proof = device_flow::identity_proof(
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

        let idempotency_key = crate::idempotency::generate_idempotency_key()
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
                )
                .map_err(Into::into),
            BoundHumanSession::ActingSessionChanged => output
                .acting_session_changed(deployment.fingerprint().api_url())
                .map_err(Into::into),
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
