use std::io::{self, Write as _};
use std::time::{Duration, Instant};

use scherzo_cloud_api::{
    Publication, PublicationApi, PublicationFailure, PublicationState, RunCancellationEnvelope,
    RunCancellationReceiptState, RunCancellationResolutionKind, RunFailure,
    RunPublicationHandoffState, RunRead, RunState,
};

use super::{CloudSnapshot, RunOptions, TerminalRunState, terminal_run_state};
use scherzo_cloud_human_auth::Deployment;

fn publication_failure(failure: PublicationFailure) -> RunFailure {
    match failure {
        PublicationFailure::Unauthenticated => RunFailure::Unauthenticated,
        PublicationFailure::Forbidden => RunFailure::Forbidden,
        PublicationFailure::InvalidInput => RunFailure::InvalidInput,
        PublicationFailure::NotFound => RunFailure::NotFound,
        PublicationFailure::Conflict => RunFailure::Conflict,
        PublicationFailure::Gone => RunFailure::Gone,
        PublicationFailure::Unreachable(category) => RunFailure::Unreachable(category),
        PublicationFailure::Interrupted => RunFailure::Interrupted,
        PublicationFailure::Protocol {
            credential_rejected,
        } => RunFailure::Protocol {
            credential_rejected,
        },
    }
}

fn read_publication(
    deployment: &Deployment,
    options: &RunOptions,
    organization: &str,
    run_id: &str,
    publication_id: &str,
    deadline: Option<Instant>,
) -> Result<Publication, RunFailure> {
    let client =
        super::super::human_session_client(options.http.transport_policy()).map_err(|_| {
            RunFailure::Protocol {
                credential_rejected: false,
            }
        })?;
    super::super::execute_selected_api_observation(
        super::super::principal_api_context(
            &client,
            deployment,
            &options.authentication,
            "acquire human session for Cloud publication observation",
        ),
        |access_token, _remaining| {
            let api = PublicationApi::new(
                deployment.fingerprint().api_url(),
                access_token,
                options.http.transport_policy(),
            )
            .map_err(|error| anyhow::anyhow!(error))?;
            let remaining = match super::super::observation_http_budget(deadline) {
                Ok(remaining) => remaining,
                Err(category) => return Ok(Err(PublicationFailure::Unreachable(category))),
            };
            Ok(api.get_with_timeout(organization, run_id, publication_id, remaining))
        },
        PublicationFailure::credential_rejected,
        || PublicationFailure::Unauthenticated,
        PublicationFailure::Unreachable,
        deadline,
    )
    .map_err(|_| RunFailure::Protocol {
        credential_rejected: false,
    })?
    .map_err(publication_failure)
}

fn read_run(
    deployment: &Deployment,
    options: &RunOptions,
    organization: &str,
    run_id: &str,
    deadline: Option<Instant>,
) -> Result<RunRead, RunFailure> {
    super::with_api_until(
        deployment,
        options.http.transport_policy(),
        &options.authentication,
        deadline,
        |api, remaining| api.get_with_timeout(organization, run_id, remaining),
    )
    .map_err(|_| RunFailure::Protocol {
        credential_rejected: false,
    })?
}

fn read_receipt(
    deployment: &Deployment,
    options: &RunOptions,
    organization: &str,
    run_id: &str,
    request_id: &str,
    deadline: Option<Instant>,
) -> Result<RunCancellationEnvelope, RunFailure> {
    super::with_api_until(
        deployment,
        options.http.transport_policy(),
        &options.authentication,
        deadline,
        |api, remaining| api.get_cancellation(organization, run_id, request_id, remaining),
    )
    .map_err(|_| RunFailure::Protocol {
        credential_rejected: false,
    })?
}

pub(super) fn show_once(
    deployment: &Deployment,
    options: &RunOptions,
    organization: &str,
    run_id: &str,
) -> Result<CloudSnapshot, RunFailure> {
    let mut snapshot = CloudSnapshot::for_run(run_id);
    match read_run(deployment, options, organization, run_id, None)? {
        RunRead::Pending(_) => {}
        RunRead::Materialized(run) => {
            snapshot.run = Some(run);
            if let Some(publication_id) = snapshot
                .run
                .as_ref()
                .and_then(|run| run.publication.as_ref())
                .and_then(|handoff| handoff.publication_id.as_deref())
            {
                snapshot.publication = Some(Box::new(read_publication(
                    deployment,
                    options,
                    organization,
                    run_id,
                    publication_id,
                    None,
                )?));
            }
        }
    }
    Ok(snapshot)
}

fn publication_settled(snapshot: &CloudSnapshot) -> bool {
    let Some(run) = snapshot.run.as_deref() else {
        return false;
    };
    if run.state != RunState::Succeeded {
        return true;
    }
    let Some(handoff) = run.publication.as_deref() else {
        return true;
    };
    match handoff.state {
        RunPublicationHandoffState::Pending => false,
        RunPublicationHandoffState::Failed | RunPublicationHandoffState::Skipped => true,
        RunPublicationHandoffState::Started => {
            snapshot.publication.as_deref().is_some_and(|publication| {
                matches!(
                    publication.state,
                    PublicationState::Succeeded | PublicationState::Failed
                )
            })
        }
    }
}

fn snapshot_run_id(snapshot: &CloudSnapshot) -> Result<&str, RunFailure> {
    snapshot.run_id.as_deref().ok_or(RunFailure::Protocol {
        credential_rejected: false,
    })
}

pub(super) struct ObservationContext<'a> {
    pub deployment: &'a Deployment,
    pub options: &'a RunOptions,
    pub organization: &'a str,
    pub snapshot: &'a CloudSnapshot,
    pub timeout: Option<Duration>,
    pub started: Instant,
}

pub(super) fn wait_run(
    context: ObservationContext<'_>,
    control: &impl super::super::ObservationControl,
    clock: &impl super::super::ObservationClock,
    mut record: impl FnMut(CloudSnapshot),
) -> Result<super::super::TerminalObservation<CloudSnapshot, TerminalRunState>, RunFailure> {
    let ObservationContext {
        deployment,
        options,
        organization,
        snapshot,
        timeout,
        started,
    } = context;
    let run_id = snapshot_run_id(snapshot)?;
    let deadline = timeout.map(|duration| started.checked_add(duration).unwrap_or(started));
    let mut current = snapshot.clone();
    super::super::wait_for_terminal_observation_bounded(
        |_remaining| {
            match read_run(deployment, options, organization, run_id, deadline)? {
                RunRead::Materialized(run) => {
                    current.run = Some(run);
                    current.publication = None;
                    record(current.clone());
                    if current
                        .run
                        .as_deref()
                        .is_some_and(|run| run.state == RunState::Succeeded)
                        && let Some(handoff) = current
                            .run
                            .as_ref()
                            .and_then(|run| run.publication.as_ref())
                        && let Some(publication_id) = handoff.publication_id.as_deref()
                    {
                        let budget =
                            super::super::remaining_observation_wait(timeout, started, clock.now());
                        if timeout.is_none() || budget.is_some() {
                            current.publication = Some(Box::new(read_publication(
                                deployment,
                                options,
                                organization,
                                run_id,
                                publication_id,
                                deadline,
                            )?));
                        }
                    }
                }
                RunRead::Pending(_) => {
                    current.run = None;
                    current.publication = None;
                }
            }
            record(current.clone());
            let _ = writeln!(
                io::stderr().lock(),
                "Run observation: {}",
                current
                    .run
                    .as_ref()
                    .map_or("creation pending", |run| match run.state {
                        RunState::Queued => "queued",
                        RunState::Assigning => "assigning",
                        RunState::Preparing => "preparing",
                        RunState::Assigned => "assigned",
                        RunState::Running => "running",
                        RunState::Cancelling => "cancelling",
                        RunState::Succeeded => "succeeded",
                        RunState::Failed => "failed",
                        RunState::Cancelled => "cancelled",
                        RunState::Interrupted => "interrupted",
                        RunState::Rejected => "rejected",
                    })
            );
            Ok(current.clone())
        },
        |snapshot| {
            snapshot
                .run
                .as_deref()
                .and_then(|run| terminal_run_state(run.state))
                .filter(|_| publication_settled(snapshot))
        },
        RunFailure::retryable_observation,
        timeout,
        started,
        control,
        clock,
    )
}

// Receipt observation has a different terminal predicate and read authority from Run reads;
// keeping its polling boundary separate makes resolved-receipt ordering explicit.
// jscpd:ignore-start
pub(super) fn wait_cancellation(
    context: ObservationContext<'_>,
    control: &impl super::super::ObservationControl,
    clock: &impl super::super::ObservationClock,
    record: impl FnMut(CloudSnapshot),
) -> Result<super::super::TerminalObservation<CloudSnapshot, ()>, RunFailure> {
    let ObservationContext {
        deployment,
        options,
        organization,
        snapshot,
        timeout,
        started,
    } = context;
    let run_id = snapshot_run_id(snapshot)?;
    let Some(request_id) = snapshot
        .cancellation_request
        .as_deref()
        .map(|receipt| receipt.id.as_str())
    else {
        return Err(RunFailure::Protocol {
            credential_rejected: false,
        });
    };
    // jscpd:ignore-end
    let deadline = timeout.map(|duration| started.checked_add(duration).unwrap_or(started));
    wait_receipt_with(
        |_remaining| {
            read_receipt(
                deployment,
                options,
                organization,
                run_id,
                request_id,
                deadline,
            )
        },
        snapshot,
        timeout,
        started,
        control,
        clock,
        record,
    )
}

pub(super) fn wait_receipt_with(
    mut observe: impl FnMut(Option<Duration>) -> Result<RunCancellationEnvelope, RunFailure>,
    snapshot: &CloudSnapshot,
    timeout: Option<Duration>,
    started: Instant,
    control: &impl super::super::ObservationControl,
    clock: &impl super::super::ObservationClock,
    mut record: impl FnMut(CloudSnapshot),
) -> Result<super::super::TerminalObservation<CloudSnapshot, ()>, RunFailure> {
    let mut current = snapshot.clone();
    super::super::wait_for_terminal_observation_bounded(
        |remaining| {
            let envelope = observe(remaining)?;
            current.run = envelope.run;
            current.cancellation_request = Some(envelope.request);
            record(current.clone());
            let _ =
                writeln!(
                    io::stderr().lock(),
                    "Cancellation observation: {}",
                    if current.cancellation_request.as_deref().is_some_and(
                        |receipt| receipt.state == RunCancellationReceiptState::Resolved
                    ) {
                        "resolved"
                    } else {
                        "pending"
                    }
                );
            Ok(current.clone())
        },
        |snapshot| {
            let receipt = snapshot.cancellation_request.as_deref()?;
            if receipt.state != RunCancellationReceiptState::Resolved {
                return None;
            }
            if receipt.resolution.as_deref().is_some_and(|resolution| {
                resolution.kind == RunCancellationResolutionKind::CreationRejected
            }) {
                return Some(());
            }
            snapshot
                .run
                .as_deref()
                .and_then(|run| terminal_run_state(run.state))
                .map(|_| ())
        },
        RunFailure::retryable_observation,
        timeout,
        started,
        control,
        clock,
    )
}
