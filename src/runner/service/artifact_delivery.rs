use std::collections::BTreeMap;
use std::io::{self, Cursor, Read};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use reqwest::StatusCode;
use reqwest::blocking::Body;
use reqwest::header::{
    CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, IF_NONE_MATCH,
};
use ring::digest::{SHA256, digest};
use tokio::sync::{mpsc, oneshot};

use super::Sleeper;
use super::assignment::{ArtifactRequest, AssignmentObservation, ObservationOutbox, OutboxFailure};
use super::backoff::Backoff;
use crate::execution::workflow::artifact::{ArtifactStaging, StagedCarrier};
use crate::execution::workflow::publication::{CloudCarrierBody, CloudResultCarrier};
use crate::runner_protocol::{
    ArtifactConfirmationOutcome, ArtifactConfirmationResponse, ArtifactRegistrationOutcome,
    ArtifactRegistrationResponse, ArtifactResultConfirmationOutcome,
    ArtifactResultConfirmationResponse, ArtifactResultRegistrationOutcome,
    ArtifactResultRegistrationResponse, ArtifactUploadCapability,
};

const CHECKSUM_HEADER: HeaderName = HeaderName::from_static("x-amz-checksum-sha256");
const MAXIMUM_DELIVERY_RETRIES: u8 = 3;

type UploadReader = Box<dyn Read + Send>;

pub(super) trait ArtifactUploadBody: Send + Sync {
    fn open(&self) -> io::Result<UploadReader>;
}

struct StagedArtifactUploadBody {
    staging: ArtifactStaging,
    carrier: StagedCarrier,
}

impl ArtifactUploadBody for StagedArtifactUploadBody {
    fn open(&self) -> io::Result<UploadReader> {
        self.staging
            .open_artifact(self.carrier.handle())
            .map(|file| Box::new(file) as UploadReader)
            .map_err(io::Error::other)
    }
}

struct BytesArtifactUploadBody(Arc<[u8]>);

impl ArtifactUploadBody for BytesArtifactUploadBody {
    fn open(&self) -> io::Result<UploadReader> {
        Ok(Box::new(Cursor::new(Arc::clone(&self.0))))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ArtifactMember {
    Carrier {
        portable_owner_path: String,
        idempotency_key: String,
    },
    Result,
}

pub(super) struct ArtifactDeliverySpec {
    pub(super) assignment_id: String,
    pub(super) attempt_id: String,
    member: ArtifactMember,
    pub(super) media_type: String,
    pub(super) size_bytes: u64,
    pub(super) sha256: String,
    body: Arc<dyn ArtifactUploadBody>,
}

impl ArtifactDeliverySpec {
    pub(super) fn cloud_carrier(
        assignment_id: String,
        attempt_id: String,
        staging: &ArtifactStaging,
        carrier: CloudResultCarrier,
    ) -> Self {
        let body: Arc<dyn ArtifactUploadBody> = match carrier.body {
            CloudCarrierBody::Staged(carrier) => Arc::new(StagedArtifactUploadBody {
                staging: staging.clone(),
                carrier,
            }),
            CloudCarrierBody::Bytes(bytes) => Arc::new(BytesArtifactUploadBody(bytes)),
        };
        Self {
            assignment_id,
            attempt_id,
            member: ArtifactMember::Carrier {
                portable_owner_path: carrier.portable_owner_path,
                idempotency_key: carrier.idempotency_key,
            },
            media_type: carrier.media_type,
            size_bytes: carrier.size_bytes,
            sha256: carrier.sha256,
            body,
        }
    }

    pub(super) fn result(
        assignment_id: String,
        attempt_id: String,
        result_json: Arc<[u8]>,
    ) -> Self {
        Self {
            assignment_id,
            attempt_id,
            member: ArtifactMember::Result,
            media_type: "application/json".to_owned(),
            size_bytes: u64::try_from(result_json.len()).unwrap_or(u64::MAX),
            sha256: hex_digest(digest(&SHA256, &result_json).as_ref()),
            body: Arc::new(BytesArtifactUploadBody(result_json)),
        }
    }

    #[cfg(test)]
    pub(super) fn fixture(
        assignment: (String, String),
        identity: (String, String),
        metadata: (String, u64, String),
        body: Arc<dyn ArtifactUploadBody>,
    ) -> Self {
        Self {
            assignment_id: assignment.0,
            attempt_id: assignment.1,
            member: ArtifactMember::Carrier {
                portable_owner_path: identity.0,
                idempotency_key: identity.1,
            },
            media_type: metadata.0,
            size_bytes: metadata.1,
            sha256: metadata.2,
            body,
        }
    }

    fn is_result(&self) -> bool {
        self.member == ArtifactMember::Result
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ClosedArtifactDeliveryFailure {
    pub(super) phase: String,
    pub(super) code: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ArtifactDeliveryOutcome {
    Delivered { artifact_set_id: String },
    Prepared { artifact_set_id: String },
    Failed(ClosedArtifactDeliveryFailure),
    AuthorityLost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ArtifactDeliveryProtocolFailure;

pub(super) enum ArtifactCloudResponse {
    CarrierRegistration(ArtifactRegistrationResponse),
    CarrierConfirmation(ArtifactConfirmationResponse),
    ResultRegistration(ArtifactResultRegistrationResponse),
    ResultConfirmation(ArtifactResultConfirmationResponse),
}

#[derive(Clone)]
pub(super) struct ArtifactDeliveryBroker {
    state: Arc<Mutex<ArtifactDeliveryState>>,
    outbox: ObservationOutbox,
    uploads: mpsc::UnboundedSender<UploadCompleted>,
    sleeper: Arc<dyn Sleeper>,
}

struct ArtifactDeliveryState {
    next_id: u64,
    deliveries: BTreeMap<u64, Delivery>,
    upload_results: mpsc::UnboundedReceiver<UploadCompleted>,
}

struct Delivery {
    spec: ArtifactDeliverySpec,
    phase: DeliveryPhase,
    completion: oneshot::Sender<ArtifactDeliveryOutcome>,
    retries: u8,
    retry_generation: u64,
    backoff: Backoff,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeliveryPhase {
    Registering,
    Uploading {
        artifact_set_id: String,
        carrier_id: Option<String>,
        upload_capability: ArtifactUploadCapability,
    },
    Confirming {
        artifact_set_id: String,
        carrier_id: Option<String>,
    },
}

struct UploadCompleted {
    delivery_id: u64,
    result: Result<(), ()>,
}

struct RetryWork {
    delivery_id: u64,
    generation: u64,
    delay: Duration,
    action: RetryAction,
}

enum RetryAction {
    Request(AssignmentObservation),
    Upload {
        artifact_set_id: String,
        carrier_id: Option<String>,
        upload_capability: ArtifactUploadCapability,
    },
}

impl ArtifactDeliveryBroker {
    pub(super) fn new(outbox: ObservationOutbox, sleeper: Arc<dyn Sleeper>) -> Self {
        let (uploads, upload_results) = mpsc::unbounded_channel();
        Self {
            state: Arc::new(Mutex::new(ArtifactDeliveryState {
                next_id: 1,
                deliveries: BTreeMap::new(),
                upload_results,
            })),
            outbox,
            uploads,
            sleeper,
        }
    }

    pub(super) fn start(
        &self,
        spec: ArtifactDeliverySpec,
    ) -> Result<oneshot::Receiver<ArtifactDeliveryOutcome>, OutboxFailure> {
        let (completion, receiver) = oneshot::channel();
        let mut state = self.lock();
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or(OutboxFailure::Sequence)?;
        state.deliveries.insert(
            id,
            Delivery {
                spec,
                phase: DeliveryPhase::Registering,
                completion,
                retries: 0,
                retry_generation: 0,
                backoff: Backoff::new(),
            },
        );
        let request = register_observation(id, &state.deliveries[&id].spec);
        if let Err(failure) = self.outbox.enqueue(request) {
            state.deliveries.remove(&id);
            return Err(failure);
        }
        Ok(receiver)
    }

    pub(super) fn handle_response(
        &self,
        delivery_id: u64,
        response: ArtifactCloudResponse,
    ) -> Result<(), ArtifactDeliveryProtocolFailure> {
        let mut state = self.lock();
        let delivery = state
            .deliveries
            .get_mut(&delivery_id)
            .ok_or(ArtifactDeliveryProtocolFailure)?;
        let mut upload = None;
        let mut retry = None;
        let mut completion = None;

        match (&delivery.phase, response) {
            (
                DeliveryPhase::Registering,
                ArtifactCloudResponse::CarrierRegistration(ArtifactRegistrationResponse {
                    outcome:
                        ArtifactRegistrationOutcome::Succeeded {
                            artifact_set_id,
                            carrier_id,
                            upload_capability,
                        },
                    ..
                }),
            ) if !delivery.spec.is_result() => {
                upload = Some(begin_upload(
                    delivery_id,
                    delivery,
                    artifact_set_id,
                    Some(carrier_id),
                    upload_capability,
                ));
            }
            (
                DeliveryPhase::Registering,
                ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                    outcome:
                        ArtifactResultRegistrationOutcome::Succeeded {
                            artifact_set_id,
                            upload_capability,
                            ..
                        },
                    ..
                }),
            ) if delivery.spec.is_result() => {
                upload = Some(begin_upload(
                    delivery_id,
                    delivery,
                    artifact_set_id,
                    None,
                    upload_capability,
                ));
            }
            (
                DeliveryPhase::Registering,
                ArtifactCloudResponse::CarrierRegistration(ArtifactRegistrationResponse {
                    outcome: ArtifactRegistrationOutcome::Retryable,
                    ..
                }),
            ) if !delivery.spec.is_result() => {
                select_registration_retry(delivery_id, delivery, &mut retry, &mut completion);
            }
            (
                DeliveryPhase::Registering,
                ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                    outcome: ArtifactResultRegistrationOutcome::Retryable,
                    ..
                }),
            ) if delivery.spec.is_result() => {
                select_registration_retry(delivery_id, delivery, &mut retry, &mut completion);
            }
            (
                DeliveryPhase::Registering,
                ArtifactCloudResponse::CarrierRegistration(ArtifactRegistrationResponse {
                    outcome: ArtifactRegistrationOutcome::Failed { code },
                    ..
                }),
            ) if !delivery.spec.is_result() => completion = Some(failed_for_code(code)),
            (
                DeliveryPhase::Registering,
                ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                    outcome: ArtifactResultRegistrationOutcome::Failed { code },
                    ..
                }),
            ) if delivery.spec.is_result() => completion = Some(failed_for_code(code)),
            (
                DeliveryPhase::Confirming {
                    artifact_set_id: expected_set,
                    carrier_id: Some(expected_carrier),
                },
                ArtifactCloudResponse::CarrierConfirmation(ArtifactConfirmationResponse {
                    outcome:
                        ArtifactConfirmationOutcome::Confirmed {
                            artifact_set_id,
                            carrier_id,
                        },
                    ..
                }),
            ) if expected_set == &artifact_set_id && expected_carrier == &carrier_id => {
                completion = Some(ArtifactDeliveryOutcome::Delivered { artifact_set_id });
            }
            // Absence and retry are distinct Cloud facts even though both retain
            // the exact carrier identity for another bounded delivery attempt.
            // jscpd:ignore-start
            (
                DeliveryPhase::Confirming {
                    artifact_set_id: expected_set,
                    carrier_id: Some(expected_carrier),
                },
                ArtifactCloudResponse::CarrierConfirmation(ArtifactConfirmationResponse {
                    outcome:
                        ArtifactConfirmationOutcome::Absent {
                            artifact_set_id,
                            carrier_id,
                            upload_capability,
                        },
                    ..
                }),
            ) if expected_set == &artifact_set_id && expected_carrier == &carrier_id => {
                select_upload_retry(
                    delivery_id,
                    delivery,
                    artifact_set_id,
                    Some(carrier_id),
                    upload_capability,
                    &mut retry,
                    &mut completion,
                );
            }
            (
                DeliveryPhase::Confirming {
                    artifact_set_id: expected_set,
                    carrier_id: Some(expected_carrier),
                },
                ArtifactCloudResponse::CarrierConfirmation(ArtifactConfirmationResponse {
                    outcome:
                        ArtifactConfirmationOutcome::Retryable {
                            artifact_set_id,
                            carrier_id,
                        },
                    ..
                }),
            ) if expected_set == &artifact_set_id && expected_carrier == &carrier_id => {
                select_confirmation_retry(
                    delivery_id,
                    delivery,
                    artifact_set_id,
                    Some(carrier_id),
                    &mut retry,
                    &mut completion,
                );
            }
            // jscpd:ignore-end
            (
                DeliveryPhase::Confirming {
                    artifact_set_id: expected_set,
                    carrier_id: Some(expected_carrier),
                },
                ArtifactCloudResponse::CarrierConfirmation(ArtifactConfirmationResponse {
                    outcome:
                        ArtifactConfirmationOutcome::Failed {
                            artifact_set_id,
                            carrier_id,
                            code,
                        },
                    ..
                }),
            ) if expected_set == &artifact_set_id && expected_carrier == &carrier_id => {
                completion = Some(failed_for_code(code));
            }
            (
                DeliveryPhase::Confirming {
                    artifact_set_id: expected_set,
                    carrier_id: None,
                },
                ArtifactCloudResponse::ResultConfirmation(ArtifactResultConfirmationResponse {
                    outcome: ArtifactResultConfirmationOutcome::Confirmed { artifact_set_id },
                    ..
                }),
            ) if expected_set == &artifact_set_id => {
                completion = Some(ArtifactDeliveryOutcome::Prepared { artifact_set_id });
            }
            (
                DeliveryPhase::Confirming {
                    artifact_set_id: expected_set,
                    carrier_id: None,
                },
                ArtifactCloudResponse::ResultConfirmation(ArtifactResultConfirmationResponse {
                    outcome:
                        outcome @ (ArtifactResultConfirmationOutcome::Absent { .. }
                        | ArtifactResultConfirmationOutcome::Retryable { .. }),
                    ..
                }),
            ) => {
                let (artifact_set_id, upload_capability) = match outcome {
                    ArtifactResultConfirmationOutcome::Absent {
                        artifact_set_id,
                        upload_capability,
                    } => (artifact_set_id, Some(upload_capability)),
                    ArtifactResultConfirmationOutcome::Retryable { artifact_set_id } => {
                        (artifact_set_id, None)
                    }
                    _ => unreachable!("result retry alternatives are closed"),
                };
                if expected_set != &artifact_set_id {
                    return Err(ArtifactDeliveryProtocolFailure);
                }
                select_result_retry(
                    (delivery_id, delivery, artifact_set_id),
                    upload_capability,
                    (&mut retry, &mut completion),
                );
            }
            (
                DeliveryPhase::Confirming {
                    artifact_set_id: expected_set,
                    carrier_id: None,
                },
                ArtifactCloudResponse::ResultConfirmation(ArtifactResultConfirmationResponse {
                    outcome:
                        ArtifactResultConfirmationOutcome::Failed {
                            artifact_set_id,
                            phase,
                            code,
                        },
                    ..
                }),
            ) if expected_set == &artifact_set_id => {
                completion = Some(ArtifactDeliveryOutcome::Failed(
                    ClosedArtifactDeliveryFailure { phase, code },
                ));
            }
            _ => return Err(ArtifactDeliveryProtocolFailure),
        }

        if let Some(result) = completion {
            complete(&mut state, delivery_id, result);
        }
        drop(state);
        if let Some(upload) = upload {
            self.spawn_upload(upload);
        } else if let Some(retry) = retry {
            self.spawn_retry(retry);
        }
        Ok(())
    }

    pub(super) fn drain_uploads(&self) {
        let mut state = self.lock();
        let mut retries = Vec::new();
        while let Ok(completed) = state.upload_results.try_recv() {
            let Some(delivery) = state.deliveries.get_mut(&completed.delivery_id) else {
                continue;
            };
            let DeliveryPhase::Uploading {
                artifact_set_id,
                carrier_id,
                upload_capability,
            } = &delivery.phase
            else {
                continue;
            };
            let artifact_set_id = artifact_set_id.clone();
            let carrier_id = carrier_id.clone();
            let upload_capability = upload_capability.clone();
            if completed.result.is_err() {
                let exhausted = upload_retry_failure(&delivery.spec);
                match plan_retry(
                    completed.delivery_id,
                    delivery,
                    RetryAction::Upload {
                        artifact_set_id,
                        carrier_id,
                        upload_capability,
                    },
                    exhausted,
                ) {
                    Ok(work) => retries.push(work),
                    Err(result) => complete(&mut state, completed.delivery_id, result),
                }
                continue;
            }
            let request = confirm_observation(
                completed.delivery_id,
                &delivery.spec,
                artifact_set_id.clone(),
                carrier_id.clone(),
            );
            delivery.phase = DeliveryPhase::Confirming {
                artifact_set_id,
                carrier_id,
            };
            if self.outbox.enqueue(request).is_err() {
                complete(
                    &mut state,
                    completed.delivery_id,
                    internal_failure("upload"),
                );
            }
        }
        drop(state);
        for retry in retries {
            self.spawn_retry(retry);
        }
    }

    pub(super) fn cancel_assignment(&self, assignment_id: &str) {
        let mut state = self.lock();
        let ids = state
            .deliveries
            .iter()
            .filter_map(|(id, delivery)| {
                (delivery.spec.assignment_id == assignment_id).then_some(*id)
            })
            .collect::<Vec<_>>();
        for id in ids {
            complete(&mut state, id, ArtifactDeliveryOutcome::AuthorityLost);
        }
    }

    fn spawn_upload(&self, upload: UploadWork) {
        let sender = self.uploads.clone();
        let notification = self.outbox.notification();
        tokio::task::spawn_blocking(move || {
            let delivery_id = upload.delivery_id;
            let result = upload.run();
            let _ = sender.send(UploadCompleted {
                delivery_id,
                result,
            });
            notification.notify_one();
        });
    }

    fn spawn_retry(&self, retry: RetryWork) {
        let broker = self.clone();
        let sleeper = Arc::clone(&self.sleeper);
        tokio::spawn(async move {
            sleeper.sleep(retry.delay).await;
            broker.resume_retry(retry);
        });
    }

    fn resume_retry(&self, retry: RetryWork) {
        let mut state = self.lock();
        let Some(delivery) = state.deliveries.get_mut(&retry.delivery_id) else {
            return;
        };
        if delivery.retry_generation != retry.generation {
            return;
        }
        let mut upload = None;
        match retry.action {
            RetryAction::Request(request) => {
                if self.outbox.enqueue(request).is_err() {
                    let phase = match delivery.phase {
                        DeliveryPhase::Registering => "registration",
                        DeliveryPhase::Uploading { .. } | DeliveryPhase::Confirming { .. } => {
                            "upload"
                        }
                    };
                    complete(&mut state, retry.delivery_id, internal_failure(phase));
                }
            }
            RetryAction::Upload {
                artifact_set_id,
                carrier_id,
                upload_capability,
            } => {
                upload = Some(begin_upload(
                    retry.delivery_id,
                    delivery,
                    artifact_set_id,
                    carrier_id,
                    upload_capability,
                ));
            }
        }
        drop(state);
        if let Some(upload) = upload {
            self.spawn_upload(upload);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ArtifactDeliveryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn failed_for_code(code: String) -> ArtifactDeliveryOutcome {
    let phase = match code.as_str() {
        "stored_object_integrity_mismatch" => "upload",
        _ => "registration",
    };
    ArtifactDeliveryOutcome::Failed(ClosedArtifactDeliveryFailure {
        phase: phase.to_owned(),
        code,
    })
}

fn internal_failure(phase: &str) -> ArtifactDeliveryOutcome {
    ArtifactDeliveryOutcome::Failed(ClosedArtifactDeliveryFailure {
        phase: phase.to_owned(),
        code: "delivery_internal_failure".to_owned(),
    })
}

fn upload_retry_failure(spec: &ArtifactDeliverySpec) -> ArtifactDeliveryOutcome {
    ArtifactDeliveryOutcome::Failed(ClosedArtifactDeliveryFailure {
        phase: "upload".to_owned(),
        code: if spec.is_result() {
            "result_upload_failed".to_owned()
        } else {
            "carrier_upload_failed".to_owned()
        },
    })
}

fn plan_registration_retry(
    delivery_id: u64,
    delivery: &mut Delivery,
) -> Result<RetryWork, ArtifactDeliveryOutcome> {
    let request = register_observation(delivery_id, &delivery.spec);
    plan_retry(
        delivery_id,
        delivery,
        RetryAction::Request(request),
        internal_failure("registration"),
    )
}

fn plan_upload_retry(
    delivery_id: u64,
    delivery: &mut Delivery,
    artifact_set_id: String,
    carrier_id: Option<String>,
    upload_capability: ArtifactUploadCapability,
) -> Result<RetryWork, ArtifactDeliveryOutcome> {
    let exhausted = upload_retry_failure(&delivery.spec);
    plan_retry(
        delivery_id,
        delivery,
        RetryAction::Upload {
            artifact_set_id,
            carrier_id,
            upload_capability,
        },
        exhausted,
    )
}

fn plan_confirmation_retry(
    delivery_id: u64,
    delivery: &mut Delivery,
    artifact_set_id: String,
    carrier_id: Option<String>,
) -> Result<RetryWork, ArtifactDeliveryOutcome> {
    let exhausted = upload_retry_failure(&delivery.spec);
    let request = confirm_observation(delivery_id, &delivery.spec, artifact_set_id, carrier_id);
    plan_retry(
        delivery_id,
        delivery,
        RetryAction::Request(request),
        exhausted,
    )
}

fn select_result_retry(
    identity: (u64, &mut Delivery, String),
    upload_capability: Option<ArtifactUploadCapability>,
    selections: (&mut Option<RetryWork>, &mut Option<ArtifactDeliveryOutcome>),
) {
    let (delivery_id, delivery, artifact_set_id) = identity;
    let (retry, completion) = selections;
    if let Some(upload_capability) = upload_capability {
        select_upload_retry(
            delivery_id,
            delivery,
            artifact_set_id,
            None,
            upload_capability,
            retry,
            completion,
        );
    } else {
        select_confirmation_retry(
            delivery_id,
            delivery,
            artifact_set_id,
            None,
            retry,
            completion,
        );
    }
}

fn select_registration_retry(
    delivery_id: u64,
    delivery: &mut Delivery,
    retry: &mut Option<RetryWork>,
    completion: &mut Option<ArtifactDeliveryOutcome>,
) {
    select_retry(
        plan_registration_retry(delivery_id, delivery),
        retry,
        completion,
    );
}

fn select_upload_retry(
    delivery_id: u64,
    delivery: &mut Delivery,
    artifact_set_id: String,
    carrier_id: Option<String>,
    upload_capability: ArtifactUploadCapability,
    retry: &mut Option<RetryWork>,
    completion: &mut Option<ArtifactDeliveryOutcome>,
) {
    select_retry(
        plan_upload_retry(
            delivery_id,
            delivery,
            artifact_set_id,
            carrier_id,
            upload_capability,
        ),
        retry,
        completion,
    );
}

fn select_confirmation_retry(
    delivery_id: u64,
    delivery: &mut Delivery,
    artifact_set_id: String,
    carrier_id: Option<String>,
    retry: &mut Option<RetryWork>,
    completion: &mut Option<ArtifactDeliveryOutcome>,
) {
    select_retry(
        plan_confirmation_retry(delivery_id, delivery, artifact_set_id, carrier_id),
        retry,
        completion,
    );
}

fn select_retry(
    planned: Result<RetryWork, ArtifactDeliveryOutcome>,
    retry: &mut Option<RetryWork>,
    completion: &mut Option<ArtifactDeliveryOutcome>,
) {
    match planned {
        Ok(work) => *retry = Some(work),
        Err(result) => *completion = Some(result),
    }
}

fn plan_retry(
    delivery_id: u64,
    delivery: &mut Delivery,
    action: RetryAction,
    exhausted: ArtifactDeliveryOutcome,
) -> Result<RetryWork, ArtifactDeliveryOutcome> {
    if delivery.retries >= MAXIMUM_DELIVERY_RETRIES {
        return Err(exhausted);
    }
    delivery.retries += 1;
    let Some(generation) = delivery.retry_generation.checked_add(1) else {
        return Err(exhausted);
    };
    delivery.retry_generation = generation;
    Ok(RetryWork {
        delivery_id,
        generation,
        delay: delivery.backoff.next_delay(),
        action,
    })
}

fn begin_upload(
    delivery_id: u64,
    delivery: &mut Delivery,
    artifact_set_id: String,
    carrier_id: Option<String>,
    capability: ArtifactUploadCapability,
) -> UploadWork {
    let upload = UploadWork::new(delivery_id, &delivery.spec, capability.clone());
    delivery.phase = DeliveryPhase::Uploading {
        artifact_set_id,
        carrier_id,
        upload_capability: capability,
    };
    upload
}

struct UploadWork {
    delivery_id: u64,
    body: Arc<dyn ArtifactUploadBody>,
    media_type: String,
    size_bytes: u64,
    sha256: String,
    capability: ArtifactUploadCapability,
}

impl UploadWork {
    fn new(
        delivery_id: u64,
        spec: &ArtifactDeliverySpec,
        capability: ArtifactUploadCapability,
    ) -> Self {
        Self {
            delivery_id,
            body: Arc::clone(&spec.body),
            media_type: spec.media_type.clone(),
            size_bytes: spec.size_bytes,
            sha256: spec.sha256.clone(),
            capability,
        }
    }

    fn run(self) -> Result<(), ()> {
        validate_capability(
            &self.capability,
            self.size_bytes,
            &self.media_type,
            &self.sha256,
        )?;
        let body = self.body.open().map_err(|_| ())?;
        let mut headers = HeaderMap::new();
        for (name, value) in [
            (CONTENT_LENGTH, self.capability.content_length.as_str()),
            (CONTENT_TYPE, self.capability.content_type.as_str()),
            (IF_NONE_MATCH, self.capability.if_none_match.as_str()),
            (CHECKSUM_HEADER, self.capability.checksum_sha256.as_str()),
        ] {
            let value = HeaderValue::from_str(value).map_err(|_| ())?;
            headers.insert(name, value);
        }
        crate::tls::install_provider();
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|_| ())?;
        // Every completed provider attempt is confirmed through Cloud: a success
        // may lose its response, 412 may mean the first PUT won, and any other
        // status or transport failure may still have stored the exact bytes.
        match client
            .put(&self.capability.url)
            .headers(headers)
            .body(Body::new(body))
            .send()
        {
            Ok(response)
                if response.status().is_success()
                    || response.status() == StatusCode::PRECONDITION_FAILED => {}
            Ok(_) | Err(_) => {}
        }
        Ok(())
    }
}

fn validate_capability(
    capability: &ArtifactUploadCapability,
    size_bytes: u64,
    media_type: &str,
    sha256: &str,
) -> Result<(), ()> {
    let url = url::Url::parse(&capability.url).map_err(|_| ())?;
    let secure = url.scheme() == "https";
    #[cfg(test)]
    let secure = secure
        || (url.scheme() == "http"
            && url
                .host_str()
                .is_some_and(|host| matches!(host, "127.0.0.1" | "::1" | "localhost")));
    if !secure
        || capability.content_length != size_bytes.to_string()
        || capability.content_type != media_type
        || capability.if_none_match != "*"
        || capability.expires_at.is_empty()
        || capability.checksum_sha256 != checksum_base64(sha256)?
    {
        return Err(());
    }
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn checksum_base64(sha256: &str) -> Result<String, ()> {
    if sha256.len() != 64 {
        return Err(());
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&sha256[start..start + 2], 16).map_err(|_| ())?;
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(digest))
}

fn register_observation(delivery_id: u64, spec: &ArtifactDeliverySpec) -> AssignmentObservation {
    let request = match &spec.member {
        ArtifactMember::Carrier {
            portable_owner_path,
            idempotency_key,
        } => ArtifactRequest::RegisterCarrier {
            assignment_id: spec.assignment_id.clone(),
            attempt_id: spec.attempt_id.clone(),
            portable_owner_path: portable_owner_path.clone(),
            media_type: spec.media_type.clone(),
            size_bytes: spec.size_bytes,
            sha256: spec.sha256.clone(),
            idempotency_key: idempotency_key.clone(),
        },
        ArtifactMember::Result => ArtifactRequest::RegisterResult {
            assignment_id: spec.assignment_id.clone(),
            attempt_id: spec.attempt_id.clone(),
            size_bytes: spec.size_bytes,
            sha256: spec.sha256.clone(),
        },
    };
    AssignmentObservation::Artifact {
        delivery_id,
        request,
    }
}

fn confirm_observation(
    delivery_id: u64,
    spec: &ArtifactDeliverySpec,
    artifact_set_id: String,
    carrier_id: Option<String>,
) -> AssignmentObservation {
    let request = match (&spec.member, carrier_id) {
        (ArtifactMember::Carrier { .. }, Some(carrier_id)) => ArtifactRequest::ConfirmCarrier {
            assignment_id: spec.assignment_id.clone(),
            attempt_id: spec.attempt_id.clone(),
            artifact_set_id,
            carrier_id,
        },
        (ArtifactMember::Result, None) => ArtifactRequest::ConfirmResult {
            assignment_id: spec.assignment_id.clone(),
            attempt_id: spec.attempt_id.clone(),
            artifact_set_id,
        },
        _ => unreachable!("registration and confirmation identities must agree"),
    };
    AssignmentObservation::Artifact {
        delivery_id,
        request,
    }
}

fn complete(state: &mut ArtifactDeliveryState, delivery_id: u64, result: ArtifactDeliveryOutcome) {
    if let Some(delivery) = state.deliveries.remove(&delivery_id) {
        let _ = delivery.completion.send(result);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::*;
    use crate::runner::service::test_support::{controlled_sleeper, with_watchdog};

    fn start_result_delivery(
        outbox: &ObservationOutbox,
        sleeper: Arc<dyn Sleeper>,
    ) -> (
        ArtifactDeliveryBroker,
        oneshot::Receiver<ArtifactDeliveryOutcome>,
    ) {
        let broker = ArtifactDeliveryBroker::new(outbox.clone(), sleeper);
        let completion = broker
            .start(ArtifactDeliverySpec::result(
                "asn_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned(),
                "atm_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned(),
                Arc::from(&b"{}"[..]),
            ))
            .unwrap();
        (broker, completion)
    }

    #[tokio::test]
    async fn retryable_registration_exhausts_to_a_closed_failure() {
        let outbox = ObservationOutbox::new();
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let (broker, mut completion) = start_result_delivery(&outbox, sleeper);

        for _ in 0..4 {
            broker
                .handle_response(
                    1,
                    ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                        request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                        outcome: ArtifactResultRegistrationOutcome::Retryable,
                    }),
                )
                .unwrap();
        }

        assert_eq!(
            completion.try_recv(),
            Ok(ArtifactDeliveryOutcome::Failed(
                ClosedArtifactDeliveryFailure {
                    phase: "registration".to_owned(),
                    code: "delivery_internal_failure".to_owned(),
                }
            ))
        );
    }

    #[tokio::test]
    async fn retryable_result_confirmation_exhausts_as_an_upload_failure() {
        let outbox = ObservationOutbox::new();
        let (sleeper, _sleep_requests) = controlled_sleeper();
        let (broker, mut completion) = start_result_delivery(&outbox, sleeper);
        let artifact_set_id = "ats_01k0z6r1w8f4jy2m7q9v3x5ac0".to_owned();
        broker.lock().deliveries.get_mut(&1).unwrap().phase = DeliveryPhase::Confirming {
            artifact_set_id: artifact_set_id.clone(),
            carrier_id: None,
        };

        for _ in 0..4 {
            broker
                .handle_response(
                    1,
                    ArtifactCloudResponse::ResultConfirmation(ArtifactResultConfirmationResponse {
                        request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                        outcome: ArtifactResultConfirmationOutcome::Retryable {
                            artifact_set_id: artifact_set_id.clone(),
                        },
                    }),
                )
                .unwrap();
        }

        assert_eq!(
            completion.try_recv(),
            Ok(ArtifactDeliveryOutcome::Failed(
                ClosedArtifactDeliveryFailure {
                    phase: "upload".to_owned(),
                    code: "result_upload_failed".to_owned(),
                }
            ))
        );
    }

    #[tokio::test]
    async fn retryable_registration_waits_for_backoff_before_reenqueueing() {
        let outbox = ObservationOutbox::new();
        let (sleeper, mut sleep_requests) = controlled_sleeper();
        let (broker, _completion) = start_result_delivery(&outbox, sleeper);
        broker
            .handle_response(
                1,
                ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                    request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    outcome: ArtifactResultRegistrationOutcome::Retryable,
                }),
            )
            .unwrap();

        assert_eq!(outbox.pending(&BTreeSet::new(), 10).len(), 1);
        let (duration, release) = with_watchdog(sleep_requests.recv())
            .await
            .expect("retry backoff was not scheduled")
            .expect("retry backoff channel closed");
        assert!(duration <= Duration::from_secs(1));
        assert_eq!(outbox.pending(&BTreeSet::new(), 10).len(), 1);

        let notification = outbox.notification();
        release.release();
        with_watchdog(async {
            loop {
                let notified = notification.notified();
                tokio::pin!(notified);
                if outbox.pending(&BTreeSet::new(), 10).len() == 2 {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("registration was not re-enqueued after released backoff");
    }
}
