use std::collections::BTreeMap;
use std::fmt::{self, Debug, Write as _};
use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Add;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::admission::{AdmittedWorkflow, CancellationOperation};
use super::evidence::NodeFailureSource;
use super::runtime::{
    self, ActionId, CancellationRequest, ConditionOutput, Occurrence, OutputSet, RecoveryDecision,
    RecoveryDecisionKind, RecoveryRoundNumber, Reduction, RequestedAction, RuntimeState,
    TransitionEvent, WorkflowState,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct OccurrenceOrdinal(u64);

impl OccurrenceOrdinal {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DriverDeadline {}

const OCCURRENCE_DIGEST_BYTES: usize = 32;
const OCCURRENCE_DIGEST_DOMAIN: &[u8] = b"scherzo-driver-occurrence-content\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DriverOccurrenceContentDigest([u8; OCCURRENCE_DIGEST_BYTES]);

// Implementations must encode every field that can distinguish two occurrence payloads.
pub(crate) trait DriverOccurrenceContent {
    fn update_occurrence_digest(&self, digest: &mut OccurrenceDigestEncoder<'_>);
}

pub(crate) struct OccurrenceDigestEncoder<'a>(&'a mut ring::digest::Context);

impl OccurrenceDigestEncoder<'_> {
    pub(crate) fn write_tag(&mut self, tag: u8) {
        self.0.update(&[tag]);
    }

    pub(crate) fn write_u64(&mut self, value: u64) {
        self.0.update(&value.to_be_bytes());
    }

    pub(crate) fn write_bytes(&mut self, content: &[u8]) {
        let length = u64::try_from(content.len()).unwrap_or(u64::MAX);
        self.write_u64(length);
        self.0.update(content);
    }

    pub(crate) fn write_debug(&mut self, content: &impl Debug) {
        let formatting = write!(OccurrenceDigestWriter(self.0), "{content:?}");
        if formatting.is_err() {
            self.0.update(b"\0formatting-failed");
        }
    }
}

struct OccurrenceDigestWriter<'a>(&'a mut ring::digest::Context);

impl fmt::Write for OccurrenceDigestWriter<'_> {
    fn write_str(&mut self, content: &str) -> fmt::Result {
        self.0.update(content.as_bytes());
        Ok(())
    }
}

impl DriverOccurrenceContent for String {
    fn update_occurrence_digest(&self, digest: &mut OccurrenceDigestEncoder<'_>) {
        digest.write_bytes(self.as_bytes());
    }
}

fn occurrence_content_digest<Provisional, Cause, Output>(
    occurrence: &Occurrence<Provisional, Cause, Output, DriverDeadline>,
) -> DriverOccurrenceContentDigest
where
    Provisional: DriverOccurrenceContent,
    Cause: DriverOccurrenceContent,
    Output: DriverOccurrenceContent,
{
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    context.update(OCCURRENCE_DIGEST_DOMAIN);
    let mut digest = OccurrenceDigestEncoder(&mut context);
    match occurrence {
        Occurrence::StepStarted { step, action } => {
            digest.write_tag(0);
            write_step_action(&mut digest, step, *action);
        }
        Occurrence::StepStartFailed {
            step,
            action,
            cause,
        } => {
            digest.write_tag(1);
            write_step_action(&mut digest, step, *action);
            cause.update_occurrence_digest(&mut digest);
        }
        Occurrence::StepExecutionCompleted {
            step,
            action,
            provisional,
        } => {
            digest.write_tag(2);
            write_step_action(&mut digest, step, *action);
            provisional.update_occurrence_digest(&mut digest);
        }
        Occurrence::StepExecutionFailed {
            step,
            action,
            cause,
        } => {
            digest.write_tag(3);
            write_step_action(&mut digest, step, *action);
            cause.update_occurrence_digest(&mut digest);
        }
        Occurrence::OutputsCaptured {
            step,
            action,
            outputs,
        } => {
            digest.write_tag(4);
            write_step_action(&mut digest, step, *action);
            digest.write_u64(u64::try_from(outputs.len()).unwrap_or(u64::MAX));
            for (name, output) in outputs {
                digest.write_bytes(name.as_bytes());
                output.update_occurrence_digest(&mut digest);
            }
        }
        Occurrence::OutputCaptureFailed {
            step,
            action,
            cause,
        } => {
            digest.write_tag(5);
            write_step_action(&mut digest, step, *action);
            cause.update_occurrence_digest(&mut digest);
        }
        Occurrence::RecoveryHandlerStarted {
            step,
            round,
            action,
        } => {
            digest.write_tag(6);
            write_step_round_action(&mut digest, step, *round, *action);
        }
        Occurrence::RecoveryHandlerStartFailed {
            step,
            round,
            action,
            cause,
        } => {
            digest.write_tag(7);
            write_step_round_action(&mut digest, step, *round, *action);
            cause.update_occurrence_digest(&mut digest);
        }
        Occurrence::RecoveryHandlerCompleted {
            step,
            round,
            action,
            decision,
        } => {
            digest.write_tag(8);
            write_step_round_action(&mut digest, step, *round, *action);
            digest.write_tag(match decision.kind {
                RecoveryDecisionKind::Recheck => 0,
                RecoveryDecisionKind::GaveUp => 1,
            });
            digest.write_bytes(decision.summary.as_bytes());
            digest.write_bytes(decision.reason.as_bytes());
        }
        Occurrence::RecoveryHandlerExecutionFailed {
            step,
            round,
            action,
            cause,
        } => {
            digest.write_tag(9);
            write_step_round_action(&mut digest, step, *round, *action);
            cause.update_occurrence_digest(&mut digest);
        }
        Occurrence::StepQuiesced { step, action } => {
            digest.write_tag(10);
            write_step_action(&mut digest, step, *action);
        }
        #[cfg(test)]
        Occurrence::CancellationRequested { deadline, .. } => match *deadline {},
        Occurrence::CancellationOperationRequested { deadline, .. }
        | Occurrence::ForceAbortRequested { deadline, .. } => match *deadline {},
    }
    let digest = context.finish();
    let mut bytes = [0_u8; OCCURRENCE_DIGEST_BYTES];
    bytes.copy_from_slice(digest.as_ref());
    DriverOccurrenceContentDigest(bytes)
}

fn write_step_action(digest: &mut OccurrenceDigestEncoder<'_>, step: &str, action: ActionId) {
    digest.write_bytes(step.as_bytes());
    digest.write_u64(action.transition_sequence.get());
}

fn write_step_round_action(
    digest: &mut OccurrenceDigestEncoder<'_>,
    step: &str,
    round: RecoveryRoundNumber,
    action: ActionId,
) {
    write_step_action(digest, step, action);
    digest.write_tag(round.get());
}

#[derive(Clone, Debug)]
pub(crate) struct DriverOccurrence<Provisional, Cause, Output> {
    occurrence: Occurrence<Provisional, Cause, Output, DriverDeadline>,
    content_digest: DriverOccurrenceContentDigest,
}

impl<Provisional, Cause, Output> DriverOccurrence<Provisional, Cause, Output>
where
    Provisional: DriverOccurrenceContent,
    Cause: DriverOccurrenceContent,
    Output: DriverOccurrenceContent,
{
    fn new(occurrence: Occurrence<Provisional, Cause, Output, DriverDeadline>) -> Self {
        let content_digest = occurrence_content_digest(&occurrence);
        Self {
            occurrence,
            content_digest,
        }
    }

    pub(crate) fn step_started(step: String, action: ActionId) -> Self {
        Self::new(Occurrence::StepStarted { step, action })
    }

    pub(crate) fn step_start_failed(step: String, action: ActionId, cause: Cause) -> Self {
        Self::new(Occurrence::StepStartFailed {
            step,
            action,
            cause,
        })
    }

    pub(crate) fn step_execution_completed(
        step: String,
        action: ActionId,
        provisional: Provisional,
    ) -> Self {
        Self::new(Occurrence::StepExecutionCompleted {
            step,
            action,
            provisional,
        })
    }

    pub(crate) fn step_execution_failed(step: String, action: ActionId, cause: Cause) -> Self {
        Self::new(Occurrence::StepExecutionFailed {
            step,
            action,
            cause,
        })
    }

    pub(crate) fn outputs_captured(
        step: String,
        action: ActionId,
        outputs: OutputSet<Output>,
    ) -> Self {
        Self::new(Occurrence::OutputsCaptured {
            step,
            action,
            outputs,
        })
    }

    pub(crate) fn output_capture_failed(step: String, action: ActionId, cause: Cause) -> Self {
        Self::new(Occurrence::OutputCaptureFailed {
            step,
            action,
            cause,
        })
    }

    pub(crate) fn recovery_handler_started(
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
    ) -> Self {
        Self::new(Occurrence::RecoveryHandlerStarted {
            step,
            round,
            action,
        })
    }

    pub(crate) fn recovery_handler_start_failed(
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        cause: Cause,
    ) -> Self {
        Self::new(Occurrence::RecoveryHandlerStartFailed {
            step,
            round,
            action,
            cause,
        })
    }

    pub(crate) fn recovery_handler_completed(
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        decision: RecoveryDecision,
    ) -> Self {
        Self::new(Occurrence::RecoveryHandlerCompleted {
            step,
            round,
            action,
            decision,
        })
    }

    pub(crate) fn recovery_handler_execution_failed(
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        cause: Cause,
    ) -> Self {
        Self::new(Occurrence::RecoveryHandlerExecutionFailed {
            step,
            round,
            action,
            cause,
        })
    }

    pub(crate) fn step_quiesced(step: String, action: ActionId) -> Self {
        Self::new(Occurrence::StepQuiesced { step, action })
    }
}

impl<Provisional, Cause, Output> DriverOccurrence<Provisional, Cause, Output> {
    fn content_digest(&self) -> DriverOccurrenceContentDigest {
        self.content_digest
    }

    pub(crate) fn into_runtime<Deadline>(self) -> Occurrence<Provisional, Cause, Output, Deadline> {
        match self.occurrence {
            Occurrence::StepStarted { step, action } => Occurrence::StepStarted { step, action },
            Occurrence::StepStartFailed {
                step,
                action,
                cause,
            } => Occurrence::StepStartFailed {
                step,
                action,
                cause,
            },
            Occurrence::StepExecutionCompleted {
                step,
                action,
                provisional,
            } => Occurrence::StepExecutionCompleted {
                step,
                action,
                provisional,
            },
            Occurrence::StepExecutionFailed {
                step,
                action,
                cause,
            } => Occurrence::StepExecutionFailed {
                step,
                action,
                cause,
            },
            Occurrence::OutputsCaptured {
                step,
                action,
                outputs,
            } => Occurrence::OutputsCaptured {
                step,
                action,
                outputs,
            },
            Occurrence::OutputCaptureFailed {
                step,
                action,
                cause,
            } => Occurrence::OutputCaptureFailed {
                step,
                action,
                cause,
            },
            Occurrence::RecoveryHandlerStarted {
                step,
                round,
                action,
            } => Occurrence::RecoveryHandlerStarted {
                step,
                round,
                action,
            },
            Occurrence::RecoveryHandlerStartFailed {
                step,
                round,
                action,
                cause,
            } => Occurrence::RecoveryHandlerStartFailed {
                step,
                round,
                action,
                cause,
            },
            Occurrence::RecoveryHandlerCompleted {
                step,
                round,
                action,
                decision,
            } => Occurrence::RecoveryHandlerCompleted {
                step,
                round,
                action,
                decision,
            },
            Occurrence::RecoveryHandlerExecutionFailed {
                step,
                round,
                action,
                cause,
            } => Occurrence::RecoveryHandlerExecutionFailed {
                step,
                round,
                action,
                cause,
            },
            Occurrence::StepQuiesced { step, action } => Occurrence::StepQuiesced { step, action },
            #[cfg(test)]
            Occurrence::CancellationRequested { deadline, .. } => match deadline {},
            Occurrence::CancellationOperationRequested {
                operation: _,
                deadline,
                ..
            } => match deadline {},
            Occurrence::ForceAbortRequested {
                operation: _,
                deadline,
            } => match deadline {},
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DriverOccurrenceKind {
    StepStarted,
    StepStartFailed,
    StepExecutionCompleted,
    StepExecutionFailed,
    OutputsCaptured,
    OutputCaptureFailed,
    RecoveryHandlerStarted,
    RecoveryHandlerStartFailed,
    RecoveryHandlerCompleted,
    RecoveryHandlerExecutionFailed,
    StepQuiesced,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DriverOccurrenceIdentity {
    action: ActionId,
    kind: DriverOccurrenceKind,
    round: Option<RecoveryRoundNumber>,
}

impl<Provisional, Cause, Output> DriverOccurrence<Provisional, Cause, Output> {
    fn identity(&self) -> DriverOccurrenceIdentity {
        let (action, kind, round) = match &self.occurrence {
            Occurrence::StepStarted { action, .. } => {
                (*action, DriverOccurrenceKind::StepStarted, None)
            }
            Occurrence::StepStartFailed { action, .. } => {
                (*action, DriverOccurrenceKind::StepStartFailed, None)
            }
            Occurrence::StepExecutionCompleted { action, .. } => {
                (*action, DriverOccurrenceKind::StepExecutionCompleted, None)
            }
            Occurrence::StepExecutionFailed { action, .. } => {
                (*action, DriverOccurrenceKind::StepExecutionFailed, None)
            }
            Occurrence::OutputsCaptured { action, .. } => {
                (*action, DriverOccurrenceKind::OutputsCaptured, None)
            }
            Occurrence::OutputCaptureFailed { action, .. } => {
                (*action, DriverOccurrenceKind::OutputCaptureFailed, None)
            }
            Occurrence::RecoveryHandlerStarted { round, action, .. } => (
                *action,
                DriverOccurrenceKind::RecoveryHandlerStarted,
                Some(*round),
            ),
            Occurrence::RecoveryHandlerStartFailed { round, action, .. } => (
                *action,
                DriverOccurrenceKind::RecoveryHandlerStartFailed,
                Some(*round),
            ),
            Occurrence::RecoveryHandlerCompleted { round, action, .. } => (
                *action,
                DriverOccurrenceKind::RecoveryHandlerCompleted,
                Some(*round),
            ),
            Occurrence::RecoveryHandlerExecutionFailed { round, action, .. } => (
                *action,
                DriverOccurrenceKind::RecoveryHandlerExecutionFailed,
                Some(*round),
            ),
            Occurrence::StepQuiesced { action, .. } => {
                (*action, DriverOccurrenceKind::StepQuiesced, None)
            }
            #[cfg(test)]
            Occurrence::CancellationRequested { deadline, .. } => match *deadline {},
            Occurrence::CancellationOperationRequested { deadline, .. }
            | Occurrence::ForceAbortRequested { deadline, .. } => match *deadline {},
        };
        DriverOccurrenceIdentity {
            action,
            kind,
            round,
        }
    }
}

enum ClaimResolution {
    Publish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DriverOccurrenceClaimError {
    ReceiverClosed,
}

// Acknowledged deliveries settle in two phases: the coordinator reports the reducer
// decision, then waits for the driver to finalize or release occurrence-owned resources.
pub(crate) enum DriverOccurrenceAcceptance {
    Accepted(DriverOccurrenceFinalization),
    Rejected(DriverOccurrenceFinalization),
}

pub(crate) struct DriverOccurrenceFinalization {
    finalized: oneshot::Sender<()>,
}

impl DriverOccurrenceFinalization {
    pub(crate) fn finalize(self) {
        let _ = self.finalized.send(());
    }
}

struct DriverOccurrenceAcknowledgement {
    decision: oneshot::Sender<DriverOccurrenceAcceptance>,
}

impl DriverOccurrenceAcknowledgement {
    fn resolve(self, accepted: bool) -> Option<oneshot::Receiver<()>> {
        let (finalized, finalization) = oneshot::channel();
        let finalization_token = DriverOccurrenceFinalization { finalized };
        let decision = if accepted {
            DriverOccurrenceAcceptance::Accepted(finalization_token)
        } else {
            DriverOccurrenceAcceptance::Rejected(finalization_token)
        };
        self.decision.send(decision).ok().map(|()| finalization)
    }
}

// The receiver acknowledges a claim before the driver waits for adapter quiescence.
// Resolving this token then either publishes the lifecycle occurrence or discards it.
pub(crate) struct DriverOccurrenceClaim {
    resolution: oneshot::Sender<ClaimResolution>,
}

impl DriverOccurrenceClaim {
    pub(crate) fn publish(self) -> Result<(), DriverOccurrenceClaimError> {
        self.resolution
            .send(ClaimResolution::Publish)
            .map_err(|_| DriverOccurrenceClaimError::ReceiverClosed)
    }

    pub(crate) fn discard(self) {}
}

enum DriverOccurrenceDelivery<Provisional, Cause, Output> {
    Ready(DriverOccurrence<Provisional, Cause, Output>),
    Claimed {
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
        observed: oneshot::Sender<()>,
        resolution: oneshot::Receiver<ClaimResolution>,
    },
    Acknowledged {
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
        acknowledgement: DriverOccurrenceAcknowledgement,
    },
}

struct ResolvedDriverOccurrence<Provisional, Cause, Output> {
    occurrence: DriverOccurrence<Provisional, Cause, Output>,
    acknowledgement: Option<DriverOccurrenceAcknowledgement>,
}

enum SelectedDriverOccurrence<Provisional, Cause, Output> {
    Resolved(ResolvedDriverOccurrence<Provisional, Cause, Output>),
    Claimed {
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
        resolution: oneshot::Receiver<ClaimResolution>,
    },
    Discarded(DriverOccurrence<Provisional, Cause, Output>),
}

impl<Provisional, Cause, Output> DriverOccurrenceDelivery<Provisional, Cause, Output> {
    fn select(self) -> SelectedDriverOccurrence<Provisional, Cause, Output> {
        match self {
            Self::Ready(occurrence) => {
                SelectedDriverOccurrence::Resolved(ResolvedDriverOccurrence {
                    occurrence,
                    acknowledgement: None,
                })
            }
            Self::Claimed {
                occurrence,
                observed,
                resolution,
            } => {
                if observed.send(()).is_ok() {
                    SelectedDriverOccurrence::Claimed {
                        occurrence,
                        resolution,
                    }
                } else {
                    SelectedDriverOccurrence::Discarded(occurrence)
                }
            }
            Self::Acknowledged {
                occurrence,
                acknowledgement,
            } => SelectedDriverOccurrence::Resolved(ResolvedDriverOccurrence {
                occurrence,
                acknowledgement: Some(acknowledgement),
            }),
        }
    }

    #[cfg(test)]
    async fn resolve(self) -> Option<ResolvedDriverOccurrence<Provisional, Cause, Output>> {
        self.select().resolve().await.ok()
    }
}

impl<Provisional, Cause, Output> SelectedDriverOccurrence<Provisional, Cause, Output> {
    fn is_resolved(&self) -> bool {
        matches!(self, Self::Resolved(_))
    }

    async fn resolve(
        self,
    ) -> Result<
        ResolvedDriverOccurrence<Provisional, Cause, Output>,
        DriverOccurrence<Provisional, Cause, Output>,
    > {
        match self {
            Self::Resolved(resolved) => Ok(resolved),
            Self::Claimed {
                occurrence,
                resolution,
            } => match resolution.await {
                Ok(ClaimResolution::Publish) => Ok(ResolvedDriverOccurrence {
                    occurrence,
                    acknowledgement: None,
                }),
                Err(_) => Err(occurrence),
            },
            Self::Discarded(occurrence) => Err(occurrence),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OccurrenceSender<Provisional, Cause, Output> {
    sender: mpsc::Sender<DriverOccurrenceDelivery<Provisional, Cause, Output>>,
}

impl<Provisional, Cause, Output> OccurrenceSender<Provisional, Cause, Output> {
    pub(crate) async fn send(
        &self,
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
    ) -> Result<(), DriverOccurrence<Provisional, Cause, Output>> {
        self.sender
            .send(DriverOccurrenceDelivery::Ready(occurrence))
            .await
            .map_err(|failure| match failure.0 {
                DriverOccurrenceDelivery::Ready(occurrence)
                | DriverOccurrenceDelivery::Claimed { occurrence, .. }
                | DriverOccurrenceDelivery::Acknowledged { occurrence, .. } => occurrence,
            })
    }

    pub(crate) async fn send_acknowledged(
        &self,
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
    ) -> Result<DriverOccurrenceAcceptance, DriverOccurrenceClaimError> {
        let (decision, acceptance) = oneshot::channel();
        self.sender
            .send(DriverOccurrenceDelivery::Acknowledged {
                occurrence,
                acknowledgement: DriverOccurrenceAcknowledgement { decision },
            })
            .await
            .map_err(|_| DriverOccurrenceClaimError::ReceiverClosed)?;
        acceptance
            .await
            .map_err(|_| DriverOccurrenceClaimError::ReceiverClosed)
    }

    pub(crate) async fn claim(
        &self,
        occurrence: DriverOccurrence<Provisional, Cause, Output>,
    ) -> Result<DriverOccurrenceClaim, DriverOccurrenceClaimError> {
        let (observed, observation) = oneshot::channel();
        let (resolution, resolved) = oneshot::channel();
        self.sender
            .send(DriverOccurrenceDelivery::Claimed {
                occurrence,
                observed,
                resolution: resolved,
            })
            .await
            .map_err(|_| DriverOccurrenceClaimError::ReceiverClosed)?;
        observation
            .await
            .map_err(|_| DriverOccurrenceClaimError::ReceiverClosed)?;
        Ok(DriverOccurrenceClaim { resolution })
    }
}

pub(crate) struct OccurrenceReceiver<Provisional, Cause, Output> {
    receiver: mpsc::Receiver<DriverOccurrenceDelivery<Provisional, Cause, Output>>,
    pending: Option<DriverOccurrenceDelivery<Provisional, Cause, Output>>,
}

#[cfg(test)]
pub(crate) struct DriverOccurrenceTestAcknowledgement {
    acknowledgement: Option<DriverOccurrenceAcknowledgement>,
}

#[cfg(test)]
impl DriverOccurrenceTestAcknowledgement {
    pub(crate) async fn resolve(self, accepted: bool) {
        if let Some(finalization) = self
            .acknowledgement
            .and_then(|acknowledgement| acknowledgement.resolve(accepted))
        {
            let _ = finalization.await;
        }
    }
}

impl<Provisional, Cause, Output> OccurrenceReceiver<Provisional, Cause, Output> {
    #[cfg(test)]
    pub(crate) async fn recv(&mut self) -> Option<DriverOccurrence<Provisional, Cause, Output>> {
        loop {
            let delivery = self.recv_delivery().await?;
            if let Some(resolved) = delivery.resolve().await {
                if let Some(acknowledgement) = resolved.acknowledgement {
                    acknowledgement.resolve(false);
                }
                return Some(resolved.occurrence);
            }
        }
    }

    async fn recv_delivery(
        &mut self,
    ) -> Option<DriverOccurrenceDelivery<Provisional, Cause, Output>> {
        match self.pending.take() {
            Some(delivery) => Some(delivery),
            None => self.receiver.recv().await,
        }
    }

    fn retain_delivery(&mut self, delivery: DriverOccurrenceDelivery<Provisional, Cause, Output>) {
        self.pending = Some(delivery);
    }

    #[cfg(test)]
    pub(crate) async fn recv_with_acknowledgement(
        &mut self,
    ) -> Option<(
        DriverOccurrence<Provisional, Cause, Output>,
        DriverOccurrenceTestAcknowledgement,
    )> {
        loop {
            let delivery = self.recv_delivery().await?;
            if let Some(resolved) = delivery.resolve().await {
                return Some((
                    resolved.occurrence,
                    DriverOccurrenceTestAcknowledgement {
                        acknowledgement: resolved.acknowledgement,
                    },
                ));
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Option<DriverOccurrence<Provisional, Cause, Output>> {
        if self.pending.is_some() {
            return None;
        }
        match self.receiver.try_recv().ok()? {
            DriverOccurrenceDelivery::Ready(occurrence) => Some(occurrence),
            pending @ (DriverOccurrenceDelivery::Claimed { .. }
            | DriverOccurrenceDelivery::Acknowledged { .. }) => {
                self.pending = Some(pending);
                None
            }
        }
    }
}

pub(crate) fn occurrence_channel<Provisional, Cause, Output>(
    capacity: NonZeroUsize,
) -> (
    OccurrenceSender<Provisional, Cause, Output>,
    OccurrenceReceiver<Provisional, Cause, Output>,
) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        OccurrenceSender { sender },
        OccurrenceReceiver {
            receiver,
            pending: None,
        },
    )
}

pub(crate) trait CoordinatorClock: Clone + Send + Sync + 'static {
    type Instant: Add<Duration, Output = Self::Instant> + Clone + Send + 'static;

    fn now(&mut self) -> Self::Instant;

    fn wait_until(&self, deadline: Self::Instant) -> impl Future<Output = ()> + Send;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommittedActionKind {
    StartStep,
    StartRecoveryHandler,
    CaptureOutputs,
    CancelStep,
    ForceAbortStep,
    FinishRun,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedAction {
    pub(crate) id: ActionId,
    pub(crate) kind: CommittedActionKind,
    pub(crate) step: Option<String>,
    pub(crate) execution_number: Option<runtime::TargetExecutionNumber>,
    pub(crate) recovery_round: Option<RecoveryRoundNumber>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedReduction<Cause, Output, Deadline> {
    pub(crate) occurrence_ordinal: OccurrenceOrdinal,
    pub(crate) occurrence_accepted: bool,
    pub(crate) state: RuntimeState<Cause, Output, Deadline>,
    pub(crate) events: Vec<TransitionEvent<Deadline>>,
    pub(crate) actions: Vec<CommittedAction>,
    pub(crate) diagnostic: Option<CoordinationDiagnostic>,
}

pub(crate) trait CommitPort<Commit> {
    type Error;

    fn commit(&mut self, commit: Commit) -> impl Future<Output = Result<(), Self::Error>>;
}

pub(crate) trait ActionPort<Action> {
    fn release(&mut self, action: Action) -> impl Future<Output = ()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoordinationDiagnostic {
    TransitionCapacityExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoordinationError {
    ArtifactStagingMismatch,
    InputStagingMismatch,
    AgentInputStagingMismatch,
    AgentRuntimeUnavailable,
    CommitFailed,
    OccurrenceChannelClosed,
    OccurrenceConflict,
    OccurrenceIdentityCapacityExceeded,
    OccurrenceOrdinalExhausted,
    ReducerStateUnavailable,
    TransitionCapacityExceeded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CoordinationResult<Cause, Output, Deadline = ()> {
    pub(crate) state: RuntimeState<Cause, Output, Deadline>,
    pub(crate) last_occurrence_ordinal: OccurrenceOrdinal,
}

enum CoordinatorInput<Provisional, Cause, Output, Deadline> {
    Cancellation(Occurrence<Provisional, Cause, Output, Deadline>),
    Driver {
        selected: SelectedDriverOccurrence<Provisional, Cause, Output>,
        ordinal_assigned: bool,
    },
}

pub(crate) struct Coordinator<Provisional, Cause, Output, Clock, Commits, Actions>
where
    Clock: CoordinatorClock,
{
    admitted: AdmittedWorkflow,
    occurrences: OccurrenceReceiver<Provisional, Cause, Output>,
    clock: Clock,
    commits: Commits,
    actions: Actions,
    state: Option<RuntimeState<Cause, Output, Clock::Instant>>,
}

impl<Provisional, Cause, Output, Clock, Commits, Actions>
    Coordinator<Provisional, Cause, Output, Clock, Commits, Actions>
where
    Provisional: Clone + runtime::ProvisionalStepResources,
    Cause: Clone + NodeFailureSource,
    Output: Clone + ConditionOutput,
    Clock: CoordinatorClock,
    Commits: CommitPort<CommittedReduction<Cause, Output, Clock::Instant>>,
    Actions: ActionPort<RequestedAction<Provisional, Cause, Output, Clock::Instant>>,
{
    pub(crate) fn new(
        admitted: AdmittedWorkflow,
        occurrences: OccurrenceReceiver<Provisional, Cause, Output>,
        clock: Clock,
        commits: Commits,
        actions: Actions,
    ) -> Self {
        Self {
            admitted,
            occurrences,
            clock,
            commits,
            actions,
            state: None,
        }
    }

    pub(crate) async fn run(
        mut self,
    ) -> Result<CoordinationResult<Cause, Output, Clock::Instant>, CoordinationError> {
        let cancellation_source = self.admitted.execution().cancellation().source().clone();
        let mut cancellation = cancellation_source.subscribe_operations();
        let grace = self.admitted.execution().cancellation().grace();
        let initial_operation = cancellation.next_operation();
        let (initial_cancellation, initial_cancellation_operation) = match initial_operation {
            Some(CancellationOperation::Graceful { id, reason }) => (
                Some(CancellationRequest {
                    reason,
                    deadline: self.clock.now() + grace,
                }),
                Some(id),
            ),
            Some(CancellationOperation::ForceAbort { .. }) | None => (None, None),
        };
        let mut ordinal = OccurrenceOrdinal(0)
            .next()
            .ok_or(CoordinationError::OccurrenceOrdinalExhausted)?;
        let mut observed_driver_occurrences: BTreeMap<
            DriverOccurrenceIdentity,
            DriverOccurrenceContentDigest,
        > = BTreeMap::new();
        let occurrence_identity_capacity = self.admitted.capacity().maximum_transitions;
        let initialization =
            runtime::initialize_with_operation::<Provisional, Cause, Output, Clock::Instant>(
                &self.admitted,
                initial_cancellation,
                initial_cancellation_operation,
            );
        if initialization.state.transition_capacity_exceeded() {
            self.commit_coordination_diagnostic(
                ordinal,
                initialization.state,
                None,
                CoordinationDiagnostic::TransitionCapacityExceeded,
            )
            .await?;
            return Err(CoordinationError::TransitionCapacityExceeded);
        }
        if self.commit(ordinal, initialization, None).await? {
            return self.result(ordinal);
        }

        loop {
            let (occurrence, acknowledgement, ordinal_assigned, exact_replay) = loop {
                let input = tokio::select! {
                    biased;
                    changed = cancellation.changed() => {
                        if changed.is_err() {
                            return Err(CoordinationError::OccurrenceChannelClosed);
                        }
                        let Some(operation) = cancellation.next_operation() else {
                            continue;
                        };
                        let occurrence = match operation {
                            CancellationOperation::Graceful { id, reason } => {
                                Occurrence::CancellationOperationRequested {
                                    operation: id,
                                    reason,
                                    deadline: self.clock.now() + grace,
                                }
                            }
                            CancellationOperation::ForceAbort { id } => {
                                Occurrence::ForceAbortRequested {
                                    operation: id,
                                    deadline: self.clock.now(),
                                }
                            }
                        };
                        CoordinatorInput::Cancellation(occurrence)
                    }
                    driver_occurrence = self.occurrences.recv_delivery() => {
                        let Some(driver_occurrence) = driver_occurrence else {
                            return Err(CoordinationError::OccurrenceChannelClosed);
                        };
                        // Bias only orders this select poll. The operation queue is read again
                        // before assigning a driver ordinal so cancellation cannot be skipped.
                        if let Some(operation) = cancellation.next_operation() {
                            self.occurrences.retain_delivery(driver_occurrence);
                            let occurrence = match operation {
                                CancellationOperation::Graceful { id, reason } => {
                                    Occurrence::CancellationOperationRequested {
                                        operation: id,
                                        reason,
                                        deadline: self.clock.now() + grace,
                                    }
                                }
                                CancellationOperation::ForceAbort { id } => {
                                    Occurrence::ForceAbortRequested {
                                        operation: id,
                                        deadline: self.clock.now(),
                                    }
                                }
                            };
                            CoordinatorInput::Cancellation(occurrence)
                        } else {
                            let selected = driver_occurrence.select();
                            let ordinal_assigned = selected.is_resolved();
                            if ordinal_assigned {
                                ordinal = ordinal.next().ok_or(
                                    CoordinationError::OccurrenceOrdinalExhausted,
                                )?;
                            }
                            CoordinatorInput::Driver {
                                selected,
                                ordinal_assigned,
                            }
                        }
                    }
                };
                match input {
                    CoordinatorInput::Cancellation(occurrence) => {
                        break (occurrence, None, false, false);
                    }
                    CoordinatorInput::Driver {
                        selected,
                        ordinal_assigned,
                    } => {
                        // An acknowledged claim owns arbitration until its adapter settles, so
                        // later cancellation cannot gain priority from diagnostic drain timing.
                        let resolved = match selected.resolve().await {
                            Ok(resolved) => resolved,
                            Err(discarded) => {
                                if let Some(release) =
                                    runtime::occurrence_resource_release(discarded.into_runtime())
                                {
                                    self.actions.release(release).await;
                                }
                                continue;
                            }
                        };
                        let identity = resolved.occurrence.identity();
                        let content_digest = resolved.occurrence.content_digest();
                        let exact_replay = if let Some(observed_digest) =
                            observed_driver_occurrences.get(&identity)
                        {
                            if observed_digest != &content_digest {
                                if let Some(release) = runtime::occurrence_resource_release(
                                    resolved.occurrence.into_runtime(),
                                ) {
                                    self.actions.release(release).await;
                                }
                                return Err(CoordinationError::OccurrenceConflict);
                            }
                            true
                        } else {
                            if u64::try_from(observed_driver_occurrences.len()).unwrap_or(u64::MAX)
                                >= occurrence_identity_capacity
                            {
                                if let Some(release) = runtime::occurrence_resource_release(
                                    resolved.occurrence.into_runtime(),
                                ) {
                                    self.actions.release(release).await;
                                }
                                return Err(CoordinationError::OccurrenceIdentityCapacityExceeded);
                            }
                            observed_driver_occurrences.insert(identity, content_digest);
                            false
                        };
                        break (
                            resolved.occurrence.into_runtime(),
                            resolved.acknowledgement,
                            ordinal_assigned,
                            exact_replay,
                        );
                    }
                }
            };
            if !ordinal_assigned {
                ordinal = ordinal
                    .next()
                    .ok_or(CoordinationError::OccurrenceOrdinalExhausted)?;
            }
            let Some(state) = self.state.as_ref() else {
                return Err(CoordinationError::ReducerStateUnavailable);
            };
            let current_state = state.clone();
            let reduction = if exact_replay {
                runtime::discard_occurrence(&current_state, occurrence)
            } else {
                runtime::reduce(&current_state, occurrence)
            };
            if reduction.state.transition_capacity_exceeded() {
                self.release_action_resources(reduction.actions).await;
                self.commit_coordination_diagnostic(
                    ordinal,
                    current_state,
                    acknowledgement,
                    CoordinationDiagnostic::TransitionCapacityExceeded,
                )
                .await?;
                return Err(CoordinationError::TransitionCapacityExceeded);
            }
            if self.commit(ordinal, reduction, acknowledgement).await? {
                return self.result(ordinal);
            }
        }
    }

    async fn commit_coordination_diagnostic(
        &mut self,
        occurrence_ordinal: OccurrenceOrdinal,
        state: RuntimeState<Cause, Output, Clock::Instant>,
        acknowledgement: Option<DriverOccurrenceAcknowledgement>,
        diagnostic: CoordinationDiagnostic,
    ) -> Result<(), CoordinationError> {
        if self
            .commits
            .commit(CommittedReduction {
                occurrence_ordinal,
                occurrence_accepted: false,
                state,
                events: Vec::new(),
                actions: Vec::new(),
                diagnostic: Some(diagnostic),
            })
            .await
            .is_err()
        {
            return Err(CoordinationError::CommitFailed);
        }
        if let Some(finalization) =
            acknowledgement.and_then(|acknowledgement| acknowledgement.resolve(false))
        {
            let _ = finalization.await;
        }
        Ok(())
    }

    async fn commit(
        &mut self,
        occurrence_ordinal: OccurrenceOrdinal,
        reduction: Reduction<Provisional, Cause, Output, Clock::Instant>,
        acknowledgement: Option<DriverOccurrenceAcknowledgement>,
    ) -> Result<bool, CoordinationError> {
        let Reduction {
            state,
            events,
            actions,
            occurrence_accepted,
        } = reduction;
        let terminal = matches!(
            &state.workflow,
            WorkflowState::Succeeded
                | WorkflowState::Failed { .. }
                | WorkflowState::Cancelled { .. }
        );
        let entered_finalization = events.iter().any(|event| match event {
            TransitionEvent::Workflow { to, .. } => {
                matches!(to.as_ref(), WorkflowState::Finalizing { .. })
            }
            _ => false,
        });
        let committed_actions = actions.iter().filter_map(committed_action).collect();
        let committed_state = state.clone();
        let cancellation = self.admitted.execution().cancellation().source();
        let finalization_arm_started =
            entered_finalization && cancellation.begin_finalization_arm();
        if self
            .commits
            .commit(CommittedReduction {
                occurrence_ordinal,
                occurrence_accepted,
                state: committed_state,
                events,
                actions: committed_actions,
                diagnostic: None,
            })
            .await
            .is_err()
        {
            if finalization_arm_started {
                cancellation.abort_finalization_arm();
            }
            self.release_action_resources(actions).await;
            return Err(CoordinationError::CommitFailed);
        }
        self.state = Some(state);
        if finalization_arm_started {
            cancellation.complete_finalization_arm();
        }
        if let Some(finalization) =
            acknowledgement.and_then(|acknowledgement| acknowledgement.resolve(occurrence_accepted))
        {
            let _ = finalization.await;
        }
        if terminal {
            self.occurrences.receiver.close();
        }
        for action in actions {
            self.actions.release(action).await;
        }
        Ok(terminal)
    }

    async fn release_action_resources(
        &mut self,
        actions: Vec<RequestedAction<Provisional, Cause, Output, Clock::Instant>>,
    ) {
        for requested in actions {
            if let Some(release) = action_resource_release(requested) {
                self.actions.release(release).await;
            }
        }
    }

    fn result(
        &self,
        last_occurrence_ordinal: OccurrenceOrdinal,
    ) -> Result<CoordinationResult<Cause, Output, Clock::Instant>, CoordinationError> {
        let Some(state) = self.state.clone() else {
            return Err(CoordinationError::ReducerStateUnavailable);
        };
        Ok(CoordinationResult {
            state,
            last_occurrence_ordinal,
        })
    }
}

fn action_resource_release<Provisional, Cause, Output, Deadline>(
    requested: RequestedAction<Provisional, Cause, Output, Deadline>,
) -> Option<RequestedAction<Provisional, Cause, Output, Deadline>>
where
    Provisional: runtime::ProvisionalStepResources,
{
    let RequestedAction { id, action } = requested;
    match action {
        runtime::Action::CaptureOutputs { provisional, .. } if provisional.requires_release() => {
            Some(RequestedAction {
                id,
                action: runtime::Action::ReleaseStepResources { provisional },
            })
        }
        action @ runtime::Action::ReleaseStepResources { .. } => {
            Some(RequestedAction { id, action })
        }
        runtime::Action::StartStep { .. }
        | runtime::Action::StartRecoveryHandler { .. }
        | runtime::Action::CaptureOutputs { .. }
        | runtime::Action::CancelStep { .. }
        | runtime::Action::ForceAbortStep { .. }
        | runtime::Action::FinishRun { .. } => None,
    }
}

fn committed_action<Provisional, Cause, Output, Deadline>(
    requested: &RequestedAction<Provisional, Cause, Output, Deadline>,
) -> Option<CommittedAction> {
    let (kind, step, execution_number, recovery_round) = match &requested.action {
        runtime::Action::StartStep {
            step,
            execution_number,
            ..
        } => (
            CommittedActionKind::StartStep,
            Some(step.clone()),
            Some(*execution_number),
            None,
        ),
        runtime::Action::StartRecoveryHandler { step, round, .. } => (
            CommittedActionKind::StartRecoveryHandler,
            Some(step.clone()),
            None,
            Some(*round),
        ),
        runtime::Action::CaptureOutputs { step, .. } => (
            CommittedActionKind::CaptureOutputs,
            Some(step.clone()),
            None,
            None,
        ),
        // Cleanup is post-commit process housekeeping, not a reconstructable workflow action.
        runtime::Action::ReleaseStepResources { .. } => return None,
        runtime::Action::CancelStep { step, .. } => (
            CommittedActionKind::CancelStep,
            Some(step.clone()),
            None,
            None,
        ),
        runtime::Action::ForceAbortStep { step, .. } => (
            CommittedActionKind::ForceAbortStep,
            Some(step.clone()),
            None,
            None,
        ),
        runtime::Action::FinishRun { .. } => (CommittedActionKind::FinishRun, None, None, None),
    };
    Some(CommittedAction {
        id: requested.id,
        kind,
        step,
        execution_number,
        recovery_round,
    })
}

#[cfg(test)]
mod tests;
