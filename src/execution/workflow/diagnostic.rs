use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, MutexGuard};

use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;

use super::observation::{
    CommandOutputClosedObservation, CommandOutputObservation, CommandOutputSource,
    ExecutionObservation, ExecutionObserver, SourceSequence,
};
use super::runtime::ActionId;

const READ_BUFFER_BYTES: usize = 8 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessSpawnDiagnostic {
    schema_version: u8,
    stage: &'static str,
    error_category: String,
    raw_os_error: Option<i32>,
    message: String,
}

impl ProcessSpawnDiagnostic {
    fn from_error(error: &io::Error) -> Self {
        Self {
            schema_version: 1,
            stage: "process_spawn",
            error_category: format!("{:?}", error.kind()),
            raw_os_error: error.raw_os_error(),
            message: error.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DiagnosticTruncation {
    discarded_bytes: u64,
}

impl DiagnosticTruncation {
    pub(crate) fn discarded_bytes(self) -> u64 {
        self.discarded_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedDiagnosticStream {
    bytes: Arc<[u8]>,
    truncation: Option<DiagnosticTruncation>,
    fully_drained: bool,
}

impl CapturedDiagnosticStream {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn truncation(&self) -> Option<DiagnosticTruncation> {
        self.truncation
    }

    pub(crate) fn fully_drained(&self) -> bool {
        self.fully_drained
    }

    fn reader_unavailable() -> Self {
        Self {
            bytes: Arc::from([]),
            truncation: None,
            fully_drained: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
        bytes: impl Into<Arc<[u8]>>,
        discarded_bytes: u64,
        fully_drained: bool,
    ) -> Self {
        Self {
            bytes: bytes.into(),
            truncation: (discarded_bytes != 0).then_some(DiagnosticTruncation { discarded_bytes }),
            fully_drained,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepDiagnostic {
    standard_output: CapturedDiagnosticStream,
    standard_error: CapturedDiagnosticStream,
}

impl StepDiagnostic {
    #[cfg(test)]
    pub(crate) fn from_streams(
        standard_output: CapturedDiagnosticStream,
        standard_error: CapturedDiagnosticStream,
    ) -> Self {
        Self {
            standard_output,
            standard_error,
        }
    }

    pub(crate) fn standard_output(&self) -> &CapturedDiagnosticStream {
        &self.standard_output
    }

    pub(crate) fn standard_error(&self) -> &CapturedDiagnosticStream {
        &self.standard_error
    }
}

#[derive(Clone, Default)]
pub(crate) struct StepDiagnosticLog {
    entries: Arc<Mutex<BTreeMap<(String, ActionId), StepDiagnostic>>>,
    recovery_handlers: Arc<Mutex<BTreeSet<(String, ActionId)>>>,
}

impl StepDiagnosticLog {
    pub(crate) fn get(&self, step: &str) -> Option<StepDiagnostic> {
        let recovery_handlers = lock_recovery_handlers(&self.recovery_handlers).clone();
        lock_entries(&self.entries).iter().rev().find_map(
            |((entry_step, invocation), diagnostic)| {
                (entry_step == step
                    && !recovery_handlers.contains(&(entry_step.clone(), *invocation)))
                .then(|| diagnostic.clone())
            },
        )
    }

    pub(crate) fn mark_recovery_handler(&self, step: &str, invocation: ActionId) {
        lock_recovery_handlers(&self.recovery_handlers).insert((step.to_owned(), invocation));
    }

    pub(crate) fn is_recovery_handler(&self, step: &str, invocation: ActionId) -> bool {
        lock_recovery_handlers(&self.recovery_handlers).contains(&(step.to_owned(), invocation))
    }

    pub(crate) fn get_invocation(
        &self,
        step: &str,
        invocation: ActionId,
    ) -> Option<StepDiagnostic> {
        lock_entries(&self.entries)
            .get(&(step.to_owned(), invocation))
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn invocation_ids(&self, step: &str) -> Vec<ActionId> {
        lock_entries(&self.entries)
            .keys()
            .filter_map(|(entry_step, invocation)| (entry_step == step).then_some(*invocation))
            .collect()
    }

    pub(super) fn start_capture<Deadline, Observer, StandardOutput, StandardError>(
        &self,
        step: String,
        invocation: ActionId,
        maximum_stream_bytes: NonZeroU64,
        standard_output: StandardOutput,
        standard_error: StandardError,
        observer: Observer,
    ) -> PendingStepDiagnostic
    where
        Deadline: Send + 'static,
        Observer: ExecutionObserver<Deadline>,
        StandardOutput: AsyncRead + Unpin + Send + 'static,
        StandardError: AsyncRead + Unpin + Send + 'static,
    {
        PendingStepDiagnostic {
            log: self.clone(),
            step: step.clone(),
            invocation,
            standard_output: tokio::spawn(drain_stream(
                standard_output,
                maximum_stream_bytes,
                step.clone(),
                invocation,
                CommandOutputSource::StandardOutput,
                observer.clone(),
            )),
            standard_error: tokio::spawn(drain_stream(
                standard_error,
                maximum_stream_bytes,
                step,
                invocation,
                CommandOutputSource::StandardError,
                observer,
            )),
        }
    }

    pub(super) fn start_standard_error_capture<Deadline, Observer, StandardError>(
        &self,
        step: String,
        invocation: ActionId,
        maximum_stream_bytes: NonZeroU64,
        standard_error: StandardError,
        observer: Observer,
    ) -> PendingStepDiagnostic
    where
        Deadline: Send + 'static,
        Observer: ExecutionObserver<Deadline>,
        StandardError: AsyncRead + Unpin + Send + 'static,
    {
        self.start_capture::<Deadline, _, _, _>(
            step,
            invocation,
            maximum_stream_bytes,
            tokio::io::empty(),
            standard_error,
            observer,
        )
    }

    pub(super) fn record_process_spawn_failure(
        &self,
        step: String,
        invocation: ActionId,
        maximum_stream_bytes: NonZeroU64,
        error: &io::Error,
    ) -> Result<(), serde_json::Error> {
        let mut bytes = serde_json::to_vec(&ProcessSpawnDiagnostic::from_error(error))?;
        bytes.push(b'\n');
        let standard_output = DiagnosticStreamCapture::new(maximum_stream_bytes).finish(true);
        let mut standard_error = DiagnosticStreamCapture::new(maximum_stream_bytes);
        standard_error.capture(&bytes);
        self.record(
            step,
            invocation,
            standard_output,
            standard_error.finish(true),
        );
        Ok(())
    }

    pub(super) fn record(
        &self,
        step: String,
        invocation: ActionId,
        standard_output: CapturedDiagnosticStream,
        standard_error: CapturedDiagnosticStream,
    ) {
        lock_entries(&self.entries).insert(
            (step, invocation),
            StepDiagnostic {
                standard_output,
                standard_error,
            },
        );
    }
}

pub(super) struct DiagnosticStreamCapture {
    maximum_bytes: NonZeroU64,
    retained: Vec<u8>,
    discarded_bytes: u64,
}

impl DiagnosticStreamCapture {
    pub(super) fn new(maximum_bytes: NonZeroU64) -> Self {
        Self {
            maximum_bytes,
            retained: Vec::new(),
            discarded_bytes: 0,
        }
    }

    pub(super) fn capture(&mut self, bytes: &[u8]) {
        let retained_bytes = u64::try_from(self.retained.len()).unwrap_or(u64::MAX);
        let admitted = self.maximum_bytes.get().saturating_sub(retained_bytes);
        let admitted = usize::try_from(admitted)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        self.retained.extend_from_slice(&bytes[..admitted]);
        self.discarded_bytes = self.discarded_bytes.saturating_add(
            u64::try_from(bytes.len().saturating_sub(admitted)).unwrap_or(u64::MAX),
        );
    }

    pub(super) fn finish(self, fully_drained: bool) -> CapturedDiagnosticStream {
        CapturedDiagnosticStream {
            bytes: Arc::from(self.retained),
            truncation: (self.discarded_bytes > 0).then_some(DiagnosticTruncation {
                discarded_bytes: self.discarded_bytes,
            }),
            fully_drained,
        }
    }
}

pub(super) struct PendingStepDiagnostic {
    log: StepDiagnosticLog,
    step: String,
    invocation: ActionId,
    standard_output: JoinHandle<CapturedDiagnosticStream>,
    standard_error: JoinHandle<CapturedDiagnosticStream>,
}

impl PendingStepDiagnostic {
    pub(super) fn abort(&self) {
        self.standard_output.abort();
        self.standard_error.abort();
    }

    pub(super) async fn finish(self) {
        let (standard_output, standard_error) =
            tokio::join!(self.standard_output, self.standard_error);
        self.log.record(
            self.step,
            self.invocation,
            standard_output.unwrap_or_else(|_| CapturedDiagnosticStream::reader_unavailable()),
            standard_error.unwrap_or_else(|_| CapturedDiagnosticStream::reader_unavailable()),
        );
    }
}

async fn drain_stream<Deadline, Observer>(
    mut reader: impl AsyncRead + Unpin,
    maximum_bytes: NonZeroU64,
    step: String,
    invocation: ActionId,
    source: CommandOutputSource,
    observer: Observer,
) -> CapturedDiagnosticStream
where
    Deadline: Send + 'static,
    Observer: ExecutionObserver<Deadline>,
{
    let mut capture = DiagnosticStreamCapture::new(maximum_bytes);
    let mut fully_drained = false;
    let mut sequence = SourceSequence::first();
    let mut buffer = [0_u8; READ_BUFFER_BYTES];

    loop {
        let read = match reader.read(&mut buffer).await {
            Ok(0) => {
                fully_drained = true;
                break;
            }
            Ok(read) => read,
            Err(failure) if failure.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        let bytes = Arc::<[u8]>::from(&buffer[..read]);
        capture.capture(&bytes);
        observer
            .observe(ExecutionObservation::CommandOutput(
                CommandOutputObservation {
                    step: step.clone(),
                    invocation,
                    source,
                    sequence,
                    bytes,
                },
            ))
            .await;
        sequence = sequence.next();
    }

    observer
        .observe(ExecutionObservation::CommandOutputClosed(
            CommandOutputClosedObservation {
                step,
                invocation,
                source,
                sequence,
            },
        ))
        .await;
    capture.finish(fully_drained)
}

fn lock_entries(
    entries: &Mutex<BTreeMap<(String, ActionId), StepDiagnostic>>,
) -> MutexGuard<'_, BTreeMap<(String, ActionId), StepDiagnostic>> {
    match entries.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn lock_recovery_handlers(
    entries: &Mutex<BTreeSet<(String, ActionId)>>,
) -> MutexGuard<'_, BTreeSet<(String, ActionId)>> {
    match entries.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests;
