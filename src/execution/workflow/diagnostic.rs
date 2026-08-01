use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;

const READ_BUFFER_BYTES: usize = 8 * 1024;

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepDiagnostic {
    standard_output: CapturedDiagnosticStream,
    standard_error: CapturedDiagnosticStream,
}

impl StepDiagnostic {
    pub(crate) fn standard_output(&self) -> &CapturedDiagnosticStream {
        &self.standard_output
    }

    pub(crate) fn standard_error(&self) -> &CapturedDiagnosticStream {
        &self.standard_error
    }
}

#[derive(Clone, Default)]
pub(crate) struct StepDiagnosticLog {
    entries: Arc<Mutex<BTreeMap<String, StepDiagnostic>>>,
}

impl StepDiagnosticLog {
    pub(crate) fn get(&self, step: &str) -> Option<StepDiagnostic> {
        lock_entries(&self.entries).get(step).cloned()
    }

    pub(super) fn start_capture<StandardOutput, StandardError>(
        &self,
        step: String,
        maximum_stream_bytes: NonZeroU64,
        standard_output: StandardOutput,
        standard_error: StandardError,
    ) -> PendingStepDiagnostic
    where
        StandardOutput: AsyncRead + Unpin + Send + 'static,
        StandardError: AsyncRead + Unpin + Send + 'static,
    {
        PendingStepDiagnostic {
            log: self.clone(),
            step,
            standard_output: tokio::spawn(drain_stream(standard_output, maximum_stream_bytes)),
            standard_error: tokio::spawn(drain_stream(standard_error, maximum_stream_bytes)),
        }
    }

    pub(super) fn record(
        &self,
        step: String,
        standard_output: CapturedDiagnosticStream,
        standard_error: CapturedDiagnosticStream,
    ) {
        lock_entries(&self.entries).insert(
            step,
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
    standard_output: JoinHandle<CapturedDiagnosticStream>,
    standard_error: JoinHandle<CapturedDiagnosticStream>,
}

impl PendingStepDiagnostic {
    pub(super) async fn finish(self) {
        let (standard_output, standard_error) =
            tokio::join!(self.standard_output, self.standard_error);
        self.log.record(
            self.step,
            standard_output.unwrap_or_else(|_| CapturedDiagnosticStream::reader_unavailable()),
            standard_error.unwrap_or_else(|_| CapturedDiagnosticStream::reader_unavailable()),
        );
    }
}

async fn drain_stream(
    mut reader: impl AsyncRead + Unpin,
    maximum_bytes: NonZeroU64,
) -> CapturedDiagnosticStream {
    let mut capture = DiagnosticStreamCapture::new(maximum_bytes);
    let mut fully_drained = false;
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
        capture.capture(&buffer[..read]);
    }

    capture.finish(fully_drained)
}

fn lock_entries(
    entries: &Mutex<BTreeMap<String, StepDiagnostic>>,
) -> MutexGuard<'_, BTreeMap<String, StepDiagnostic>> {
    match entries.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests;
