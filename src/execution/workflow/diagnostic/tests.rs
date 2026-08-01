use std::io;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use super::*;

struct ChunkedReader {
    bytes: Arc<[u8]>,
    maximum_chunks: Arc<[usize]>,
    position: usize,
    next_chunk: usize,
    observed_eof: Arc<AtomicBool>,
}

impl ChunkedReader {
    fn new(
        bytes: impl Into<Arc<[u8]>>,
        maximum_chunks: impl Into<Arc<[usize]>>,
    ) -> (Self, Arc<AtomicBool>) {
        let observed_eof = Arc::new(AtomicBool::new(false));
        (
            Self {
                bytes: bytes.into(),
                maximum_chunks: maximum_chunks.into(),
                position: 0,
                next_chunk: 0,
                observed_eof: Arc::clone(&observed_eof),
            },
            observed_eof,
        )
    }
}

impl AsyncRead for ChunkedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position == self.bytes.len() {
            self.observed_eof.store(true, Ordering::SeqCst);
            return Poll::Ready(Ok(()));
        }

        let maximum_chunk = self.maximum_chunks[self.next_chunk % self.maximum_chunks.len()];
        self.next_chunk += 1;
        let read = maximum_chunk
            .min(buffer.remaining())
            .min(self.bytes.len() - self.position);
        let end = self.position + read;
        buffer.put_slice(&self.bytes[self.position..end]);
        self.position = end;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn nontruncated_streams_preserve_raw_bytes_independently() {
    let standard_output = Arc::<[u8]>::from([0, 0xff, b'\n', 1]);
    let standard_error = Arc::<[u8]>::from([b'e', 0x80, b'r']);
    let (stdout_reader, stdout_eof) = ChunkedReader::new(standard_output.clone(), [2, 1]);
    let (stderr_reader, stderr_eof) = ChunkedReader::new(standard_error.clone(), [1]);
    let log = StepDiagnosticLog::default();

    log.start_capture(
        "step".to_owned(),
        NonZeroU64::new(16).unwrap(),
        stdout_reader,
        stderr_reader,
    )
    .finish()
    .await;

    let captured = log.get("step").unwrap();
    assert_eq!(captured.standard_output().bytes(), standard_output.as_ref());
    assert_eq!(captured.standard_error().bytes(), standard_error.as_ref());
    assert_eq!(captured.standard_output().truncation(), None);
    assert_eq!(captured.standard_error().truncation(), None);
    assert!(captured.standard_output().fully_drained());
    assert!(captured.standard_error().fully_drained());
    assert!(stdout_eof.load(Ordering::SeqCst));
    assert!(stderr_eof.load(Ordering::SeqCst));
}

#[tokio::test]
async fn truncation_is_independent_chunk_invariant_and_drains_to_eof() {
    let stdout = Arc::<[u8]>::from(*b"abcdefghijk");
    let stderr = Arc::<[u8]>::from(*b"12345678");
    let (single_read, single_stdout_eof, single_stderr_eof) =
        capture_with_chunks(stdout.clone(), stderr.clone(), [64], [64]).await;
    let (fragmented, fragmented_stdout_eof, fragmented_stderr_eof) =
        capture_with_chunks(stdout, stderr, [1, 4, 2], [3, 1]).await;

    assert_eq!(single_read, fragmented);
    assert_eq!(single_read.standard_output().bytes(), b"abcde");
    assert_eq!(single_read.standard_error().bytes(), b"12345");
    assert_eq!(
        single_read
            .standard_output()
            .truncation()
            .map(DiagnosticTruncation::discarded_bytes),
        Some(6)
    );
    assert_eq!(
        single_read
            .standard_error()
            .truncation()
            .map(DiagnosticTruncation::discarded_bytes),
        Some(3)
    );
    for observed_eof in [
        single_stdout_eof,
        single_stderr_eof,
        fragmented_stdout_eof,
        fragmented_stderr_eof,
    ] {
        assert!(observed_eof.load(Ordering::SeqCst));
    }
}

async fn capture_with_chunks(
    stdout: Arc<[u8]>,
    stderr: Arc<[u8]>,
    stdout_chunks: impl Into<Arc<[usize]>>,
    stderr_chunks: impl Into<Arc<[usize]>>,
) -> (StepDiagnostic, Arc<AtomicBool>, Arc<AtomicBool>) {
    let (stdout, stdout_eof) = ChunkedReader::new(stdout, stdout_chunks);
    let (stderr, stderr_eof) = ChunkedReader::new(stderr, stderr_chunks);
    let log = StepDiagnosticLog::default();
    log.start_capture(
        "step".to_owned(),
        NonZeroU64::new(5).unwrap(),
        stdout,
        stderr,
    )
    .finish()
    .await;
    (log.get("step").unwrap(), stdout_eof, stderr_eof)
}
