use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub(crate) struct Cancellation {
    cancelled: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
}

impl Cancellation {
    pub(crate) fn install() -> Result<Self, CancellationError> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(()), Condvar::new()));
        let handler_cancelled = Arc::clone(&cancelled);
        let handler_wake = Arc::clone(&wake);
        ctrlc::set_handler(move || {
            handler_cancelled.store(true, Ordering::SeqCst);
            let _guard = handler_wake
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            handler_wake.1.notify_all();
        })
        .map_err(CancellationError)?;

        Ok(Self { cancelled, wake })
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub(crate) fn wait(&self, duration: Duration) -> bool {
        if self.is_cancelled() {
            return true;
        }
        let guard = self
            .wake
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = self
            .wake
            .1
            .wait_timeout_while(guard, duration, |_| !self.is_cancelled())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.is_cancelled()
    }
}

#[derive(Debug)]
pub(crate) struct CancellationError(ctrlc::Error);

impl fmt::Display for CancellationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "install interrupt handler: {}", self.0)
    }
}
