use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct Cancellation {
    state: Arc<CancellationState>,
}

struct CancellationState {
    cancelled: AtomicBool,
    wake: Mutex<()>,
    changed: Condvar,
}

impl Cancellation {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                wake: Mutex::new(()),
                changed: Condvar::new(),
            }),
        }
    }

    pub(crate) fn cancel(&self) {
        let guard = self
            .state
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.state.cancelled.store(true, Ordering::Release);
        self.state.changed.notify_all();
        drop(guard);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn wait(&self, duration: Duration) -> bool {
        if self.is_cancelled() {
            return true;
        }
        let guard = self
            .state
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = self
            .state
            .changed
            .wait_timeout_while(guard, duration, |_| !self.is_cancelled())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.is_cancelled()
    }
}
