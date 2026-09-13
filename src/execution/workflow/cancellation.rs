use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub(crate) const MINIMUM_CANCELLATION_GRACE: Duration = Duration::from_secs(1);
pub(crate) const MAXIMUM_CANCELLATION_GRACE: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Default)]
pub(crate) struct CancellationFlag {
    cancelled: Arc<AtomicBool>,
}

impl CancellationFlag {
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}
