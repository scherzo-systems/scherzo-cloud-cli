use std::fmt;
use std::sync::{Arc, Barrier};

#[derive(Clone)]
pub(super) struct SynchronousGate {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl SynchronousGate {
    pub(super) fn new() -> Self {
        Self {
            reached: Arc::new(Barrier::new(2)),
            resume: Arc::new(Barrier::new(2)),
        }
    }

    pub(super) fn wait_until_reached(&self) {
        self.reached.wait();
    }

    pub(super) fn resume(&self) {
        self.resume.wait();
    }

    pub(super) fn block_until_resumed(&self) {
        self.reached.wait();
        self.resume.wait();
    }
}

impl fmt::Debug for SynchronousGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SynchronousGate")
            .finish_non_exhaustive()
    }
}
