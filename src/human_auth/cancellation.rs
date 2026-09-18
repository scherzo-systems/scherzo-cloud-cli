use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct Cancellation {
    state: Arc<CancellationState>,
}

const CANCELLATION_ACTIVE: u8 = 0;
const CANCELLATION_CANCELLED: u8 = 1;
const CANCELLATION_BOUNDED_COMPLETION: u8 = 2;

struct CancellationState {
    ownership: AtomicU8,
    wake: Mutex<u64>,
    changed: Condvar,
}

impl Cancellation {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                ownership: AtomicU8::new(CANCELLATION_ACTIVE),
                wake: Mutex::new(0),
                changed: Condvar::new(),
            }),
        }
    }

    pub(crate) fn cancel(&self) {
        let mut generation = self
            .state
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .state
            .ownership
            .compare_exchange(
                CANCELLATION_ACTIVE,
                CANCELLATION_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            *generation = generation.wrapping_add(1);
            self.state.changed.notify_all();
        }
        drop(generation);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state.ownership.load(Ordering::Acquire) == CANCELLATION_CANCELLED
    }

    /// Claims authorization to dispatch an operation whose bounded completion must survive a
    /// later signal. Cancellation and this claim are one atomic ordering boundary; the actual
    /// network send remains outside that boundary.
    pub(crate) fn claim_bounded_completion(&self) -> bool {
        self.state
            .ownership
            .compare_exchange(
                CANCELLATION_ACTIVE,
                CANCELLATION_BOUNDED_COMPLETION,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn change_token(&self) -> u64 {
        *self
            .state
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn notify_change(&self) {
        let mut generation = self
            .state
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *generation = generation.wrapping_add(1);
        self.state.changed.notify_all();
    }

    pub(crate) fn wait_for_change(&self, observed: u64) {
        let generation = self
            .state
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _generation = self
            .state
            .changed
            .wait_while(generation, |generation| {
                *generation == observed && !self.is_cancelled()
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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

#[cfg(test)]
mod tests {
    use super::Cancellation;

    #[test]
    fn cancellation_that_wins_prevents_bounded_completion_ownership() {
        let cancellation = Cancellation::new();

        cancellation.cancel();

        assert!(cancellation.is_cancelled());
        assert!(!cancellation.claim_bounded_completion());
    }

    #[test]
    fn bounded_completion_ownership_rejects_later_cancellation() {
        let cancellation = Cancellation::new();

        assert!(cancellation.claim_bounded_completion());
        cancellation.cancel();

        assert!(!cancellation.is_cancelled());
        assert!(!cancellation.claim_bounded_completion());
    }
}
