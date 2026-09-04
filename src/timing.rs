use std::thread;
use std::time::{Duration, Instant};

use time::OffsetDateTime;

const MINIMUM_RETRY_DELAY_MILLISECONDS: u64 = 50;
const RETRY_JITTER_MILLISECONDS: u64 = 100;

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for monotonic clock reads"
)]
pub(crate) fn monotonic_now() -> Instant {
    Instant::now()
}

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for monotonic elapsed-time reads"
)]
pub(crate) fn elapsed(started_at: Instant) -> Duration {
    started_at.elapsed()
}

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for UTC wall-clock reads"
)]
pub(crate) fn utc_now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for blocking retry waits"
)]
pub(crate) fn sleep(duration: Duration) {
    thread::sleep(duration);
}

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for asynchronous deadline waits"
)]
pub(crate) async fn async_sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

pub(crate) fn short_retry_delay() -> Duration {
    let mut bytes = [0_u8; 8];
    let random = if getrandom::fill(&mut bytes).is_ok() {
        u64::from_le_bytes(bytes)
    } else {
        RETRY_JITTER_MILLISECONDS / 2
    };
    retry_delay_from(random)
}

fn retry_delay_from(random: u64) -> Duration {
    Duration::from_millis(
        MINIMUM_RETRY_DELAY_MILLISECONDS + random % RETRY_JITTER_MILLISECONDS.saturating_add(1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_has_a_nonzero_bounded_jitter_window() {
        assert_eq!(retry_delay_from(0), Duration::from_millis(50));
        assert_eq!(retry_delay_from(50), Duration::from_millis(100));
        assert_eq!(retry_delay_from(100), Duration::from_millis(150));
        assert_eq!(retry_delay_from(101), Duration::from_millis(50));
    }
}
