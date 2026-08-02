use std::thread;
use std::time::{Duration, Instant};

use time::OffsetDateTime;

#[expect(
    clippy::disallowed_methods,
    reason = "this module is the production boundary for monotonic clock reads"
)]
pub(crate) fn monotonic_now() -> Instant {
    Instant::now()
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
