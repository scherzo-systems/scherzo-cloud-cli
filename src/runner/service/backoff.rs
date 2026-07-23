use std::time::Duration;

const INITIAL_CAP: Duration = Duration::from_secs(1);
const MAXIMUM_CAP: Duration = Duration::from_secs(30);

// Backoff implements bounded exponential backoff with full jitter: every delay
// is drawn uniformly at random from zero through the current cap, and the cap
// doubles from one second to a thirty-second maximum.
pub(crate) struct Backoff {
    cap: Duration,
}

impl Backoff {
    pub(crate) fn new() -> Self {
        Self { cap: INITIAL_CAP }
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        self.next_delay_from(random_unit())
    }

    // `unit` selects a point in the closed range from zero through the current
    // cap; tests inject deterministic values.
    fn next_delay_from(&mut self, unit: f64) -> Duration {
        let cap = self.cap;
        self.cap = self.cap.saturating_mul(2).min(MAXIMUM_CAP);
        cap.mul_f64(unit.clamp(0.0, 1.0))
    }

    pub(crate) fn reset(&mut self) {
        self.cap = INITIAL_CAP;
    }
}

// random_unit returns a uniformly distributed value in [0, 1) from operating
// system entropy. Jitter tolerates a fixed midpoint if entropy is unavailable.
fn random_unit() -> f64 {
    let mut bytes = [0_u8; 8];
    if getrandom::fill(&mut bytes).is_err() {
        return 0.5;
    }
    (u64::from_le_bytes(bytes) >> 11) as f64 / (1_u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Backoff, random_unit};

    #[test]
    fn doubles_the_jitter_cap_until_the_bounded_maximum() {
        let mut backoff = Backoff::new();
        for expected_cap in [1, 2, 4, 8, 16, 30, 30] {
            assert_eq!(
                backoff.next_delay_from(1.0),
                Duration::from_secs(expected_cap)
            );
        }
    }

    #[test]
    fn applies_full_jitter_within_the_current_cap() {
        let mut backoff = Backoff::new();
        assert_eq!(backoff.next_delay_from(0.0), Duration::ZERO);
        assert_eq!(backoff.next_delay_from(0.5), Duration::from_secs(1));
        assert_eq!(backoff.next_delay_from(0.25), Duration::from_secs(1));
        assert_eq!(backoff.next_delay_from(2.0), Duration::from_secs(8));
    }

    #[test]
    fn reset_returns_the_cap_to_one_second() {
        let mut backoff = Backoff::new();
        for _ in 0..10 {
            let _ = backoff.next_delay();
        }
        backoff.reset();
        assert_eq!(backoff.next_delay_from(1.0), Duration::from_secs(1));
    }

    #[test]
    fn entropy_units_stay_within_the_half_open_unit_interval() {
        for _ in 0..100 {
            let unit = random_unit();
            assert!((0.0..1.0).contains(&unit));
        }
    }
}
