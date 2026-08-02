#[allow(
    clippy::disallowed_methods,
    reason = "the policy checker must reject timing suppression on an ordinary module"
)]
mod broadly_suppressed;
#[expect(
    clippy::disallowed_methods,
    reason = "the policy checker must also reject timing expectations on ordinary modules"
)]
mod broadly_expected {
    pub(super) fn timing_dependent_synchronization() {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
mod ordinary_production;

#[cfg(test)]
mod ordinary_test;

#[allow(dead_code)]
fn reasonless_suppression() {}

fn ordinary_warning() {}

fn panic_shortcut(value: Option<u8>) -> u8 {
    value.unwrap()
}

fn lossy_cast(value: u64) -> u8 {
    value as u8
}

fn direct_compile_time_environment_read() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

fn main() {
    let _ = broadly_suppressed::timing_dependent_synchronization as fn();
    let _ = broadly_expected::timing_dependent_synchronization as fn();
    let _ = panic_shortcut as fn(Option<u8>) -> u8;
    let _ = lossy_cast as fn(u64) -> u8;
    let _ = direct_compile_time_environment_read();
}
