pub(crate) fn direct_elapsed_time_read(started_at: std::time::Instant) -> std::time::Duration {
    started_at.elapsed()
}

pub(crate) fn direct_operational_error_rendering() {
    eprintln!("Error: bypassed the shared renderer");
}

pub(crate) fn panic_only_invariants(value: bool) {
    assert!(value);
    if !value {
        unreachable!();
    }
}
