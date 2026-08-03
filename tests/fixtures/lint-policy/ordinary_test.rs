fn direct_elapsed_time_read(started_at: std::time::Instant) -> std::time::Duration {
    started_at.elapsed()
}

#[test]
fn direct_elapsed_time_read_is_disallowed_in_tests() {
    let _ = direct_elapsed_time_read as fn(std::time::Instant) -> std::time::Duration;
}
