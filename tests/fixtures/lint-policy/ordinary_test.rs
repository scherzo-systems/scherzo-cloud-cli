fn direct_elapsed_time_read(started_at: std::time::Instant) -> std::time::Duration {
    started_at.elapsed()
}

fn direct_timeout_delay() {
    let (_sender, receiver) = std::sync::mpsc::channel::<()>();
    let _ = receiver.recv_timeout(std::time::Duration::from_millis(1));
}

#[test]
fn direct_timing_primitives_are_disallowed_in_tests() {
    let _ = direct_elapsed_time_read as fn(std::time::Instant) -> std::time::Duration;
    let _ = direct_timeout_delay as fn();
}
