pub(crate) fn direct_elapsed_time_read(started_at: std::time::Instant) -> std::time::Duration {
    started_at.elapsed()
}
