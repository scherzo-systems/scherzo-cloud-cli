pub(crate) fn timing_dependent_production_synchronization() {
    std::thread::yield_now();
    std::thread::park_timeout(std::time::Duration::from_millis(1));
}
