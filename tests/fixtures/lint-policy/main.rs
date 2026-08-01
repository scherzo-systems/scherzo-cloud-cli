#![deny(clippy::disallowed_methods)]

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

fn timing_dependent_thread_synchronization() {
    std::thread::yield_now();
    std::thread::park_timeout(std::time::Duration::from_millis(1));
}

async fn timing_dependent_task_synchronization() {
    tokio::task::yield_now().await;
}

fn main() {
    let _ = panic_shortcut as fn(Option<u8>) -> u8;
    let _ = lossy_cast as fn(u64) -> u8;
    let _ = direct_compile_time_environment_read();
}
