use std::fmt;
use std::fs;
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};

pub(super) fn write_process_fixture_signal(variable: &str, value: &[u8]) {
    fs::write(std::env::var_os(variable).unwrap(), value).unwrap();
}

pub(super) fn write_process_fixture_id(variable: &str) {
    let process = format!("{}\n", std::process::id());
    write_process_fixture_signal(variable, process.as_bytes());
}

pub(super) fn process_fixture_interrupt_receiver() -> std::sync::mpsc::Receiver<()> {
    let (interrupt, interrupted) = std::sync::mpsc::sync_channel(1);
    process_fixture_interrupt_handler(move || {
        let _ = interrupt.try_send(());
    });
    interrupted
}

pub(super) fn process_fixture_interrupt_handler(handler: impl FnOnce() + Send + 'static) {
    let (ready, registered) = std::sync::mpsc::sync_channel(0);
    let _ = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let mut interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
            ready.send(()).unwrap();
            let _ = interrupt.recv().await;
            handler();
        });
    });
    registered.recv().unwrap();
}

pub(super) fn process_fixture_output(descriptor: u8) -> fs::File {
    fs::OpenOptions::new()
        .write(true)
        .open(format!("/dev/fd/{descriptor}"))
        .unwrap()
}

pub(super) fn spawn_process_fixture(test: &str) -> std::thread::JoinHandle<()> {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--ignored", "--test-threads=1"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    std::thread::spawn(move || {
        let _ = child.wait();
    })
}

#[derive(Clone)]
pub(super) struct SynchronousGate {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl SynchronousGate {
    pub(super) fn new() -> Self {
        Self {
            reached: Arc::new(Barrier::new(2)),
            resume: Arc::new(Barrier::new(2)),
        }
    }

    pub(super) fn wait_until_reached(&self) {
        self.reached.wait();
    }

    pub(super) fn resume(&self) {
        self.resume.wait();
    }

    pub(super) fn block_until_resumed(&self) {
        self.reached.wait();
        self.resume.wait();
    }
}

impl fmt::Debug for SynchronousGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SynchronousGate")
            .finish_non_exhaustive()
    }
}
