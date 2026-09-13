use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Arc, Barrier};
use std::time::Duration;

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

pub(super) async fn run_with_stalled_child_guard(
    test: &str,
    worker_pid_variable: &str,
) -> ExitStatus {
    let current_executable = std::env::current_exe().unwrap();
    let isolated = tempfile::tempdir_in(current_executable.parent().unwrap()).unwrap();
    let dependency_directory = isolated.path().join("deps");
    fs::create_dir(&dependency_directory).unwrap();
    let fixture_executable = dependency_directory.join(current_executable.file_name().unwrap());
    fs::hard_link(&current_executable, &fixture_executable).unwrap();
    let pid_path = isolated.path().join("stalled-worker.pid");
    let guard_executable = isolated
        .path()
        .join(format!("scherzo-cloud{}", std::env::consts::EXE_SUFFIX));
    fs::write(
        &guard_executable,
        "#!/bin/sh\nroot=${0%/*}\nprintf '%s\\n' \"$$\" > \"$root/stalled-worker.pid\"\nIFS= read -r _\n",
    )
    .unwrap();
    fs::set_permissions(&guard_executable, fs::Permissions::from_mode(0o755)).unwrap();

    tokio::process::Command::new(fixture_executable)
        .args(["--exact", test, "--ignored"])
        .env(worker_pid_variable, pid_path)
        .kill_on_drop(true)
        .status()
        .await
        .unwrap()
}

pub(super) async fn wait_for_stalled_child_guard(worker_pid_variable: &str) -> i32 {
    let pid_path = std::path::PathBuf::from(std::env::var_os(worker_pid_variable).unwrap());
    tokio::task::spawn_blocking(move || {
        let started = crate::timing::monotonic_now();
        loop {
            if let Ok(pid) = fs::read_to_string(&pid_path)
                && let Ok(pid) = pid.trim().parse::<i32>()
            {
                return pid;
            }
            assert!(crate::timing::elapsed(started) < Duration::from_secs(5));
            crate::timing::sleep(Duration::from_millis(10));
        }
    })
    .await
    .unwrap()
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
