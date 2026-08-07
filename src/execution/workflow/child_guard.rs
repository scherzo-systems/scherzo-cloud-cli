use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{ExitCode, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rustix::io::Errno;
#[cfg(target_os = "linux")]
use rustix::process::waitpgid;
use rustix::process::{
    Pid, Signal, WaitId, WaitIdOptions, WaitOptions, getpid, getppid, kill_process,
    kill_process_group, waitid, waitpid,
};
use serde::{Deserialize, Serialize};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};

#[cfg(any(target_vendor = "apple", test))]
use super::process_group::process_group_is_quiescent;
use super::process_group::{
    AuthenticatedProcessGroup, AuthenticatedSignalResult, LeaderState, ProcessIdentityInspector,
    ProcessIdentityObservation, SystemProcessIdentityInspector, capture_process_group_identity,
    continue_authenticated_process_group, system_process_identity_observation,
    terminate_authenticated_process_group, terminate_authenticated_process_group_with,
};

const INTERNAL_WORKER_ENVIRONMENT: &str = "SCHERZO_INTERNAL_CHILD_GUARD_WORKER";
const INTERNAL_ROOT_ENVIRONMENT: &str = "SCHERZO_INTERNAL_CHILD_GUARD_ROOT";
const INTERNAL_PARENT_ENVIRONMENT: &str = "SCHERZO_INTERNAL_CHILD_GUARD_PARENT";
const GUARD_WORKER: &str = "guard-v1";
const LEADER_WORKER: &str = "leader-v1";
const CONTINUE: u8 = b'C';
const TERMINATE: u8 = b'K';
const MANIFEST_FILE: &str = "launch.json";
const READY_FILE: &str = "ready.json";
const RELEASED_FILE: &str = "released";
const QUIESCED_FILE: &str = "quiesced";
const EXEC_BOUNDARY_SOCKET: &str = "exec.sock";
const EXEC_FAILURE_FILE: &str = "exec.failure";
const STATUS_FILE: &str = "status";
const WORKER_FAILURE_FILE: &str = "worker.failure";
const ACTIVITY_LOCK_FILE: &str = ".activity.lock";
const TEMPORARY_DIRECTORY_PREFIX: &str = "scherzo-child-guard-v1-";
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(5);
const WORKER_BOUNDARY_TIMEOUT: Duration = Duration::from_secs(10);
const MAXIMUM_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LaunchManifest {
    program: Vec<u8>,
    arguments: Vec<Vec<u8>>,
    environment: Vec<(Vec<u8>, Vec<u8>)>,
}

impl LaunchManifest {
    fn new(program: &Path, arguments: &[OsString], environment: &[(OsString, OsString)]) -> Self {
        Self {
            program: program.as_os_str().as_bytes().to_vec(),
            arguments: arguments
                .iter()
                .map(|argument| argument.as_bytes().to_vec())
                .collect(),
            environment: environment
                .iter()
                .map(|(name, value)| (name.as_bytes().to_vec(), value.as_bytes().to_vec()))
                .collect(),
        }
    }

    fn program(&self) -> OsString {
        OsString::from_vec(self.program.clone())
    }

    fn arguments(&self) -> impl Iterator<Item = OsString> + '_ {
        self.arguments.iter().cloned().map(OsString::from_vec)
    }

    fn environment(&self) -> impl Iterator<Item = (OsString, OsString)> + '_ {
        self.environment
            .iter()
            .cloned()
            .map(|(name, value)| (OsString::from_vec(name), OsString::from_vec(value)))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReadyIdentity {
    process_group_id: i32,
    leader_start_identity: String,
}

pub(crate) struct StoppedChildGuard {
    child: Child,
    identity: AuthenticatedProcessGroup,
    owner_control: Option<File>,
    staging: tempfile::TempDir,
    _activity_lease: File,
}

impl StoppedChildGuard {
    pub(crate) fn spawn(
        program: &Path,
        arguments: &[OsString],
        environment: &[(OsString, OsString)],
        configure: impl FnOnce(&mut std::process::Command) -> io::Result<()>,
    ) -> io::Result<Self> {
        if !cfg!(any(target_os = "linux", target_vendor = "apple")) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "authenticated child process guards are unavailable",
            ));
        }
        enable_child_subreaper()?;
        let staging = tempfile::Builder::new()
            .prefix(TEMPORARY_DIRECTORY_PREFIX)
            .tempdir_in("/tmp")?;
        let activity_lease = create_activity_lease(staging.path())?;
        let manifest = LaunchManifest::new(program, arguments, environment);
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(io::Error::other)?;
        fs::write(staging.path().join(MANIFEST_FILE), manifest_bytes)?;

        let executable = std::env::current_exe()?;
        let mut command = Command::new(executable);
        command
            .env_clear()
            .envs(environment.iter().cloned())
            .env(INTERNAL_WORKER_ENVIRONMENT, GUARD_WORKER)
            .env(INTERNAL_ROOT_ENVIRONMENT, staging.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.as_std_mut().process_group(0);
        configure(command.as_std_mut())?;
        let mut child = command.spawn()?;
        let owner_control = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("guard control pipe unavailable"))?
            .into_owned_fd()
            .map(File::from)?;

        let ready = wait_for_json::<ReadyIdentity>(&mut child, &staging.path().join(READY_FILE))
            .map_err(|failure| {
                io::Error::new(
                    failure.kind(),
                    format!(
                        "{failure}; worker={}",
                        fs::read_to_string(staging.path().join(WORKER_FAILURE_FILE))
                            .unwrap_or_else(|_| "unknown".to_owned())
                    ),
                )
            })?;
        let process_group = Pid::from_raw(ready.process_group_id)
            .ok_or_else(|| io::Error::other("invalid guarded process group"))?;
        let identity = AuthenticatedProcessGroup::new(process_group, ready.leader_start_identity)
            .ok_or_else(|| io::Error::other("invalid guarded process identity"))?;
        if !matches!(
            system_process_identity_observation(&identity),
            ProcessIdentityObservation::Exact {
                leader: LeaderState::Stopped
            }
        ) {
            return Err(io::Error::other("guarded process did not remain stopped"));
        }

        Ok(Self {
            child,
            identity,
            owner_control: Some(owner_control),
            staging,
            _activity_lease: activity_lease,
        })
    }

    pub(crate) fn identity(&self) -> &AuthenticatedProcessGroup {
        &self.identity
    }

    pub(crate) fn continue_execution(&mut self) -> io::Result<()> {
        let control = self
            .owner_control
            .as_mut()
            .ok_or_else(|| io::Error::other("guard owner control unavailable"))?;
        control.write_all(&[CONTINUE])?;
        control.flush()?;
        match wait_for_file(&mut self.child, &self.staging.path().join(RELEASED_FILE)) {
            Ok(()) => Ok(()),
            Err(failure) => {
                match fs::read_to_string(self.staging.path().join(EXEC_FAILURE_FILE))
                    .ok()
                    .and_then(|value| value.parse::<i32>().ok())
                {
                    Some(raw_error) if raw_error > 0 => {
                        Err(io::Error::from_raw_os_error(raw_error))
                    }
                    _ => Err(failure),
                }
            }
        }
    }

    pub(crate) fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    pub(crate) fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }

    pub(crate) async fn wait(&mut self) -> io::Result<ExitStatus> {
        let guard_status = self.child.wait().await?;
        if !guard_status.success() {
            return Err(io::Error::other("child process guard failed"));
        }
        require_quiesced_marker(self.staging.path())?;
        let raw_status = fs::read_to_string(self.staging.path().join(STATUS_FILE))?
            .parse::<i32>()
            .map_err(|_| io::Error::other("guarded child status is invalid"))?;
        Ok(ExitStatus::from_raw(raw_status))
    }

    pub(crate) async fn force_stop(&mut self) -> io::Result<()> {
        let termination = terminate_authenticated_process_group(&self.identity);
        if let Some(mut control) = self.owner_control.take() {
            let _ = control.write_all(&[TERMINATE]);
            let _ = control.flush();
        }
        let _ = self.child.wait().await;
        if require_quiesced_marker(self.staging.path()).is_ok() {
            return Ok(());
        }
        cleanup_adopted_group(&self.identity, termination)?;
        write_atomic(&self.staging.path().join(QUIESCED_FILE), b"quiesced\n")
            .map_err(|()| io::Error::other("failed to record guarded group cleanup"))
    }
}

impl Drop for StoppedChildGuard {
    fn drop(&mut self) {
        // EOF is the owner-loss signal. The independent guard remains alive long
        // enough to terminate and reap the stopped or released process group.
        self.owner_control.take();
    }
}

fn create_activity_lease(root: &Path) -> io::Result<File> {
    let lease = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(root.join(ACTIVITY_LOCK_FILE))?;
    fs4::FileExt::lock_shared(&lease)?;
    Ok(lease)
}

pub(crate) async fn force_stop_direct_child(child: &mut Child) -> Result<(), ()> {
    let _ = child.start_kill();
    child.wait().await.map(|_| ()).map_err(|_| ())
}

fn wait_for_json<Document>(child: &mut Child, path: &Path) -> io::Result<Document>
where
    Document: for<'de> Deserialize<'de>,
{
    wait_for_boundary(child, path, |path| match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    })
}

fn wait_for_file(child: &mut Child, path: &Path) -> io::Result<()> {
    wait_for_boundary(child, path, |path| match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(())),
        Ok(_) => Err(io::Error::other("guard boundary is not a file")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    })
}

fn wait_for_boundary<Output>(
    child: &mut Child,
    path: &Path,
    mut inspect: impl FnMut(&Path) -> io::Result<Option<Output>>,
) -> io::Result<Output> {
    let started = crate::timing::monotonic_now();
    loop {
        if let Some(output) = inspect(path)? {
            return Ok(output);
        }
        check_worker_boundary(child, started)?;
    }
}

fn check_worker_boundary(child: &mut Child, started: Instant) -> io::Result<()> {
    if child.try_wait()?.is_some() {
        return Err(io::Error::other("child process guard exited early"));
    }
    if crate::timing::elapsed(started) >= WORKER_BOUNDARY_TIMEOUT {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "child process guard did not respond",
        ));
    }
    crate::timing::sleep(WORKER_POLL_INTERVAL);
    Ok(())
}

pub(crate) fn internal_worker_requested() -> bool {
    matches!(
        std::env::var(INTERNAL_WORKER_ENVIRONMENT).as_deref(),
        Ok(GUARD_WORKER | LEADER_WORKER)
    )
}

pub(crate) fn run_internal_worker() -> ExitCode {
    let mode = std::env::var(INTERNAL_WORKER_ENVIRONMENT);
    let result = match mode.as_deref() {
        Ok(GUARD_WORKER) => run_guard_worker(),
        Ok(LEADER_WORKER) => run_leader_worker(),
        _ => Err(()),
    };
    if result.is_ok() {
        ExitCode::SUCCESS
    } else {
        if let (Ok(mode), Ok(root)) = (mode, internal_root()) {
            let _ = write_atomic(&root.join(WORKER_FAILURE_FILE), mode.as_bytes());
        }
        ExitCode::FAILURE
    }
}

fn run_guard_worker() -> Result<(), ()> {
    let root = internal_root()?;
    let manifest = read_manifest(&root)?;
    enable_child_subreaper().map_err(|_| ())?;
    let executable = std::env::current_exe().map_err(|_| ())?;
    let exec_boundary = UnixListener::bind(root.join(EXEC_BOUNDARY_SOCKET)).map_err(|_| ())?;
    let mut leader = std::process::Command::new(executable);
    leader
        .env_clear()
        .envs(manifest.environment())
        .env(INTERNAL_WORKER_ENVIRONMENT, LEADER_WORKER)
        .env(INTERNAL_ROOT_ENVIRONMENT, &root)
        .env(
            INTERNAL_PARENT_ENVIRONMENT,
            getpid().as_raw_pid().to_string(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .process_group(0);
    let mut leader = leader.spawn().map_err(|_| ())?;
    let leader_pid = i32::try_from(leader.id())
        .ok()
        .and_then(Pid::from_raw)
        .ok_or(())?;
    let (_, stopped) = waitpid(Some(leader_pid), WaitOptions::UNTRACED)
        .map_err(|_| ())?
        .ok_or(())?;
    if !stopped.stopped() {
        return Err(());
    }
    let identity = identity_for_stopped_leader(leader_pid)?;
    let (mut exec_boundary, _) = exec_boundary.accept().map_err(|_| ())?;
    exec_boundary
        .set_read_timeout(Some(WORKER_BOUNDARY_TIMEOUT))
        .map_err(|_| ())?;
    write_json_atomic(
        &root.join(READY_FILE),
        &ReadyIdentity {
            process_group_id: identity.process_group().as_raw_pid(),
            leader_start_identity: identity.leader_start_identity().to_owned(),
        },
    )?;

    let inspector = SystemProcessIdentityInspector;
    let mut continuation = [0_u8; 1];
    if io::stdin().lock().read_exact(&mut continuation).is_err() {
        cleanup_owned_group(&root, &identity, &mut leader, &inspector)?;
        cleanup_owner_staging(&root);
        return Err(());
    }
    if continuation != [CONTINUE]
        || !matches!(
            continue_authenticated_process_group(&identity),
            AuthenticatedSignalResult::Signalled
        )
    {
        cleanup_owned_group(&root, &identity, &mut leader, &inspector)?;
        return Err(());
    }
    let mut exec_failure = Vec::new();
    if exec_boundary.read_to_end(&mut exec_failure).is_err() || !exec_failure.is_empty() {
        if !exec_failure.is_empty() {
            write_atomic(&root.join(EXEC_FAILURE_FILE), &exec_failure)?;
        }
        cleanup_owned_group(&root, &identity, &mut leader, &inspector)?;
        return Err(());
    }
    write_atomic(&root.join(RELEASED_FILE), b"released\n")?;

    let (owner_event, owner_events) = mpsc::channel();
    drop(std::thread::spawn(move || {
        let mut request = [0_u8; 1];
        let event = if io::stdin().read_exact(&mut request).is_ok() && request == [TERMINATE] {
            OwnerEvent::TerminationRequested
        } else {
            OwnerEvent::Lost
        };
        let _ = owner_event.send(event);
    }));

    monitor_guarded_child(&root, &identity, &mut leader, &owner_events, &inspector)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnerEvent {
    TerminationRequested,
    Lost,
}

fn monitor_guarded_child(
    root: &Path,
    identity: &AuthenticatedProcessGroup,
    leader: &mut std::process::Child,
    owner_events: &mpsc::Receiver<OwnerEvent>,
    inspector: &impl ProcessIdentityInspector,
) -> Result<(), ()> {
    loop {
        if let Ok(owner_event) = owner_events.try_recv() {
            cleanup_owned_group(root, identity, leader, inspector)?;
            if owner_event == OwnerEvent::Lost {
                cleanup_owner_staging(root);
            }
            return Err(());
        }
        let observation = match observe_owned_leader(identity, inspector) {
            Ok(observation) => observation,
            Err(Errno::INTR) => continue,
            Err(_) => ProcessIdentityObservation::Unavailable,
        };
        match observation {
            ProcessIdentityObservation::Exact {
                leader: LeaderState::Zombie,
            } => {
                let status = cleanup_owned_group(root, identity, leader, inspector)?;
                write_atomic(
                    &root.join(STATUS_FILE),
                    status.into_raw().to_string().as_bytes(),
                )?;
                return Ok(());
            }
            ProcessIdentityObservation::Exact { .. } => {}
            ProcessIdentityObservation::Absent | ProcessIdentityObservation::Unavailable => {
                cleanup_owned_group(root, identity, leader, inspector)?;
                return Err(());
            }
        }
        crate::timing::sleep(WORKER_POLL_INTERVAL);
    }
}

#[cfg(not(target_vendor = "apple"))]
fn observe_owned_leader(
    identity: &AuthenticatedProcessGroup,
    inspector: &impl ProcessIdentityInspector,
) -> Result<ProcessIdentityObservation, Errno> {
    observe_owned_leader_with(identity, inspector, || {
        match waitid(
            WaitId::Pid(identity.process_group()),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        ) {
            Ok(Some(_)) => Ok(true),
            Ok(None) | Err(Errno::CHILD) => Ok(false),
            Err(error) => Err(error),
        }
    })
}

#[cfg(target_vendor = "apple")]
fn observe_owned_leader(
    identity: &AuthenticatedProcessGroup,
    _inspector: &impl ProcessIdentityInspector,
) -> Result<ProcessIdentityObservation, Errno> {
    match waitid(
        WaitId::Pid(identity.process_group()),
        WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
    ) {
        Ok(Some(_)) => Ok(ProcessIdentityObservation::Exact {
            leader: LeaderState::Zombie,
        }),
        // The authenticated guard is the direct parent and never reaps the leader
        // outside cleanup. That relationship pins the identity while Darwin's
        // libproc view may be transiently unavailable around process exit.
        Ok(None) => Ok(ProcessIdentityObservation::Exact {
            leader: LeaderState::Running,
        }),
        Err(error) => Err(error),
    }
}

#[cfg(any(not(target_vendor = "apple"), test))]
fn observe_owned_leader_with(
    identity: &AuthenticatedProcessGroup,
    inspector: &impl ProcessIdentityInspector,
    mut exited_without_reaping: impl FnMut() -> Result<bool, Errno>,
) -> Result<ProcessIdentityObservation, Errno> {
    if exited_without_reaping()? {
        return Ok(ProcessIdentityObservation::Exact {
            leader: LeaderState::Zombie,
        });
    }
    let observation = inspector.observe(identity);
    if matches!(
        observation,
        ProcessIdentityObservation::Absent | ProcessIdentityObservation::Unavailable
    ) && exited_without_reaping()?
    {
        // Darwin's libproc stops exposing a leader as it becomes a zombie. The
        // second non-reaping child observation closes that transition race.
        return Ok(ProcessIdentityObservation::Exact {
            leader: LeaderState::Zombie,
        });
    }
    Ok(observation)
}

fn run_leader_worker() -> Result<(), ()> {
    let root = internal_root()?;
    let manifest = read_manifest(&root)?;
    let expected_parent = std::env::var(INTERNAL_PARENT_ENVIRONMENT)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .and_then(Pid::from_raw)
        .ok_or(())?;
    if getppid() != Some(expected_parent) {
        return Err(());
    }
    install_parent_death_protection()?;
    let mut exec_boundary = UnixStream::connect(root.join(EXEC_BOUNDARY_SOCKET)).map_err(|_| ())?;
    if getppid() != Some(expected_parent)
        || getpid() != rustix::process::getpgrp()
        || kill_process(getpid(), Signal::STOP).is_err()
    {
        return Err(());
    }
    if getppid() != Some(expected_parent) {
        return Err(());
    }

    let error = std::process::Command::new(manifest.program())
        .args(manifest.arguments())
        .env_clear()
        .envs(manifest.environment())
        .stdin(Stdio::null())
        .exec();
    let raw_error = error.raw_os_error().unwrap_or(-1).to_string();
    exec_boundary
        .write_all(raw_error.as_bytes())
        .and_then(|()| exec_boundary.flush())
        .map_err(|_| ())?;
    Err(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn install_parent_death_protection() -> Result<(), ()> {
    rustix::process::set_parent_process_death_signal(Some(Signal::KILL)).map_err(|_| ())
}

#[cfg(target_vendor = "apple")]
fn install_parent_death_protection() -> Result<(), ()> {
    // Darwin has no parent-death signal. The leader cannot execute before the
    // independent guard authenticates and releases it, and the guard's control
    // pipe turns execution-owner loss into process-group cleanup.
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn install_parent_death_protection() -> Result<(), ()> {
    Err(())
}

fn identity_for_stopped_leader(leader: Pid) -> Result<AuthenticatedProcessGroup, ()> {
    let identity = capture_process_group_identity(leader).ok_or(())?;
    if matches!(
        system_process_identity_observation(&identity),
        ProcessIdentityObservation::Exact {
            leader: LeaderState::Stopped
        }
    ) {
        Ok(identity)
    } else {
        Err(())
    }
}

fn cleanup_owned_group(
    root: &Path,
    identity: &AuthenticatedProcessGroup,
    leader: &mut std::process::Child,
    inspector: &impl ProcessIdentityInspector,
) -> Result<ExitStatus, ()> {
    terminate_owned_group(identity, inspector)?;
    let status = leader.wait().map_err(|_| ())?;
    reap_owned_process_group(identity.process_group()).map_err(|_| ())?;
    write_atomic(&root.join(QUIESCED_FILE), b"quiesced\n")?;
    Ok(status)
}

fn terminate_owned_group(
    identity: &AuthenticatedProcessGroup,
    inspector: &impl ProcessIdentityInspector,
) -> Result<(), ()> {
    if matches!(
        terminate_authenticated_process_group_with(identity, inspector),
        AuthenticatedSignalResult::Signalled
    ) {
        return Ok(());
    }

    // The guard created this leader, observed its authenticated stopped state,
    // and has deliberately not reaped it. That kernel parent/child relationship
    // pins the PID and authenticates this fallback even when inspection is lost.
    match kill_process_group(identity.process_group(), Signal::KILL) {
        Ok(()) | Err(Errno::SRCH) => Ok(()),
        // Darwin reports EPERM when the retained group contains only the
        // unreaped zombie leader. Reaping that owned child removes the group.
        Err(Errno::PERM) if cfg!(target_vendor = "apple") => Ok(()),
        Err(_) => Err(()),
    }
}

#[cfg(target_os = "linux")]
fn cleanup_adopted_group(
    identity: &AuthenticatedProcessGroup,
    _termination: AuthenticatedSignalResult,
) -> io::Result<()> {
    let options = WaitIdOptions::EXITED
        | WaitIdOptions::STOPPED
        | WaitIdOptions::CONTINUED
        | WaitIdOptions::NOHANG
        | WaitIdOptions::NOWAIT;
    match waitid(WaitId::Pid(identity.process_group()), options) {
        Ok(_) => {}
        Err(Errno::CHILD)
            if matches!(
                system_process_identity_observation(identity),
                ProcessIdentityObservation::Absent
            ) =>
        {
            return Ok(());
        }
        Err(error) => return Err(io::Error::from_raw_os_error(error.raw_os_error())),
    }

    // The execution owner is a child subreaper. Once the guard has exited,
    // waitid proving that the unreaped leader is now our child pins the recorded
    // identity without relying on /proc. Signal exactly once while it is pinned,
    // then reap every adopted member of that group.
    match kill_process_group(identity.process_group(), Signal::KILL) {
        Ok(()) | Err(Errno::SRCH) => {}
        Err(error) => return Err(io::Error::from_raw_os_error(error.raw_os_error())),
    }
    reap_owned_process_group(identity.process_group())
}

#[cfg(target_vendor = "apple")]
fn cleanup_adopted_group(
    identity: &AuthenticatedProcessGroup,
    termination: AuthenticatedSignalResult,
) -> io::Result<()> {
    match termination {
        AuthenticatedSignalResult::Signalled => reap_owned_process_group(identity.process_group()),
        AuthenticatedSignalResult::Absent
            if process_group_is_quiescent(identity.process_group()) =>
        {
            Ok(())
        }
        AuthenticatedSignalResult::Absent | AuthenticatedSignalResult::Unavailable => Err(
            io::Error::other("guarded process group ownership is unavailable"),
        ),
    }
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn cleanup_adopted_group(
    _identity: &AuthenticatedProcessGroup,
    _termination: AuthenticatedSignalResult,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "authenticated child process guards are unavailable",
    ))
}

#[cfg(target_os = "linux")]
fn reap_owned_process_group(process_group: Pid) -> io::Result<()> {
    let started = crate::timing::monotonic_now();
    loop {
        match waitpgid(process_group, WaitOptions::NOHANG) {
            Ok(Some(_)) => {}
            Ok(None) => {
                if crate::timing::elapsed(started) >= WORKER_BOUNDARY_TIMEOUT {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "guarded process group did not exit",
                    ));
                }
                crate::timing::sleep(WORKER_POLL_INTERVAL);
            }
            Err(Errno::CHILD) => return Ok(()),
            Err(Errno::INTR) => {}
            Err(error) => return Err(io::Error::from_raw_os_error(error.raw_os_error())),
        }
    }
}

#[cfg(target_vendor = "apple")]
fn reap_owned_process_group(process_group: Pid) -> io::Result<()> {
    let started = crate::timing::monotonic_now();
    while !process_group_is_quiescent(process_group) {
        if crate::timing::elapsed(started) >= WORKER_BOUNDARY_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "guarded process group did not exit",
            ));
        }
        crate::timing::sleep(WORKER_POLL_INTERVAL);
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn reap_owned_process_group(_process_group: Pid) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "authenticated child process guards are unavailable",
    ))
}

fn require_quiesced_marker(root: &Path) -> io::Result<()> {
    match fs::metadata(root.join(QUIESCED_FILE)) {
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_) => Err(io::Error::other("guard cleanup marker is not a file")),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "linux")]
fn enable_child_subreaper() -> io::Result<()> {
    nix::sys::prctl::set_child_subreaper(true).map_err(io::Error::other)
}

#[cfg(target_vendor = "apple")]
fn enable_child_subreaper() -> io::Result<()> {
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn enable_child_subreaper() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "authenticated child process guards are unavailable",
    ))
}

fn cleanup_owner_staging(root: &Path) {
    let _ = fs::remove_dir_all(root);
}

fn internal_root() -> Result<PathBuf, ()> {
    std::env::var_os(INTERNAL_ROOT_ENVIRONMENT)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or(())
}

fn read_manifest(root: &Path) -> Result<LaunchManifest, ()> {
    let file = File::open(root.join(MANIFEST_FILE)).map_err(|_| ())?;
    let metadata = file.metadata().map_err(|_| ())?;
    if !metadata.is_file() || metadata.len() > MAXIMUM_MANIFEST_BYTES {
        return Err(());
    }
    let mut bytes = Vec::new();
    file.take(MAXIMUM_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|size| size > MAXIMUM_MANIFEST_BYTES)
    {
        return Err(());
    }
    serde_json::from_slice(&bytes).map_err(|_| ())
}

fn write_json_atomic(path: &Path, document: &impl Serialize) -> Result<(), ()> {
    let bytes = serde_json::to_vec(document).map_err(|_| ())?;
    write_atomic(path, &bytes)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), ()> {
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary).map_err(|_| ())?;
    file.write_all(bytes).map_err(|_| ())?;
    file.flush().map_err(|_| ())?;
    fs::rename(temporary, path).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::process::Command as StdCommand;

    #[cfg(target_os = "linux")]
    use super::super::process_group::capture_process_group_identity;
    use super::*;

    struct UnavailableInspector;

    impl ProcessIdentityInspector for UnavailableInspector {
        fn observe(&self, _identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation {
            ProcessIdentityObservation::Unavailable
        }
    }

    #[test]
    fn activity_lease_blocks_an_exclusive_cleanup_lock() {
        let staging = tempfile::tempdir().unwrap();
        let lease = create_activity_lease(staging.path()).unwrap();
        let contender = OpenOptions::new()
            .read(true)
            .write(true)
            .open(staging.path().join(ACTIVITY_LOCK_FILE))
            .unwrap();

        assert!(matches!(
            fs4::FileExt::try_lock(&contender),
            Err(fs4::TryLockError::WouldBlock)
        ));

        drop(lease);
        fs4::FileExt::try_lock(&contender).unwrap();
    }

    #[test]
    fn exit_between_child_and_identity_observations_is_still_a_zombie() {
        let identity =
            AuthenticatedProcessGroup::new(Pid::from_raw(41).unwrap(), "start-identity".to_owned())
                .unwrap();
        let mut exit_observations = [false, true].into_iter();

        assert_eq!(
            observe_owned_leader_with(&identity, &UnavailableInspector, || {
                Ok(exit_observations.next().unwrap())
            })
            .unwrap(),
            ProcessIdentityObservation::Exact {
                leader: LeaderState::Zombie
            }
        );
        assert_eq!(exit_observations.next(), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unavailable_inspection_fails_closed_and_quiesces_descendants() {
        enable_child_subreaper().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let descendant_file = staging.path().join("descendant.pid");
        let mut leader = StdCommand::new("/bin/sh")
            .arg("-c")
            .arg(format!(
                "sleep 300 & descendant=$!; printf '%s\\n' \"$descendant\" > {}; wait \"$descendant\"",
                descendant_file.display()
            ))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let leader_pid = Pid::from_raw(i32::try_from(leader.id()).unwrap()).unwrap();
        let identity = capture_process_group_identity(leader_pid).unwrap();
        for _ in 0..500 {
            if descendant_file.is_file() {
                break;
            }
            crate::timing::sleep(Duration::from_millis(10));
        }
        let (_owner_event, owner_events) = mpsc::channel();

        assert!(
            monitor_guarded_child(
                staging.path(),
                &identity,
                &mut leader,
                &owner_events,
                &UnavailableInspector,
            )
            .is_err()
        );

        assert!(staging.path().join(QUIESCED_FILE).is_file());
        assert!(process_group_is_quiescent(identity.process_group()));
    }
}
