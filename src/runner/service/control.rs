use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions, Permissions};
use std::future::Future;
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fs4::{FileExt, TryLockError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::assignment::AssignmentManager;
use crate::runner::control_protocol::{
    ConnectionFailure, ConnectionState, ControlError, Operation, ProcessState, Response,
    StatusSnapshot, decode_request, encode_response,
};

const RUNTIME_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_MODE: u32 = 0o600;
const RUNTIME_LOCK_FILE: &str = ".runner-serve.lock";
const MAXIMUM_CONNECTIONS: usize = 16;
#[cfg(not(test))]
const IO_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(test)]
const IO_TIMEOUT: Duration = Duration::from_millis(50);
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(1);
const RELOAD_TIMEOUT: Duration = Duration::from_secs(30);
#[expect(
    clippy::cast_possible_wrap,
    reason = "O_NOFOLLOW fits in the signed custom_flags value on supported Unix targets"
)]
const NOFOLLOW_FLAG: i32 = rustix::fs::OFlags::NOFOLLOW.bits() as i32;

pub(super) struct ReloadRequest {
    pub(super) response: oneshot::Sender<Result<String, ControlError>>,
}

struct LiveStatusState {
    process_state: ProcessState,
    connection_state: ConnectionState,
    last_connected_at: Option<String>,
    current_credential_id: Option<String>,
    pending_credential_id: Option<String>,
    last_connection_failure: Option<ConnectionFailure>,
}

pub(super) struct LiveStatus {
    boot_id: String,
    started_at: Instant,
    state: Mutex<LiveStatusState>,
    connected: tokio::sync::Notify,
}

impl LiveStatus {
    pub(super) fn new(
        boot_id: String,
        current_credential_id: String,
        pending_credential_id: Option<String>,
    ) -> Self {
        Self {
            boot_id,
            started_at: crate::timing::monotonic_now(),
            state: Mutex::new(LiveStatusState {
                process_state: ProcessState::Running,
                connection_state: ConnectionState::Connecting,
                last_connected_at: None,
                current_credential_id: Some(current_credential_id),
                pending_credential_id,
                last_connection_failure: None,
            }),
            connected: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn connecting(&self) {
        self.lock().connection_state = ConnectionState::Connecting;
    }

    pub(super) fn connected(&self, connected_at: Option<String>) {
        let mut state = self.lock();
        state.connection_state = ConnectionState::Connected;
        if connected_at.is_some() {
            state.last_connected_at = connected_at;
        }
        state.last_connection_failure = None;
        drop(state);
        self.connected.notify_waiters();
    }

    pub(super) async fn wait_until_connected(&self) {
        loop {
            let notified = self.connected.notified();
            if self.is_connected() {
                return;
            }
            notified.await;
        }
    }

    pub(super) fn is_connected(&self) -> bool {
        self.lock().connection_state == ConnectionState::Connected
    }

    pub(super) fn backing_off(&self, failure: ConnectionFailure) {
        self.update_connection(ConnectionState::BackingOff, Some(failure));
    }

    pub(super) fn authentication_failed(&self) {
        self.update_connection(
            ConnectionState::AuthenticationFailed,
            Some(ConnectionFailure::Authentication),
        );
    }

    pub(super) fn protocol_failed(&self) {
        self.update_connection(
            ConnectionState::ProtocolFailed,
            Some(ConnectionFailure::Protocol),
        );
    }

    pub(super) fn stopping(&self) {
        let mut state = self.lock();
        state.process_state = ProcessState::Stopping;
        state.connection_state = ConnectionState::Stopping;
    }

    pub(super) fn promoted(&self, credential_id: String) {
        let mut state = self.lock();
        state.current_credential_id = Some(credential_id);
        state.pending_credential_id = None;
    }

    pub(super) fn pending(&self, credential_id: Option<String>) {
        self.lock().pending_credential_id = credential_id;
    }

    fn update_connection(
        &self,
        connection_state: ConnectionState,
        failure: Option<ConnectionFailure>,
    ) {
        let mut state = self.lock();
        state.connection_state = connection_state;
        state.last_connection_failure = failure;
    }

    fn snapshot(
        &self,
        assignment_counts: crate::runner::control_protocol::AssignmentCounts,
    ) -> StatusSnapshot {
        let state = self.lock();
        let uptime_milliseconds =
            u64::try_from(crate::timing::elapsed(self.started_at).as_millis()).unwrap_or(u64::MAX);
        StatusSnapshot {
            process_state: state.process_state,
            boot_id: self.boot_id.clone(),
            uptime_milliseconds,
            connection_state: state.connection_state,
            last_connected_at: state.last_connected_at.clone(),
            current_credential_id: state.current_credential_id.clone(),
            pending_credential_id: state.pending_credential_id.clone(),
            assignment_counts,
            last_connection_failure: state.last_connection_failure,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LiveStatusState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(super) struct ControlServer {
    task: tokio::task::JoinHandle<()>,
    _socket: SocketGuard,
}

impl ControlServer {
    pub(super) fn bind(
        socket_path: &Path,
        status: Arc<LiveStatus>,
        assignments: Arc<Mutex<AssignmentManager>>,
        reloads: mpsc::Sender<ReloadRequest>,
    ) -> Result<Self, ControlServerError> {
        let (listener, socket) = SocketGuard::bind(socket_path)?;
        let task = tokio::spawn(serve(listener, status, assignments, reloads));
        Ok(Self {
            task,
            _socket: socket,
        })
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ControlServerError {
    UnsafeRuntimeDirectory,
    RuntimeOwned,
    UnsafeSocket,
    BindSocket,
}

impl fmt::Display for ControlServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::UnsafeRuntimeDirectory => "runner control runtime directory is unsafe",
            Self::RuntimeOwned => "another Runner Serve process owns the runtime",
            Self::UnsafeSocket => "runner control socket path is unsafe",
            Self::BindSocket => "runner control socket cannot be bound",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ControlServerError {}

#[derive(Debug)]
struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}

impl SocketGuard {
    fn bind(path: &Path) -> Result<(UnixListener, Self), ControlServerError> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or(ControlServerError::UnsafeRuntimeDirectory)?;
        validate_runtime_directory(parent)?;
        let lock_path = parent.join(RUNTIME_LOCK_FILE);
        let lock = open_runtime_lock(&lock_path)?;
        match FileExt::try_lock(&lock) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(ControlServerError::RuntimeOwned),
            Err(TryLockError::Error(_)) => return Err(ControlServerError::UnsafeRuntimeDirectory),
        }
        inspect_existing_socket(path)?;
        let listener = std::os::unix::net::UnixListener::bind(path)
            .map_err(|_| ControlServerError::BindSocket)?;
        listener
            .set_nonblocking(true)
            .map_err(|_| ControlServerError::BindSocket)?;
        fs::set_permissions(path, Permissions::from_mode(PRIVATE_MODE)).map_err(|_| {
            let _ = fs::remove_file(path);
            ControlServerError::BindSocket
        })?;
        let metadata = fs::symlink_metadata(path).map_err(|_| ControlServerError::BindSocket)?;
        if !safe_socket(&metadata) {
            let _ = fs::remove_file(path);
            return Err(ControlServerError::UnsafeSocket);
        }
        let listener = UnixListener::from_std(listener).map_err(|_| {
            let _ = fs::remove_file(path);
            ControlServerError::BindSocket
        })?;
        Ok((
            listener,
            Self {
                path: path.to_owned(),
                device: metadata.dev(),
                inode: metadata.ino(),
                _lock: lock,
            },
        ))
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.dev() == self.device && metadata.ino() == self.inode && safe_socket(&metadata)
        }) {
            let _ = fs::remove_file(&self.path);
        }
        // A concurrent fork can briefly retain this close-on-exec descriptor.
        // Unlock explicitly so that child cannot delay the next bind until exec.
        let _ = FileExt::unlock(&self._lock);
    }
}

fn validate_runtime_directory(path: &Path) -> Result<(), ControlServerError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| ControlServerError::UnsafeRuntimeDirectory)?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o7777 != RUNTIME_DIRECTORY_MODE
    {
        return Err(ControlServerError::UnsafeRuntimeDirectory);
    }
    Ok(())
}

fn open_runtime_lock(path: &Path) -> Result<File, ControlServerError> {
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(PRIVATE_MODE)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(NOFOLLOW_FLAG)
            .open(path)
            .map_err(|_| ControlServerError::UnsafeRuntimeDirectory)?,
        Err(_) => return Err(ControlServerError::UnsafeRuntimeDirectory),
    };
    let metadata = file
        .metadata()
        .map_err(|_| ControlServerError::UnsafeRuntimeDirectory)?;
    if !safe_private_file(&metadata) {
        return Err(ControlServerError::UnsafeRuntimeDirectory);
    }
    Ok(file)
}

fn safe_private_file(metadata: &Metadata) -> bool {
    metadata.file_type().is_file()
        && metadata.uid() == rustix::process::geteuid().as_raw()
        && metadata.mode() & 0o7777 == PRIVATE_MODE
        && metadata.nlink() == 1
}

fn safe_socket(metadata: &Metadata) -> bool {
    metadata.file_type().is_socket()
        && metadata.uid() == rustix::process::geteuid().as_raw()
        && metadata.mode() & 0o7777 == PRIVATE_MODE
        && metadata.nlink() == 1
}

fn inspect_existing_socket(path: &Path) -> Result<(), ControlServerError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(ControlServerError::UnsafeSocket),
    };
    if !safe_socket(&metadata) {
        return Err(ControlServerError::UnsafeSocket);
    }
    match StdUnixStream::connect(path) {
        Ok(_) => Err(ControlServerError::RuntimeOwned),
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
            let current =
                fs::symlink_metadata(path).map_err(|_| ControlServerError::UnsafeSocket)?;
            if !safe_socket(&current)
                || current.dev() != metadata.dev()
                || current.ino() != metadata.ino()
            {
                return Err(ControlServerError::UnsafeSocket);
            }
            fs::remove_file(path).map_err(|_| ControlServerError::UnsafeSocket)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(ControlServerError::UnsafeSocket),
    }
}

async fn serve(
    listener: UnixListener,
    status: Arc<LiveStatus>,
    assignments: Arc<Mutex<AssignmentManager>>,
    reloads: mpsc::Sender<ReloadRequest>,
) {
    let capacity = Arc::new(Semaphore::new(MAXIMUM_CONNECTIONS));
    while let Ok((stream, _)) = listener.accept().await {
        let Ok(permit) = Arc::clone(&capacity).try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let status = Arc::clone(&status);
        let assignments = Arc::clone(&assignments);
        let reloads = reloads.clone();
        tokio::spawn(async move {
            let _permit = permit;
            handle_connection(stream, status, assignments, reloads).await;
        });
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    status: Arc<LiveStatus>,
    assignments: Arc<Mutex<AssignmentManager>>,
    reloads: mpsc::Sender<ReloadRequest>,
) {
    if stream.peer_cred().ok().map(|credential| credential.uid())
        != Some(rustix::process::geteuid().as_raw())
    {
        return;
    }
    let operation = match read_request(&mut stream).await {
        Ok(bytes) => decode_request(&bytes),
        Err(()) => Err(ControlError::InvalidRequest),
    };
    let response = match operation {
        Ok(Operation::Status) => {
            let snapshot = control_timeout(
                SNAPSHOT_TIMEOUT,
                tokio::task::spawn_blocking(move || {
                    let counts = assignments
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .status_counts();
                    status.snapshot(counts)
                }),
            )
            .await;
            match snapshot {
                Ok(Ok(snapshot)) => Response::Status(snapshot),
                Ok(Err(_)) | Err(_) => Response::Error(ControlError::InvalidRequest),
            }
        }
        Ok(Operation::ReloadCredential) => {
            let (response, receive) = oneshot::channel();
            if reloads.try_send(ReloadRequest { response }).is_err() {
                Response::Error(ControlError::PendingConnectionFailed)
            } else {
                match control_timeout(RELOAD_TIMEOUT, receive).await {
                    Ok(Ok(Ok(credential_id))) => Response::Reloaded { credential_id },
                    Ok(Ok(Err(error))) => Response::Error(error),
                    Ok(Err(_)) | Err(_) => Response::Error(ControlError::PendingConnectionFailed),
                }
            }
        }
        Err(error) => Response::Error(error),
    };
    let Ok(bytes) = encode_response(&response) else {
        return;
    };
    let _ = control_timeout(IO_TIMEOUT, stream.write_all(&bytes)).await;
}

async fn read_request(stream: &mut UnixStream) -> Result<Vec<u8>, ()> {
    let mut request = Vec::with_capacity(256);
    let mut chunk = [0_u8; 512];
    control_timeout(IO_TIMEOUT, async {
        loop {
            let read = stream.read(&mut chunk).await.map_err(|_| ())?;
            if read == 0 {
                return Err(());
            }
            request.extend_from_slice(&chunk[..read]);
            if request.len() > crate::runner::control_protocol::REQUEST_LIMIT {
                return Err(());
            }
            if let Some(newline) = request.iter().position(|byte| *byte == b'\n') {
                return (newline + 1 == request.len()).then_some(request).ok_or(());
            }
        }
    })
    .await
    .map_err(|_| ())?
}

#[expect(
    clippy::disallowed_methods,
    reason = "this is the production boundary for bounded local-control I/O and work"
)]
async fn control_timeout<F: Future>(
    duration: Duration,
    future: F,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::time::timeout(duration, future).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    use crate::runner::control_protocol::{AssignmentCounts, Response, decode_response};
    use crate::runner::credential::test_credential;
    use crate::runner::service::config::Config;
    use crate::runner::service::test_support::fixture_lease_clock;

    async fn exchange(request: &[u8]) -> Vec<u8> {
        let (mut client, server) = UnixStream::pair().unwrap();
        let config = Config::fixture(
            "ws://127.0.0.1:1/v1/runner/connect",
            test_credential(),
            true,
        )
        .unwrap();
        let assignments = Arc::new(Mutex::new(AssignmentManager::new(
            &config,
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            fixture_lease_clock(),
        )));
        let status = Arc::new(LiveStatus::new(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            None,
        ));
        let (reloads, _requests) = mpsc::channel(1);
        let server = tokio::spawn(handle_connection(server, status, assignments, reloads));
        client.write_all(request).await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        server.await.unwrap();
        response
    }

    #[tokio::test]
    async fn lifecycle_rejects_hostile_paths_and_recovers_an_owned_stale_socket() {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        fs::set_permissions(root.path(), Permissions::from_mode(0o700)).unwrap();
        let socket = root.path().join("runner.sock");

        let (listener, first) = SocketGuard::bind(&socket).unwrap();
        assert_eq!(fs::metadata(&socket).unwrap().mode() & 0o7777, 0o600);
        assert_eq!(
            SocketGuard::bind(&socket).unwrap_err(),
            ControlServerError::RuntimeOwned
        );
        drop(listener);
        drop(first);
        assert!(!socket.exists());

        let stale = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, Permissions::from_mode(0o600)).unwrap();
        drop(stale);
        let (_listener, recovered) = SocketGuard::bind(&socket).unwrap();
        drop(recovered);

        fs::write(&socket, b"hostile").unwrap();
        assert_eq!(
            SocketGuard::bind(&socket).unwrap_err(),
            ControlServerError::UnsafeSocket
        );
        fs::remove_file(&socket).unwrap();
        let target = root.path().join("target");
        fs::write(&target, b"target").unwrap();
        symlink(&target, &socket).unwrap();
        assert_eq!(
            SocketGuard::bind(&socket).unwrap_err(),
            ControlServerError::UnsafeSocket
        );
    }

    #[tokio::test]
    async fn serves_a_bounded_secret_free_idle_snapshot() {
        let response = exchange(b"{\"schemaVersion\":1,\"operation\":\"status\"}\n").await;
        let Response::Status(status) = decode_response(&response).unwrap() else {
            panic!("status request did not return a snapshot");
        };
        assert_eq!(status.assignment_counts, AssignmentCounts::default());
        assert_eq!(
            status.current_credential_id.as_deref(),
            Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc")
        );
        let encoded = String::from_utf8(response).unwrap();
        for forbidden in [
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "activationId",
            "organization",
            "connectionUrl",
            "workflow",
            "environment",
        ] {
            assert!(!encoded.contains(forbidden));
        }
    }

    #[test]
    fn reconnecting_retains_the_last_closed_failure_category() {
        let status = LiveStatus::new(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            None,
        );
        status.backing_off(ConnectionFailure::CloudUnavailable);
        status.connecting();
        let snapshot = status.snapshot(AssignmentCounts::default());
        assert_eq!(snapshot.connection_state, ConnectionState::Connecting);
        assert_eq!(
            snapshot.last_connection_failure,
            Some(ConnectionFailure::CloudUnavailable)
        );
    }

    #[tokio::test]
    async fn contains_malformed_and_oversized_requests() {
        for request in [
            b"{".to_vec(),
            b"not-json\n".to_vec(),
            b"{\"schemaVersion\":1,\"operation\":\"run_command\"}\n".to_vec(),
            {
                let mut oversized = vec![b'x'; crate::runner::control_protocol::REQUEST_LIMIT];
                oversized.push(b'\n');
                oversized
            },
        ] {
            assert_eq!(
                decode_response(&exchange(&request).await).unwrap(),
                Response::Error(ControlError::InvalidRequest)
            );
        }
    }

    #[test]
    fn runtime_directory_must_be_private_and_owned() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), Permissions::from_mode(0o770)).unwrap();
        assert_eq!(
            SocketGuard::bind(&root.path().join("runner.sock")).unwrap_err(),
            ControlServerError::UnsafeRuntimeDirectory
        );
    }
}
