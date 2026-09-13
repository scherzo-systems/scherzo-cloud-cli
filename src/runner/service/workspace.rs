use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions, Permissions};
use std::io::{self, Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use base64::Engine as _;
use fs4::{FileExt, TryLockError};
use nix::fcntl::{FcntlArg, FdFlag, fcntl};
use rustix::fs::{
    AtFlags, Dev, Dir, FileType, Mode, OFlags, RenameFlags, Stat, fstat, openat, renameat_with,
    statat,
};
use rustix::io::{Errno, fcntl_dupfd_cloexec};

use super::workflow_git::{WorkflowGitAuthority, WorkflowGitTeardownReport};
use crate::execution::owned_tree::{self, RemovalError};

const LOCK_FILE_NAME: &str = ".scherzo-runner-serve.lock";
const OWNERSHIP_MARKER_NAME: &str = ".scherzo-runner-serve-owner-v1";
const ATTEMPT_RECORD_NAME: &str = ".scherzo-runner-serve-attempt-v1";
const ATTEMPT_RECORD_STAGING_PREFIX: &str = ".scherzo-runner-serve-attempt-staging-";
const ATTEMPT_RECORD_HEADER: &str = "scherzo-runner-serve/attempt/v1";
const CLEANUP_AUTHORITY_PREFIX: &str = ".scherzo-runner-serve-cleanup-v1-";
const CLEANUP_AUTHORITY_STAGING_PREFIX: &str = ".scherzo-runner-serve-cleanup-staging-";
const CLEANUP_AUTHORITY_HEADER: &str = "scherzo-runner-serve/cleanup-authority/v1";
const CLEANUP_IDENTITY_ATTRIBUTE: &str = "user.scherzo.runner-cleanup-v1";
const CLEANUP_IDENTITY_BYTES: usize = 32;
const BOOT_MARKER: &[u8] = b"scherzo-runner-serve/boot-root/v1\n";
const ASSIGNMENT_MARKER: &[u8] = b"scherzo-runner-serve/assignment-root/v1\n";
const PRIVATE_FILE_MODE: u32 = 0o600;
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const MAXIMUM_ATTEMPT_RECORD_BYTES: u64 = 512;
const MAXIMUM_CLEANUP_AUTHORITY_BYTES: u64 = 512;
const REMOVAL_DELAYS: [Duration; 5] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_millis(1_000),
    Duration::from_millis(2_000),
];
#[expect(
    clippy::cast_possible_wrap,
    reason = "the Unix open flags fit in the signed custom_flags value on supported targets"
)]
const NOFOLLOW_FLAG: i32 = rustix::fs::OFlags::NOFOLLOW.bits() as i32;
#[expect(
    clippy::cast_possible_wrap,
    reason = "the Unix open flags fit in the signed custom_flags value on supported targets"
)]
const DIRECTORY_NOFOLLOW_FLAGS: i32 =
    (rustix::fs::OFlags::DIRECTORY.bits() | rustix::fs::OFlags::NOFOLLOW.bits()) as i32;

#[cfg(target_vendor = "apple")]
fn normalized_device(device: Dev) -> u64 {
    // Darwin exposes dev_t as signed while MetadataExt::dev uses u64.
    // Preserve the signed value's bits without a lint-suppressed integer cast.
    u64::from_ne_bytes(i64::from(device).to_ne_bytes())
}

#[cfg(not(target_vendor = "apple"))]
#[allow(
    clippy::useless_conversion,
    reason = "dev_t width varies across Unix targets; widening is a no-op on Linux"
)]
fn normalized_device(device: Dev) -> u64 {
    u64::from(device)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CleanupResult {
    Released,
    Retained,
    Quarantined(CleanupFailure),
    Preempted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RetentionReason {
    Failed,
    Cancelled,
    Interrupted,
    ArtifactDeliveryFailed,
    OutcomeUnknown,
    ProcessStopFailed,
    CredentialTeardownFailed,
    ReleaseWorkerUnavailable,
}

impl RetentionReason {
    const ALL: [Self; 8] = [
        Self::Failed,
        Self::Cancelled,
        Self::Interrupted,
        Self::ArtifactDeliveryFailed,
        Self::OutcomeUnknown,
        Self::ProcessStopFailed,
        Self::CredentialTeardownFailed,
        Self::ReleaseWorkerUnavailable,
    ];

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
            Self::ArtifactDeliveryFailed => "artifact_delivery_failed",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::ProcessStopFailed => "process_stop_failed",
            Self::CredentialTeardownFailed => "credential_teardown_failed",
            Self::ReleaseWorkerUnavailable => "release_worker_unavailable",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|reason| reason.as_str() == value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkspaceDisposition {
    Remove,
    Retain(RetentionReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CleanupFailure {
    OrdinaryRemovalExhausted,
    Safety,
    Quiescence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProcessQuiescence {
    Proven,
    Failed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum WorkRootError {
    WorkRootInUse,
    UnsafeWorkRoot,
    CreateBootRoot,
}

impl fmt::Display for WorkRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WorkRootInUse => "runner work root is already in use",
            Self::UnsafeWorkRoot => "runner work root ownership state is unsafe",
            Self::CreateBootRoot => "runner boot root could not be created",
        })
    }
}

impl WorkRootError {
    pub(super) const fn error_type(self) -> &'static str {
        match self {
            Self::WorkRootInUse => "work_root_in_use",
            Self::UnsafeWorkRoot => "unsafe_work_root",
            Self::CreateBootRoot => "boot_root_creation_failed",
        }
    }
}

impl std::error::Error for WorkRootError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AssignmentRootCreationError {
    Unavailable,
    CleanupFailed,
}

pub(super) trait TreeRemover: Send + Sync {
    fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()>;
}

pub(super) trait CleanupSleeper: Send + Sync {
    fn sleep(&self, duration: Duration, cancellation: &CleanupCancellation) -> bool;
}

pub(super) trait WorkRootHook: Send + Sync {
    fn before_child_enumeration(&self);
}

trait WorkspaceReleaseSpawner: Send + Sync {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) -> io::Result<()>;
}

struct ThreadWorkspaceReleaseSpawner;

impl WorkspaceReleaseSpawner for ThreadWorkspaceReleaseSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) -> io::Result<()> {
        std::thread::Builder::new()
            .name("runner-workspace-boundary-release".to_owned())
            .spawn(task)
            .map(drop)
    }
}

struct SystemTreeRemover;

impl TreeRemover for SystemTreeRemover {
    fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()> {
        match tree.validate(true) {
            Ok(TreePresence::Present) => {}
            Ok(TreePresence::Missing) => return Err(io::ErrorKind::NotFound.into()),
            Err(()) => return Err(safety_removal_error()),
        }
        let link = tree.link().ok_or_else(safety_removal_error)?;
        let directory = link.directory.as_ref().ok_or_else(safety_removal_error)?;
        owned_tree::remove_open_tree_at(
            link.parent.as_ref(),
            link.identity.as_ref(),
            directory.as_ref(),
        )
        .map_err(|error| match error {
            RemovalError::Filesystem(error) => filesystem_removal_error(error),
            RemovalError::Replaced => safety_removal_error(),
        })
    }
}

fn filesystem_removal_error(error: Errno) -> io::Error {
    io::Error::from_raw_os_error(error.raw_os_error())
}

fn safety_removal_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "owned tree changed during removal",
    )
}

struct InterruptibleSleeper;

impl CleanupSleeper for InterruptibleSleeper {
    fn sleep(&self, duration: Duration, cancellation: &CleanupCancellation) -> bool {
        cancellation.wait(duration)
    }
}

struct NoopWorkRootHook;

impl WorkRootHook for NoopWorkRootHook {
    fn before_child_enumeration(&self) {}
}

trait CleanupIdentityStore: Send + Sync {
    fn read(&self, directory: &OwnedFd) -> Result<Option<[u8; CLEANUP_IDENTITY_BYTES]>, ()>;
    fn create(
        &self,
        directory: &OwnedFd,
        identity: &[u8; CLEANUP_IDENTITY_BYTES],
    ) -> Result<(), ()>;
}

struct ExtendedAttributeCleanupIdentityStore;

impl CleanupIdentityStore for ExtendedAttributeCleanupIdentityStore {
    fn read(&self, directory: &OwnedFd) -> Result<Option<[u8; CLEANUP_IDENTITY_BYTES]>, ()> {
        let mut buffer = [0_u8; CLEANUP_IDENTITY_BYTES + 1];
        match rustix::fs::fgetxattr(directory, CLEANUP_IDENTITY_ATTRIBUTE, &mut buffer) {
            Ok(length) if length == CLEANUP_IDENTITY_BYTES => {
                let mut identity = [0_u8; CLEANUP_IDENTITY_BYTES];
                identity.copy_from_slice(&buffer[..CLEANUP_IDENTITY_BYTES]);
                Ok(Some(identity))
            }
            Ok(_) => Err(()),
            Err(error) if missing_cleanup_identity(error) => Ok(None),
            Err(_) => Err(()),
        }
    }

    fn create(
        &self,
        directory: &OwnedFd,
        identity: &[u8; CLEANUP_IDENTITY_BYTES],
    ) -> Result<(), ()> {
        rustix::fs::fsetxattr(
            directory,
            CLEANUP_IDENTITY_ATTRIBUTE,
            identity,
            rustix::fs::XattrFlags::CREATE,
        )
        .map_err(|_| ())
    }
}

// Unit tests exercise cleanup state transitions on filesystems such as the Nix
// build sandbox that intentionally reject extended attributes. Device and inode
// identity preserves replacement detection without weakening the system store.
#[cfg(test)]
struct MetadataCleanupIdentityStore;

#[cfg(test)]
impl CleanupIdentityStore for MetadataCleanupIdentityStore {
    fn read(&self, directory: &OwnedFd) -> Result<Option<[u8; CLEANUP_IDENTITY_BYTES]>, ()> {
        let metadata = fstat(directory).map_err(|_| ())?;
        let device = normalized_device(metadata.st_dev);
        let mut identity = [0_u8; CLEANUP_IDENTITY_BYTES];
        identity[..8].copy_from_slice(&device.to_le_bytes());
        identity[8..16].copy_from_slice(&metadata.st_ino.to_le_bytes());
        identity[16..24].copy_from_slice(&(!device).to_le_bytes());
        identity[24..].copy_from_slice(&(!metadata.st_ino).to_le_bytes());
        Ok(Some(identity))
    }

    fn create(
        &self,
        _directory: &OwnedFd,
        _identity: &[u8; CLEANUP_IDENTITY_BYTES],
    ) -> Result<(), ()> {
        Err(())
    }
}

#[derive(Clone)]
pub(super) struct WorkspaceFilesystem {
    remover: Arc<dyn TreeRemover>,
    sleeper: Arc<dyn CleanupSleeper>,
    hook: Arc<dyn WorkRootHook>,
    cleanup_identity: Arc<dyn CleanupIdentityStore>,
    workspace_release_spawner: Arc<dyn WorkspaceReleaseSpawner>,
}

impl WorkspaceFilesystem {
    pub(super) fn system() -> Self {
        Self {
            remover: Arc::new(SystemTreeRemover),
            sleeper: Arc::new(InterruptibleSleeper),
            hook: Arc::new(NoopWorkRootHook),
            cleanup_identity: Arc::new(ExtendedAttributeCleanupIdentityStore),
            workspace_release_spawner: Arc::new(ThreadWorkspaceReleaseSpawner),
        }
    }

    #[cfg(test)]
    pub(super) fn testing() -> Self {
        Self {
            remover: Arc::new(SystemTreeRemover),
            sleeper: Arc::new(InterruptibleSleeper),
            hook: Arc::new(NoopWorkRootHook),
            cleanup_identity: Arc::new(MetadataCleanupIdentityStore),
            workspace_release_spawner: Arc::new(ThreadWorkspaceReleaseSpawner),
        }
    }

    #[cfg(test)]
    pub(super) fn injected(
        remover: Arc<dyn TreeRemover>,
        sleeper: Arc<dyn CleanupSleeper>,
        hook: Arc<dyn WorkRootHook>,
    ) -> Self {
        Self::injected_with_cleanup_identity(
            remover,
            sleeper,
            hook,
            Arc::new(MetadataCleanupIdentityStore),
        )
    }

    #[cfg(test)]
    fn injected_with_cleanup_identity(
        remover: Arc<dyn TreeRemover>,
        sleeper: Arc<dyn CleanupSleeper>,
        hook: Arc<dyn WorkRootHook>,
        cleanup_identity: Arc<dyn CleanupIdentityStore>,
    ) -> Self {
        Self {
            remover,
            sleeper,
            hook,
            cleanup_identity,
            workspace_release_spawner: Arc::new(ThreadWorkspaceReleaseSpawner),
        }
    }

    #[cfg(test)]
    fn with_workspace_release_spawner(mut self, spawner: Arc<dyn WorkspaceReleaseSpawner>) -> Self {
        self.workspace_release_spawner = spawner;
        self
    }
}

#[derive(Default)]
pub(super) struct CleanupCancellation {
    cancelled: Mutex<bool>,
    changed: Condvar,
}

impl CleanupCancellation {
    fn cancel(&self) {
        let mut cancelled = self
            .cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *cancelled = true;
        self.changed.notify_all();
    }

    pub(super) fn is_cancelled(&self) -> bool {
        *self
            .cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn wait(&self, duration: Duration) -> bool {
        let cancelled = self
            .cancelled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (cancelled, _timeout) = self
            .changed
            .wait_timeout_while(cancelled, duration, |cancelled| !*cancelled)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !*cancelled
    }
}

#[derive(Clone)]
struct WorkRootAuthority {
    shared: Arc<WorkRootAuthorityShared>,
}

struct WorkRootAuthorityShared {
    path: PathBuf,
    device: u64,
    inode: u64,
    directory_lock: File,
    lock_path: PathBuf,
    lock_file: File,
}

impl WorkRootAuthority {
    fn validate(&self) -> Result<(), ()> {
        let opened_root = self.shared.directory_lock.metadata().map_err(|_| ())?;
        let linked_root = fs::symlink_metadata(&self.shared.path).map_err(|_| ())?;
        if !safe_owned_directory(&opened_root)
            || !safe_owned_directory(&linked_root)
            || opened_root.dev() != self.shared.device
            || opened_root.ino() != self.shared.inode
            || linked_root.dev() != self.shared.device
            || linked_root.ino() != self.shared.inode
        {
            return Err(());
        }
        verify_lock_identity(&self.shared.lock_path, &self.shared.lock_file).map_err(|_| ())
    }

    fn work_root(&self) -> &Path {
        &self.shared.path
    }

    fn sync(&self) -> Result<(), ()> {
        self.shared.directory_lock.sync_all().map_err(|_| ())
    }
}

impl Drop for WorkRootAuthorityShared {
    fn drop(&mut self) {
        // A concurrent fork can retain the open file descriptions until exec,
        // so closing our descriptors alone does not release these locks promptly.
        let _ = FileExt::unlock(&self.lock_file);
        let _ = FileExt::unlock(&self.directory_lock);
    }
}

#[derive(Clone, Eq, PartialEq)]
struct AttemptRecord {
    assignment_id: String,
    run_id: String,
    attempt_id: String,
    disposition: RetentionReason,
}

impl AttemptRecord {
    fn new(assignment_id: &str, run_id: &str, attempt_id: &str) -> Result<Self, ()> {
        assignment_id
            .parse::<crate::runner_protocol::generated::AssignmentId>()
            .map_err(|_| ())?;
        run_id
            .parse::<crate::runner_protocol::generated::RunId>()
            .map_err(|_| ())?;
        attempt_id
            .parse::<crate::runner_protocol::generated::AttemptId>()
            .map_err(|_| ())?;
        Ok(Self {
            assignment_id: assignment_id.to_owned(),
            run_id: run_id.to_owned(),
            attempt_id: attempt_id.to_owned(),
            disposition: RetentionReason::OutcomeUnknown,
        })
    }

    fn encode(&self) -> Vec<u8> {
        format!(
            "{ATTEMPT_RECORD_HEADER}\nassignment-id={}\nrun-id={}\nattempt-id={}\ndisposition={}\n",
            self.assignment_id,
            self.run_id,
            self.attempt_id,
            self.disposition.as_str(),
        )
        .into_bytes()
    }

    fn decode(contents: &[u8]) -> Result<Self, ()> {
        let contents = std::str::from_utf8(contents).map_err(|_| ())?;
        let mut lines = contents.split('\n');
        if lines.next() != Some(ATTEMPT_RECORD_HEADER) {
            return Err(());
        }
        let assignment_id = parse_authority_field(&mut lines, "assignment-id=")?;
        let run_id = parse_authority_field(&mut lines, "run-id=")?;
        let attempt_id = parse_authority_field(&mut lines, "attempt-id=")?;
        let disposition =
            RetentionReason::parse(parse_authority_field(&mut lines, "disposition=")?).ok_or(())?;
        if lines.next() != Some("") || lines.next().is_some() {
            return Err(());
        }
        let mut record = Self::new(assignment_id, run_id, attempt_id)?;
        record.disposition = disposition;
        Ok(record)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RetainedWorkspace {
    pub(super) assignment_id: Option<String>,
    pub(super) run_id: Option<String>,
    pub(super) attempt_id: Option<String>,
    pub(super) path: PathBuf,
    pub(super) reason: RetentionReason,
}

struct CleanupAuthorityRecord {
    relative_path: String,
    parent_device: u64,
    parent_inode: u64,
    device: u64,
    inode: u64,
    marker_device: u64,
    marker_inode: u64,
    cleanup_identity: [u8; CLEANUP_IDENTITY_BYTES],
}

impl CleanupAuthorityRecord {
    fn for_tree(
        work_root: &Path,
        tree: &OwnedTree,
        cleanup_identity: [u8; CLEANUP_IDENTITY_BYTES],
    ) -> Result<Self, ()> {
        let relative_path = tree.path.strip_prefix(work_root).map_err(|_| ())?;
        let relative_path = relative_path.to_str().ok_or(())?.to_owned();
        validate_cleanup_target(&relative_path)?;
        let marker = tree.marker.as_ref().ok_or(())?;
        let link = tree.link().ok_or(())?;
        let parent = fstat(link.parent.as_ref()).map_err(|_| ())?;
        if !safe_owned_directory_stat(&parent) {
            return Err(());
        }
        Ok(Self {
            relative_path,
            parent_device: normalized_device(parent.st_dev),
            parent_inode: parent.st_ino,
            device: link.device,
            inode: link.inode,
            marker_device: marker.device,
            marker_inode: marker.inode,
            cleanup_identity,
        })
    }

    fn encode(&self) -> Vec<u8> {
        format!(
            "{CLEANUP_AUTHORITY_HEADER}\npath={}\nparent-device={}\nparent-inode={}\ndevice={}\ninode={}\nmarker-device={}\nmarker-inode={}\ncleanup-identity={}\n",
            self.relative_path,
            self.parent_device,
            self.parent_inode,
            self.device,
            self.inode,
            self.marker_device,
            self.marker_inode,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.cleanup_identity),
        )
        .into_bytes()
    }
}

fn parse_authority_field<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    prefix: &str,
) -> Result<&'a str, ()> {
    lines
        .next()
        .and_then(|line| line.strip_prefix(prefix))
        .ok_or(())
}

fn validate_cleanup_target(relative_path: &str) -> Result<(), ()> {
    let components = relative_path.split('/').collect::<Vec<_>>();
    match components.as_slice() {
        [boot] if valid_boot_id(boot) => Ok(()),
        [boot, assignment] if valid_boot_id(boot) && valid_assignment_id(assignment) => Ok(()),
        [boot, assignment, "workspace"]
            if valid_boot_id(boot) && valid_assignment_id(assignment) =>
        {
            Ok(())
        }
        _ => Err(()),
    }
}

fn valid_boot_id(value: &str) -> bool {
    value
        .parse::<crate::runner_protocol::generated::BootId>()
        .is_ok()
}

fn valid_assignment_id(value: &str) -> bool {
    value
        .parse::<crate::runner_protocol::generated::AssignmentId>()
        .is_ok()
}

struct CleanupAuthorityProof {
    path: PathBuf,
    contents: Vec<u8>,
    device: u64,
    inode: u64,
    record: CleanupAuthorityRecord,
}

impl CleanupAuthorityProof {
    fn create(
        authority: &WorkRootAuthority,
        tree: &OwnedTree,
        cleanup_identity_store: &dyn CleanupIdentityStore,
    ) -> Result<Self, ()> {
        authority.validate()?;
        let cleanup_identity = tree.prepare_cleanup_identity(cleanup_identity_store)?;
        let record =
            CleanupAuthorityRecord::for_tree(authority.work_root(), tree, cleanup_identity)?;
        let contents = record.encode();
        if u64::try_from(contents.len()).map_err(|_| ())? > MAXIMUM_CLEANUP_AUTHORITY_BYTES {
            return Err(());
        }
        let (staging_name, staging_path, final_name, final_path, file) =
            create_cleanup_authority_staging(authority)?;
        write_private_record(
            file.try_clone().map_err(|_| ())?,
            &contents,
            MAXIMUM_CLEANUP_AUTHORITY_BYTES,
        )?;
        authority.validate()?;
        verify_private_file_identity(&staging_path, &file)?;
        renameat_with(
            &authority.shared.directory_lock,
            &staging_name,
            &authority.shared.directory_lock,
            &final_name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|_| ())?;
        authority.sync()?;
        verify_private_file_identity(&final_path, &file)?;
        let metadata = file.metadata().map_err(|_| ())?;
        Ok(Self {
            path: final_path,
            contents,
            device: metadata.dev(),
            inode: metadata.ino(),
            record,
        })
    }

    fn validate(&self) -> Result<(), ()> {
        verify_private_contents(&self.path, &self.contents, self.device, self.inode)
    }

    fn clear(self, authority: &WorkRootAuthority) -> Result<(), ()> {
        authority.validate()?;
        self.validate()?;
        fs::remove_file(&self.path).map_err(|_| ())?;
        authority.sync()
    }
}

fn create_cleanup_authority_staging(
    authority: &WorkRootAuthority,
) -> Result<(String, PathBuf, String, PathBuf, File), ()> {
    for _ in 0..8 {
        let mut identity = [0_u8; 16];
        getrandom::fill(&mut identity).map_err(|_| ())?;
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(identity);
        let name = format!("{CLEANUP_AUTHORITY_STAGING_PREFIX}{encoded}");
        let path = authority.work_root().join(&name);
        let final_name = format!("{CLEANUP_AUTHORITY_PREFIX}{encoded}");
        let final_path = authority.work_root().join(&final_name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(PRIVATE_FILE_MODE)
            .custom_flags(NOFOLLOW_FLAG)
            .open(&path)
        {
            Ok(file) => {
                set_close_on_exec(&file)?;
                return Ok((name, path, final_name, final_path, file));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(()),
        }
    }
    Err(())
}

fn verify_private_file_identity(path: &Path, file: &File) -> Result<(), ()> {
    let opened = file.metadata().map_err(|_| ())?;
    let linked = fs::symlink_metadata(path).map_err(|_| ())?;
    (safe_private_file(&opened)
        && safe_private_file(&linked)
        && opened.dev() == linked.dev()
        && opened.ino() == linked.ino())
    .then_some(())
    .ok_or(())
}

#[derive(Clone)]
struct CleanupEngine {
    remover: Arc<dyn TreeRemover>,
    sleeper: Arc<dyn CleanupSleeper>,
    cleanup_identity: Arc<dyn CleanupIdentityStore>,
    cancellation: Arc<CleanupCancellation>,
    authority: WorkRootAuthority,
    serialized: Arc<Mutex<()>>,
}

impl CleanupEngine {
    fn remove(&self, tree: &OwnedTree) -> CleanupResult {
        let _serialized = self
            .serialized
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancellation.is_cancelled() {
            return CleanupResult::Preempted;
        }
        let proof = match CleanupAuthorityProof::create(
            &self.authority,
            tree,
            self.cleanup_identity.as_ref(),
        ) {
            Ok(proof) => proof,
            Err(()) => return CleanupResult::Quarantined(CleanupFailure::Safety),
        };
        self.remove_with_proof(tree, proof)
    }

    fn remove_with_proof(&self, tree: &OwnedTree, proof: CleanupAuthorityProof) -> CleanupResult {
        let mut delays = REMOVAL_DELAYS.into_iter();
        let mut attempted = false;
        loop {
            if self.authority.validate().is_err() || proof.validate().is_err() {
                return CleanupResult::Quarantined(CleanupFailure::Safety);
            }
            match tree.validate_during_cleanup(
                self.cleanup_identity.as_ref(),
                &proof.record.cleanup_identity,
            ) {
                Ok(TreePresence::Missing) => return self.finish_removal(tree, proof),
                Ok(TreePresence::Present) => {}
                Err(()) => return CleanupResult::Quarantined(CleanupFailure::Safety),
            }
            if attempted {
                let Some(delay) = delays.next() else {
                    return CleanupResult::Quarantined(CleanupFailure::OrdinaryRemovalExhausted);
                };
                if !self.sleeper.sleep(delay, &self.cancellation) {
                    return CleanupResult::Preempted;
                }
                attempted = false;
                continue;
            }
            if self.cancellation.is_cancelled() {
                return CleanupResult::Preempted;
            }
            if self
                .remover
                .remove_tree(tree)
                .is_err_and(|error| error.kind() == io::ErrorKind::InvalidData)
            {
                return CleanupResult::Quarantined(CleanupFailure::Safety);
            }
            attempted = true;
        }
    }

    fn finish_removal(&self, tree: &OwnedTree, proof: CleanupAuthorityProof) -> CleanupResult {
        if tree.sync_parent().is_err() {
            return CleanupResult::Quarantined(CleanupFailure::Safety);
        }
        match proof.clear(&self.authority) {
            Ok(()) => CleanupResult::Released,
            Err(()) => CleanupResult::Quarantined(CleanupFailure::Safety),
        }
    }
}

#[derive(Clone)]
struct MarkerProof {
    parent: Arc<OwnedFd>,
    contents: &'static [u8],
    device: u64,
    inode: u64,
}

impl MarkerProof {
    fn capture(parent: Arc<OwnedFd>, contents: &'static [u8]) -> Result<Self, ()> {
        let metadata = statat(
            parent.as_ref(),
            OWNERSHIP_MARKER_NAME,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| ())?;
        let proof = Self {
            parent,
            contents,
            device: normalized_device(metadata.st_dev),
            inode: metadata.st_ino,
        };
        verify_marker(&proof)?;
        Ok(proof)
    }

    fn is_missing(&self) -> bool {
        matches!(
            statat(
                self.parent.as_ref(),
                OWNERSHIP_MARKER_NAME,
                AtFlags::SYMLINK_NOFOLLOW,
            ),
            Err(Errno::NOENT)
        )
    }
}

#[derive(Clone)]
struct DirectoryLink {
    parent: Arc<OwnedFd>,
    identity: Arc<str>,
    directory: Option<Arc<OwnedFd>>,
    device: u64,
    inode: u64,
}

impl DirectoryLink {
    fn capture(parent: Arc<OwnedFd>, identity: Arc<str>) -> Result<Self, ()> {
        let observed = statat(
            parent.as_ref(),
            identity.as_ref(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| ())?;
        let directory = Arc::new(
            owned_tree::open_directory_at(parent.as_ref(), identity.as_ref()).map_err(|_| ())?,
        );
        let opened = fstat(directory.as_ref()).map_err(|_| ())?;
        let named = statat(
            parent.as_ref(),
            identity.as_ref(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| ())?;
        if !safe_owned_directory_stat(&observed)
            || !safe_owned_directory_stat(&opened)
            || !safe_owned_directory_stat(&named)
            || opened.st_dev != observed.st_dev
            || opened.st_ino != observed.st_ino
            || opened.st_dev != named.st_dev
            || opened.st_ino != named.st_ino
        {
            return Err(());
        }
        Ok(Self {
            parent,
            identity,
            directory: Some(directory),
            device: normalized_device(opened.st_dev),
            inode: opened.st_ino,
        })
    }

    fn validate(&self) -> Result<TreePresence, ()> {
        let parent = fstat(self.parent.as_ref()).map_err(|_| ())?;
        if !safe_owned_directory_stat(&parent) {
            return Err(());
        }
        let Some(directory) = &self.directory else {
            return match statat(
                self.parent.as_ref(),
                self.identity.as_ref(),
                AtFlags::SYMLINK_NOFOLLOW,
            ) {
                Err(Errno::NOENT) => Ok(TreePresence::Missing),
                _ => Err(()),
            };
        };
        let opened = fstat(directory.as_ref()).map_err(|_| ())?;
        if !safe_owned_directory_stat(&opened)
            || normalized_device(opened.st_dev) != self.device
            || opened.st_ino != self.inode
        {
            return Err(());
        }
        let named = match statat(
            self.parent.as_ref(),
            self.identity.as_ref(),
            AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(metadata) => metadata,
            Err(Errno::NOENT) => return Ok(TreePresence::Missing),
            Err(_) => return Err(()),
        };
        if !safe_owned_directory_stat(&named)
            || normalized_device(named.st_dev) != self.device
            || named.st_ino != self.inode
        {
            return Err(());
        }
        Ok(TreePresence::Present)
    }
}

#[derive(Clone)]
pub(super) struct OwnedTree {
    path: PathBuf,
    lineage: Vec<DirectoryLink>,
    marker: Option<MarkerProof>,
}

#[derive(Clone, Copy)]
enum TreePresence {
    Missing,
    Present,
}

impl OwnedTree {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    fn capture_root(parent: &File, path: PathBuf) -> Result<Self, ()> {
        let parent = Arc::new(fcntl_dupfd_cloexec(parent, 0).map_err(|_| ())?);
        Self::capture_with_parent(Vec::new(), parent, path)
    }

    fn capture_child(parent: &Self, path: PathBuf) -> Result<Self, ()> {
        if path.parent() != Some(parent.path()) {
            return Err(());
        }
        let directory = Arc::clone(parent.link().ok_or(())?.directory.as_ref().ok_or(())?);
        Self::capture_with_parent(parent.lineage.clone(), directory, path)
    }

    fn capture_with_parent(
        mut lineage: Vec<DirectoryLink>,
        parent: Arc<OwnedFd>,
        path: PathBuf,
    ) -> Result<Self, ()> {
        let identity = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(Arc::<str>::from)
            .ok_or(())?;
        lineage.push(DirectoryLink::capture(parent, identity)?);
        Ok(Self {
            path,
            lineage,
            marker: None,
        })
    }

    fn prepare_cleanup_identity(
        &self,
        cleanup_identity_store: &dyn CleanupIdentityStore,
    ) -> Result<[u8; CLEANUP_IDENTITY_BYTES], ()> {
        if !matches!(self.validate_linked_directory()?, TreePresence::Present) {
            return Err(());
        }
        self.validate_marker(false)?;
        let directory = self.directory()?;
        if let Some(identity) = cleanup_identity_store.read(directory.as_ref())? {
            sync_directory(directory.as_ref())?;
            return Ok(identity);
        }
        let mut identity = [0_u8; CLEANUP_IDENTITY_BYTES];
        getrandom::fill(&mut identity).map_err(|_| ())?;
        cleanup_identity_store.create(directory.as_ref(), &identity)?;
        sync_directory(directory.as_ref())?;
        (cleanup_identity_store.read(directory.as_ref())? == Some(identity))
            .then_some(identity)
            .ok_or(())
    }

    fn sync_parent(&self) -> Result<(), ()> {
        if !matches!(self.validate_linked_directory()?, TreePresence::Missing) {
            return Err(());
        }
        let parent = &self.link().ok_or(())?.parent;
        sync_directory(parent.as_ref())
    }

    fn validate_during_cleanup(
        &self,
        cleanup_identity_store: &dyn CleanupIdentityStore,
        cleanup_identity: &[u8; CLEANUP_IDENTITY_BYTES],
    ) -> Result<TreePresence, ()> {
        match self.validate_linked_directory()? {
            TreePresence::Missing => return Ok(TreePresence::Missing),
            TreePresence::Present => {}
        }
        if cleanup_identity_store.read(self.directory()?.as_ref())? != Some(*cleanup_identity) {
            return Err(());
        }
        self.validate_marker(true)?;
        Ok(TreePresence::Present)
    }

    fn link(&self) -> Option<&DirectoryLink> {
        self.lineage.last()
    }

    fn directory(&self) -> Result<&Arc<OwnedFd>, ()> {
        self.link().ok_or(())?.directory.as_ref().ok_or(())
    }

    fn install_marker(&mut self, marker: MarkerProof) {
        self.marker = Some(marker);
    }

    fn validate_linked_directory(&self) -> Result<TreePresence, ()> {
        let last = self.lineage.len().checked_sub(1).ok_or(())?;
        for (index, link) in self.lineage.iter().enumerate() {
            match link.validate()? {
                TreePresence::Present => {}
                TreePresence::Missing if index == last => return Ok(TreePresence::Missing),
                TreePresence::Missing => return Err(()),
            }
        }
        Ok(TreePresence::Present)
    }

    fn validate(&self, marker_may_be_missing: bool) -> Result<TreePresence, ()> {
        match self.validate_linked_directory()? {
            TreePresence::Missing => Ok(TreePresence::Missing),
            TreePresence::Present => {
                self.validate_marker(marker_may_be_missing)?;
                Ok(TreePresence::Present)
            }
        }
    }

    fn validate_marker(&self, may_be_missing: bool) -> Result<(), ()> {
        let marker = self.marker.as_ref().ok_or(())?;
        match verify_marker(marker) {
            Ok(()) => Ok(()),
            Err(()) if may_be_missing && marker.is_missing() => Ok(()),
            Err(()) => Err(()),
        }
    }
}

fn missing_cleanup_identity(error: Errno) -> bool {
    error == Errno::NODATA || {
        #[cfg(target_vendor = "apple")]
        {
            error == Errno::NOATTR
        }
        #[cfg(not(target_vendor = "apple"))]
        {
            false
        }
    }
}

fn sync_directory(directory: &OwnedFd) -> Result<(), ()> {
    let duplicate = fcntl_dupfd_cloexec(directory, 0).map_err(|_| ())?;
    File::from(duplicate).sync_all().map_err(|_| ())
}

fn safe_directory(path: &Path) -> Result<Metadata, ()> {
    let metadata = fs::symlink_metadata(path).map_err(|_| ())?;
    metadata.file_type().is_dir().then_some(metadata).ok_or(())
}

fn safe_owned_directory(metadata: &Metadata) -> bool {
    metadata.file_type().is_dir()
        && metadata.uid() == rustix::process::geteuid().as_raw()
        && metadata.mode() & 0o7777 == PRIVATE_DIRECTORY_MODE
}

#[allow(
    clippy::useless_conversion,
    reason = "st_mode is u16 on macOS and u32 on Linux"
)]
fn safe_owned_directory_stat(metadata: &Stat) -> bool {
    FileType::from_raw_mode(metadata.st_mode) == FileType::Directory
        && metadata.st_uid == rustix::process::geteuid().as_raw()
        && u32::from(metadata.st_mode) & 0o7777 == PRIVATE_DIRECTORY_MODE
}

fn verify_marker(marker: &MarkerProof) -> Result<(), ()> {
    let descriptor = openat(
        marker.parent.as_ref(),
        OWNERSHIP_MARKER_NAME,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ())?;
    let metadata = fstat(&descriptor).map_err(|_| ())?;
    let named = statat(
        marker.parent.as_ref(),
        OWNERSHIP_MARKER_NAME,
        AtFlags::SYMLINK_NOFOLLOW,
    )
    .map_err(|_| ())?;
    if !safe_private_file_stat(&metadata)
        || !safe_private_file_stat(&named)
        || named.st_dev != metadata.st_dev
        || named.st_ino != metadata.st_ino
        || normalized_device(metadata.st_dev) != marker.device
        || metadata.st_ino != marker.inode
    {
        return Err(());
    }
    let mut file = File::from(descriptor);
    let mut contents = Vec::with_capacity(marker.contents.len());
    std::io::Read::by_ref(&mut file)
        .take(u64::try_from(marker.contents.len()).map_err(|_| ())? + 1)
        .read_to_end(&mut contents)
        .map_err(|_| ())?;
    (contents == marker.contents).then_some(()).ok_or(())
}

fn verify_private_contents(
    path: &Path,
    expected: &[u8],
    device: u64,
    inode: u64,
) -> Result<(), ()> {
    let maximum = u64::try_from(expected.len()).map_err(|_| ())?;
    let (metadata, contents) = read_private_record(path, maximum)?;
    (metadata.dev() == device && metadata.ino() == inode && contents == expected)
        .then_some(())
        .ok_or(())
}

#[allow(
    clippy::useless_conversion,
    reason = "st_mode is u16 on macOS and u32 on Linux"
)]
fn safe_private_file_stat(metadata: &Stat) -> bool {
    FileType::from_raw_mode(metadata.st_mode) == FileType::RegularFile
        && metadata.st_uid == rustix::process::geteuid().as_raw()
        && u32::from(metadata.st_mode) & 0o7777 == PRIVATE_FILE_MODE
        && metadata.st_nlink == 1
}

fn safe_private_file(metadata: &Metadata) -> bool {
    metadata.file_type().is_file()
        && metadata.uid() == rustix::process::geteuid().as_raw()
        && metadata.mode() & 0o7777 == PRIVATE_FILE_MODE
        && metadata.nlink() == 1
}

fn discover_retained_workspaces(boot: &OwnedTree, retained: &mut Vec<RetainedWorkspace>) {
    let initial_count = retained.len();
    let Ok(directory) = boot.directory() else {
        retained.push(unknown_retained_root(boot.path()));
        return;
    };
    let Ok(children) = Dir::read_from(directory.as_ref()) else {
        retained.push(unknown_retained_root(boot.path()));
        return;
    };
    let mut assignment_ids = Vec::new();
    for child in children {
        let Ok(child) = child else {
            retained.push(unknown_retained_root(boot.path()));
            return;
        };
        let Ok(assignment_id) = std::str::from_utf8(child.file_name().to_bytes()) else {
            continue;
        };
        if valid_assignment_id(assignment_id) {
            assignment_ids.push(assignment_id.to_owned());
        }
    }
    for assignment_id in assignment_ids {
        let assignment_path = boot.path().join(&assignment_id);
        let Ok(mut assignment_tree) = OwnedTree::capture_child(boot, assignment_path.clone())
        else {
            retained.push(unknown_retained_root(&assignment_path));
            continue;
        };
        let Some(marker) = assignment_tree.directory().ok().and_then(|directory| {
            MarkerProof::capture(Arc::clone(directory), ASSIGNMENT_MARKER).ok()
        }) else {
            retained.push(unknown_retained_root(&assignment_path));
            continue;
        };
        assignment_tree.install_marker(marker);
        let Ok(record) = assignment_tree
            .directory()
            .and_then(|directory| read_attempt_record_at(directory.as_ref()))
        else {
            retained.push(unknown_retained_root(&assignment_path));
            continue;
        };
        let workspace_path = assignment_path.join("workspace");
        let Ok(workspace_tree) = OwnedTree::capture_child(&assignment_tree, workspace_path.clone())
        else {
            retained.push(unknown_retained_root(&assignment_path));
            continue;
        };
        if record.assignment_id != assignment_id
            || !matches!(assignment_tree.validate(false), Ok(TreePresence::Present))
            || !matches!(
                workspace_tree.validate_linked_directory(),
                Ok(TreePresence::Present)
            )
        {
            retained.push(unknown_retained_root(&assignment_path));
            continue;
        }
        retained.push(RetainedWorkspace {
            assignment_id: Some(record.assignment_id),
            run_id: Some(record.run_id),
            attempt_id: Some(record.attempt_id),
            path: workspace_path,
            reason: record.disposition,
        });
    }
    if retained.len() == initial_count {
        retained.push(unknown_retained_root(boot.path()));
    }
}

fn unknown_retained_root(path: &Path) -> RetainedWorkspace {
    RetainedWorkspace {
        assignment_id: None,
        run_id: None,
        attempt_id: None,
        path: path.to_owned(),
        reason: RetentionReason::OutcomeUnknown,
    }
}

pub(super) struct WorkRootLease {
    boot_tree: OwnedTree,
    engine: CleanupEngine,
    cancellation: Arc<CleanupCancellation>,
    retained: Arc<AtomicBool>,
    startup_retained: Vec<RetainedWorkspace>,
    workspace_release_spawner: Arc<dyn WorkspaceReleaseSpawner>,
    _authority: WorkRootAuthority,
}

impl WorkRootLease {
    pub(super) fn acquire(work_root: &Path, boot_id: &str) -> Result<Arc<Self>, WorkRootError> {
        Self::acquire_with(work_root, boot_id, WorkspaceFilesystem::system())
    }

    #[cfg(test)]
    pub(super) fn acquire_for_test(
        work_root: &Path,
        boot_id: &str,
    ) -> Result<Arc<Self>, WorkRootError> {
        Self::acquire_with(work_root, boot_id, WorkspaceFilesystem::testing())
    }

    pub(super) fn acquire_with(
        work_root: &Path,
        boot_id: &str,
        filesystem: WorkspaceFilesystem,
    ) -> Result<Arc<Self>, WorkRootError> {
        let directory_lock = open_work_root(work_root)?;
        match FileExt::try_lock(&directory_lock) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(WorkRootError::WorkRootInUse),
            Err(TryLockError::Error(_)) => return Err(WorkRootError::UnsafeWorkRoot),
        }
        let work_root_metadata = directory_lock
            .metadata()
            .map_err(|_| WorkRootError::UnsafeWorkRoot)?;
        let lock_path = work_root.join(LOCK_FILE_NAME);
        let lock_file = open_lock(&lock_path)?;
        match FileExt::try_lock(&lock_file) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(WorkRootError::WorkRootInUse),
            Err(TryLockError::Error(_)) => return Err(WorkRootError::UnsafeWorkRoot),
        }
        let authority = WorkRootAuthority {
            shared: Arc::new(WorkRootAuthorityShared {
                path: work_root.to_owned(),
                device: work_root_metadata.dev(),
                inode: work_root_metadata.ino(),
                directory_lock,
                lock_path,
                lock_file,
            }),
        };
        authority
            .validate()
            .map_err(|()| WorkRootError::UnsafeWorkRoot)?;
        let cancellation = Arc::new(CleanupCancellation::default());
        let engine = CleanupEngine {
            remover: filesystem.remover,
            sleeper: filesystem.sleeper,
            cleanup_identity: filesystem.cleanup_identity,
            cancellation: Arc::clone(&cancellation),
            authority: authority.clone(),
            serialized: Arc::new(Mutex::new(())),
        };

        filesystem.hook.before_child_enumeration();
        let children = fs::read_dir(work_root)
            .map_err(|_| WorkRootError::UnsafeWorkRoot)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| WorkRootError::UnsafeWorkRoot)?;
        let mut startup_retained = Vec::new();
        for child in children {
            let name = child.file_name();
            if name == LOCK_FILE_NAME {
                continue;
            }
            let Some(name) = name.to_str() else {
                continue;
            };
            if name
                .parse::<crate::runner_protocol::generated::BootId>()
                .is_err()
            {
                continue;
            }
            let path = work_root.join(name);
            let tree = OwnedTree::capture_root(&authority.shared.directory_lock, path.clone());
            let Some(mut tree) = tree.ok() else {
                startup_retained.push(unknown_retained_root(&path));
                continue;
            };
            let marker = tree.directory().ok().and_then(|directory| {
                MarkerProof::capture(Arc::clone(directory), BOOT_MARKER).ok()
            });
            let Some(marker) = marker else {
                startup_retained.push(unknown_retained_root(&path));
                continue;
            };
            tree.install_marker(marker);
            discover_retained_workspaces(&tree, &mut startup_retained);
        }

        authority
            .validate()
            .map_err(|()| WorkRootError::UnsafeWorkRoot)?;
        let boot_path = work_root.join(boot_id);
        create_private_directory(&boot_path).map_err(|_| WorkRootError::CreateBootRoot)?;
        let mut boot_tree = OwnedTree::capture_root(&authority.shared.directory_lock, boot_path)
            .map_err(|_| WorkRootError::CreateBootRoot)?;
        let marker =
            create_marker(&boot_tree, BOOT_MARKER).map_err(|_| WorkRootError::CreateBootRoot)?;
        boot_tree.install_marker(marker);
        Ok(Arc::new(Self {
            boot_tree,
            engine,
            cancellation,
            retained: Arc::new(AtomicBool::new(false)),
            startup_retained,
            workspace_release_spawner: filesystem.workspace_release_spawner,
            _authority: authority,
        }))
    }

    pub(super) fn create_assignment_for_attempt(
        &self,
        assignment_id: &str,
        run_id: &str,
        attempt_id: &str,
        recorder: Option<Arc<crate::runner::telemetry::Recorder>>,
    ) -> Result<AssignmentRoot, AssignmentRootCreationError> {
        let attempt_record = AttemptRecord::new(assignment_id, run_id, attempt_id)
            .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
        let assignment_path = self.boot_tree.path.join(assignment_id);
        create_private_directory(&assignment_path)
            .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
        let mut assignment_tree =
            OwnedTree::capture_child(&self.boot_tree, assignment_path.clone())
                .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
        let assignment_marker = create_marker(&assignment_tree, ASSIGNMENT_MARKER)
            .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
        assignment_tree.install_marker(assignment_marker.clone());
        if create_attempt_record(&assignment_tree, &attempt_record).is_err() {
            return Err(assignment_creation_failure(
                self.engine.remove(&assignment_tree),
            ));
        }
        let private_path = assignment_path.join("private");
        let workspace_path = assignment_path.join("workspace");
        if create_private_directory(&private_path).is_err()
            || create_private_directory(&workspace_path).is_err()
        {
            return Err(assignment_creation_failure(
                self.engine.remove(&assignment_tree),
            ));
        }
        let workspace_tree = match OwnedTree::capture_child(&assignment_tree, workspace_path) {
            Ok(mut tree) => {
                tree.install_marker(assignment_marker);
                tree
            }
            Err(()) => {
                return Err(assignment_creation_failure(
                    self.engine.remove(&assignment_tree),
                ));
            }
        };
        Ok(AssignmentRoot {
            assignment_tree,
            execution: workspace_tree.path.clone(),
            private: PrivateStaging { path: private_path },
            workspace: WorkspaceLease::new(workspace_tree, self.engine.clone()),
            workflow_git: None,
            attempt_record,
            recorder,
            retained: Arc::clone(&self.retained),
            engine: self.engine.clone(),
            workspace_release_spawner: Arc::clone(&self.workspace_release_spawner),
            workspace_release: Arc::new(AssignmentReleaseState {
                started: AtomicBool::new(false),
                completion: ReleaseCompletion::new(),
            }),
            release: Arc::new(AssignmentReleaseState {
                started: AtomicBool::new(false),
                completion: ReleaseCompletion::new(),
            }),
        })
    }

    #[cfg(test)]
    pub(super) fn create_assignment(
        &self,
        assignment_id: &str,
    ) -> Result<AssignmentRoot, AssignmentRootCreationError> {
        self.create_assignment_for_attempt(
            assignment_id,
            "run_01k0z6r1w8f4jy2m7q9v3x5abc",
            "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
            None,
        )
    }

    pub(super) fn release_boot_root_pending(&self) -> PendingRelease {
        let completion = ReleaseCompletion::new();
        let pending = completion.pending();
        if self.retained.load(Ordering::Acquire) {
            completion.complete(CleanupResult::Retained);
            return pending;
        }
        let worker_completion = completion.clone();
        let tree = self.boot_tree.clone();
        let engine = self.engine.clone();
        if std::thread::Builder::new()
            .name("runner-boot-root-release".to_owned())
            .spawn(move || {
                let result = engine.remove(&tree);
                drop(engine);
                drop(tree);
                worker_completion.complete(result);
            })
            .is_err()
        {
            completion.complete(CleanupResult::Quarantined(CleanupFailure::Safety));
        }
        pending
    }

    pub(super) fn cancel_cleanup(&self) {
        self.cancellation.cancel();
    }

    pub(super) fn startup_retained(&self) -> &[RetainedWorkspace] {
        &self.startup_retained
    }

    #[cfg(test)]
    pub(super) fn boot_path(&self) -> &Path {
        &self.boot_tree.path
    }
}

fn assignment_creation_failure(result: CleanupResult) -> AssignmentRootCreationError {
    match result {
        CleanupResult::Released => AssignmentRootCreationError::Unavailable,
        CleanupResult::Retained | CleanupResult::Quarantined(_) | CleanupResult::Preempted => {
            AssignmentRootCreationError::CleanupFailed
        }
    }
}

fn open_work_root(path: &Path) -> Result<File, WorkRootError> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(DIRECTORY_NOFOLLOW_FLAGS)
        .open(path)
        .map_err(|_| WorkRootError::UnsafeWorkRoot)?;
    set_close_on_exec(&directory).map_err(|()| WorkRootError::UnsafeWorkRoot)?;
    let metadata = directory
        .metadata()
        .map_err(|_| WorkRootError::UnsafeWorkRoot)?;
    let linked = fs::symlink_metadata(path).map_err(|_| WorkRootError::UnsafeWorkRoot)?;
    if !safe_owned_directory(&metadata)
        || !safe_owned_directory(&linked)
        || metadata.dev() != linked.dev()
        || metadata.ino() != linked.ino()
    {
        return Err(WorkRootError::UnsafeWorkRoot);
    }
    Ok(directory)
}

fn open_lock(path: &Path) -> Result<File, WorkRootError> {
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
    {
        Ok(file) => {
            file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))
                .map_err(|_| WorkRootError::UnsafeWorkRoot)?;
            file
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(NOFOLLOW_FLAG)
            .open(path)
            .map_err(|_| WorkRootError::UnsafeWorkRoot)?,
        Err(_) => return Err(WorkRootError::UnsafeWorkRoot),
    };
    set_close_on_exec(&file).map_err(|_| WorkRootError::UnsafeWorkRoot)?;
    if !safe_private_file(&file.metadata().map_err(|_| WorkRootError::UnsafeWorkRoot)?) {
        return Err(WorkRootError::UnsafeWorkRoot);
    }
    Ok(file)
}

fn verify_lock_identity(path: &Path, file: &File) -> Result<(), WorkRootError> {
    let opened = file.metadata().map_err(|_| WorkRootError::UnsafeWorkRoot)?;
    let linked = fs::symlink_metadata(path).map_err(|_| WorkRootError::UnsafeWorkRoot)?;
    if opened.dev() != linked.dev() || opened.ino() != linked.ino() || !safe_private_file(&linked) {
        return Err(WorkRootError::UnsafeWorkRoot);
    }
    Ok(())
}

fn set_close_on_exec(file: &File) -> Result<(), ()> {
    fcntl(file, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))
        .map(|_| ())
        .map_err(|_| ())
}

fn create_private_directory(path: &Path) -> Result<(), ()> {
    fs::create_dir(path).map_err(|_| ())?;
    fs::set_permissions(path, Permissions::from_mode(PRIVATE_DIRECTORY_MODE)).map_err(|_| ())?;
    let metadata = safe_directory(path)?;
    safe_owned_directory(&metadata).then_some(()).ok_or(())
}

fn create_marker(parent: &OwnedTree, contents: &'static [u8]) -> Result<MarkerProof, ()> {
    let directory = Arc::clone(parent.directory()?);
    let descriptor = openat(
        directory.as_ref(),
        OWNERSHIP_MARKER_NAME,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| ())?;
    let mut marker = File::from(descriptor);
    marker
        .set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))
        .and_then(|()| marker.write_all(contents))
        .and_then(|()| marker.sync_all())
        .map_err(|_| ())?;
    set_close_on_exec(&marker)?;
    MarkerProof::capture(directory, contents)
}

fn create_attempt_record(tree: &OwnedTree, record: &AttemptRecord) -> Result<(), ()> {
    let directory = tree.directory()?;
    let descriptor = openat(
        directory.as_ref(),
        ATTEMPT_RECORD_NAME,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| ())?;
    write_private_record(
        File::from(descriptor),
        &record.encode(),
        MAXIMUM_ATTEMPT_RECORD_BYTES,
    )?;
    sync_directory(directory.as_ref())
}

fn retain_attempt_record(
    tree: &OwnedTree,
    record: &AttemptRecord,
    reason: RetentionReason,
) -> Result<(), ()> {
    if !matches!(tree.validate(false)?, TreePresence::Present)
        || read_attempt_record_at(tree.directory()?.as_ref())? != *record
    {
        return Err(());
    }
    let mut retained = record.clone();
    retained.disposition = reason;
    let contents = retained.encode();
    let directory = tree.directory()?;
    let (staging_name, _file) = create_record_staging(
        directory.as_ref(),
        ATTEMPT_RECORD_STAGING_PREFIX,
        &contents,
        MAXIMUM_ATTEMPT_RECORD_BYTES,
    )?;
    renameat_with(
        directory.as_ref(),
        &staging_name,
        directory.as_ref(),
        ATTEMPT_RECORD_NAME,
        RenameFlags::empty(),
    )
    .map_err(|_| ())?;
    sync_directory(directory.as_ref())
}

fn create_record_staging(
    directory: &OwnedFd,
    prefix: &str,
    contents: &[u8],
    maximum_bytes: u64,
) -> Result<(String, File), ()> {
    for _ in 0..8 {
        let mut identity = [0_u8; 16];
        getrandom::fill(&mut identity).map_err(|_| ())?;
        let name = format!(
            "{prefix}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(identity)
        );
        match openat(
            directory,
            &name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(descriptor) => {
                let file = File::from(descriptor);
                write_private_record(file.try_clone().map_err(|_| ())?, contents, maximum_bytes)?;
                return Ok((name, file));
            }
            Err(Errno::EXIST) => {}
            Err(_) => return Err(()),
        }
    }
    Err(())
}

fn write_private_record(mut file: File, contents: &[u8], maximum_bytes: u64) -> Result<(), ()> {
    if u64::try_from(contents.len()).map_err(|_| ())? > maximum_bytes {
        return Err(());
    }
    file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))
        .and_then(|()| file.write_all(contents))
        .and_then(|()| file.sync_all())
        .map_err(|_| ())
}

fn read_attempt_record_at(directory: &OwnedFd) -> Result<AttemptRecord, ()> {
    let contents =
        read_private_record_at(directory, ATTEMPT_RECORD_NAME, MAXIMUM_ATTEMPT_RECORD_BYTES)?;
    AttemptRecord::decode(&contents)
}

fn read_private_record_at(
    directory: &OwnedFd,
    name: &str,
    maximum_bytes: u64,
) -> Result<Vec<u8>, ()> {
    let (descriptor, metadata) =
        owned_tree::open_regular_file_at(directory, name).map_err(|_| ())?;
    if !safe_private_file_stat(&metadata) {
        return Err(());
    }
    let mut contents = Vec::new();
    std::io::Read::by_ref(&mut File::from(descriptor))
        .take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut contents)
        .map_err(|_| ())?;
    if u64::try_from(contents.len()).map_err(|_| ())? > maximum_bytes {
        return Err(());
    }
    Ok(contents)
}

fn read_private_record(path: &Path, maximum_bytes: u64) -> Result<(Metadata, Vec<u8>), ()> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
        .map_err(|_| ())?;
    set_close_on_exec(&file)?;
    let metadata = file.metadata().map_err(|_| ())?;
    let linked = fs::symlink_metadata(path).map_err(|_| ())?;
    if !safe_private_file(&metadata)
        || linked.dev() != metadata.dev()
        || linked.ino() != metadata.ino()
    {
        return Err(());
    }
    let mut contents = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut contents)
        .map_err(|_| ())?;
    if u64::try_from(contents.len()).map_err(|_| ())? > maximum_bytes {
        return Err(());
    }
    Ok((metadata, contents))
}

#[derive(Clone)]
pub(super) struct WorkspaceLease {
    state: Arc<WorkspaceLeaseState>,
}

struct WorkspaceLeaseState {
    path: PathBuf,
    release: Mutex<WorkspaceReleaseState>,
    completion: ReleaseCompletion,
    engine: CleanupEngine,
}

struct WorkspaceReleaseState {
    tree: Option<OwnedTree>,
    started: bool,
}

impl WorkspaceLease {
    fn new(tree: OwnedTree, engine: CleanupEngine) -> Self {
        let path = tree.path.clone();
        Self {
            state: Arc::new(WorkspaceLeaseState {
                path,
                release: Mutex::new(WorkspaceReleaseState {
                    tree: Some(tree),
                    started: false,
                }),
                completion: ReleaseCompletion::new(),
                engine,
            }),
        }
    }

    pub(super) fn path(&self) -> PathBuf {
        self.state.path.clone()
    }

    pub(super) fn release_pending(&self, quiescence: ProcessQuiescence) -> PendingRelease {
        let (pending, tree) = self.claim_release(quiescence);
        let Some(tree) = tree else {
            return pending;
        };
        let state = Arc::clone(&self.state);
        if std::thread::Builder::new()
            .name("runner-workspace-release".to_owned())
            .spawn(move || state.completion.complete(state.engine.remove(&tree)))
            .is_err()
        {
            self.state
                .completion
                .complete(CleanupResult::Quarantined(CleanupFailure::Safety));
        }
        pending
    }

    fn retain_pending(&self) -> PendingRelease {
        let pending = self.state.completion.pending();
        let mut release = self
            .state
            .release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !release.started {
            release.started = true;
            drop(release.tree.take());
            self.state.completion.complete(CleanupResult::Retained);
        }
        pending
    }

    fn retain_and_complete(self) -> CleanupResult {
        let completion = self.state.completion.clone();
        let pending = completion.pending();
        let mut release = self
            .state
            .release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if release.started {
            drop(release);
            drop(self);
            return pending.wait();
        }
        release.started = true;
        drop(release.tree.take());
        drop(release);
        drop(self);
        completion.complete(CleanupResult::Retained);
        CleanupResult::Retained
    }

    fn claim_release(&self, quiescence: ProcessQuiescence) -> (PendingRelease, Option<OwnedTree>) {
        let pending = self.state.completion.pending();
        let tree = {
            let mut release = self
                .state
                .release
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if release.started {
                return (pending, None);
            }
            release.started = true;
            release.tree.take()
        };
        let Some(tree) = tree else {
            self.state.completion.complete(CleanupResult::Released);
            return (pending, None);
        };
        if quiescence == ProcessQuiescence::Failed {
            self.state
                .completion
                .complete(CleanupResult::Quarantined(CleanupFailure::Quiescence));
            return (pending, None);
        }
        (pending, Some(tree))
    }
}

#[derive(Clone)]
pub(super) struct PrivateStaging {
    path: PathBuf,
}

impl PrivateStaging {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl std::ops::Deref for PrivateStaging {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl AsRef<Path> for PrivateStaging {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

struct AssignmentReleaseState {
    started: AtomicBool,
    completion: ReleaseCompletion,
}

#[derive(Clone)]
pub(super) struct AssignmentRoot {
    assignment_tree: OwnedTree,
    pub(super) execution: PathBuf,
    pub(super) private: PrivateStaging,
    pub(super) workspace: WorkspaceLease,
    workflow_git: Option<WorkflowGitAuthority>,
    attempt_record: AttemptRecord,
    recorder: Option<Arc<crate::runner::telemetry::Recorder>>,
    retained: Arc<AtomicBool>,
    engine: CleanupEngine,
    workspace_release_spawner: Arc<dyn WorkspaceReleaseSpawner>,
    workspace_release: Arc<AssignmentReleaseState>,
    release: Arc<AssignmentReleaseState>,
}

impl AssignmentRoot {
    pub(super) fn install_workflow_git(&mut self, authority: WorkflowGitAuthority) {
        self.workflow_git = Some(authority);
    }

    pub(super) fn workflow_git(&self) -> Option<WorkflowGitAuthority> {
        self.workflow_git.clone()
    }

    pub(super) fn release_workspace_pending(
        &self,
        quiescence: ProcessQuiescence,
        disposition: WorkspaceDisposition,
    ) -> PendingRelease {
        let pending = self.workspace_release.completion.pending();
        if self
            .workspace_release
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return pending;
        }
        let authority = self.workflow_git.clone();
        let worker_authority = authority.clone();
        let workspace = self.workspace.clone();
        let completion = self.workspace_release.completion.clone();
        let worker_completion = completion.clone();
        let assignment_tree = self.assignment_tree.clone();
        let attempt_record = self.attempt_record.clone();
        let recorder = self.recorder.clone();
        let retained = Arc::clone(&self.retained);
        if self
            .workspace_release_spawner
            .spawn(Box::new(move || {
                let report = worker_authority
                    .map_or_else(WorkflowGitTeardownReport::no_issuance, |authority| {
                        authority.teardown(quiescence)
                    });
                let disposition = effective_disposition(disposition, quiescence, &report);
                let result = match disposition {
                    WorkspaceDisposition::Remove => {
                        let result = workspace.release_pending(quiescence).wait();
                        drop(workspace);
                        result
                    }
                    WorkspaceDisposition::Retain(reason) => {
                        retained.store(true, Ordering::Release);
                        let recorded =
                            retain_attempt_record(&assignment_tree, &attempt_record, reason)
                                .is_ok();
                        record_retention(
                            recorder.as_ref(),
                            &attempt_record,
                            workspace.path(),
                            reason,
                            quiescence,
                            &report,
                            recorded,
                        );
                        workspace.retain_and_complete()
                    }
                };
                drop(assignment_tree);
                worker_completion.complete(result);
            }))
            .is_err()
        {
            self.retained.store(true, Ordering::Release);
            let report = authority
                .map_or_else(WorkflowGitTeardownReport::no_issuance, |authority| {
                    authority.teardown(quiescence)
                });
            let reason = match effective_disposition(disposition, quiescence, &report) {
                WorkspaceDisposition::Retain(reason) => reason,
                WorkspaceDisposition::Remove => RetentionReason::ReleaseWorkerUnavailable,
            };
            let recorded =
                retain_attempt_record(&self.assignment_tree, &self.attempt_record, reason).is_ok();
            record_retention(
                self.recorder.as_ref(),
                &self.attempt_record,
                self.workspace.path(),
                reason,
                quiescence,
                &report,
                recorded,
            );
            self.workspace.retain_pending();
            completion.complete(CleanupResult::Retained);
        }
        pending
    }

    pub(super) fn release_pending(
        &self,
        quiescence: ProcessQuiescence,
        disposition: WorkspaceDisposition,
    ) -> PendingRelease {
        let pending = self.release.completion.pending();
        if self
            .release
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return pending;
        }
        let workspace = self.release_workspace_pending(quiescence, disposition);
        let assignment_tree = self.assignment_tree.clone();
        let engine = self.engine.clone();
        let completion = self.release.completion.clone();
        let worker_completion = completion.clone();
        if std::thread::Builder::new()
            .name("runner-assignment-root-release".to_owned())
            .spawn(move || {
                let result = match workspace.wait() {
                    CleanupResult::Released => engine.remove(&assignment_tree),
                    CleanupResult::Retained => CleanupResult::Retained,
                    failure => failure,
                };
                drop(engine);
                drop(assignment_tree);
                worker_completion.complete(result);
            })
            .is_err()
        {
            completion.complete(CleanupResult::Quarantined(CleanupFailure::Safety));
        }
        pending
    }
}

fn effective_disposition(
    disposition: WorkspaceDisposition,
    quiescence: ProcessQuiescence,
    credentials: &WorkflowGitTeardownReport,
) -> WorkspaceDisposition {
    match disposition {
        WorkspaceDisposition::Retain(reason) => WorkspaceDisposition::Retain(reason),
        WorkspaceDisposition::Remove if quiescence == ProcessQuiescence::Failed => {
            WorkspaceDisposition::Retain(RetentionReason::ProcessStopFailed)
        }
        WorkspaceDisposition::Remove if !credentials.succeeded() => {
            WorkspaceDisposition::Retain(RetentionReason::CredentialTeardownFailed)
        }
        WorkspaceDisposition::Remove => WorkspaceDisposition::Remove,
    }
}

fn record_retention(
    recorder: Option<&Arc<crate::runner::telemetry::Recorder>>,
    attempt: &AttemptRecord,
    path: PathBuf,
    reason: RetentionReason,
    quiescence: ProcessQuiescence,
    credentials: &WorkflowGitTeardownReport,
    disposition_recorded: bool,
) {
    let Some(recorder) = recorder else {
        return;
    };
    recorder.record(
        "runner.workspace_retained",
        [
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::ASSIGNMENT_ID,
                attempt.assignment_id.clone(),
            ),
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::RUN_ID,
                attempt.run_id.clone(),
            ),
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::ATTEMPT_ID,
                attempt.attempt_id.clone(),
            ),
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::WORKSPACE_PATH,
                path.to_string_lossy().into_owned(),
            ),
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::RETENTION_REASON,
                reason.as_str(),
            ),
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::OWNED_PROCESSES_STOPPED,
                quiescence == ProcessQuiescence::Proven,
            ),
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::RUNNER_CREDENTIALS_REMOVED,
                credentials.local_state_destroyed,
            ),
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::RUNNER_CREDENTIALS_REVOKED,
                credentials.revocation_succeeded(),
            ),
            opentelemetry::KeyValue::new(
                crate::runner::telemetry::attribute::RETENTION_RECORDED,
                disposition_recorded,
            ),
        ],
    );
}

struct CompletionState {
    result: Mutex<Option<CleanupResult>>,
    changed: Condvar,
    async_changed: tokio::sync::Notify,
}

#[derive(Clone)]
struct ReleaseCompletion {
    state: Arc<CompletionState>,
}

impl ReleaseCompletion {
    fn new() -> Self {
        Self {
            state: Arc::new(CompletionState {
                result: Mutex::new(None),
                changed: Condvar::new(),
                async_changed: tokio::sync::Notify::new(),
            }),
        }
    }

    fn pending(&self) -> PendingRelease {
        PendingRelease {
            completion: self.clone(),
        }
    }

    fn complete(&self, result: CleanupResult) {
        let mut retained = self
            .state
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if retained.is_none() {
            *retained = Some(result);
            self.state.changed.notify_all();
            self.state.async_changed.notify_waiters();
        }
    }
}

#[derive(Clone)]
pub(super) struct PendingRelease {
    completion: ReleaseCompletion,
}

impl PendingRelease {
    pub(super) fn wait(&self) -> CleanupResult {
        let retained = self
            .completion
            .state
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = self
            .completion
            .state
            .changed
            .wait_while(retained, |result| result.is_none())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match *result {
            Some(result) => result,
            None => CleanupResult::Quarantined(CleanupFailure::Safety),
        }
    }

    pub(super) async fn wait_async(&self) -> CleanupResult {
        loop {
            let notified = self.completion.state.async_changed.notified();
            tokio::pin!(notified);
            if let Some(result) = *self
                .completion
                .state
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
            {
                return result;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::Read as _;
    use std::os::unix::fs::symlink;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const BOOT_A: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abc";
    const BOOT_B: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abd";
    const ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abc";

    #[derive(Clone, Copy)]
    enum RemovalOutcome {
        Error,
        NotFound,
        Success,
        Partial,
    }

    struct ScriptedRemover {
        outcomes: Mutex<VecDeque<RemovalOutcome>>,
        calls: AtomicUsize,
    }

    impl ScriptedRemover {
        fn new(outcomes: impl IntoIterator<Item = RemovalOutcome>) -> Arc<Self> {
            Arc::new(Self {
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                calls: AtomicUsize::new(0),
            })
        }
    }

    impl TreeRemover for ScriptedRemover {
        fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()> {
            let path = tree.path();
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self
                .outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(RemovalOutcome::Success)
            {
                RemovalOutcome::Error => Err(io::Error::other("injected removal failure")),
                RemovalOutcome::NotFound => Err(io::Error::from(io::ErrorKind::NotFound)),
                RemovalOutcome::Success => fs::remove_dir_all(path),
                RemovalOutcome::Partial => {
                    let _ = fs::remove_file(path.join(OWNERSHIP_MARKER_NAME));
                    if let Ok(entries) = fs::read_dir(path) {
                        for entry in entries.flatten() {
                            if entry.file_name() != OWNERSHIP_MARKER_NAME {
                                let child = entry.path();
                                let _ = if child.is_dir() {
                                    fs::remove_dir_all(child)
                                } else {
                                    fs::remove_file(child)
                                };
                                break;
                            }
                        }
                    }
                    Err(io::Error::other("injected partial removal"))
                }
            }
        }
    }

    struct ReplacingRemover {
        calls: AtomicUsize,
    }

    impl TreeRemover for ReplacingRemover {
        fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            fs::remove_dir_all(tree.path())?;
            create_private_directory(tree.path()).map_err(|()| io::Error::other("replace tree"))?;
            fs::write(
                tree.path().join("replacement-sentinel"),
                b"unproven replacement",
            )?;
            Err(io::Error::other("tree replaced during removal"))
        }
    }

    struct ReplacingAndDeletingRemover;

    impl TreeRemover for ReplacingAndDeletingRemover {
        fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()> {
            let parent = tree
                .path()
                .parent()
                .ok_or_else(|| io::Error::other("tree has no parent"))?;
            fs::rename(tree.path(), parent.join("original-moved-by-racer"))?;
            fs::create_dir(tree.path())?;
            fs::set_permissions(tree.path(), Permissions::from_mode(0o700))?;
            fs::write(
                tree.path().join("replacement-sentinel"),
                b"unproven replacement",
            )?;
            SystemTreeRemover.remove_tree(tree)
        }
    }

    struct RejectingWorkspaceReleaseSpawner;

    impl WorkspaceReleaseSpawner for RejectingWorkspaceReleaseSpawner {
        fn spawn(&self, _task: Box<dyn FnOnce() + Send + 'static>) -> io::Result<()> {
            Err(io::Error::other("injected workspace release spawn failure"))
        }
    }

    #[derive(Default)]
    struct RecordingSleeper {
        delays: Mutex<Vec<Duration>>,
    }

    impl CleanupSleeper for RecordingSleeper {
        fn sleep(&self, duration: Duration, cancellation: &CleanupCancellation) -> bool {
            self.delays.lock().unwrap().push(duration);
            !cancellation.is_cancelled()
        }
    }

    struct CancellationSleeper {
        started: std::sync::mpsc::SyncSender<()>,
    }

    impl CleanupSleeper for CancellationSleeper {
        fn sleep(&self, _duration: Duration, cancellation: &CleanupCancellation) -> bool {
            let _ = self.started.send(());
            cancellation.wait(Duration::from_secs(60))
        }
    }

    struct MarkerMutatingSleeper {
        marker: PathBuf,
    }

    impl CleanupSleeper for MarkerMutatingSleeper {
        fn sleep(&self, _duration: Duration, cancellation: &CleanupCancellation) -> bool {
            fs::write(&self.marker, b"changed-owner\n").unwrap();
            !cancellation.is_cancelled()
        }
    }

    struct SubstitutingRemover {
        displaced: PathBuf,
        outside: PathBuf,
        calls: AtomicUsize,
    }

    impl TreeRemover for SubstitutingRemover {
        fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()> {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                fs::rename(tree.path(), &self.displaced)?;
                symlink(&self.outside, tree.path())?;
            }
            SystemTreeRemover.remove_tree(tree)
        }
    }

    struct AncestorSubstitutingRemover {
        ancestor: PathBuf,
        displaced: PathBuf,
    }

    impl TreeRemover for AncestorSubstitutingRemover {
        fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()> {
            fs::rename(&self.ancestor, &self.displaced)?;
            symlink(&self.displaced, &self.ancestor)?;
            SystemTreeRemover.remove_tree(tree)
        }
    }

    #[derive(Default)]
    struct CountingHook(AtomicUsize);

    impl WorkRootHook for CountingHook {
        fn before_child_enumeration(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn private_work_root() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), Permissions::from_mode(PRIVATE_DIRECTORY_MODE)).unwrap();
        root
    }

    fn filesystem(
        remover: Arc<dyn TreeRemover>,
        sleeper: Arc<dyn CleanupSleeper>,
        hook: Arc<dyn WorkRootHook>,
    ) -> WorkspaceFilesystem {
        WorkspaceFilesystem::injected(remover, sleeper, hook)
    }

    fn owner_with_remover(root: &Path, remover: Arc<dyn TreeRemover>) -> Arc<WorkRootLease> {
        WorkRootLease::acquire_with(
            root,
            BOOT_A,
            filesystem(
                remover,
                Arc::new(RecordingSleeper::default()),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap()
    }

    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
    }

    fn assignment_with_retained_file(owner: &WorkRootLease) -> AssignmentRoot {
        let assignment = owner.create_assignment(ASSIGNMENT).unwrap();
        fs::write(assignment.workspace.path().join("owned"), b"retained").unwrap();
        assignment
    }

    fn release_workspace(assignment: &AssignmentRoot) -> CleanupResult {
        assignment
            .workspace
            .release_pending(ProcessQuiescence::Proven)
            .wait()
    }

    fn spawn_ready_helper_child() -> Child {
        let mut child = Command::new("sh")
            .args(["-c", "printf ready; while :; do sleep 60; done"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut ready = [0_u8; 5];
        child
            .stdout
            .as_mut()
            .unwrap()
            .read_exact(&mut ready)
            .unwrap();
        assert_eq!(&ready, b"ready");
        child
    }

    #[test]
    fn contention_precedes_enumeration_and_independent_roots_remain_usable() {
        let shared = private_work_root();
        let other = private_work_root();
        let first_hook = Arc::new(CountingHook::default());
        let first = WorkRootLease::acquire_with(
            shared.path(),
            BOOT_A,
            filesystem(
                Arc::new(SystemTreeRemover),
                Arc::new(InterruptibleSleeper),
                first_hook.clone(),
            ),
        )
        .unwrap();
        fs::write(first.boot_path().join("unchanged"), b"owned").unwrap();
        let blocked_hook = Arc::new(CountingHook::default());
        let blocked_remover = ScriptedRemover::new([]);
        assert_eq!(
            WorkRootLease::acquire_with(
                shared.path(),
                BOOT_B,
                filesystem(
                    blocked_remover.clone(),
                    Arc::new(RecordingSleeper::default()),
                    blocked_hook.clone(),
                ),
            )
            .err()
            .unwrap(),
            WorkRootError::WorkRootInUse
        );
        assert_eq!(blocked_hook.0.load(Ordering::Relaxed), 0);
        assert_eq!(blocked_remover.calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            fs::read(first.boot_path().join("unchanged")).unwrap(),
            b"owned"
        );
        let independent = WorkRootLease::acquire(other.path(), BOOT_B).unwrap();
        assert_eq!(independent.boot_path(), other.path().join(BOOT_B));
        assert_eq!(first.boot_path(), shared.path().join(BOOT_A));
    }

    #[test]
    fn replacing_the_locked_file_cannot_create_a_second_owner() {
        let root = private_work_root();
        let first = WorkRootLease::acquire(root.path(), BOOT_A).unwrap();
        let first_boot = first.boot_path().to_owned();

        fs::remove_file(root.path().join(LOCK_FILE_NAME)).unwrap();
        let second = WorkRootLease::acquire(root.path(), BOOT_B);

        assert!(matches!(second, Err(WorkRootError::WorkRootInUse)));
        assert!(first_boot.exists());
    }

    #[test]
    fn shared_work_root_fails_before_lock_creation_or_child_inspection() {
        let root = private_work_root();
        fs::set_permissions(root.path(), Permissions::from_mode(0o770)).unwrap();
        let hook = Arc::new(CountingHook::default());
        let remover = ScriptedRemover::new([]);

        assert_eq!(
            WorkRootLease::acquire_with(
                root.path(),
                BOOT_A,
                filesystem(
                    remover.clone(),
                    Arc::new(RecordingSleeper::default()),
                    hook.clone(),
                ),
            )
            .err()
            .unwrap(),
            WorkRootError::UnsafeWorkRoot
        );
        assert!(!root.path().join(LOCK_FILE_NAME).exists());
        assert_eq!(hook.0.load(Ordering::Relaxed), 0);
        assert_eq!(remover.calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn startup_retains_old_boots_and_creates_a_fresh_root() {
        let root = private_work_root();
        {
            let first = WorkRootLease::acquire(root.path(), BOOT_A).unwrap();
            fs::write(first.boot_path().join("stale"), b"owned").unwrap();
        }
        let second = WorkRootLease::acquire_for_test(root.path(), BOOT_B).unwrap();
        assert_eq!(
            fs::read(root.path().join(BOOT_A).join("stale")).unwrap(),
            b"owned"
        );
        assert_eq!(
            second.startup_retained(),
            &[unknown_retained_root(&root.path().join(BOOT_A))]
        );
        assert!(second.boot_path().exists());
    }

    #[test]
    fn unsafe_lock_fails_while_unknown_old_boot_shapes_are_retained() {
        let unsafe_lock = private_work_root();
        let lock_target = unsafe_lock.path().join("operator-lock-target");
        fs::write(&lock_target, b"operator").unwrap();
        symlink(&lock_target, unsafe_lock.path().join(LOCK_FILE_NAME)).unwrap();
        assert_eq!(
            WorkRootLease::acquire(unsafe_lock.path(), BOOT_B)
                .err()
                .unwrap(),
            WorkRootError::UnsafeWorkRoot
        );
        assert_eq!(fs::read(&lock_target).unwrap(), b"operator");

        for marker_kind in ["malformed", "directory", "symlink"] {
            let root = private_work_root();
            let boot = root.path().join(BOOT_A);
            create_private_directory(&boot).unwrap();
            let unchanged = boot.join("unchanged");
            fs::write(&unchanged, b"owned").unwrap();
            let marker = boot.join(OWNERSHIP_MARKER_NAME);
            match marker_kind {
                "malformed" => {
                    fs::write(&marker, b"wrong-version\n").unwrap();
                    fs::set_permissions(&marker, Permissions::from_mode(PRIVATE_FILE_MODE))
                        .unwrap();
                }
                "directory" => create_private_directory(&marker).unwrap(),
                "symlink" => {
                    let target = root.path().join("operator-marker-target");
                    fs::write(&target, BOOT_MARKER).unwrap();
                    symlink(target, marker).unwrap();
                }
                _ => panic!("unknown owned-root fixture"),
            }
            let current = WorkRootLease::acquire(root.path(), BOOT_B).unwrap();
            assert_eq!(current.startup_retained(), &[unknown_retained_root(&boot)]);
            assert_eq!(fs::read(&unchanged).unwrap(), b"owned");
        }

        let non_directory_root = private_work_root();
        let recognized_file = non_directory_root.path().join(BOOT_A);
        fs::write(&recognized_file, b"not-a-root").unwrap();
        let current = WorkRootLease::acquire(non_directory_root.path(), BOOT_B).unwrap();
        assert_eq!(
            current.startup_retained(),
            &[unknown_retained_root(&recognized_file)]
        );
        assert_eq!(fs::read(recognized_file).unwrap(), b"not-a-root");
    }

    #[test]
    fn release_uses_each_exact_delay_prefix_and_coalesces_callers() {
        for success_index in 0..6 {
            let root = private_work_root();
            let mut outcomes = vec![RemovalOutcome::Error; success_index];
            outcomes.push(RemovalOutcome::Success);
            let remover = ScriptedRemover::new(outcomes);
            let sleeper = Arc::new(RecordingSleeper::default());
            let owner = WorkRootLease::acquire_with(
                root.path(),
                BOOT_A,
                filesystem(remover.clone(), sleeper.clone(), Arc::new(NoopWorkRootHook)),
            )
            .unwrap();
            let assignment = owner.create_assignment(ASSIGNMENT).unwrap();
            fs::write(assignment.workspace.path().join("content"), b"content").unwrap();
            let workspace = assignment.workspace.clone();
            let first = workspace.release_pending(ProcessQuiescence::Proven);
            let second = workspace.release_pending(ProcessQuiescence::Proven);
            assert_eq!(first.wait(), CleanupResult::Released);
            assert_eq!(second.wait(), CleanupResult::Released);
            assert_eq!(remover.calls.load(Ordering::Relaxed), success_index + 1);
            assert_eq!(
                *sleeper.delays.lock().unwrap(),
                REMOVAL_DELAYS[..success_index]
            );
        }
    }

    #[test]
    fn partial_removal_rechecks_not_found_and_exhaustion_quarantines_enclosing_root() {
        let recovered_root = private_work_root();
        let recovered_remover = ScriptedRemover::new([
            RemovalOutcome::Partial,
            RemovalOutcome::NotFound,
            RemovalOutcome::Success,
        ]);
        let recovered_sleeper = Arc::new(RecordingSleeper::default());
        let recovered = WorkRootLease::acquire_with(
            recovered_root.path(),
            BOOT_A,
            filesystem(
                recovered_remover,
                recovered_sleeper.clone(),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
        let assignment = recovered.create_assignment(ASSIGNMENT).unwrap();
        fs::write(assignment.workspace.path().join("content"), b"content").unwrap();
        let workspace_path = assignment.workspace.path();
        assert_eq!(
            assignment
                .workspace
                .release_pending(ProcessQuiescence::Proven)
                .wait(),
            CleanupResult::Released
        );
        assert_eq!(
            *recovered_sleeper.delays.lock().unwrap(),
            vec![REMOVAL_DELAYS[0], REMOVAL_DELAYS[1]]
        );
        assert!(!workspace_path.exists());

        let failed_root = private_work_root();
        let failed_remover = ScriptedRemover::new([RemovalOutcome::Error; 6]);
        let failed = WorkRootLease::acquire_with(
            failed_root.path(),
            BOOT_A,
            filesystem(
                failed_remover.clone(),
                Arc::new(RecordingSleeper::default()),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
        let assignment = failed.create_assignment(ASSIGNMENT).unwrap();
        let assignment_path = assignment.execution.parent().unwrap().to_owned();
        let pending =
            assignment.release_pending(ProcessQuiescence::Proven, WorkspaceDisposition::Remove);
        assert_eq!(
            pending.wait(),
            CleanupResult::Quarantined(CleanupFailure::OrdinaryRemovalExhausted)
        );
        assert!(assignment_path.exists());
        assert_eq!(failed_remover.calls.load(Ordering::Relaxed), 6);
    }

    #[test]
    fn retained_attempt_survives_restart_with_identity_and_original_bytes() {
        let root = private_work_root();
        let remover = ScriptedRemover::new([]);
        let first = owner_with_remover(root.path(), remover.clone());
        let assignment = first.create_assignment(ASSIGNMENT).unwrap();
        let workspace = assignment.workspace.path();
        fs::create_dir(workspace.join(".git")).unwrap();
        fs::write(workspace.join(".git/HEAD"), b"ref: refs/heads/retained\n").unwrap();
        fs::write(workspace.join("tracked"), b"dirty tracked\n").unwrap();
        fs::write(workspace.join("untracked"), b"untracked bytes\n").unwrap();
        fs::write(workspace.join("ignored.cache"), b"ignored bytes\n").unwrap();
        fs::create_dir(workspace.join("build")).unwrap();
        fs::write(workspace.join("build/output"), b"build output\n").unwrap();

        assert_eq!(
            assignment
                .release_pending(
                    ProcessQuiescence::Proven,
                    WorkspaceDisposition::Retain(RetentionReason::Failed),
                )
                .wait(),
            CleanupResult::Retained
        );
        assert_eq!(remover.calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            first.release_boot_root_pending().wait(),
            CleanupResult::Retained
        );
        drop(assignment);
        drop(first);

        let second = WorkRootLease::acquire_for_test(root.path(), BOOT_B).unwrap();
        assert_eq!(
            fs::read(workspace.join("tracked")).unwrap(),
            b"dirty tracked\n"
        );
        assert_eq!(
            fs::read(workspace.join("untracked")).unwrap(),
            b"untracked bytes\n"
        );
        assert_eq!(
            fs::read(workspace.join("ignored.cache")).unwrap(),
            b"ignored bytes\n"
        );
        assert_eq!(
            fs::read(workspace.join("build/output")).unwrap(),
            b"build output\n"
        );
        assert_eq!(
            fs::read(workspace.join(".git/HEAD")).unwrap(),
            b"ref: refs/heads/retained\n"
        );
        assert_eq!(
            second.startup_retained(),
            &[RetainedWorkspace {
                assignment_id: Some(ASSIGNMENT.to_owned()),
                run_id: Some("run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
                attempt_id: Some("atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
                path: workspace,
                reason: RetentionReason::Failed,
            }]
        );
    }

    #[test]
    fn release_worker_spawn_failure_still_tears_down_runner_credentials() {
        let root = private_work_root();
        let filesystem = WorkspaceFilesystem::testing()
            .with_workspace_release_spawner(Arc::new(RejectingWorkspaceReleaseSpawner));
        let owner = WorkRootLease::acquire_with(root.path(), BOOT_A, filesystem).unwrap();
        let (recorder, capture) = crate::runner::telemetry::test_recorder(BOOT_A);
        let mut assignment = owner
            .create_assignment_for_attempt(
                ASSIGNMENT,
                "run_01k0z6r1w8f4jy2m7q9v3x5abc",
                "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
                Some(recorder),
            )
            .unwrap();
        let workspace = assignment.workspace.path();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&workspace)
                .status()
                .unwrap()
                .success()
        );
        let environment = crate::execution::workflow::admission::EnvironmentSnapshot::new([(
            "PATH",
            std::env::var_os("PATH").unwrap(),
        )]);
        let cancellation = crate::execution::workflow::artifact::CaptureCancellation::default();
        let authority = super::super::workflow_git::WorkflowGitAuthority::install(
            super::super::workflow_git::WorkflowGitInstall {
                broker: super::super::source::test_support::unavailable_source_broker(),
                assignment_id: ASSIGNMENT,
                origin: Arc::from("https://github.example/acme/private.git"),
                workspace: &workspace,
                private_root: assignment.private.path(),
                environment: &environment,
                helper_executable: &std::env::current_exe().unwrap(),
                clock: Arc::new(super::super::TokioSleeper),
                recorder: None,
                cancellation: &cancellation,
            },
        )
        .unwrap();
        let helper = assignment.private.path().join("workflow-git-credential");
        assert!(helper.exists());
        assignment.install_workflow_git(authority);

        assert_eq!(
            assignment
                .release_workspace_pending(ProcessQuiescence::Proven, WorkspaceDisposition::Remove)
                .wait(),
            CleanupResult::Retained
        );

        assert!(!helper.exists());
        assert!(workspace.exists());
        let event = capture
            .records()
            .into_iter()
            .find(|event| event["event.name"] == "runner.workspace_retained")
            .expect("retention diagnostic");
        assert_eq!(
            event["scherzo.workspace.retention_reason"],
            "release_worker_unavailable"
        );
        assert_eq!(event["scherzo.teardown.runner_credentials_removed"], true);
        assert_eq!(event["scherzo.teardown.runner_credentials_revoked"], true);
    }

    #[test]
    fn startup_does_not_trust_an_attempt_record_through_an_assignment_symlink() {
        let root = private_work_root();
        let external = private_work_root();
        let first = WorkRootLease::acquire(root.path(), BOOT_A).unwrap();
        let external_owner = WorkRootLease::acquire(external.path(), BOOT_A).unwrap();
        let external_assignment = external_owner
            .create_assignment_for_attempt(
                ASSIGNMENT,
                "run_01k0z6r1w8f4jy2m7q9v3x5abd",
                "atm_01k0z6r1w8f4jy2m7q9v3x5abd",
                None,
            )
            .unwrap();
        let external_assignment_path = external_assignment.execution.parent().unwrap();
        let assignment_path = first.boot_path().join(ASSIGNMENT);
        symlink(external_assignment_path, &assignment_path).unwrap();
        drop(first);

        let second = WorkRootLease::acquire_for_test(root.path(), BOOT_B).unwrap();

        assert_eq!(
            second.startup_retained(),
            &[unknown_retained_root(&assignment_path)],
            "a symlinked assignment is not a confined owned tree and must not borrow another attempt's identity",
        );
        assert!(assignment_path.is_symlink());
        assert!(external_assignment_path.join(ATTEMPT_RECORD_NAME).exists());
    }

    #[test]
    fn process_stop_failure_retains_without_invoking_the_remover() {
        let root = private_work_root();
        let remover = ScriptedRemover::new([]);
        let owner = owner_with_remover(root.path(), remover.clone());
        let (recorder, capture) = crate::runner::telemetry::test_recorder(BOOT_A);
        let assignment = owner
            .create_assignment_for_attempt(
                ASSIGNMENT,
                "run_01k0z6r1w8f4jy2m7q9v3x5abc",
                "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
                Some(recorder),
            )
            .unwrap();
        fs::write(assignment.workspace.path().join("owned"), b"retained").unwrap();
        let workspace = assignment.workspace.path();

        assert_eq!(
            assignment
                .release_pending(ProcessQuiescence::Failed, WorkspaceDisposition::Remove,)
                .wait(),
            CleanupResult::Retained
        );
        assert_eq!(remover.calls.load(Ordering::Relaxed), 0);
        assert_eq!(fs::read(workspace.join("owned")).unwrap(), b"retained");
        let event = capture
            .records()
            .into_iter()
            .find(|event| event["event.name"] == "runner.workspace_retained")
            .expect("retention diagnostic");
        assert_eq!(event["scherzo.assignment.id"], ASSIGNMENT);
        assert_eq!(event["scherzo.run.id"], "run_01k0z6r1w8f4jy2m7q9v3x5abc");
        assert_eq!(
            event["scherzo.attempt.id"],
            "atm_01k0z6r1w8f4jy2m7q9v3x5abc"
        );
        assert_eq!(
            event["scherzo.workspace.path"],
            workspace.to_string_lossy().as_ref()
        );
        assert_eq!(event["scherzo.teardown.owned_processes_stopped"], false);
        assert_eq!(event["scherzo.teardown.runner_credentials_removed"], true);
        assert_eq!(event["scherzo.teardown.runner_credentials_revoked"], true);
    }

    #[test]
    fn replacement_during_removal_is_quarantined_before_another_traversal() {
        let root = private_work_root();
        let remover = Arc::new(ReplacingRemover {
            calls: AtomicUsize::new(0),
        });
        let owner = owner_with_remover(root.path(), remover.clone());
        let boot_path = owner.boot_path().to_owned();

        assert_eq!(
            owner.release_boot_root_pending().wait(),
            CleanupResult::Quarantined(CleanupFailure::Safety)
        );
        assert_eq!(remover.calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            fs::read(boot_path.join("replacement-sentinel")).unwrap(),
            b"unproven replacement"
        );
        assert!(fs::read_dir(root.path()).unwrap().flatten().any(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(CLEANUP_AUTHORITY_PREFIX))
        }));
    }

    #[test]
    fn replacement_during_the_destructive_call_is_not_deleted() {
        let root = private_work_root();
        let owner = owner_with_remover(root.path(), Arc::new(ReplacingAndDeletingRemover));
        let boot_path = owner.boot_path().to_owned();

        let result = owner.release_boot_root_pending().wait();

        assert_eq!(
            fs::read(boot_path.join("replacement-sentinel")).unwrap(),
            b"unproven replacement"
        );
        assert_eq!(result, CleanupResult::Quarantined(CleanupFailure::Safety));
    }

    #[test]
    fn changed_surviving_marker_stops_assignment_root_retry() {
        let root = private_work_root();
        let assignment_path = root.path().join(BOOT_A).join(ASSIGNMENT);
        let marker = assignment_path.join(OWNERSHIP_MARKER_NAME);
        let remover = ScriptedRemover::new([
            RemovalOutcome::Success,
            RemovalOutcome::Error,
            RemovalOutcome::Success,
        ]);
        let owner = WorkRootLease::acquire_with(
            root.path(),
            BOOT_A,
            filesystem(
                remover.clone(),
                Arc::new(MarkerMutatingSleeper { marker }),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
        let assignment = owner.create_assignment(ASSIGNMENT).unwrap();

        assert_eq!(
            assignment
                .release_pending(ProcessQuiescence::Proven, WorkspaceDisposition::Remove)
                .wait(),
            CleanupResult::Quarantined(CleanupFailure::Safety)
        );
        assert!(assignment_path.exists());
        assert_eq!(remover.calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn assignment_release_removes_nested_read_only_inputs_without_following_links() {
        let root = private_work_root();
        let outside = root.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("sentinel");
        fs::write(&sentinel, b"outside remains").unwrap();
        fs::set_permissions(&sentinel, Permissions::from_mode(0o400)).unwrap();
        fs::set_permissions(&outside, Permissions::from_mode(0o500)).unwrap();
        let sentinel_mode = mode(&sentinel);
        let outside_mode = mode(&outside);

        let owner = WorkRootLease::acquire_for_test(root.path(), BOOT_A).unwrap();
        let assignment = owner.create_assignment(ASSIGNMENT).unwrap();
        let assignment_path = assignment.execution.parent().unwrap().to_owned();
        fs::write(assignment.execution.join("ordinary"), b"writable sibling").unwrap();
        let workflow = assignment.execution.join(
            "delivery-rounds/0001/run/.private/workflow-fixture/.inputs-fixture/view-fixture",
        );
        let values = workflow.join("values");
        fs::create_dir_all(&values).unwrap();
        fs::write(values.join("result"), b"immutable input").unwrap();
        symlink(&sentinel, workflow.join("outside-link")).unwrap();
        fs::set_permissions(values.join("result"), Permissions::from_mode(0o400)).unwrap();
        for directory in [
            values.as_path(),
            workflow.as_path(),
            workflow.parent().unwrap(),
            workflow.parent().unwrap().parent().unwrap(),
        ] {
            fs::set_permissions(directory, Permissions::from_mode(0o500)).unwrap();
        }

        assert_eq!(
            assignment
                .release_pending(ProcessQuiescence::Proven, WorkspaceDisposition::Remove)
                .wait(),
            CleanupResult::Released
        );
        assert!(!assignment_path.exists());
        assert_eq!(fs::read(&sentinel).unwrap(), b"outside remains");
        assert_eq!(mode(&sentinel), sentinel_mode);
        assert_eq!(mode(&outside), outside_mode);

        fs::set_permissions(&outside, Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn concurrent_root_substitution_fails_closed_without_mutating_the_outside_tree() {
        let root = private_work_root();
        let outside = root.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("sentinel");
        fs::write(&sentinel, b"operator content").unwrap();
        fs::set_permissions(&sentinel, Permissions::from_mode(0o400)).unwrap();
        fs::set_permissions(&outside, Permissions::from_mode(0o500)).unwrap();
        let displaced = root.path().join("displaced-workspace");
        let remover = Arc::new(SubstitutingRemover {
            displaced: displaced.clone(),
            outside: outside.clone(),
            calls: AtomicUsize::new(0),
        });
        let owner = owner_with_remover(root.path(), remover.clone());
        let assignment = assignment_with_retained_file(&owner);

        assert_eq!(
            release_workspace(&assignment),
            CleanupResult::Quarantined(CleanupFailure::Safety)
        );
        assert_eq!(remover.calls.load(Ordering::Relaxed), 1);
        assert_eq!(fs::read(displaced.join("owned")).unwrap(), b"retained");
        assert_eq!(fs::read(&sentinel).unwrap(), b"operator content");
        assert_eq!(mode(&sentinel), 0o400);
        assert_eq!(mode(&outside), 0o500);

        fs::set_permissions(&outside, Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn concurrent_ancestor_substitution_invalidates_workspace_authority() {
        let root = private_work_root();
        let boot = root.path().join(BOOT_A);
        let displaced = root.path().join("displaced-boot");
        let owner = owner_with_remover(
            root.path(),
            Arc::new(AncestorSubstitutingRemover {
                ancestor: boot,
                displaced: displaced.clone(),
            }),
        );
        let assignment = assignment_with_retained_file(&owner);

        assert_eq!(
            release_workspace(&assignment),
            CleanupResult::Quarantined(CleanupFailure::Safety)
        );
        assert_eq!(
            fs::read(displaced.join(ASSIGNMENT).join("workspace").join("owned")).unwrap(),
            b"retained"
        );
    }

    #[test]
    fn safety_and_quiescence_failures_make_no_destructive_call() {
        let root = private_work_root();
        let remover = ScriptedRemover::new([]);
        let owner = owner_with_remover(root.path(), remover.clone());
        let assignment = owner.create_assignment(ASSIGNMENT).unwrap();
        let path = assignment.workspace.path();
        assert_eq!(
            assignment
                .workspace
                .release_pending(ProcessQuiescence::Failed)
                .wait(),
            CleanupResult::Quarantined(CleanupFailure::Quiescence)
        );
        assert!(path.exists());
        assert_eq!(remover.calls.load(Ordering::Relaxed), 0);

        let unsafe_root = private_work_root();
        let unsafe_remover = ScriptedRemover::new([]);
        let unsafe_owner = owner_with_remover(unsafe_root.path(), unsafe_remover.clone());
        let unsafe_assignment = unsafe_owner.create_assignment(ASSIGNMENT).unwrap();
        let workspace = unsafe_assignment.workspace.path();
        fs::remove_dir(&workspace).unwrap();
        let target = unsafe_root.path().join("operator-target");
        create_private_directory(&target).unwrap();
        symlink(&target, &workspace).unwrap();
        assert_eq!(
            unsafe_assignment
                .workspace
                .release_pending(ProcessQuiescence::Proven)
                .wait(),
            CleanupResult::Quarantined(CleanupFailure::Safety)
        );
        assert_eq!(unsafe_remover.calls.load(Ordering::Relaxed), 0);
        assert!(target.exists());
    }

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "the timeout only bounds failure to reach the deterministic cleanup-sleep handshake"
    )]
    fn cleanup_retry_sleep_is_preemptible_within_the_shutdown_reserve() {
        assert_eq!(
            REMOVAL_DELAYS.into_iter().sum::<Duration>(),
            Duration::from_millis(3_850)
        );
        assert!(REMOVAL_DELAYS.into_iter().sum::<Duration>() < Duration::from_secs(5));
        let root = private_work_root();
        let remover = ScriptedRemover::new([RemovalOutcome::Error, RemovalOutcome::Success]);
        let (started, sleeping) = std::sync::mpsc::sync_channel(1);
        let owner = WorkRootLease::acquire_with(
            root.path(),
            BOOT_A,
            filesystem(
                remover.clone(),
                Arc::new(CancellationSleeper { started }),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
        let assignment = owner.create_assignment(ASSIGNMENT).unwrap();
        let workspace = assignment.workspace.path();
        let pending = assignment
            .workspace
            .release_pending(ProcessQuiescence::Proven);
        sleeping
            .recv_timeout(Duration::from_secs(1))
            .expect("cleanup did not reach its interruptible retry sleep");
        owner.cancel_cleanup();
        assert_eq!(pending.wait(), CleanupResult::Preempted);
        assert!(workspace.exists());
        assert_eq!(remover.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn dropping_authority_releases_locks_inherited_by_a_child() {
        let root = private_work_root();
        let first = WorkRootLease::acquire(root.path(), BOOT_A).unwrap();
        let inherited_directory_lock = first._authority.shared.directory_lock.try_clone().unwrap();
        let inherited_lock_file = first._authority.shared.lock_file.try_clone().unwrap();
        // Retain duplicates in the helper to make the ordinary fork-to-exec
        // inheritance window deterministic.
        fcntl(
            &inherited_directory_lock,
            FcntlArg::F_SETFD(FdFlag::empty()),
        )
        .unwrap();
        fcntl(&inherited_lock_file, FcntlArg::F_SETFD(FdFlag::empty())).unwrap();
        let mut child = spawn_ready_helper_child();
        drop(inherited_directory_lock);
        drop(inherited_lock_file);

        drop(first);
        let second = WorkRootLease::acquire_for_test(root.path(), BOOT_B);

        let _ = child.kill();
        let _ = child.wait();
        assert_eq!(second.unwrap().boot_path(), root.path().join(BOOT_B));
    }

    #[test]
    fn close_on_exec_lock_is_not_retained_by_a_helper_child() {
        let root = private_work_root();
        let first = WorkRootLease::acquire(root.path(), BOOT_A).unwrap();
        let mut child = spawn_ready_helper_child();
        drop(first);
        let second = WorkRootLease::acquire_for_test(root.path(), BOOT_B).unwrap();
        assert_eq!(second.boot_path(), root.path().join(BOOT_B));
        let _ = child.kill();
        let _ = child.wait();
    }
}
