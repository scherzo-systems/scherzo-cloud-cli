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
    AtFlags, Dev, FileType, Mode, OFlags, RenameFlags, Stat, fstat, openat, renameat_with, statat,
};
use rustix::io::{Errno, fcntl_dupfd_cloexec};

use super::workflow_git::WorkflowGitAuthority;
use crate::execution::owned_tree::{self, RemovalError};

const LOCK_FILE_NAME: &str = ".scherzo-runner-serve.lock";
const OWNERSHIP_MARKER_NAME: &str = ".scherzo-runner-serve-owner-v1";
const CLEANUP_AUTHORITY_NAME: &str = ".scherzo-runner-serve-cleanup-v1";
const CLEANUP_AUTHORITY_STAGING_PREFIX: &str = ".scherzo-runner-serve-cleanup-staging-";
const CLEANUP_AUTHORITY_HEADER: &str = "scherzo-runner-serve/cleanup-authority/v1";
const CLEANUP_IDENTITY_ATTRIBUTE: &str = "user.scherzo.runner-cleanup-v1";
const CLEANUP_IDENTITY_BYTES: usize = 32;
const BOOT_MARKER: &[u8] = b"scherzo-runner-serve/boot-root/v1\n";
const ASSIGNMENT_MARKER: &[u8] = b"scherzo-runner-serve/assignment-root/v1\n";
const PRIVATE_FILE_MODE: u32 = 0o600;
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
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
    Quarantined(CleanupFailure),
    Preempted,
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
    AmbiguousOwnedRoot,
    InvalidCleanupAuthority,
    StaleRootCleanupFailed,
    CreateBootRoot,
}

impl fmt::Display for WorkRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WorkRootInUse => "runner work root is already in use",
            Self::UnsafeWorkRoot => "runner work root ownership state is unsafe",
            Self::AmbiguousOwnedRoot => "runner work root contains ambiguous boot state",
            Self::InvalidCleanupAuthority => "runner work root cleanup authority is invalid",
            Self::StaleRootCleanupFailed => "runner stale work-root cleanup failed",
            Self::CreateBootRoot => "runner boot root could not be created",
        })
    }
}

impl WorkRootError {
    pub(super) const fn error_type(self) -> &'static str {
        match self {
            Self::WorkRootInUse => "work_root_in_use",
            Self::UnsafeWorkRoot => "unsafe_work_root",
            Self::AmbiguousOwnedRoot => "ambiguous_owned_root",
            Self::InvalidCleanupAuthority => "invalid_cleanup_authority",
            Self::StaleRootCleanupFailed => "stale_root_cleanup_exhausted",
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
}

impl WorkspaceFilesystem {
    pub(super) fn system() -> Self {
        Self {
            remover: Arc::new(SystemTreeRemover),
            sleeper: Arc::new(InterruptibleSleeper),
            hook: Arc::new(NoopWorkRootHook),
            cleanup_identity: Arc::new(ExtendedAttributeCleanupIdentityStore),
        }
    }

    #[cfg(test)]
    pub(super) fn testing() -> Self {
        Self {
            remover: Arc::new(SystemTreeRemover),
            sleeper: Arc::new(InterruptibleSleeper),
            hook: Arc::new(NoopWorkRootHook),
            cleanup_identity: Arc::new(MetadataCleanupIdentityStore),
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
        }
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
        let _ = CleanupTarget::parse(&relative_path)?;
        let Some(MarkerState::Present(marker)) = tree.marker.as_ref() else {
            return Err(());
        };
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

    fn decode(contents: &[u8]) -> Result<Self, ()> {
        let contents = std::str::from_utf8(contents).map_err(|_| ())?;
        let mut lines = contents.split('\n');
        if lines.next() != Some(CLEANUP_AUTHORITY_HEADER) {
            return Err(());
        }
        let relative_path = parse_authority_field(&mut lines, "path=")?.to_owned();
        let _ = CleanupTarget::parse(&relative_path)?;
        let parent_device = parse_authority_number(&mut lines, "parent-device=")?;
        let parent_inode = parse_authority_number(&mut lines, "parent-inode=")?;
        let device = parse_authority_number(&mut lines, "device=")?;
        let inode = parse_authority_number(&mut lines, "inode=")?;
        let marker_device = parse_authority_number(&mut lines, "marker-device=")?;
        let marker_inode = parse_authority_number(&mut lines, "marker-inode=")?;
        let cleanup_identity = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parse_authority_field(&mut lines, "cleanup-identity=")?)
            .map_err(|_| ())?
            .try_into()
            .map_err(|_| ())?;
        if lines.next() != Some("") || lines.next().is_some() {
            return Err(());
        }
        Ok(Self {
            relative_path,
            parent_device,
            parent_inode,
            device,
            inode,
            marker_device,
            marker_inode,
            cleanup_identity,
        })
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

fn parse_authority_number<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    prefix: &str,
) -> Result<u64, ()> {
    parse_authority_field(lines, prefix)?
        .parse()
        .map_err(|_| ())
}

struct CleanupTarget {
    relative_path: PathBuf,
    marker_contents: &'static [u8],
    marker_in_parent: bool,
}

impl CleanupTarget {
    fn parse(relative_path: &str) -> Result<Self, ()> {
        let components = relative_path.split('/').collect::<Vec<_>>();
        match components.as_slice() {
            [boot] if valid_boot_id(boot) => Ok(Self {
                relative_path: PathBuf::from(boot),
                marker_contents: BOOT_MARKER,
                marker_in_parent: false,
            }),
            [boot, assignment] if valid_boot_id(boot) && valid_assignment_id(assignment) => {
                Ok(Self {
                    relative_path: PathBuf::from(boot).join(assignment),
                    marker_contents: ASSIGNMENT_MARKER,
                    marker_in_parent: false,
                })
            }
            [boot, assignment, "workspace"]
                if valid_boot_id(boot) && valid_assignment_id(assignment) =>
            {
                Ok(Self {
                    relative_path: PathBuf::from(boot).join(assignment).join("workspace"),
                    marker_contents: ASSIGNMENT_MARKER,
                    marker_in_parent: true,
                })
            }
            _ => Err(()),
        }
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
        let (staging_name, staging_path, mut file) = create_cleanup_authority_staging(authority)?;
        file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))
            .and_then(|()| file.write_all(&contents))
            .and_then(|()| file.sync_all())
            .map_err(|_| ())?;
        authority.validate()?;
        verify_private_file_identity(&staging_path, &file)?;
        renameat_with(
            &authority.shared.directory_lock,
            &staging_name,
            &authority.shared.directory_lock,
            CLEANUP_AUTHORITY_NAME,
            RenameFlags::NOREPLACE,
        )
        .map_err(|_| ())?;
        authority.sync()?;
        Self::capture(authority.work_root().join(CLEANUP_AUTHORITY_NAME))
    }

    fn load(authority: &WorkRootAuthority) -> Result<Option<Self>, ()> {
        authority.validate()?;
        let path = authority.work_root().join(CLEANUP_AUTHORITY_NAME);
        match fs::symlink_metadata(&path) {
            Ok(_) => Self::capture(path).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(()),
        }
    }

    fn capture(path: PathBuf) -> Result<Self, ()> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(NOFOLLOW_FLAG)
            .open(&path)
            .map_err(|_| ())?;
        set_close_on_exec(&file)?;
        let metadata = file.metadata().map_err(|_| ())?;
        let path_metadata = fs::symlink_metadata(&path).map_err(|_| ())?;
        if !safe_private_file(&metadata)
            || path_metadata.dev() != metadata.dev()
            || path_metadata.ino() != metadata.ino()
        {
            return Err(());
        }
        let mut contents = Vec::new();
        std::io::Read::by_ref(&mut file)
            .take(MAXIMUM_CLEANUP_AUTHORITY_BYTES + 1)
            .read_to_end(&mut contents)
            .map_err(|_| ())?;
        if u64::try_from(contents.len()).map_err(|_| ())? > MAXIMUM_CLEANUP_AUTHORITY_BYTES {
            return Err(());
        }
        let record = CleanupAuthorityRecord::decode(&contents)?;
        Ok(Self {
            path,
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
) -> Result<(String, PathBuf, File), ()> {
    for _ in 0..8 {
        let mut identity = [0_u8; 16];
        getrandom::fill(&mut identity).map_err(|_| ())?;
        let name = format!(
            "{CLEANUP_AUTHORITY_STAGING_PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(identity)
        );
        let path = authority.work_root().join(&name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(PRIVATE_FILE_MODE)
            .custom_flags(NOFOLLOW_FLAG)
            .open(&path)
        {
            Ok(file) => {
                set_close_on_exec(&file)?;
                return Ok((name, path, file));
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

    fn resume(&self, tree: &OwnedTree, proof: CleanupAuthorityProof) -> CleanupResult {
        let _serialized = self
            .serialized
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
enum MarkerState {
    Present(MarkerProof),
    Missing(Option<Arc<OwnedFd>>),
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

    fn recover(
        parent: Arc<OwnedFd>,
        identity: Arc<str>,
        device: u64,
        inode: u64,
    ) -> Result<Self, ()> {
        match statat(
            parent.as_ref(),
            identity.as_ref(),
            AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(_) => {
                let link = Self::capture(parent, identity)?;
                if link.device != device || link.inode != inode {
                    return Err(());
                }
                Ok(link)
            }
            Err(Errno::NOENT) => {
                let parent_metadata = fstat(parent.as_ref()).map_err(|_| ())?;
                if !safe_owned_directory_stat(&parent_metadata) {
                    return Err(());
                }
                Ok(Self {
                    parent,
                    identity,
                    directory: None,
                    device,
                    inode,
                })
            }
            Err(_) => Err(()),
        }
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
    marker: Option<MarkerState>,
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

    fn recover(
        work_root_directory: &File,
        work_root: &Path,
        record: &CleanupAuthorityRecord,
    ) -> Result<Self, ()> {
        let target = CleanupTarget::parse(&record.relative_path)?;
        let path = work_root.join(&target.relative_path);
        let component_count = target.relative_path.iter().count();
        let mut parent = Arc::new(fcntl_dupfd_cloexec(work_root_directory, 0).map_err(|_| ())?);
        let mut lineage = Vec::with_capacity(component_count);
        for (index, component) in target.relative_path.iter().enumerate() {
            let identity = component.to_str().map(Arc::<str>::from).ok_or(())?;
            let link = if index + 1 == component_count {
                DirectoryLink::recover(Arc::clone(&parent), identity, record.device, record.inode)?
            } else {
                DirectoryLink::capture(Arc::clone(&parent), identity)?
            };
            if index + 1 < component_count {
                parent = Arc::clone(link.directory.as_ref().ok_or(())?);
            }
            lineage.push(link);
        }
        let mut tree = Self {
            path,
            lineage,
            marker: None,
        };
        let link = tree.link().ok_or(())?;
        let parent_metadata = fstat(link.parent.as_ref()).map_err(|_| ())?;
        if normalized_device(parent_metadata.st_dev) != record.parent_device
            || parent_metadata.st_ino != record.parent_inode
        {
            return Err(());
        }
        tree.marker = Some(match tree.validate_linked_directory()? {
            TreePresence::Present => {
                let marker_parent = if target.marker_in_parent {
                    Arc::clone(&tree.link().ok_or(())?.parent)
                } else {
                    Arc::clone(tree.directory()?)
                };
                let marker = MarkerProof {
                    parent: marker_parent,
                    contents: target.marker_contents,
                    device: record.marker_device,
                    inode: record.marker_inode,
                };
                match verify_marker(&marker) {
                    Ok(()) => MarkerState::Present(marker),
                    Err(()) if marker.is_missing() => {
                        MarkerState::Missing(Some(Arc::clone(&marker.parent)))
                    }
                    Err(()) => return Err(()),
                }
            }
            TreePresence::Missing => MarkerState::Missing(None),
        });
        Ok(tree)
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

    #[cfg(test)]
    fn read_cleanup_identity(
        &self,
        cleanup_identity_store: &dyn CleanupIdentityStore,
    ) -> Result<Option<[u8; CLEANUP_IDENTITY_BYTES]>, ()> {
        cleanup_identity_store.read(self.directory()?.as_ref())
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
        self.marker = Some(MarkerState::Present(marker));
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
        match self.marker.as_ref().ok_or(())? {
            MarkerState::Present(marker) => match verify_marker(marker) {
                Ok(()) => Ok(()),
                Err(()) if may_be_missing && marker.is_missing() => Ok(()),
                Err(()) => Err(()),
            },
            MarkerState::Missing(Some(parent))
                if may_be_missing
                    && matches!(
                        statat(
                            parent.as_ref(),
                            OWNERSHIP_MARKER_NAME,
                            AtFlags::SYMLINK_NOFOLLOW,
                        ),
                        Err(Errno::NOENT)
                    ) =>
            {
                Ok(())
            }
            MarkerState::Missing(_) => Err(()),
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
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
        .map_err(|_| ())?;
    set_close_on_exec(&file)?;
    let metadata = file.metadata().map_err(|_| ())?;
    let path_metadata = fs::symlink_metadata(path).map_err(|_| ())?;
    if !safe_private_file(&metadata)
        || path_metadata.dev() != metadata.dev()
        || path_metadata.ino() != metadata.ino()
        || metadata.dev() != device
        || metadata.ino() != inode
    {
        return Err(());
    }
    let mut contents = Vec::with_capacity(expected.len());
    std::io::Read::by_ref(&mut file)
        .take(u64::try_from(expected.len()).map_err(|_| ())? + 1)
        .read_to_end(&mut contents)
        .map_err(|_| ())?;
    (contents == expected).then_some(()).ok_or(())
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

pub(super) struct WorkRootLease {
    boot_tree: OwnedTree,
    engine: CleanupEngine,
    cancellation: Arc<CleanupCancellation>,
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

        if let Some(proof) = CleanupAuthorityProof::load(&authority)
            .map_err(|()| WorkRootError::InvalidCleanupAuthority)?
        {
            let tree =
                OwnedTree::recover(&authority.shared.directory_lock, work_root, &proof.record)
                    .map_err(|()| WorkRootError::InvalidCleanupAuthority)?;
            classify_startup_cleanup(
                engine.resume(&tree, proof),
                WorkRootError::InvalidCleanupAuthority,
            )?;
        }

        filesystem.hook.before_child_enumeration();
        let children = fs::read_dir(work_root)
            .map_err(|_| WorkRootError::UnsafeWorkRoot)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| WorkRootError::UnsafeWorkRoot)?;
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
            let mut tree = OwnedTree::capture_root(&authority.shared.directory_lock, path)
                .map_err(|()| WorkRootError::AmbiguousOwnedRoot)?;
            let marker = MarkerProof::capture(
                Arc::clone(
                    tree.directory()
                        .map_err(|()| WorkRootError::AmbiguousOwnedRoot)?,
                ),
                BOOT_MARKER,
            )
            .map_err(|()| WorkRootError::AmbiguousOwnedRoot)?;
            tree.install_marker(marker);
            classify_startup_cleanup(engine.remove(&tree), WorkRootError::AmbiguousOwnedRoot)?;
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
            _authority: authority,
        }))
    }

    pub(super) fn create_assignment(
        &self,
        assignment_id: &str,
    ) -> Result<AssignmentRoot, AssignmentRootCreationError> {
        assignment_id
            .parse::<crate::runner_protocol::generated::AssignmentId>()
            .map_err(|_| AssignmentRootCreationError::CleanupFailed)?;
        let assignment_path = self.boot_tree.path.join(assignment_id);
        create_private_directory(&assignment_path)
            .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
        let mut assignment_tree =
            OwnedTree::capture_child(&self.boot_tree, assignment_path.clone())
                .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
        let assignment_marker = create_marker(&assignment_tree, ASSIGNMENT_MARKER)
            .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
        assignment_tree.install_marker(assignment_marker.clone());
        let private_path = assignment_path.join("private");
        let workspace_path = assignment_path.join("workspace");
        if create_private_directory(&private_path).is_err()
            || create_private_directory(&workspace_path).is_err()
        {
            return Err(match self.engine.remove(&assignment_tree) {
                CleanupResult::Released => AssignmentRootCreationError::Unavailable,
                CleanupResult::Quarantined(_) | CleanupResult::Preempted => {
                    AssignmentRootCreationError::CleanupFailed
                }
            });
        }
        let workspace_tree = match OwnedTree::capture_child(&assignment_tree, workspace_path) {
            Ok(mut tree) => {
                tree.install_marker(assignment_marker);
                tree
            }
            Err(()) => {
                return Err(match self.engine.remove(&assignment_tree) {
                    CleanupResult::Released => AssignmentRootCreationError::Unavailable,
                    CleanupResult::Quarantined(_) | CleanupResult::Preempted => {
                        AssignmentRootCreationError::CleanupFailed
                    }
                });
            }
        };
        Ok(AssignmentRoot {
            assignment_tree,
            execution: workspace_tree.path.clone(),
            private: PrivateStaging { path: private_path },
            workspace: WorkspaceLease::new(workspace_tree, self.engine.clone()),
            workflow_git: None,
            engine: self.engine.clone(),
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

    pub(super) fn release_boot_root_pending(&self) -> PendingRelease {
        let completion = ReleaseCompletion::new();
        let pending = completion.pending();
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

    #[cfg(test)]
    pub(super) fn boot_path(&self) -> &Path {
        &self.boot_tree.path
    }
}

fn classify_startup_cleanup(
    result: CleanupResult,
    safety_error: WorkRootError,
) -> Result<(), WorkRootError> {
    match result {
        CleanupResult::Released => Ok(()),
        CleanupResult::Quarantined(CleanupFailure::Safety) => Err(safety_error),
        CleanupResult::Quarantined(
            CleanupFailure::OrdinaryRemovalExhausted | CleanupFailure::Quiescence,
        )
        | CleanupResult::Preempted => Err(WorkRootError::StaleRootCleanupFailed),
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
    engine: CleanupEngine,
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
        let workspace = self.workspace.clone();
        let completion = self.workspace_release.completion.clone();
        let worker_completion = completion.clone();
        if std::thread::Builder::new()
            .name("runner-workspace-boundary-release".to_owned())
            .spawn(move || {
                if let Some(authority) = authority {
                    let report = authority.teardown(quiescence);
                    if !report.local_state_destroyed {
                        worker_completion
                            .complete(CleanupResult::Quarantined(CleanupFailure::Safety));
                        return;
                    }
                }
                worker_completion.complete(workspace.release_pending(quiescence).wait());
            })
            .is_err()
        {
            completion.complete(CleanupResult::Quarantined(CleanupFailure::Safety));
        }
        pending
    }

    pub(super) fn release_pending(&self, quiescence: ProcessQuiescence) -> PendingRelease {
        let pending = self.release.completion.pending();
        if self
            .release
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return pending;
        }
        let workspace = self.release_workspace_pending(quiescence);
        let assignment_tree = self.assignment_tree.clone();
        let engine = self.engine.clone();
        let completion = self.release.completion.clone();
        let worker_completion = completion.clone();
        if std::thread::Builder::new()
            .name("runner-assignment-root-release".to_owned())
            .spawn(move || {
                let result = match workspace.wait() {
                    CleanupResult::Released => engine.remove(&assignment_tree),
                    failure => failure,
                };
                worker_completion.complete(result);
            })
            .is_err()
        {
            completion.complete(CleanupResult::Quarantined(CleanupFailure::Safety));
        }
        pending
    }
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
    use std::io::{Read as _, Write as _};
    use std::os::unix::fs::symlink;
    use std::os::unix::process::ExitStatusExt as _;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const BOOT_A: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abc";
    const BOOT_B: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abd";
    const ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abc";
    const TERMINATION_FIXTURE_ROOT: &str = "SCHERZO_CLEANUP_TERMINATION_FIXTURE_ROOT";
    const AUTHORITY_PUBLICATION_FIXTURE_ROOT: &str =
        "SCHERZO_CLEANUP_AUTHORITY_PUBLICATION_FIXTURE_ROOT";
    const TERMINATION_FIXTURE_STATUS: i32 = 86;
    const TERMINATION_REMOVED_DESCENDANT: &str = "removed-before-termination";

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

    struct ProcessTerminatingRemover;

    impl TreeRemover for ProcessTerminatingRemover {
        fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()> {
            fs::remove_file(tree.path().join(OWNERSHIP_MARKER_NAME))?;
            fs::remove_file(tree.path().join(TERMINATION_REMOVED_DESCENDANT))?;
            std::process::exit(TERMINATION_FIXTURE_STATUS);
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

    struct ControlledCleanupIdentityStore(Mutex<[u8; CLEANUP_IDENTITY_BYTES]>);

    impl CleanupIdentityStore for ControlledCleanupIdentityStore {
        fn read(&self, _directory: &OwnedFd) -> Result<Option<[u8; CLEANUP_IDENTITY_BYTES]>, ()> {
            Ok(Some(*self.0.lock().unwrap()))
        }

        fn create(
            &self,
            _directory: &OwnedFd,
            _identity: &[u8; CLEANUP_IDENTITY_BYTES],
        ) -> Result<(), ()> {
            Err(())
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
    fn startup_removes_only_exactly_marked_stale_boot_roots() {
        let root = private_work_root();
        {
            let first = WorkRootLease::acquire(root.path(), BOOT_A).unwrap();
            fs::write(first.boot_path().join("stale"), b"owned").unwrap();
        }
        let second = WorkRootLease::acquire_for_test(root.path(), BOOT_B).unwrap();
        assert!(!root.path().join(BOOT_A).exists());
        assert!(second.boot_path().exists());

        let ambiguous = private_work_root();
        let legacy = ambiguous.path().join(BOOT_A);
        create_private_directory(&legacy).unwrap();
        fs::write(legacy.join("unchanged"), b"legacy").unwrap();
        assert_eq!(
            WorkRootLease::acquire(ambiguous.path(), BOOT_B)
                .err()
                .unwrap(),
            WorkRootError::AmbiguousOwnedRoot
        );
        assert_eq!(fs::read(legacy.join("unchanged")).unwrap(), b"legacy");

        let linked = private_work_root();
        let target = linked.path().join("operator-target");
        create_private_directory(&target).unwrap();
        symlink(&target, linked.path().join(BOOT_A)).unwrap();
        assert_eq!(
            WorkRootLease::acquire(linked.path(), BOOT_B).err().unwrap(),
            WorkRootError::AmbiguousOwnedRoot
        );
        assert!(target.exists());
    }

    #[test]
    fn unsafe_lock_and_marker_shapes_fail_without_recursive_mutation() {
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
            assert_eq!(
                WorkRootLease::acquire(root.path(), BOOT_B).err().unwrap(),
                WorkRootError::AmbiguousOwnedRoot
            );
            assert_eq!(fs::read(&unchanged).unwrap(), b"owned");
        }

        let non_directory_root = private_work_root();
        let recognized_file = non_directory_root.path().join(BOOT_A);
        fs::write(&recognized_file, b"not-a-root").unwrap();
        assert_eq!(
            WorkRootLease::acquire(non_directory_root.path(), BOOT_B)
                .err()
                .unwrap(),
            WorkRootError::AmbiguousOwnedRoot
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
        let pending = assignment.release_pending(ProcessQuiescence::Proven);
        assert_eq!(
            pending.wait(),
            CleanupResult::Quarantined(CleanupFailure::OrdinaryRemovalExhausted)
        );
        assert!(assignment_path.exists());
        assert_eq!(failed_remover.calls.load(Ordering::Relaxed), 6);
    }

    #[test]
    fn process_termination_after_partial_cleanup_recovers_in_a_fresh_process() {
        let root = private_work_root();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runner::service::workspace::tests::cleanup_termination_process",
                "--ignored",
            ])
            .env(TERMINATION_FIXTURE_ROOT, root.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(TERMINATION_FIXTURE_STATUS));

        let boot_path = root.path().join(BOOT_A);
        assert!(!boot_path.join(OWNERSHIP_MARKER_NAME).exists());
        assert!(!boot_path.join(TERMINATION_REMOVED_DESCENDANT).exists());
        assert!(root.path().join(CLEANUP_AUTHORITY_NAME).exists());

        let recovered = WorkRootLease::acquire_for_test(root.path(), BOOT_B).unwrap();
        assert!(!boot_path.exists());
        assert!(!root.path().join(CLEANUP_AUTHORITY_NAME).exists());
        assert_eq!(recovered.boot_path(), root.path().join(BOOT_B));
    }

    #[test]
    #[ignore = "subprocess fixture invoked only by the termination recovery test"]
    fn cleanup_termination_process() {
        let root = std::env::var_os(TERMINATION_FIXTURE_ROOT)
            .map(PathBuf::from)
            .expect("termination fixture root is required");
        let owner = WorkRootLease::acquire_with(
            &root,
            BOOT_A,
            filesystem(
                Arc::new(ProcessTerminatingRemover),
                Arc::new(RecordingSleeper::default()),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
        fs::write(
            owner.boot_path().join(TERMINATION_REMOVED_DESCENDANT),
            b"owned",
        )
        .unwrap();
        let result = owner.release_boot_root_pending().wait();
        panic!("termination fixture unexpectedly completed with {result:?}");
    }

    #[test]
    fn process_termination_during_authority_publication_recovers_in_a_fresh_process() {
        let root = private_work_root();
        let stale = WorkRootLease::acquire(root.path(), BOOT_A).unwrap();
        let stale_boot = stale.boot_path().to_owned();
        drop(stale);

        let current_exe = std::env::current_exe().unwrap();
        let status = Command::new("sh")
            .args([
                "-c",
                "ulimit -f 0; exec \"$0\" \"$@\"",
                current_exe.to_str().unwrap(),
                "--exact",
                "runner::service::workspace::tests::cleanup_authority_publication_process",
                "--ignored",
            ])
            .env(AUTHORITY_PUBLICATION_FIXTURE_ROOT, root.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.signal(), Some(libc::SIGXFSZ));
        assert!(stale_boot.join(OWNERSHIP_MARKER_NAME).exists());
        assert!(!root.path().join(CLEANUP_AUTHORITY_NAME).exists());

        let recovered = WorkRootLease::acquire_for_test(root.path(), BOOT_B);
        assert!(recovered.is_ok());
    }

    #[test]
    #[ignore = "subprocess fixture invoked only by the authority publication recovery test"]
    fn cleanup_authority_publication_process() {
        let root = std::env::var_os(AUTHORITY_PUBLICATION_FIXTURE_ROOT)
            .map(PathBuf::from)
            .expect("authority publication fixture root is required");
        let _result = WorkRootLease::acquire_for_test(&root, BOOT_B);
        panic!("authority publication fixture unexpectedly completed");
    }

    #[test]
    fn persistent_recovery_retains_authority_and_never_creates_a_new_boot() {
        let root = private_work_root();
        let first_remover = ScriptedRemover::new([
            RemovalOutcome::Partial,
            RemovalOutcome::Error,
            RemovalOutcome::Error,
            RemovalOutcome::Error,
            RemovalOutcome::Error,
            RemovalOutcome::Error,
        ]);
        let first = WorkRootLease::acquire_with(
            root.path(),
            BOOT_A,
            filesystem(
                first_remover,
                Arc::new(RecordingSleeper::default()),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
        let stale_boot = first.boot_path().to_owned();
        fs::write(stale_boot.join("removed-before-failure"), b"owned").unwrap();
        assert_eq!(
            first.release_boot_root_pending().wait(),
            CleanupResult::Quarantined(CleanupFailure::OrdinaryRemovalExhausted)
        );
        assert!(!stale_boot.join(OWNERSHIP_MARKER_NAME).exists());
        assert!(
            first
                .boot_tree
                .read_cleanup_identity(first.engine.cleanup_identity.as_ref())
                .unwrap()
                .is_some()
        );
        assert!(root.path().join(CLEANUP_AUTHORITY_NAME).exists());
        drop(first);

        let persistent_remover = ScriptedRemover::new([RemovalOutcome::Error; 6]);
        assert_eq!(
            WorkRootLease::acquire_with(
                root.path(),
                BOOT_B,
                filesystem(
                    persistent_remover.clone(),
                    Arc::new(RecordingSleeper::default()),
                    Arc::new(NoopWorkRootHook),
                ),
            )
            .err()
            .unwrap(),
            WorkRootError::StaleRootCleanupFailed
        );
        assert_eq!(persistent_remover.calls.load(Ordering::Relaxed), 6);
        assert!(root.path().join(CLEANUP_AUTHORITY_NAME).exists());
        assert!(!root.path().join(BOOT_B).exists());

        let recovered = WorkRootLease::acquire_for_test(root.path(), BOOT_B).unwrap();
        assert!(!stale_boot.exists());
        assert!(!root.path().join(CLEANUP_AUTHORITY_NAME).exists());
        assert_eq!(recovered.boot_path(), root.path().join(BOOT_B));
    }

    #[test]
    fn recovery_rejects_a_same_contents_replacement_marker() {
        let root = private_work_root();
        let failed_remover = ScriptedRemover::new([RemovalOutcome::Error; 6]);
        let first = WorkRootLease::acquire_with(
            root.path(),
            BOOT_A,
            filesystem(
                failed_remover,
                Arc::new(RecordingSleeper::default()),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
        let stale_boot = first.boot_path().to_owned();
        let marker_path = stale_boot.join(OWNERSHIP_MARKER_NAME);
        let original_marker = File::open(&marker_path).unwrap();
        let sentinel = stale_boot.join("operator-sentinel");
        fs::write(&sentinel, b"do-not-remove").unwrap();
        assert_eq!(
            first.release_boot_root_pending().wait(),
            CleanupResult::Quarantined(CleanupFailure::OrdinaryRemovalExhausted)
        );
        drop(first);

        fs::remove_file(&marker_path).unwrap();
        let mut replacement = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&marker_path)
            .unwrap();
        replacement.write_all(BOOT_MARKER).unwrap();
        replacement
            .set_permissions(Permissions::from_mode(0o600))
            .unwrap();
        replacement.sync_all().unwrap();

        let recovery_remover = ScriptedRemover::new([]);
        let recovered = WorkRootLease::acquire_with(
            root.path(),
            BOOT_B,
            filesystem(
                recovery_remover.clone(),
                Arc::new(RecordingSleeper::default()),
                Arc::new(NoopWorkRootHook),
            ),
        );

        assert_eq!(
            recovered.err().unwrap(),
            WorkRootError::InvalidCleanupAuthority
        );
        assert_eq!(recovery_remover.calls.load(Ordering::Relaxed), 0);
        assert_eq!(fs::read(sentinel).unwrap(), b"do-not-remove");
        assert!(root.path().join(CLEANUP_AUTHORITY_NAME).exists());
        drop(original_marker);
    }

    #[test]
    fn recovery_rejects_authority_or_marker_tampering_and_path_replacement() {
        for mutation in ["authority", "identity", "marker", "replacement"] {
            let root = private_work_root();
            let cleanup_identity = Arc::new(ControlledCleanupIdentityStore(Mutex::new(
                [3_u8; CLEANUP_IDENTITY_BYTES],
            )));
            let failed_remover = ScriptedRemover::new([RemovalOutcome::Error; 6]);
            let first = WorkRootLease::acquire_with(
                root.path(),
                BOOT_A,
                WorkspaceFilesystem::injected_with_cleanup_identity(
                    failed_remover,
                    Arc::new(RecordingSleeper::default()),
                    Arc::new(NoopWorkRootHook),
                    cleanup_identity.clone(),
                ),
            )
            .unwrap();
            let stale_boot = first.boot_path().to_owned();
            let sentinel = stale_boot.join("operator-sentinel");
            let external_sentinel = root.path().join("operator-external-sentinel");
            fs::write(&sentinel, b"do-not-remove").unwrap();
            fs::write(&external_sentinel, b"outside-authority").unwrap();
            assert!(matches!(
                first.release_boot_root_pending().wait(),
                CleanupResult::Quarantined(CleanupFailure::OrdinaryRemovalExhausted)
            ));
            drop(first);

            match mutation {
                "authority" => {
                    fs::write(
                        root.path().join(CLEANUP_AUTHORITY_NAME),
                        b"changed-authority\n",
                    )
                    .unwrap();
                }
                "identity" => {
                    *cleanup_identity.0.lock().unwrap() = [7_u8; CLEANUP_IDENTITY_BYTES];
                }
                "marker" => {
                    fs::write(stale_boot.join(OWNERSHIP_MARKER_NAME), b"changed-owner\n").unwrap();
                }
                "replacement" => {
                    fs::remove_dir_all(&stale_boot).unwrap();
                    create_private_directory(&stale_boot).unwrap();
                    fs::write(&sentinel, b"replacement").unwrap();
                    *cleanup_identity.0.lock().unwrap() = [8_u8; CLEANUP_IDENTITY_BYTES];
                }
                _ => panic!("unknown recovery mutation"),
            }
            let recovery_remover = ScriptedRemover::new([]);
            assert_eq!(
                WorkRootLease::acquire_with(
                    root.path(),
                    BOOT_B,
                    WorkspaceFilesystem::injected_with_cleanup_identity(
                        recovery_remover.clone(),
                        Arc::new(RecordingSleeper::default()),
                        Arc::new(NoopWorkRootHook),
                        cleanup_identity.clone(),
                    ),
                )
                .err()
                .unwrap_or_else(|| panic!("{mutation} mutation was accepted")),
                WorkRootError::InvalidCleanupAuthority
            );
            assert_eq!(recovery_remover.calls.load(Ordering::Relaxed), 0);
            assert_eq!(
                fs::read(&sentinel).unwrap(),
                if mutation == "replacement" {
                    b"replacement".as_slice()
                } else {
                    b"do-not-remove".as_slice()
                }
            );
            assert_eq!(fs::read(external_sentinel).unwrap(), b"outside-authority");
            assert!(root.path().join(CLEANUP_AUTHORITY_NAME).exists());
            assert!(!root.path().join(BOOT_B).exists());
        }
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
        assert!(root.path().join(CLEANUP_AUTHORITY_NAME).exists());
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
            assignment.release_pending(ProcessQuiescence::Proven).wait(),
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
            assignment.release_pending(ProcessQuiescence::Proven).wait(),
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
