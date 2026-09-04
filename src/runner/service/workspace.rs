use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions, Permissions};
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use fs4::{FileExt, TryLockError};
use nix::fcntl::{FcntlArg, FdFlag, fcntl};

use super::workflow_git::WorkflowGitAuthority;

const LOCK_FILE_NAME: &str = ".scherzo-runner-serve.lock";
const OWNERSHIP_MARKER_NAME: &str = ".scherzo-runner-serve-owner-v1";
const BOOT_MARKER: &[u8] = b"scherzo-runner-serve/boot-root/v1\n";
const ASSIGNMENT_MARKER: &[u8] = b"scherzo-runner-serve/assignment-root/v1\n";
const PRIVATE_FILE_MODE: u32 = 0o600;
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
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
    StaleRootCleanupFailed,
    CreateBootRoot,
}

impl fmt::Display for WorkRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WorkRootInUse => "runner work root is already in use",
            Self::UnsafeWorkRoot => "runner work root ownership state is unsafe",
            Self::AmbiguousOwnedRoot => "runner work root contains ambiguous boot state",
            Self::StaleRootCleanupFailed => "runner stale work-root cleanup failed",
            Self::CreateBootRoot => "runner boot root could not be created",
        })
    }
}

impl std::error::Error for WorkRootError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AssignmentRootCreationError {
    Unavailable,
    CleanupFailed,
}

pub(super) trait TreeRemover: Send + Sync {
    fn remove_tree(&self, path: &Path) -> io::Result<()>;
}

pub(super) trait CleanupSleeper: Send + Sync {
    fn sleep(&self, duration: Duration, cancellation: &CleanupCancellation) -> bool;
}

pub(super) trait WorkRootHook: Send + Sync {
    fn before_child_enumeration(&self);
}

struct SystemTreeRemover;

impl TreeRemover for SystemTreeRemover {
    fn remove_tree(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }
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

#[derive(Clone)]
pub(super) struct WorkspaceFilesystem {
    remover: Arc<dyn TreeRemover>,
    sleeper: Arc<dyn CleanupSleeper>,
    hook: Arc<dyn WorkRootHook>,
}

impl WorkspaceFilesystem {
    pub(super) fn system() -> Self {
        Self {
            remover: Arc::new(SystemTreeRemover),
            sleeper: Arc::new(InterruptibleSleeper),
            hook: Arc::new(NoopWorkRootHook),
        }
    }

    #[cfg(test)]
    pub(super) fn injected(
        remover: Arc<dyn TreeRemover>,
        sleeper: Arc<dyn CleanupSleeper>,
        hook: Arc<dyn WorkRootHook>,
    ) -> Self {
        Self {
            remover,
            sleeper,
            hook,
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
}

impl Drop for WorkRootAuthorityShared {
    fn drop(&mut self) {
        // A concurrent fork can retain the open file descriptions until exec,
        // so closing our descriptors alone does not release these locks promptly.
        let _ = FileExt::unlock(&self.lock_file);
        let _ = FileExt::unlock(&self.directory_lock);
    }
}

#[derive(Clone)]
struct CleanupEngine {
    remover: Arc<dyn TreeRemover>,
    sleeper: Arc<dyn CleanupSleeper>,
    cancellation: Arc<CleanupCancellation>,
    authority: WorkRootAuthority,
}

impl CleanupEngine {
    fn remove(&self, tree: &OwnedTree) -> CleanupResult {
        if self.cancellation.is_cancelled() {
            return CleanupResult::Preempted;
        }
        let mut marker_may_be_missing = false;
        let mut delays = REMOVAL_DELAYS.into_iter();
        loop {
            if self.authority.validate().is_err() {
                return CleanupResult::Quarantined(CleanupFailure::Safety);
            }
            match tree.validate(marker_may_be_missing) {
                Ok(TreePresence::Missing) => return CleanupResult::Released,
                Ok(TreePresence::Present) => {}
                Err(()) => {
                    return CleanupResult::Quarantined(CleanupFailure::Safety);
                }
            }
            if self.cancellation.is_cancelled() {
                return CleanupResult::Preempted;
            }
            let removal = self.remover.remove_tree(&tree.path);
            marker_may_be_missing = true;
            match removal {
                Ok(()) => match tree.validate(true) {
                    Ok(TreePresence::Missing) => return CleanupResult::Released,
                    Ok(TreePresence::Present) => {}
                    Err(()) => {
                        return CleanupResult::Quarantined(CleanupFailure::Safety);
                    }
                },
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return CleanupResult::Released;
                }
                Err(_) => {}
            }
            let Some(delay) = delays.next() else {
                return CleanupResult::Quarantined(CleanupFailure::OrdinaryRemovalExhausted);
            };
            if !self.sleeper.sleep(delay, &self.cancellation) {
                return CleanupResult::Preempted;
            }
        }
    }
}

#[derive(Clone)]
struct MarkerProof {
    path: PathBuf,
    contents: &'static [u8],
    device: u64,
    inode: u64,
}

impl MarkerProof {
    fn capture(path: PathBuf, contents: &'static [u8]) -> Result<Self, ()> {
        let metadata = fs::symlink_metadata(&path).map_err(|_| ())?;
        let proof = Self {
            path,
            contents,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        verify_marker(&proof)?;
        Ok(proof)
    }
}

#[derive(Clone)]
struct OwnedTree {
    path: PathBuf,
    parent_device: u64,
    parent_inode: u64,
    device: u64,
    inode: u64,
    marker: Option<MarkerProof>,
}

enum TreePresence {
    Missing,
    Present,
}

impl OwnedTree {
    fn capture(parent: &Path, path: PathBuf, marker: Option<MarkerProof>) -> Result<Self, ()> {
        let parent_metadata = safe_directory(parent)?;
        let metadata = safe_directory(&path)?;
        Ok(Self {
            path,
            parent_device: parent_metadata.dev(),
            parent_inode: parent_metadata.ino(),
            device: metadata.dev(),
            inode: metadata.ino(),
            marker,
        })
    }

    fn validate(&self, marker_may_be_missing: bool) -> Result<TreePresence, ()> {
        let parent = self.path.parent().ok_or(())?;
        let parent_metadata = safe_directory(parent)?;
        if parent_metadata.dev() != self.parent_device || parent_metadata.ino() != self.parent_inode
        {
            return Err(());
        }
        let metadata = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(TreePresence::Missing);
            }
            Err(_) => return Err(()),
        };
        if !safe_owned_directory(&metadata)
            || metadata.dev() != self.device
            || metadata.ino() != self.inode
        {
            return Err(());
        }
        if let Some(marker) = &self.marker {
            match verify_marker(marker) {
                Ok(()) => {}
                Err(())
                    if marker_may_be_missing
                        && fs::symlink_metadata(&marker.path)
                            .is_err_and(|error| error.kind() == io::ErrorKind::NotFound) => {}
                Err(()) => return Err(()),
            }
        }
        Ok(TreePresence::Present)
    }
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

fn verify_marker(marker: &MarkerProof) -> Result<(), ()> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW_FLAG)
        .open(&marker.path)
        .map_err(|_| ())?;
    let metadata = file.metadata().map_err(|_| ())?;
    let path_metadata = fs::symlink_metadata(&marker.path).map_err(|_| ())?;
    if !safe_private_file(&metadata)
        || path_metadata.dev() != metadata.dev()
        || path_metadata.ino() != metadata.ino()
        || metadata.dev() != marker.device
        || metadata.ino() != marker.inode
    {
        return Err(());
    }
    let mut contents = Vec::with_capacity(marker.contents.len());
    std::io::Read::by_ref(&mut file)
        .take(u64::try_from(marker.contents.len()).map_err(|_| ())? + 1)
        .read_to_end(&mut contents)
        .map_err(|_| ())?;
    (contents == marker.contents).then_some(()).ok_or(())
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
            cancellation: Arc::clone(&cancellation),
            authority: authority.clone(),
        };

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
            let marker = MarkerProof::capture(path.join(OWNERSHIP_MARKER_NAME), BOOT_MARKER)
                .map_err(|()| WorkRootError::AmbiguousOwnedRoot)?;
            let tree = OwnedTree::capture(work_root, path, Some(marker))
                .map_err(|()| WorkRootError::AmbiguousOwnedRoot)?;
            match engine.remove(&tree) {
                CleanupResult::Released => {}
                CleanupResult::Quarantined(CleanupFailure::Safety) => {
                    return Err(WorkRootError::AmbiguousOwnedRoot);
                }
                CleanupResult::Quarantined(
                    CleanupFailure::OrdinaryRemovalExhausted | CleanupFailure::Quiescence,
                )
                | CleanupResult::Preempted => {
                    return Err(WorkRootError::StaleRootCleanupFailed);
                }
            }
        }

        authority
            .validate()
            .map_err(|()| WorkRootError::UnsafeWorkRoot)?;
        let boot_path = work_root.join(boot_id);
        create_private_directory(&boot_path).map_err(|_| WorkRootError::CreateBootRoot)?;
        let marker =
            create_marker(&boot_path, BOOT_MARKER).map_err(|_| WorkRootError::CreateBootRoot)?;
        let boot_tree = OwnedTree::capture(work_root, boot_path, Some(marker))
            .map_err(|_| WorkRootError::CreateBootRoot)?;
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
        let assignment_marker = create_marker(&assignment_path, ASSIGNMENT_MARKER)
            .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
        let assignment_tree = OwnedTree::capture(
            &self.boot_tree.path,
            assignment_path.clone(),
            Some(assignment_marker.clone()),
        )
        .map_err(|()| AssignmentRootCreationError::CleanupFailed)?;
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
        let workspace_tree =
            match OwnedTree::capture(&assignment_path, workspace_path, Some(assignment_marker)) {
                Ok(tree) => tree,
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
            .spawn(move || worker_completion.complete(engine.remove(&tree)))
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

fn create_marker(parent: &Path, contents: &'static [u8]) -> Result<MarkerProof, ()> {
    let path = parent.join(OWNERSHIP_MARKER_NAME);
    let mut marker = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(NOFOLLOW_FLAG)
        .open(&path)
        .map_err(|_| ())?;
    marker
        .set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))
        .and_then(|()| marker.write_all(contents))
        .and_then(|()| marker.sync_all())
        .map_err(|_| ())?;
    set_close_on_exec(&marker)?;
    MarkerProof::capture(path, contents)
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
        fn remove_tree(&self, path: &Path) -> io::Result<()> {
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
        let second = WorkRootLease::acquire(root.path(), BOOT_B).unwrap();
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
    fn partial_removal_not_found_recovers_but_exhaustion_quarantines_enclosing_root() {
        let recovered_root = private_work_root();
        let recovered_remover =
            ScriptedRemover::new([RemovalOutcome::Partial, RemovalOutcome::NotFound]);
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
            vec![REMOVAL_DELAYS[0]]
        );
        assert!(
            workspace_path.exists(),
            "injected NotFound leaves its fixture path"
        );

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
    fn safety_and_quiescence_failures_make_no_destructive_call() {
        let root = private_work_root();
        let remover = ScriptedRemover::new([]);
        let owner = WorkRootLease::acquire_with(
            root.path(),
            BOOT_A,
            filesystem(
                remover.clone(),
                Arc::new(RecordingSleeper::default()),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
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
        let unsafe_owner = WorkRootLease::acquire_with(
            unsafe_root.path(),
            BOOT_A,
            filesystem(
                unsafe_remover.clone(),
                Arc::new(RecordingSleeper::default()),
                Arc::new(NoopWorkRootHook),
            ),
        )
        .unwrap();
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
        let second = WorkRootLease::acquire(root.path(), BOOT_B);

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
        let second = WorkRootLease::acquire(root.path(), BOOT_B).unwrap();
        assert_eq!(second.boot_path(), root.path().join(BOOT_B));
        let _ = child.kill();
        let _ = child.wait();
    }
}
