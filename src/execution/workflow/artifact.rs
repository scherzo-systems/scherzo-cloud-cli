use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::fd::OwnedFd;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

#[cfg(test)]
use std::path::PathBuf;

use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, fchmod, fstat, mkdirat, open, openat, statat, unlinkat,
};
use rustix::io::Errno;

use super::admission::AdmittedExecutionContext;

const COPY_BUFFER_BYTES: usize = 64 * 1024;
const COPY_BUFFER_BYTES_U64: u64 = 64 * 1024;
const IDENTITY_ATTEMPTS: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactStagingFailure {
    ExecutionRootUnavailable,
    StagingParentUnavailable,
    StagingParentExposed,
    IdentityUnavailable,
}

impl fmt::Display for ArtifactStagingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "artifact staging failure: {self:?}")
    }
}

impl std::error::Error for ArtifactStagingFailure {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactReleaseFailure {
    CleanupUnavailable,
}

impl fmt::Display for ArtifactReleaseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "artifact release failure: {self:?}")
    }
}

impl std::error::Error for ArtifactReleaseFailure {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CaptureFailureKind {
    AbsolutePath,
    LexicalEscape,
    EmptyPath,
    Missing,
    SymbolicLink,
    NotDirectory,
    NotRegularFile,
    SourceUnavailable,
    FileCountLimitExceeded,
    FileSizeLimitExceeded,
    TotalSizeLimitExceeded,
    StagingUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CaptureFailure {
    output_identity: Arc<str>,
    kind: CaptureFailureKind,
}

impl CaptureFailure {
    pub(crate) fn output_identity(&self) -> &str {
        &self.output_identity
    }

    pub(crate) fn kind(&self) -> CaptureFailureKind {
        self.kind
    }

    fn new(output_identity: Arc<str>, kind: CaptureFailureKind) -> Self {
        Self {
            output_identity,
            kind,
        }
    }
}

impl fmt::Display for CaptureFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "output capture failure for {:?}: {:?}",
            self.output_identity, self.kind
        )
    }
}

impl std::error::Error for CaptureFailure {}

#[derive(Clone)]
pub(crate) struct ArtifactHandle {
    store_identity: Arc<str>,
    artifact_identity: Arc<str>,
    lease: Arc<ArtifactLease>,
}

struct ArtifactLease {
    store: Weak<ArtifactStagingInner>,
    artifact_identity: Arc<str>,
    size: u64,
    budgeted: AtomicBool,
}

impl ArtifactLease {
    fn commit_budget(&self) {
        self.budgeted.store(true, Ordering::Release);
    }

    fn release_budget(&self, store: &ArtifactStagingInner) {
        if self.budgeted.swap(false, Ordering::AcqRel) {
            store.release_budget(self.size);
        }
    }
}

impl Drop for ArtifactLease {
    fn drop(&mut self) {
        if let Some(store) = self.store.upgrade()
            && store.remove_artifact(&self.artifact_identity)
        {
            self.release_budget(&store);
        }
    }
}

impl ArtifactHandle {
    pub(crate) fn opaque_id(&self) -> &str {
        &self.artifact_identity
    }
}

impl PartialEq for ArtifactHandle {
    fn eq(&self, other: &Self) -> bool {
        self.store_identity == other.store_identity
            && self.artifact_identity == other.artifact_identity
    }
}

impl Eq for ArtifactHandle {}

impl fmt::Debug for ArtifactHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ArtifactHandle")
            .field(&self.artifact_identity)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedArtifact {
    handle: ArtifactHandle,
    output_identity: Arc<str>,
    size: u64,
    media_type: Arc<str>,
}

impl CapturedArtifact {
    pub(crate) fn handle(&self) -> &ArtifactHandle {
        &self.handle
    }

    pub(crate) fn output_identity(&self) -> &str {
        &self.output_identity
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn media_type(&self) -> &str {
        &self.media_type
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CaptureDeclaration<'a> {
    output_identity: &'a str,
    declared_path: &'a Path,
    media_type: &'a str,
}

impl<'a> CaptureDeclaration<'a> {
    pub(crate) fn new(
        output_identity: &'a str,
        declared_path: &'a Path,
        media_type: &'a str,
    ) -> Self {
        Self {
            output_identity,
            declared_path,
            media_type,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactReadFailure {
    UnknownHandle,
    Unavailable,
    DestinationWrite,
}

impl fmt::Display for ArtifactReadFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "artifact read failure: {self:?}")
    }
}

impl std::error::Error for ArtifactReadFailure {}

#[derive(Clone)]
pub(crate) struct ArtifactStaging {
    inner: Arc<ArtifactStagingInner>,
}

struct ArtifactStagingInner {
    execution_root: OwnedFd,
    staging_parent: OwnedFd,
    staging_root: OwnedFd,
    #[cfg(test)]
    staging_path: PathBuf,
    store_identity: Arc<str>,
    maximum_files: NonZeroUsize,
    maximum_file_bytes: NonZeroU64,
    maximum_total_bytes: NonZeroU64,
    lifecycle: RwLock<ArtifactStagingLifecycle>,
    artifacts: Mutex<BTreeSet<Arc<str>>>,
    budget: Mutex<CaptureBudgetLedger>,
    #[cfg(test)]
    artifact_unlinks_blocked: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CaptureBudgetLedger {
    captured_files: usize,
    captured_bytes: u64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ArtifactStagingLifecycle {
    Active,
    CleanupFailed,
    Released,
}

impl ArtifactStaging {
    pub(crate) fn create(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
    ) -> Result<Self, ArtifactStagingFailure> {
        let limits = execution.limits();
        Self::create_for_execution(
            execution.root(),
            staging_parent,
            limits.maximum_captured_files(),
            limits.maximum_captured_file_bytes(),
            limits.maximum_total_captured_bytes(),
        )
    }

    fn create_for_execution(
        execution_root: &Path,
        staging_parent: &Path,
        maximum_files: NonZeroUsize,
        maximum_file_bytes: NonZeroU64,
        maximum_total_bytes: NonZeroU64,
    ) -> Result<Self, ArtifactStagingFailure> {
        let canonical_execution_root = std::fs::canonicalize(execution_root)
            .map_err(|_| ArtifactStagingFailure::ExecutionRootUnavailable)?;
        let canonical_staging_parent = std::fs::canonicalize(staging_parent)
            .map_err(|_| ArtifactStagingFailure::StagingParentUnavailable)?;
        if canonical_staging_parent.starts_with(&canonical_execution_root) {
            return Err(ArtifactStagingFailure::StagingParentExposed);
        }

        let execution_root = open_directory(&canonical_execution_root)
            .map_err(|_| ArtifactStagingFailure::ExecutionRootUnavailable)?;
        let staging_parent_handle = open_directory(&canonical_staging_parent)
            .map_err(|_| ArtifactStagingFailure::StagingParentUnavailable)?;
        let (store_identity, staging_root) = create_staging_root(&staging_parent_handle)?;
        #[cfg(test)]
        let staging_path = canonical_staging_parent.join(store_identity.as_ref());

        Ok(Self {
            inner: Arc::new(ArtifactStagingInner {
                execution_root,
                staging_parent: staging_parent_handle,
                staging_root,
                #[cfg(test)]
                staging_path,
                store_identity,
                maximum_files,
                maximum_file_bytes,
                maximum_total_bytes,
                lifecycle: RwLock::new(ArtifactStagingLifecycle::Active),
                artifacts: Mutex::new(BTreeSet::new()),
                budget: Mutex::new(CaptureBudgetLedger::default()),
                #[cfg(test)]
                artifact_unlinks_blocked: AtomicBool::new(false),
            }),
        })
    }

    pub(super) fn is_bound_to(&self, execution: &AdmittedExecutionContext) -> bool {
        let limits = execution.limits();
        if self.inner.maximum_files != limits.maximum_captured_files()
            || self.inner.maximum_file_bytes != limits.maximum_captured_file_bytes()
            || self.inner.maximum_total_bytes != limits.maximum_total_captured_bytes()
        {
            return false;
        }
        let Ok(candidate_root) = open_directory(execution.root()) else {
            return false;
        };
        let (Ok(bound_metadata), Ok(candidate_metadata)) =
            (fstat(&self.inner.execution_root), fstat(&candidate_root))
        else {
            return false;
        };
        bound_metadata.st_dev == candidate_metadata.st_dev
            && bound_metadata.st_ino == candidate_metadata.st_ino
    }

    pub(crate) fn capture_files(
        &self,
        declarations: &[CaptureDeclaration<'_>],
    ) -> Result<BTreeMap<String, CapturedArtifact>, CaptureFailure> {
        let Some(first) = declarations.first() else {
            return Ok(BTreeMap::new());
        };
        let failure_identity = || Arc::<str>::from(first.output_identity);
        let mut budget = self.inner.budget.lock().map_err(|_| {
            CaptureFailure::new(failure_identity(), CaptureFailureKind::StagingUnavailable)
        })?;
        let remaining_files = self
            .inner
            .maximum_files
            .get()
            .saturating_sub(budget.captured_files);
        if declarations.len() > remaining_files {
            return Err(CaptureFailure::new(
                failure_identity(),
                CaptureFailureKind::FileCountLimitExceeded,
            ));
        }

        let mut captured = BTreeMap::new();
        let mut candidate_bytes = 0_u64;
        for declaration in declarations {
            let available_total_bytes = self
                .inner
                .maximum_total_bytes
                .get()
                .saturating_sub(budget.captured_bytes)
                .saturating_sub(candidate_bytes);
            let (maximum_bytes, overflow_kind) =
                if available_total_bytes < self.inner.maximum_file_bytes.get() {
                    (
                        available_total_bytes,
                        CaptureFailureKind::TotalSizeLimitExceeded,
                    )
                } else {
                    (
                        self.inner.maximum_file_bytes.get(),
                        CaptureFailureKind::FileSizeLimitExceeded,
                    )
                };
            let artifact = match self.stage(
                Arc::from(declaration.output_identity),
                declaration.declared_path,
                Arc::from(declaration.media_type),
                maximum_bytes,
                overflow_kind,
            ) {
                Ok(artifact) => artifact,
                Err(failure) => return Err(self.rollback_capture_set(&captured, failure)),
            };
            let Some(updated_candidate_bytes) = candidate_bytes.checked_add(artifact.size) else {
                captured.insert(declaration.output_identity.to_owned(), artifact);
                let failure = CaptureFailure::new(
                    Arc::from(declaration.output_identity),
                    CaptureFailureKind::TotalSizeLimitExceeded,
                );
                return Err(self.rollback_capture_set(&captured, failure));
            };
            candidate_bytes = updated_candidate_bytes;
            captured.insert(declaration.output_identity.to_owned(), artifact);
        }

        budget.captured_files += declarations.len();
        budget.captured_bytes += candidate_bytes;
        for artifact in captured.values() {
            artifact.handle.lease.commit_budget();
        }
        drop(budget);
        Ok(captured)
    }

    pub(crate) fn copy_to(
        &self,
        handle: &ArtifactHandle,
        destination: &mut impl Write,
    ) -> Result<u64, ArtifactReadFailure> {
        let mut source = self.open_artifact(handle)?;
        let mut copied = 0_u64;
        let mut buffer = [0_u8; COPY_BUFFER_BYTES];
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(|_| ArtifactReadFailure::Unavailable)?;
            if read == 0 {
                return Ok(copied);
            }
            destination
                .write_all(&buffer[..read])
                .map_err(|_| ArtifactReadFailure::DestinationWrite)?;
            let read = u64::try_from(read).map_err(|_| ArtifactReadFailure::Unavailable)?;
            copied = copied
                .checked_add(read)
                .ok_or(ArtifactReadFailure::Unavailable)?;
        }
    }

    pub(crate) fn release(&self) -> Result<(), ArtifactReleaseFailure> {
        self.inner.cleanup()
    }

    #[cfg(test)]
    pub(crate) fn staged_artifact_count(&self) -> usize {
        self.inner
            .artifacts
            .lock()
            .map_or(0, |artifacts| artifacts.len())
    }

    #[cfg(test)]
    pub(crate) fn budget_usage(&self) -> (usize, u64) {
        let budget = self
            .inner
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (budget.captured_files, budget.captured_bytes)
    }

    #[cfg(test)]
    fn block_artifact_unlinks(&self) {
        self.inner
            .artifact_unlinks_blocked
            .store(true, Ordering::Release);
    }

    fn rollback_capture_set(
        &self,
        captured: &BTreeMap<String, CapturedArtifact>,
        failure: CaptureFailure,
    ) -> CaptureFailure {
        let mut rollback_complete = true;
        for artifact in captured.values() {
            rollback_complete &= self
                .inner
                .remove_artifact_while_active(&artifact.handle.artifact_identity);
        }
        if rollback_complete {
            failure
        } else {
            self.inner.mark_cleanup_failed();
            CaptureFailure::new(
                Arc::clone(&failure.output_identity),
                CaptureFailureKind::StagingUnavailable,
            )
        }
    }

    fn stage(
        &self,
        output_identity: Arc<str>,
        declared_path: &Path,
        media_type: Arc<str>,
        maximum_bytes: u64,
        overflow_kind: CaptureFailureKind,
    ) -> Result<CapturedArtifact, CaptureFailure> {
        self.stage_with_copier(
            output_identity,
            declared_path,
            media_type,
            maximum_bytes,
            overflow_kind,
            &mut PortableCopier,
        )
    }

    #[cfg(test)]
    fn capture_with_copier(
        &self,
        output_identity: Arc<str>,
        declared_path: &Path,
        media_type: Arc<str>,
        copier: &mut impl StreamCopier,
    ) -> Result<CapturedArtifact, CaptureFailure> {
        self.stage_with_copier(
            output_identity,
            declared_path,
            media_type,
            self.inner.maximum_file_bytes.get(),
            CaptureFailureKind::FileSizeLimitExceeded,
            copier,
        )
    }

    fn stage_with_copier(
        &self,
        output_identity: Arc<str>,
        declared_path: &Path,
        media_type: Arc<str>,
        maximum_bytes: u64,
        overflow_kind: CaptureFailureKind,
        copier: &mut impl StreamCopier,
    ) -> Result<CapturedArtifact, CaptureFailure> {
        let lifecycle = self.inner.lifecycle.read().map_err(|_| {
            CaptureFailure::new(
                Arc::clone(&output_identity),
                CaptureFailureKind::StagingUnavailable,
            )
        })?;
        if *lifecycle != ArtifactStagingLifecycle::Active {
            return Err(CaptureFailure::new(
                output_identity,
                CaptureFailureKind::StagingUnavailable,
            ));
        }
        let components = capture_components(declared_path)
            .map_err(|kind| CaptureFailure::new(Arc::clone(&output_identity), kind))?;
        let mut source = open_regular_file(&self.inner.execution_root, &components)
            .map_err(|kind| CaptureFailure::new(Arc::clone(&output_identity), kind))?;
        if source
            .metadata()
            .is_ok_and(|metadata| metadata.len() > maximum_bytes)
        {
            return Err(CaptureFailure::new(output_identity, overflow_kind));
        }

        let (artifact_identity, mut destination) = self
            .create_destination()
            .map_err(|kind| CaptureFailure::new(Arc::clone(&output_identity), kind))?;
        let capture_result = copier
            .copy(&mut source, &mut destination, maximum_bytes)
            .map_err(|kind| {
                if kind == CaptureFailureKind::FileSizeLimitExceeded {
                    overflow_kind
                } else {
                    kind
                }
            })
            .and_then(|size| {
                destination
                    .flush()
                    .map_err(|_| CaptureFailureKind::StagingUnavailable)?;
                fchmod(&destination, Mode::RUSR)
                    .map_err(|_| CaptureFailureKind::StagingUnavailable)?;
                Ok(size)
            });
        drop(destination);

        match capture_result {
            Ok(size) => Ok(CapturedArtifact {
                handle: ArtifactHandle {
                    store_identity: Arc::clone(&self.inner.store_identity),
                    artifact_identity: Arc::clone(&artifact_identity),
                    lease: Arc::new(ArtifactLease {
                        store: Arc::downgrade(&self.inner),
                        artifact_identity,
                        size,
                        budgeted: AtomicBool::new(false),
                    }),
                },
                output_identity,
                size,
                media_type,
            }),
            Err(kind) => {
                drop(lifecycle);
                if self.inner.remove_artifact_while_active(&artifact_identity) {
                    Err(CaptureFailure::new(output_identity, kind))
                } else {
                    self.inner.mark_cleanup_failed();
                    Err(CaptureFailure::new(
                        output_identity,
                        CaptureFailureKind::StagingUnavailable,
                    ))
                }
            }
        }
    }

    fn create_destination(&self) -> Result<(Arc<str>, File), CaptureFailureKind> {
        for _ in 0..IDENTITY_ATTEMPTS {
            let identity = Arc::<str>::from(format!(
                "art_{}",
                ulid::Ulid::generate().to_string().to_ascii_lowercase()
            ));
            match openat(
                &self.inner.staging_root,
                identity.as_ref(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            ) {
                Ok(file) => {
                    let Ok(mut artifacts) = self.inner.artifacts.lock() else {
                        let _ = unlinkat(
                            &self.inner.staging_root,
                            identity.as_ref(),
                            AtFlags::empty(),
                        );
                        return Err(CaptureFailureKind::StagingUnavailable);
                    };
                    artifacts.insert(Arc::clone(&identity));
                    return Ok((identity, File::from(file)));
                }
                Err(Errno::EXIST) => {}
                Err(_) => return Err(CaptureFailureKind::StagingUnavailable),
            }
        }
        Err(CaptureFailureKind::StagingUnavailable)
    }

    fn open_artifact(&self, handle: &ArtifactHandle) -> Result<File, ArtifactReadFailure> {
        let lifecycle = self
            .inner
            .lifecycle
            .read()
            .map_err(|_| ArtifactReadFailure::Unavailable)?;
        if *lifecycle != ArtifactStagingLifecycle::Active
            || handle.store_identity != self.inner.store_identity
        {
            return Err(ArtifactReadFailure::UnknownHandle);
        }
        let opened = openat(
            &self.inner.staging_root,
            handle.artifact_identity.as_ref(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|failure| match failure {
            Errno::NOENT | Errno::LOOP => ArtifactReadFailure::UnknownHandle,
            _ => ArtifactReadFailure::Unavailable,
        })?;
        let metadata = fstat(&opened).map_err(|_| ArtifactReadFailure::Unavailable)?;
        if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
            return Err(ArtifactReadFailure::Unavailable);
        }
        Ok(File::from(opened))
    }

    pub(super) fn discard(&self, artifact: &CapturedArtifact) {
        if artifact.handle.store_identity == self.inner.store_identity
            && self
                .inner
                .remove_artifact(&artifact.handle.artifact_identity)
        {
            artifact.handle.lease.release_budget(&self.inner);
        }
    }
}

impl ArtifactStagingInner {
    fn release_budget(&self, size: u64) {
        let Ok(mut budget) = self.budget.lock() else {
            return;
        };
        budget.captured_files = budget.captured_files.saturating_sub(1);
        budget.captured_bytes = budget.captured_bytes.saturating_sub(size);
    }

    fn remove_artifact(&self, artifact_identity: &str) -> bool {
        let Ok(lifecycle) = self.lifecycle.read() else {
            return false;
        };
        if *lifecycle == ArtifactStagingLifecycle::Released {
            return true;
        }
        self.remove_artifact_while_active(artifact_identity)
    }

    fn remove_artifact_while_active(&self, artifact_identity: &str) -> bool {
        #[cfg(test)]
        if self.artifact_unlinks_blocked.load(Ordering::Acquire) {
            return false;
        }
        let removed = match unlinkat(&self.staging_root, artifact_identity, AtFlags::empty()) {
            Ok(()) | Err(Errno::NOENT) => true,
            Err(_) => false,
        };
        if removed && let Ok(mut artifacts) = self.artifacts.lock() {
            artifacts.remove(artifact_identity);
        }
        removed
    }

    fn mark_cleanup_failed(&self) {
        let Ok(mut lifecycle) = self.lifecycle.write() else {
            return;
        };
        if *lifecycle == ArtifactStagingLifecycle::Active {
            *lifecycle = ArtifactStagingLifecycle::CleanupFailed;
        }
    }

    fn cleanup(&self) -> Result<(), ArtifactReleaseFailure> {
        let mut lifecycle = self
            .lifecycle
            .write()
            .map_err(|_| ArtifactReleaseFailure::CleanupUnavailable)?;
        if *lifecycle == ArtifactStagingLifecycle::Released {
            return Ok(());
        }

        let cleanup_result = self.cleanup_active();
        *lifecycle = if cleanup_result.is_ok() {
            ArtifactStagingLifecycle::Released
        } else {
            ArtifactStagingLifecycle::CleanupFailed
        };
        cleanup_result
    }

    fn cleanup_active(&self) -> Result<(), ArtifactReleaseFailure> {
        let mut artifacts = self
            .artifacts
            .lock()
            .map_err(|_| ArtifactReleaseFailure::CleanupUnavailable)?;
        let identities = artifacts.iter().cloned().collect::<Vec<_>>();
        for identity in identities {
            match unlinkat(&self.staging_root, identity.as_ref(), AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => {
                    artifacts.remove(&identity);
                }
                Err(_) => return Err(ArtifactReleaseFailure::CleanupUnavailable),
            }
        }
        drop(artifacts);

        let opened =
            fstat(&self.staging_root).map_err(|_| ArtifactReleaseFailure::CleanupUnavailable)?;
        let named = statat(
            &self.staging_parent,
            self.store_identity.as_ref(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| ArtifactReleaseFailure::CleanupUnavailable)?;
        if opened.st_dev != named.st_dev
            || opened.st_ino != named.st_ino
            || FileType::from_raw_mode(named.st_mode) != FileType::Directory
        {
            return Err(ArtifactReleaseFailure::CleanupUnavailable);
        }
        unlinkat(
            &self.staging_parent,
            self.store_identity.as_ref(),
            AtFlags::REMOVEDIR,
        )
        .map_err(|_| ArtifactReleaseFailure::CleanupUnavailable)
    }
}

impl Drop for ArtifactStagingInner {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

trait StreamCopier {
    fn copy(
        &mut self,
        source: &mut File,
        destination: &mut File,
        maximum_bytes: u64,
    ) -> Result<u64, CaptureFailureKind>;
}

struct PortableCopier;

impl StreamCopier for PortableCopier {
    fn copy(
        &mut self,
        source: &mut File,
        destination: &mut File,
        maximum_bytes: u64,
    ) -> Result<u64, CaptureFailureKind> {
        copy_bounded(source, destination, maximum_bytes)
    }
}

fn copy_bounded(
    source: &mut impl Read,
    destination: &mut impl Write,
    maximum_bytes: u64,
) -> Result<u64, CaptureFailureKind> {
    let mut copied = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let remaining = maximum_bytes.saturating_sub(copied);
        let permitted_read = remaining.saturating_add(1).min(COPY_BUFFER_BYTES_U64);
        let permitted_read = usize::try_from(permitted_read)
            .map_err(|_| CaptureFailureKind::FileSizeLimitExceeded)?;
        let read = source
            .read(&mut buffer[..permitted_read])
            .map_err(|_| CaptureFailureKind::SourceUnavailable)?;
        if read == 0 {
            return Ok(copied);
        }
        let read_length =
            u64::try_from(read).map_err(|_| CaptureFailureKind::FileSizeLimitExceeded)?;
        if read_length > remaining {
            return Err(CaptureFailureKind::FileSizeLimitExceeded);
        }
        destination
            .write_all(&buffer[..read])
            .map_err(|_| CaptureFailureKind::StagingUnavailable)?;
        copied += read_length;
    }
}

fn capture_components(path: &Path) -> Result<Vec<OsString>, CaptureFailureKind> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {
                return Err(CaptureFailureKind::AbsolutePath);
            }
            Component::ParentDir => return Err(CaptureFailureKind::LexicalEscape),
            Component::CurDir => {}
            Component::Normal(component) => components.push(component.to_owned()),
        }
    }
    if components.is_empty() {
        return Err(CaptureFailureKind::EmptyPath);
    }
    Ok(components)
}

fn open_directory(path: &Path) -> Result<OwnedFd, Errno> {
    open(path, directory_open_flags(), Mode::empty())
}

fn directory_open_flags() -> OFlags {
    let common = OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        common | OFlags::PATH
    }
    #[cfg(target_vendor = "apple")]
    {
        common | OFlags::from_bits_retain(libc::O_SEARCH.unsigned_abs())
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    {
        common | OFlags::RDONLY
    }
}

fn create_staging_root(
    staging_parent: &OwnedFd,
) -> Result<(Arc<str>, OwnedFd), ArtifactStagingFailure> {
    for _ in 0..IDENTITY_ATTEMPTS {
        let identity = Arc::<str>::from(format!(
            ".capture-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        ));
        match mkdirat(staging_parent, identity.as_ref(), Mode::RWXU) {
            Ok(()) => {
                let directory = openat(
                    staging_parent,
                    identity.as_ref(),
                    directory_open_flags(),
                    Mode::empty(),
                );
                match directory {
                    Ok(directory) => return Ok((identity, directory)),
                    Err(_) => {
                        let _ = unlinkat(staging_parent, identity.as_ref(), AtFlags::REMOVEDIR);
                        return Err(ArtifactStagingFailure::IdentityUnavailable);
                    }
                }
            }
            Err(Errno::EXIST) => {}
            Err(_) => return Err(ArtifactStagingFailure::IdentityUnavailable),
        }
    }
    Err(ArtifactStagingFailure::IdentityUnavailable)
}

fn open_regular_file(
    execution_root: &OwnedFd,
    components: &[OsString],
) -> Result<File, CaptureFailureKind> {
    let (file_name, directories) = components
        .split_last()
        .ok_or(CaptureFailureKind::EmptyPath)?;
    let mut current_directory = openat(execution_root, ".", directory_open_flags(), Mode::empty())
        .map_err(classify_source_open_failure)?;

    for directory in directories {
        let file_type = component_type(&current_directory, directory)?;
        if file_type == FileType::Symlink {
            return Err(CaptureFailureKind::SymbolicLink);
        }
        if file_type != FileType::Directory {
            return Err(CaptureFailureKind::NotDirectory);
        }
        let opened = openat(
            &current_directory,
            directory,
            directory_open_flags(),
            Mode::empty(),
        )
        .map_err(classify_source_open_failure)?;
        let metadata = fstat(&opened).map_err(|_| CaptureFailureKind::SourceUnavailable)?;
        if FileType::from_raw_mode(metadata.st_mode) != FileType::Directory {
            return Err(CaptureFailureKind::NotDirectory);
        }
        current_directory = opened;
    }

    let file_type = component_type(&current_directory, file_name)?;
    if file_type == FileType::Symlink {
        return Err(CaptureFailureKind::SymbolicLink);
    }
    if file_type != FileType::RegularFile {
        return Err(CaptureFailureKind::NotRegularFile);
    }
    let opened = openat(
        &current_directory,
        file_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(classify_source_open_failure)?;
    let metadata = fstat(&opened).map_err(|_| CaptureFailureKind::SourceUnavailable)?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
        return Err(CaptureFailureKind::NotRegularFile);
    }
    Ok(File::from(opened))
}

fn component_type(directory: &OwnedFd, component: &OsStr) -> Result<FileType, CaptureFailureKind> {
    let metadata = statat(directory, component, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(classify_source_open_failure)?;
    Ok(FileType::from_raw_mode(metadata.st_mode))
}

fn classify_source_open_failure(failure: Errno) -> CaptureFailureKind {
    match failure {
        Errno::NOENT => CaptureFailureKind::Missing,
        Errno::LOOP => CaptureFailureKind::SymbolicLink,
        Errno::NOTDIR => CaptureFailureKind::NotDirectory,
        _ => CaptureFailureKind::SourceUnavailable,
    }
}

#[cfg(test)]
mod tests;
