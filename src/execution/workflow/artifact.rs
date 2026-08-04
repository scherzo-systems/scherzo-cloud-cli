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
    AtFlags, FileType, Mode, OFlags, fchmod, fstat, mkdirat, openat, statat, unlinkat,
};
use rustix::io::Errno;

use super::admission::AdmittedExecutionContext;
use super::execution_root::{AdmittedExecutionRoot, directory_open_flags, open_directory};
use super::private_staging::{
    StagingLifecycle, cleanup_staging, mark_cleanup_failed as mark_staging_cleanup_failed,
};

const COPY_BUFFER_BYTES: usize = 64 * 1024;
const COPY_BUFFER_BYTES_U64: u64 = 64 * 1024;
const IDENTITY_ATTEMPTS: usize = 16;

#[derive(Clone, Default)]
pub(crate) struct CaptureCancellation {
    cancelled: Arc<AtomicBool>,
    #[cfg(test)]
    observer: Option<Arc<dyn CaptureBoundaryObserver>>,
}

impl CaptureCancellation {
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn check(&self) -> Result<(), CaptureAttemptFailure> {
        if self.is_cancelled() {
            Err(CaptureAttemptFailure::Cancelled)
        } else {
            Ok(())
        }
    }

    fn boundary(
        &self,
        output_identity: &Arc<str>,
        kind: CaptureBoundaryKind,
    ) -> Result<(), CaptureAttemptFailure> {
        #[cfg(not(test))]
        let _ = (output_identity, kind);
        #[cfg(test)]
        if let Some(observer) = &self.observer {
            observer.reached(CaptureBoundary {
                output_identity: Arc::clone(output_identity),
                kind,
            });
        }
        self.check()
    }

    #[cfg(test)]
    pub(crate) fn with_observer(observer: Arc<dyn CaptureBoundaryObserver>) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            observer: Some(observer),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CaptureBoundaryKind {
    BeforeSourceOpen,
    BeforeRead,
    BeforeWrite,
    AfterWrite,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CaptureBoundary {
    pub(crate) output_identity: Arc<str>,
    pub(crate) kind: CaptureBoundaryKind,
}

#[cfg(test)]
pub(crate) trait CaptureBoundaryObserver: Send + Sync {
    fn reached(&self, boundary: CaptureBoundary);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CaptureAttemptFailure {
    Cancelled,
    Capture(CaptureFailure),
}

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

pub(crate) struct CaptureCandidateSet {
    outputs: BTreeMap<String, CapturedArtifact>,
    reservation: Option<CaptureReservation>,
}

impl CaptureCandidateSet {
    pub(crate) fn outputs(&self) -> &BTreeMap<String, CapturedArtifact> {
        &self.outputs
    }

    pub(crate) fn commit(mut self) -> BTreeMap<String, CapturedArtifact> {
        if let Some(mut reservation) = self.reservation.take() {
            reservation.commit(&self.outputs);
        }
        std::mem::take(&mut self.outputs)
    }

    pub(crate) fn abort(mut self) {
        self.rollback();
    }

    fn rollback(&mut self) {
        let Some(reservation) = self.reservation.take() else {
            return;
        };
        if !reservation.store.remove_capture_set(&self.outputs) {
            reservation.store.mark_cleanup_failed();
        }
    }
}

impl Drop for CaptureCandidateSet {
    fn drop(&mut self) {
        self.rollback();
    }
}

#[derive(Clone, Copy)]
struct CaptureBounds {
    maximum_bytes: u64,
    overflow_kind: CaptureFailureKind,
}

struct CaptureReservation {
    store: Arc<ArtifactStagingInner>,
    files: usize,
    bytes: u64,
    active: bool,
}

impl CaptureReservation {
    fn reserve_bytes(&mut self, bytes: u64) -> Result<(), CaptureFailureKind> {
        let mut budget = self
            .store
            .budget
            .lock()
            .map_err(|_| CaptureFailureKind::StagingUnavailable)?;
        let updated_budget = budget
            .reserved_bytes
            .checked_add(bytes)
            .ok_or(CaptureFailureKind::TotalSizeLimitExceeded)?;
        let updated_reservation = self
            .bytes
            .checked_add(bytes)
            .ok_or(CaptureFailureKind::TotalSizeLimitExceeded)?;
        budget.reserved_bytes = updated_budget;
        self.bytes = updated_reservation;
        Ok(())
    }

    fn commit(&mut self, outputs: &BTreeMap<String, CapturedArtifact>) {
        let mut budget = self
            .store
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        budget.reserved_files = budget.reserved_files.saturating_sub(self.files);
        budget.reserved_bytes = budget.reserved_bytes.saturating_sub(self.bytes);
        budget.captured_files += self.files;
        budget.captured_bytes += self.bytes;
        for artifact in outputs.values() {
            artifact.handle.lease.commit_budget();
        }
        self.active = false;
    }
}

impl Drop for CaptureReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut budget = self
            .store
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        budget.reserved_files = budget.reserved_files.saturating_sub(self.files);
        budget.reserved_bytes = budget.reserved_bytes.saturating_sub(self.bytes);
    }
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
    execution_root: AdmittedExecutionRoot,
    staging_parent: OwnedFd,
    staging_root: OwnedFd,
    #[cfg(test)]
    staging_path: PathBuf,
    store_identity: Arc<str>,
    maximum_files: NonZeroUsize,
    maximum_file_bytes: NonZeroU64,
    maximum_total_bytes: NonZeroU64,
    lifecycle: RwLock<StagingLifecycle>,
    artifacts: Mutex<BTreeSet<Arc<str>>>,
    budget: Mutex<CaptureBudgetLedger>,
    capture_serial: Mutex<()>,
    #[cfg(test)]
    artifact_unlinks_blocked: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CaptureBudgetLedger {
    captured_files: usize,
    captured_bytes: u64,
    reserved_files: usize,
    reserved_bytes: u64,
}

impl ArtifactStaging {
    pub(crate) fn create(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
    ) -> Result<Self, ArtifactStagingFailure> {
        let limits = execution.limits();
        Self::create_for_root(
            execution.root_identity().clone(),
            staging_parent,
            limits.maximum_captured_files(),
            limits.maximum_captured_file_bytes(),
            limits.maximum_total_captured_bytes(),
        )
    }

    #[cfg(test)]
    fn create_for_execution(
        execution_root: &Path,
        staging_parent: &Path,
        maximum_files: NonZeroUsize,
        maximum_file_bytes: NonZeroU64,
        maximum_total_bytes: NonZeroU64,
    ) -> Result<Self, ArtifactStagingFailure> {
        let execution_root = AdmittedExecutionRoot::admit(execution_root)
            .map_err(|_| ArtifactStagingFailure::ExecutionRootUnavailable)?;
        Self::create_for_root(
            execution_root,
            staging_parent,
            maximum_files,
            maximum_file_bytes,
            maximum_total_bytes,
        )
    }

    fn create_for_root(
        execution_root: AdmittedExecutionRoot,
        staging_parent: &Path,
        maximum_files: NonZeroUsize,
        maximum_file_bytes: NonZeroU64,
        maximum_total_bytes: NonZeroU64,
    ) -> Result<Self, ArtifactStagingFailure> {
        if !execution_root.pathname_is_bound() {
            return Err(ArtifactStagingFailure::ExecutionRootUnavailable);
        }
        let canonical_staging_parent = std::fs::canonicalize(staging_parent)
            .map_err(|_| ArtifactStagingFailure::StagingParentUnavailable)?;
        if canonical_staging_parent.starts_with(execution_root.provenance_path()) {
            return Err(ArtifactStagingFailure::StagingParentExposed);
        }

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
                lifecycle: RwLock::new(StagingLifecycle::Active),
                artifacts: Mutex::new(BTreeSet::new()),
                budget: Mutex::new(CaptureBudgetLedger::default()),
                capture_serial: Mutex::new(()),
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
        self.inner
            .execution_root
            .is_same_directory(execution.root_identity())
    }

    pub(crate) fn capture_files(
        &self,
        declarations: &[CaptureDeclaration<'_>],
    ) -> Result<BTreeMap<String, CapturedArtifact>, CaptureFailure> {
        match self.capture_file_candidates(declarations, &CaptureCancellation::default()) {
            Ok(candidates) => Ok(candidates.commit()),
            Err(CaptureAttemptFailure::Capture(failure)) => Err(failure),
            Err(CaptureAttemptFailure::Cancelled) => {
                unreachable!("a private uncancelled capture cannot be cancelled")
            }
        }
    }

    pub(crate) fn capture_file_candidates(
        &self,
        declarations: &[CaptureDeclaration<'_>],
        cancellation: &CaptureCancellation,
    ) -> Result<CaptureCandidateSet, CaptureAttemptFailure> {
        cancellation.check()?;
        let Some(first) = declarations.first() else {
            return Ok(CaptureCandidateSet {
                outputs: BTreeMap::new(),
                reservation: None,
            });
        };
        let failure_identity = || Arc::<str>::from(first.output_identity);
        let _serial = self.inner.capture_serial.lock().map_err(|_| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                failure_identity(),
                CaptureFailureKind::StagingUnavailable,
            ))
        })?;
        cancellation.check()?;

        let mut budget = self.inner.budget.lock().map_err(|_| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                failure_identity(),
                CaptureFailureKind::StagingUnavailable,
            ))
        })?;
        let remaining_files = self
            .inner
            .maximum_files
            .get()
            .saturating_sub(budget.captured_files.saturating_add(budget.reserved_files));
        if declarations.len() > remaining_files {
            return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                failure_identity(),
                CaptureFailureKind::FileCountLimitExceeded,
            )));
        }
        budget.reserved_files += declarations.len();
        drop(budget);
        let mut reservation = CaptureReservation {
            store: Arc::clone(&self.inner),
            files: declarations.len(),
            bytes: 0,
            active: true,
        };

        let mut captured = BTreeMap::new();
        for declaration in declarations {
            if cancellation.check().is_err() {
                self.rollback_cancelled_capture_set(&captured);
                return Err(CaptureAttemptFailure::Cancelled);
            }
            let available_total_bytes = {
                let budget = self.inner.budget.lock().map_err(|_| {
                    CaptureAttemptFailure::Capture(CaptureFailure::new(
                        failure_identity(),
                        CaptureFailureKind::StagingUnavailable,
                    ))
                })?;
                self.inner
                    .maximum_total_bytes
                    .get()
                    .saturating_sub(budget.captured_bytes.saturating_add(budget.reserved_bytes))
            };
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
                CaptureBounds {
                    maximum_bytes,
                    overflow_kind,
                },
                cancellation,
            ) {
                Ok(artifact) => artifact,
                Err(CaptureAttemptFailure::Capture(failure)) => {
                    return Err(CaptureAttemptFailure::Capture(
                        self.rollback_capture_set(&captured, failure),
                    ));
                }
                Err(CaptureAttemptFailure::Cancelled) => {
                    self.rollback_cancelled_capture_set(&captured);
                    return Err(CaptureAttemptFailure::Cancelled);
                }
            };
            let artifact_size = artifact.size;
            captured.insert(declaration.output_identity.to_owned(), artifact);
            if let Err(kind) = reservation.reserve_bytes(artifact_size) {
                let failure = CaptureFailure::new(Arc::from(declaration.output_identity), kind);
                return Err(CaptureAttemptFailure::Capture(
                    self.rollback_capture_set(&captured, failure),
                ));
            }
        }
        if cancellation.check().is_err() {
            self.rollback_cancelled_capture_set(&captured);
            return Err(CaptureAttemptFailure::Cancelled);
        }

        Ok(CaptureCandidateSet {
            outputs: captured,
            reservation: Some(reservation),
        })
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
    pub(crate) fn reservation_usage(&self) -> (usize, u64) {
        let budget = self
            .inner
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (budget.reserved_files, budget.reserved_bytes)
    }

    #[cfg(test)]
    pub(crate) fn block_artifact_unlinks(&self) {
        self.inner
            .artifact_unlinks_blocked
            .store(true, Ordering::Release);
    }

    fn rollback_capture_set(
        &self,
        captured: &BTreeMap<String, CapturedArtifact>,
        failure: CaptureFailure,
    ) -> CaptureFailure {
        if self.inner.remove_capture_set(captured) {
            failure
        } else {
            self.inner.mark_cleanup_failed();
            CaptureFailure::new(
                Arc::clone(&failure.output_identity),
                CaptureFailureKind::StagingUnavailable,
            )
        }
    }

    fn rollback_cancelled_capture_set(&self, captured: &BTreeMap<String, CapturedArtifact>) {
        if !self.inner.remove_capture_set(captured) {
            self.inner.mark_cleanup_failed();
        }
    }

    fn stage(
        &self,
        output_identity: Arc<str>,
        declared_path: &Path,
        media_type: Arc<str>,
        bounds: CaptureBounds,
        cancellation: &CaptureCancellation,
    ) -> Result<CapturedArtifact, CaptureAttemptFailure> {
        self.stage_with_copier(
            output_identity,
            declared_path,
            media_type,
            bounds,
            cancellation,
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
        match self.stage_with_copier(
            output_identity,
            declared_path,
            media_type,
            CaptureBounds {
                maximum_bytes: self.inner.maximum_file_bytes.get(),
                overflow_kind: CaptureFailureKind::FileSizeLimitExceeded,
            },
            &CaptureCancellation::default(),
            copier,
        ) {
            Ok(artifact) => Ok(artifact),
            Err(CaptureAttemptFailure::Capture(failure)) => Err(failure),
            Err(CaptureAttemptFailure::Cancelled) => {
                unreachable!("a private uncancelled capture cannot be cancelled")
            }
        }
    }

    fn stage_with_copier(
        &self,
        output_identity: Arc<str>,
        declared_path: &Path,
        media_type: Arc<str>,
        bounds: CaptureBounds,
        cancellation: &CaptureCancellation,
        copier: &mut impl StreamCopier,
    ) -> Result<CapturedArtifact, CaptureAttemptFailure> {
        cancellation.check()?;
        let lifecycle = self.inner.lifecycle.read().map_err(|_| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::clone(&output_identity),
                CaptureFailureKind::StagingUnavailable,
            ))
        })?;
        if *lifecycle != StagingLifecycle::Active {
            return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                output_identity,
                CaptureFailureKind::StagingUnavailable,
            )));
        }
        let components = capture_components(declared_path).map_err(|kind| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(Arc::clone(&output_identity), kind))
        })?;
        cancellation.boundary(&output_identity, CaptureBoundaryKind::BeforeSourceOpen)?;
        let mut source = open_regular_file(self.inner.execution_root.directory(), &components)
            .map_err(|kind| {
                CaptureAttemptFailure::Capture(CaptureFailure::new(
                    Arc::clone(&output_identity),
                    kind,
                ))
            })?;
        cancellation.check()?;
        if source
            .metadata()
            .is_ok_and(|metadata| metadata.len() > bounds.maximum_bytes)
        {
            return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                output_identity,
                bounds.overflow_kind,
            )));
        }
        cancellation.check()?;

        let (artifact_identity, mut destination) = self.create_destination().map_err(|kind| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(Arc::clone(&output_identity), kind))
        })?;
        let capture_result = copier
            .copy(CopyRequest {
                source: &mut source,
                destination: &mut destination,
                maximum_bytes: bounds.maximum_bytes,
                output_identity: &output_identity,
                cancellation,
            })
            .map_err(|failure| match failure {
                CaptureAttemptFailure::Capture(failure)
                    if failure.kind == CaptureFailureKind::FileSizeLimitExceeded =>
                {
                    CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(&output_identity),
                        bounds.overflow_kind,
                    ))
                }
                failure => failure,
            })
            .and_then(|size| {
                cancellation.check()?;
                destination.flush().map_err(|_| {
                    CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(&output_identity),
                        CaptureFailureKind::StagingUnavailable,
                    ))
                })?;
                cancellation.check()?;
                fchmod(&destination, Mode::RUSR).map_err(|_| {
                    CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(&output_identity),
                        CaptureFailureKind::StagingUnavailable,
                    ))
                })?;
                cancellation.check()?;
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
            Err(failure) => {
                drop(lifecycle);
                if !self.inner.remove_artifact_while_active(&artifact_identity) {
                    self.inner.mark_cleanup_failed();
                    return match failure {
                        CaptureAttemptFailure::Cancelled => Err(CaptureAttemptFailure::Cancelled),
                        CaptureAttemptFailure::Capture(_) => {
                            Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                                output_identity,
                                CaptureFailureKind::StagingUnavailable,
                            )))
                        }
                    };
                }
                Err(failure)
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
        if *lifecycle != StagingLifecycle::Active
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
    fn remove_capture_set(&self, captured: &BTreeMap<String, CapturedArtifact>) -> bool {
        let mut rollback_complete = true;
        for artifact in captured.values() {
            rollback_complete &=
                self.remove_artifact_while_active(&artifact.handle.artifact_identity);
        }
        rollback_complete
    }

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
        if *lifecycle == StagingLifecycle::Released {
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
        mark_staging_cleanup_failed(&self.lifecycle);
    }

    fn cleanup(&self) -> Result<(), ArtifactReleaseFailure> {
        cleanup_staging(
            &self.lifecycle,
            ArtifactReleaseFailure::CleanupUnavailable,
            || self.cleanup_active(),
        )
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

struct CopyRequest<'a> {
    source: &'a mut File,
    destination: &'a mut File,
    maximum_bytes: u64,
    output_identity: &'a Arc<str>,
    cancellation: &'a CaptureCancellation,
}

trait StreamCopier {
    fn copy(&mut self, request: CopyRequest<'_>) -> Result<u64, CaptureAttemptFailure>;
}

struct PortableCopier;

impl StreamCopier for PortableCopier {
    fn copy(&mut self, request: CopyRequest<'_>) -> Result<u64, CaptureAttemptFailure> {
        copy_bounded(
            request.source,
            request.destination,
            request.maximum_bytes,
            request.output_identity,
            request.cancellation,
        )
    }
}

fn copy_bounded(
    source: &mut impl Read,
    destination: &mut impl Write,
    maximum_bytes: u64,
    output_identity: &Arc<str>,
    cancellation: &CaptureCancellation,
) -> Result<u64, CaptureAttemptFailure> {
    let capture_failure = |kind| {
        CaptureAttemptFailure::Capture(CaptureFailure::new(Arc::clone(output_identity), kind))
    };
    let mut copied = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let remaining = maximum_bytes.saturating_sub(copied);
        let permitted_read = remaining.saturating_add(1).min(COPY_BUFFER_BYTES_U64);
        let permitted_read = usize::try_from(permitted_read)
            .map_err(|_| capture_failure(CaptureFailureKind::FileSizeLimitExceeded))?;
        cancellation.boundary(output_identity, CaptureBoundaryKind::BeforeRead)?;
        let read = source
            .read(&mut buffer[..permitted_read])
            .map_err(|_| capture_failure(CaptureFailureKind::SourceUnavailable))?;
        cancellation.check()?;
        if read == 0 {
            return Ok(copied);
        }
        let read_length = u64::try_from(read)
            .map_err(|_| capture_failure(CaptureFailureKind::FileSizeLimitExceeded))?;
        if read_length > remaining {
            return Err(capture_failure(CaptureFailureKind::FileSizeLimitExceeded));
        }

        let mut written = 0;
        while written < read {
            cancellation.boundary(output_identity, CaptureBoundaryKind::BeforeWrite)?;
            let next = destination
                .write(&buffer[written..read])
                .map_err(|_| capture_failure(CaptureFailureKind::StagingUnavailable))?;
            if next == 0 {
                return Err(capture_failure(CaptureFailureKind::StagingUnavailable));
            }
            written += next;
            cancellation.boundary(output_identity, CaptureBoundaryKind::AfterWrite)?;
        }
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
