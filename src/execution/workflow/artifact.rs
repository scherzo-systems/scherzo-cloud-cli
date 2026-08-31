use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::fd::OwnedFd;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, Weak};

#[cfg(test)]
use std::path::PathBuf;

use ring::digest::{Context as DigestContext, SHA256};
use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, fchmod, fstat, linkat, mkdirat, openat, statat, unlinkat,
};
use rustix::io::Errno;

use super::admission::AdmittedExecutionContext;
use super::canonical_json::{self, CanonicalJsonError};
use super::execution_root::{AdmittedExecutionRoot, directory_open_flags, open_directory};
use super::private_staging::{
    StagingLifecycle, cleanup_staging, mark_cleanup_failed as mark_staging_cleanup_failed,
    same_file,
};
use super::result_validation::RetainedJsonSchema;
use super::schema_common::lowercase_hex;
use super::strict_json;
use super::validated::WorkflowValueType;
use super::value::{CapturedJson, CapturedText, CapturedValue};

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

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn check(&self) -> Result<(), CaptureAttemptFailure> {
        if self.is_cancelled() {
            Err(CaptureAttemptFailure::Cancelled)
        } else {
            Ok(())
        }
    }

    pub(crate) fn boundary(
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
    BeforeGitRecheck,
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
    InvalidTextEncoding,
    InvalidJson,
    DuplicateJsonMember,
    JsonSchemaMismatch,
    FileCountLimitExceeded,
    FileSizeLimitExceeded,
    TotalSizeLimitExceeded,
    GitCarrierCountLimitExceeded,
    GitCarrierSizeLimitExceeded,
    TotalGitCarrierSizeLimitExceeded,
    CarrierProducerUnavailable,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CarrierBudgetClass {
    File,
    Git,
}

impl CarrierBudgetClass {
    const ALL: [Self; 2] = [Self::File, Self::Git];

    fn total_overflow_kind(self) -> CaptureFailureKind {
        match self {
            Self::File => CaptureFailureKind::TotalSizeLimitExceeded,
            Self::Git => CaptureFailureKind::TotalGitCarrierSizeLimitExceeded,
        }
    }
}

#[derive(Clone, Copy)]
struct CarrierFileIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

#[derive(Clone)]
pub(crate) struct ArtifactHandle {
    store_identity: Arc<str>,
    artifact_identity: Arc<str>,
    lease: Arc<ArtifactLease>,
}

struct ArtifactLease {
    store: Weak<ArtifactStagingInner>,
    artifact_identity: Arc<str>,
    file_identity: CarrierFileIdentity,
    metadata: StagedCarrierMetadata,
    budgeted: AtomicBool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StagedCarrierMetadata {
    output_identity: Arc<str>,
    size: u64,
    media_type: Arc<str>,
    sha256: Arc<str>,
    budget_class: CarrierBudgetClass,
}

impl ArtifactLease {
    fn commit_budget(&self) {
        self.budgeted.store(true, Ordering::Release);
    }

    fn release_budget(&self, store: &ArtifactStagingInner) {
        if self.budgeted.swap(false, Ordering::AcqRel) {
            store.release_budget(self.metadata.budget_class, self.metadata.size);
        }
    }
}

impl Drop for ArtifactLease {
    fn drop(&mut self) {
        if let Some(store) = self.store.upgrade() {
            store.remove_artifact(&self.artifact_identity);
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

#[derive(Clone, Debug)]
pub(crate) struct StagedCarrier {
    handle: ArtifactHandle,
}

impl StagedCarrier {
    pub(crate) fn handle(&self) -> &ArtifactHandle {
        &self.handle
    }

    pub(crate) fn identity(&self) -> &str {
        self.handle.opaque_id()
    }

    pub(crate) fn output_identity(&self) -> &str {
        &self.handle.lease.metadata.output_identity
    }

    pub(crate) fn size(&self) -> u64 {
        self.handle.lease.metadata.size
    }

    pub(crate) fn media_type(&self) -> &str {
        &self.handle.lease.metadata.media_type
    }

    pub(crate) fn sha256(&self) -> &str {
        &self.handle.lease.metadata.sha256
    }

    pub(crate) fn budget_class(&self) -> CarrierBudgetClass {
        self.handle.lease.metadata.budget_class
    }
}

impl PartialEq for StagedCarrier {
    fn eq(&self, other: &Self) -> bool {
        self.handle == other.handle
    }
}

impl Eq for StagedCarrier {}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct CaptureLease {
    carrier: StagedCarrier,
}

impl CaptureLease {
    pub(super) fn carrier(&self) -> &StagedCarrier {
        &self.carrier
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedArtifact {
    carrier: StagedCarrier,
}

impl CapturedArtifact {
    pub(crate) fn handle(&self) -> &ArtifactHandle {
        self.carrier.handle()
    }

    pub(crate) fn output_identity(&self) -> &str {
        self.carrier.output_identity()
    }

    pub(crate) fn size(&self) -> u64 {
        self.carrier.size()
    }

    pub(crate) fn media_type(&self) -> &str {
        self.carrier.media_type()
    }

    pub(crate) fn sha256(&self) -> &str {
        self.carrier.sha256()
    }

    pub(crate) fn carrier(&self) -> &StagedCarrier {
        &self.carrier
    }

    pub(super) fn into_capture_lease(self) -> CaptureLease {
        CaptureLease {
            carrier: self.carrier,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GitObjectFormat {
    Sha1,
}

impl GitObjectFormat {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sha1 => "sha1",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GitBranchMetadata {
    artifact_version: u8,
    object_format: GitObjectFormat,
    base_oid: Arc<str>,
    head_oid: Arc<str>,
    tree_oid: Arc<str>,
}

impl GitBranchMetadata {
    pub(crate) fn new(base_oid: Arc<str>, head_oid: Arc<str>, tree_oid: Arc<str>) -> Self {
        Self {
            artifact_version: 1,
            object_format: GitObjectFormat::Sha1,
            base_oid,
            head_oid,
            tree_oid,
        }
    }

    pub(crate) fn artifact_version(&self) -> u8 {
        self.artifact_version
    }

    pub(crate) fn object_format(&self) -> GitObjectFormat {
        self.object_format
    }

    pub(crate) fn base_oid(&self) -> &str {
        &self.base_oid
    }

    pub(crate) fn head_oid(&self) -> &str {
        &self.head_oid
    }

    pub(crate) fn tree_oid(&self) -> &str {
        &self.tree_oid
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GitBranchCarrier {
    staged: StagedCarrier,
}

impl GitBranchCarrier {
    pub(crate) fn handle(&self) -> &ArtifactHandle {
        self.staged.handle()
    }

    pub(crate) fn identity(&self) -> &str {
        self.staged.identity()
    }

    pub(crate) fn size(&self) -> u64 {
        self.staged.size()
    }

    pub(crate) fn media_type(&self) -> &str {
        self.staged.media_type()
    }

    pub(crate) fn budget_class(&self) -> CarrierBudgetClass {
        self.staged.budget_class()
    }

    pub(crate) fn sha256(&self) -> &str {
        self.staged.sha256()
    }

    pub(crate) fn staged(&self) -> &StagedCarrier {
        &self.staged
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedGitBranch {
    output_identity: Arc<str>,
    metadata: GitBranchMetadata,
    carrier: Option<GitBranchCarrier>,
}

impl CapturedGitBranch {
    pub(crate) fn output_identity(&self) -> &str {
        &self.output_identity
    }

    pub(crate) fn metadata(&self) -> &GitBranchMetadata {
        &self.metadata
    }

    pub(crate) fn carrier(&self) -> Option<&GitBranchCarrier> {
        self.carrier.as_ref()
    }
}

pub(crate) struct CaptureCandidateSet {
    outputs: BTreeMap<String, CapturedValue>,
    reservation: Option<CaptureReservation>,
}

impl CaptureCandidateSet {
    pub(crate) fn outputs(&self) -> &BTreeMap<String, CapturedValue> {
        &self.outputs
    }

    pub(crate) fn commit(mut self) -> BTreeMap<String, CapturedValue> {
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

struct PendingStagedCarrier {
    artifact_identity: Arc<str>,
    output_identity: Arc<str>,
    destination: File,
    capture_result: Result<(u64, Arc<str>), CaptureAttemptFailure>,
    profile: (Arc<str>, CarrierBudgetClass),
}

#[derive(Clone, Copy, Default)]
struct BudgetCounts {
    files: usize,
    git_carriers: usize,
}

impl BudgetCounts {
    fn get(self, class: CarrierBudgetClass) -> usize {
        match class {
            CarrierBudgetClass::File => self.files,
            CarrierBudgetClass::Git => self.git_carriers,
        }
    }

    fn get_mut(&mut self, class: CarrierBudgetClass) -> &mut usize {
        match class {
            CarrierBudgetClass::File => &mut self.files,
            CarrierBudgetClass::Git => &mut self.git_carriers,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct BudgetBytes {
    files: u64,
    git_carriers: u64,
}

impl BudgetBytes {
    fn get(self, class: CarrierBudgetClass) -> u64 {
        match class {
            CarrierBudgetClass::File => self.files,
            CarrierBudgetClass::Git => self.git_carriers,
        }
    }

    fn get_mut(&mut self, class: CarrierBudgetClass) -> &mut u64 {
        match class {
            CarrierBudgetClass::File => &mut self.files,
            CarrierBudgetClass::Git => &mut self.git_carriers,
        }
    }
}

struct CaptureReservation {
    store: Arc<ArtifactStagingInner>,
    counts: BudgetCounts,
    bytes: BudgetBytes,
    active: bool,
}

impl CaptureReservation {
    fn reserve_bytes(
        &mut self,
        class: CarrierBudgetClass,
        bytes: u64,
    ) -> Result<(), CaptureFailureKind> {
        let overflow = class.total_overflow_kind();
        let mut budget = self
            .store
            .budget
            .lock()
            .map_err(|_| CaptureFailureKind::StagingUnavailable)?;
        let usage = budget.usage_mut(class);
        let updated_budget = usage.reserved_bytes.checked_add(bytes).ok_or(overflow)?;
        let updated_reservation = self.bytes.get(class).checked_add(bytes).ok_or(overflow)?;
        usage.reserved_bytes = updated_budget;
        *self.bytes.get_mut(class) = updated_reservation;
        Ok(())
    }

    fn commit(&mut self, outputs: &BTreeMap<String, CapturedValue>) {
        let mut budget = self
            .store
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for class in CarrierBudgetClass::ALL {
            let usage = budget.usage_mut(class);
            let count = self.counts.get(class);
            let bytes = self.bytes.get(class);
            usage.reserved_count = usage.reserved_count.saturating_sub(count);
            usage.reserved_bytes = usage.reserved_bytes.saturating_sub(bytes);
            usage.captured_count += count;
            usage.captured_bytes += bytes;
        }
        for carrier in outputs
            .values()
            .filter_map(CapturedValue::private_capture_carrier)
        {
            carrier.handle.lease.commit_budget();
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
        for class in CarrierBudgetClass::ALL {
            let usage = budget.usage_mut(class);
            usage.reserved_count = usage.reserved_count.saturating_sub(self.counts.get(class));
            usage.reserved_bytes = usage.reserved_bytes.saturating_sub(self.bytes.get(class));
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PathCaptureProfile<'a> {
    Text,
    Json { schema: &'a RetainedJsonSchema },
    File { media_type: &'a str },
}

impl<'a> PathCaptureProfile<'a> {
    pub(crate) const fn value_type(self) -> WorkflowValueType {
        match self {
            Self::Text => WorkflowValueType::Text,
            Self::Json { .. } => WorkflowValueType::Json,
            Self::File { .. } => WorkflowValueType::File,
        }
    }

    fn media_type(self) -> &'a str {
        match self {
            Self::Text => "text/plain; charset=utf-8",
            Self::Json { .. } => "application/json",
            Self::File { media_type } => media_type,
        }
    }

    pub(crate) const fn json_schema(self) -> Option<&'a RetainedJsonSchema> {
        match self {
            Self::Json { schema } => Some(schema),
            Self::Text | Self::File { .. } => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CaptureDeclaration<'a> {
    output_identity: &'a str,
    declared_path: &'a Path,
    profile: PathCaptureProfile<'a>,
}

impl<'a> CaptureDeclaration<'a> {
    pub(crate) fn text(output_identity: &'a str, declared_path: &'a Path) -> Self {
        Self {
            output_identity,
            declared_path,
            profile: PathCaptureProfile::Text,
        }
    }

    pub(crate) fn json(
        output_identity: &'a str,
        declared_path: &'a Path,
        schema: &'a RetainedJsonSchema,
    ) -> Self {
        Self {
            output_identity,
            declared_path,
            profile: PathCaptureProfile::Json { schema },
        }
    }

    pub(crate) fn file(
        output_identity: &'a str,
        declared_path: &'a Path,
        media_type: &'a str,
    ) -> Self {
        Self {
            output_identity,
            declared_path,
            profile: PathCaptureProfile::File { media_type },
        }
    }
}

pub(crate) trait CarrierProducer: Send {
    fn stream_to(&mut self, destination: &mut CarrierDestination<'_>) -> io::Result<()>;
}

pub(crate) struct GitBranchCaptureDeclaration<'a> {
    output_identity: &'a str,
    metadata: GitBranchMetadata,
    producer: Option<&'a mut dyn CarrierProducer>,
}

impl<'a> GitBranchCaptureDeclaration<'a> {
    pub(crate) fn new(
        output_identity: &'a str,
        metadata: GitBranchMetadata,
        producer: Option<&'a mut dyn CarrierProducer>,
    ) -> Self {
        Self {
            output_identity,
            metadata,
            producer,
        }
    }
}

pub(crate) enum CaptureCandidateDeclaration<'a> {
    File(CaptureDeclaration<'a>),
    GitBranch(GitBranchCaptureDeclaration<'a>),
}

impl CaptureCandidateDeclaration<'_> {
    fn output_identity(&self) -> &str {
        match self {
            Self::File(declaration) => declaration.output_identity,
            Self::GitBranch(declaration) => declaration.output_identity,
        }
    }

    fn budget_class(&self) -> Option<CarrierBudgetClass> {
        match self {
            Self::File(_) => Some(CarrierBudgetClass::File),
            Self::GitBranch(declaration) => declaration
                .producer
                .is_some()
                .then_some(CarrierBudgetClass::Git),
        }
    }

    fn carrier_presence_matches_delta(&self) -> bool {
        match self {
            Self::File(_) => true,
            Self::GitBranch(declaration) => {
                let has_delta = declaration.metadata.base_oid != declaration.metadata.head_oid;
                has_delta == declaration.producer.is_some()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactReadFailure {
    UnknownHandle,
    Unavailable,
    DestinationWrite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactExposeFailure {
    UnknownHandle,
    InvalidDestination,
    DestinationExists,
    Unavailable,
}

impl fmt::Display for ArtifactExposeFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "artifact exposure failure: {self:?}")
    }
}

impl std::error::Error for ArtifactExposeFailure {}

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
    file_limits: CarrierLimits,
    git_limits: CarrierLimits,
    lifecycle: RwLock<StagingLifecycle>,
    artifacts: Mutex<BTreeSet<Arc<str>>>,
    identity_guards: Mutex<BTreeMap<Arc<str>, Arc<str>>>,
    budget: Mutex<CaptureBudgetLedger>,
    capture_serial: Mutex<()>,
    #[cfg(test)]
    artifact_unlinks_blocked: AtomicBool,
    #[cfg(test)]
    artifact_links_blocked: AtomicBool,
}

#[derive(Clone, Copy)]
struct CarrierLimits {
    maximum_count: NonZeroUsize,
    maximum_bytes: NonZeroU64,
    maximum_total_bytes: NonZeroU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BudgetUsage {
    captured_count: usize,
    captured_bytes: u64,
    reserved_count: usize,
    reserved_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CaptureBudgetLedger {
    files: BudgetUsage,
    git_carriers: BudgetUsage,
}

impl CaptureBudgetLedger {
    fn usage(self, class: CarrierBudgetClass) -> BudgetUsage {
        match class {
            CarrierBudgetClass::File => self.files,
            CarrierBudgetClass::Git => self.git_carriers,
        }
    }

    fn usage_mut(&mut self, class: CarrierBudgetClass) -> &mut BudgetUsage {
        match class {
            CarrierBudgetClass::File => &mut self.files,
            CarrierBudgetClass::Git => &mut self.git_carriers,
        }
    }
}

impl ArtifactStagingInner {
    fn limits(&self, class: CarrierBudgetClass) -> CarrierLimits {
        match class {
            CarrierBudgetClass::File => self.file_limits,
            CarrierBudgetClass::Git => self.git_limits,
        }
    }
}

impl ArtifactStaging {
    pub(crate) fn create(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
    ) -> Result<Self, ArtifactStagingFailure> {
        Self::create_for_execution_context(execution, staging_parent, None)
    }

    pub(crate) fn create_bound(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
        expected_parent: &OwnedFd,
    ) -> Result<Self, ArtifactStagingFailure> {
        Self::create_for_execution_context(execution, staging_parent, Some(expected_parent))
    }

    fn create_for_execution_context(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
        expected_parent: Option<&OwnedFd>,
    ) -> Result<Self, ArtifactStagingFailure> {
        let limits = execution.limits();
        Self::create_for_root(
            execution.root_identity().clone(),
            staging_parent,
            expected_parent,
            CarrierLimits {
                maximum_count: limits.maximum_captured_files(),
                maximum_bytes: limits.maximum_captured_file_bytes(),
                maximum_total_bytes: limits.maximum_total_captured_bytes(),
            },
            CarrierLimits {
                maximum_count: limits.maximum_captured_git_carriers(),
                maximum_bytes: limits.maximum_captured_git_carrier_bytes(),
                maximum_total_bytes: limits.maximum_total_captured_git_carrier_bytes(),
            },
        )
    }

    #[cfg(test)]
    fn create_for_execution(
        execution_root: &Path,
        staging_parent: &Path,
        file_limits: CarrierLimits,
        git_limits: CarrierLimits,
    ) -> Result<Self, ArtifactStagingFailure> {
        let execution_root = AdmittedExecutionRoot::admit(execution_root)
            .map_err(|_| ArtifactStagingFailure::ExecutionRootUnavailable)?;
        Self::create_for_root(
            execution_root,
            staging_parent,
            None,
            file_limits,
            git_limits,
        )
    }

    fn create_for_root(
        execution_root: AdmittedExecutionRoot,
        staging_parent: &Path,
        expected_parent: Option<&OwnedFd>,
        file_limits: CarrierLimits,
        git_limits: CarrierLimits,
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
        if expected_parent
            .is_some_and(|expected| !same_file(expected, &staging_parent_handle).unwrap_or(false))
        {
            return Err(ArtifactStagingFailure::StagingParentUnavailable);
        }
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
                file_limits,
                git_limits,
                lifecycle: RwLock::new(StagingLifecycle::Active),
                artifacts: Mutex::new(BTreeSet::new()),
                identity_guards: Mutex::new(BTreeMap::new()),
                budget: Mutex::new(CaptureBudgetLedger::default()),
                capture_serial: Mutex::new(()),
                #[cfg(test)]
                artifact_unlinks_blocked: AtomicBool::new(false),
                #[cfg(test)]
                artifact_links_blocked: AtomicBool::new(false),
            }),
        })
    }

    pub(super) fn is_bound_to(&self, execution: &AdmittedExecutionContext) -> bool {
        let limits = execution.limits();
        if self.inner.file_limits.maximum_count != limits.maximum_captured_files()
            || self.inner.file_limits.maximum_bytes != limits.maximum_captured_file_bytes()
            || self.inner.file_limits.maximum_total_bytes != limits.maximum_total_captured_bytes()
            || self.inner.git_limits.maximum_count != limits.maximum_captured_git_carriers()
            || self.inner.git_limits.maximum_bytes != limits.maximum_captured_git_carrier_bytes()
            || self.inner.git_limits.maximum_total_bytes
                != limits.maximum_total_captured_git_carrier_bytes()
        {
            return false;
        }
        self.is_bound_to_root(execution.root_identity())
    }

    pub(super) fn is_bound_to_root(&self, root: &AdmittedExecutionRoot) -> bool {
        self.inner.execution_root.is_same_directory(root)
    }

    pub(crate) fn capture_files(
        &self,
        declarations: &[CaptureDeclaration<'_>],
    ) -> Result<BTreeMap<String, CapturedArtifact>, CaptureFailure> {
        match self.capture_file_candidates(declarations, &CaptureCancellation::default()) {
            Ok(candidates) => Ok(candidates
                .commit()
                .into_iter()
                .filter_map(|(identity, value)| value.into_file().map(|file| (identity, file)))
                .collect()),
            Err(CaptureAttemptFailure::Capture(failure)) => Err(failure),
            Err(CaptureAttemptFailure::Cancelled) => Err(CaptureFailure::new(
                Arc::from(
                    declarations
                        .first()
                        .map_or("@capture", |declaration| declaration.output_identity),
                ),
                CaptureFailureKind::StagingUnavailable,
            )),
        }
    }

    pub(crate) fn capture_file_candidates(
        &self,
        declarations: &[CaptureDeclaration<'_>],
        cancellation: &CaptureCancellation,
    ) -> Result<CaptureCandidateSet, CaptureAttemptFailure> {
        let mut declarations = declarations
            .iter()
            .copied()
            .map(CaptureCandidateDeclaration::File)
            .collect::<Vec<_>>();
        self.capture_candidates(&mut declarations, cancellation)
    }

    pub(crate) fn capture_candidates(
        &self,
        declarations: &mut [CaptureCandidateDeclaration<'_>],
        cancellation: &CaptureCancellation,
    ) -> Result<CaptureCandidateSet, CaptureAttemptFailure> {
        cancellation.check()?;
        let Some(first) = declarations.first() else {
            return Ok(CaptureCandidateSet {
                outputs: BTreeMap::new(),
                reservation: None,
            });
        };
        let failure_identity = || Arc::<str>::from(first.output_identity());
        let _serial = self.inner.capture_serial.lock().map_err(|_| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                failure_identity(),
                CaptureFailureKind::StagingUnavailable,
            ))
        })?;
        cancellation.check()?;

        let mut identities = BTreeSet::new();
        if let Some(duplicate) = declarations
            .iter()
            .find(|declaration| !identities.insert(declaration.output_identity()))
        {
            return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::from(duplicate.output_identity()),
                CaptureFailureKind::StagingUnavailable,
            )));
        }
        if let Some(invalid) = declarations
            .iter()
            .find(|declaration| !declaration.carrier_presence_matches_delta())
        {
            return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::from(invalid.output_identity()),
                CaptureFailureKind::StagingUnavailable,
            )));
        }
        let mut requested = BudgetCounts::default();
        for class in declarations
            .iter()
            .filter_map(CaptureCandidateDeclaration::budget_class)
        {
            *requested.get_mut(class) = requested.get(class).saturating_add(1);
        }
        let mut budget = self.inner.budget.lock().map_err(|_| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                failure_identity(),
                CaptureFailureKind::StagingUnavailable,
            ))
        })?;
        let exceeds = |class| {
            let usage = budget.usage(class);
            let remaining = self
                .inner
                .limits(class)
                .maximum_count
                .get()
                .saturating_sub(usage.captured_count.saturating_add(usage.reserved_count));
            requested.get(class) > remaining
        };
        if let Some(declaration) = declarations
            .iter()
            .find(|declaration| declaration.budget_class().is_some_and(&exceeds))
        {
            let kind = match declaration.budget_class() {
                Some(CarrierBudgetClass::File) => CaptureFailureKind::FileCountLimitExceeded,
                Some(CarrierBudgetClass::Git) => CaptureFailureKind::GitCarrierCountLimitExceeded,
                None => CaptureFailureKind::StagingUnavailable,
            };
            return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::from(declaration.output_identity()),
                kind,
            )));
        }
        for class in CarrierBudgetClass::ALL {
            budget.usage_mut(class).reserved_count += requested.get(class);
        }
        drop(budget);
        let mut reservation = CaptureReservation {
            store: Arc::clone(&self.inner),
            counts: requested,
            bytes: BudgetBytes::default(),
            active: true,
        };

        let mut captured = BTreeMap::new();
        for declaration in declarations {
            if cancellation.check().is_err() {
                self.rollback_cancelled_capture_set(&captured);
                return Err(CaptureAttemptFailure::Cancelled);
            }
            let output_identity = Arc::<str>::from(declaration.output_identity());
            let staged = match declaration {
                CaptureCandidateDeclaration::File(declaration) => self
                    .capture_bounds(CarrierBudgetClass::File, &output_identity)
                    .and_then(|bounds| {
                        self.stage(
                            Arc::clone(&output_identity),
                            declaration.declared_path,
                            Arc::from(declaration.profile.media_type()),
                            bounds,
                            cancellation,
                        )
                        .and_then(|file| {
                            self.semantic_path_value(
                                &output_identity,
                                file,
                                declaration.profile,
                                bounds,
                            )
                        })
                    })
                    .map(|value| (value, Some(CarrierBudgetClass::File))),
                CaptureCandidateDeclaration::GitBranch(declaration) => {
                    let carrier = match declaration.producer.as_deref_mut() {
                        Some(producer) => self
                            .capture_bounds(CarrierBudgetClass::Git, &output_identity)
                            .and_then(|bounds| {
                                self.stage_git_carrier(
                                    Arc::clone(&output_identity),
                                    producer,
                                    bounds,
                                    cancellation,
                                )
                            })
                            .map(Some),
                        None => Ok(None),
                    };
                    carrier.map(|carrier| {
                        let budget_class = carrier
                            .as_ref()
                            .map(|carrier| carrier.staged().budget_class());
                        (
                            CapturedValue::git_branch(CapturedGitBranch {
                                output_identity: Arc::clone(&output_identity),
                                metadata: declaration.metadata.clone(),
                                carrier,
                            }),
                            budget_class,
                        )
                    })
                }
            };
            let (value, budget_class) = match staged {
                Ok(value) => value,
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
            let size = value.private_capture_carrier().map(StagedCarrier::size);
            captured.insert(output_identity.to_string(), value);
            if let Some((class, size)) = budget_class.zip(size)
                && let Err(kind) = reservation.reserve_bytes(class, size)
            {
                let failure = CaptureFailure::new(output_identity, kind);
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

    fn semantic_path_value(
        &self,
        output_identity: &Arc<str>,
        file: CapturedArtifact,
        profile: PathCaptureProfile<'_>,
        bounds: CaptureBounds,
    ) -> Result<CapturedValue, CaptureAttemptFailure> {
        if matches!(profile, PathCaptureProfile::File { .. }) {
            return Ok(CapturedValue::file(file));
        }
        let mut source = Vec::new();
        self.copy_to(file.handle(), &mut source).map_err(|_| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::clone(output_identity),
                CaptureFailureKind::StagingUnavailable,
            ))
        })?;
        match profile {
            PathCaptureProfile::Text => {
                if std::str::from_utf8(&source).is_err() {
                    return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(output_identity),
                        CaptureFailureKind::InvalidTextEncoding,
                    )));
                }
                let carrier = Arc::<[u8]>::from(source);
                CapturedText::from_bounded_carrier(carrier, file.into_capture_lease())
                    .map(CapturedValue::Text)
                    .map_err(|_| {
                        CaptureAttemptFailure::Capture(CaptureFailure::new(
                            Arc::clone(output_identity),
                            CaptureFailureKind::InvalidTextEncoding,
                        ))
                    })
            }
            PathCaptureProfile::Json { schema } => {
                if std::str::from_utf8(&source).is_err() {
                    return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(output_identity),
                        CaptureFailureKind::InvalidTextEncoding,
                    )));
                }
                let value = serde_json::from_slice::<serde_json::Value>(&source).map_err(|_| {
                    CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(output_identity),
                        CaptureFailureKind::InvalidJson,
                    ))
                })?;
                if strict_json::from_slice(&source).is_err() {
                    return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(output_identity),
                        CaptureFailureKind::DuplicateJsonMember,
                    )));
                }
                if !schema.is_valid(&value) {
                    return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(output_identity),
                        CaptureFailureKind::JsonSchemaMismatch,
                    )));
                }
                let value = Arc::new(value);
                let carrier = canonical_json::to_bounded_bytes(&value, bounds.maximum_bytes)
                    .map_err(|failure| {
                        let kind = match failure {
                            CanonicalJsonError::SizeLimitExceeded => bounds.overflow_kind,
                            CanonicalJsonError::SerializationFailed => {
                                CaptureFailureKind::StagingUnavailable
                            }
                        };
                        CaptureAttemptFailure::Capture(CaptureFailure::new(
                            Arc::clone(output_identity),
                            kind,
                        ))
                    })?;
                CapturedJson::from_bounded_carrier(
                    value,
                    carrier,
                    schema.clone(),
                    file.into_capture_lease(),
                )
                .map(CapturedValue::Json)
                .map_err(|_| {
                    CaptureAttemptFailure::Capture(CaptureFailure::new(
                        Arc::clone(output_identity),
                        CaptureFailureKind::StagingUnavailable,
                    ))
                })
            }
            PathCaptureProfile::File { .. } => Ok(CapturedValue::file(file)),
        }
    }

    fn capture_bounds(
        &self,
        class: CarrierBudgetClass,
        output_identity: &Arc<str>,
    ) -> Result<CaptureBounds, CaptureAttemptFailure> {
        let budget = self.inner.budget.lock().map_err(|_| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::clone(output_identity),
                CaptureFailureKind::StagingUnavailable,
            ))
        })?;
        let limits = self.inner.limits(class);
        let usage = budget.usage(class);
        let available_total_bytes = limits
            .maximum_total_bytes
            .get()
            .saturating_sub(usage.captured_bytes.saturating_add(usage.reserved_bytes));
        let per_carrier_overflow = match class {
            CarrierBudgetClass::File => CaptureFailureKind::FileSizeLimitExceeded,
            CarrierBudgetClass::Git => CaptureFailureKind::GitCarrierSizeLimitExceeded,
        };
        let (maximum_bytes, overflow_kind) = if available_total_bytes < limits.maximum_bytes.get() {
            (available_total_bytes, class.total_overflow_kind())
        } else {
            (limits.maximum_bytes.get(), per_carrier_overflow)
        };
        Ok(CaptureBounds {
            maximum_bytes,
            overflow_kind,
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

    pub(crate) fn expose_carrier(
        &self,
        carrier: &StagedCarrier,
        expected_output_identity: &str,
        destination: &OwnedFd,
        destination_name: &OsStr,
    ) -> Result<(), ArtifactExposeFailure> {
        if !is_single_path_component(destination_name) {
            return Err(ArtifactExposeFailure::InvalidDestination);
        }
        let lifecycle = self
            .inner
            .lifecycle
            .read()
            .map_err(|_| ArtifactExposeFailure::Unavailable)?;
        if *lifecycle != StagingLifecycle::Active
            || carrier.output_identity() != expected_output_identity
        {
            return Err(ArtifactExposeFailure::UnknownHandle);
        }
        let source = self
            .open_artifact_while_active(carrier.handle())
            .map_err(expose_read_failure)?;
        let source_metadata = fstat(&source).map_err(|_| ArtifactExposeFailure::Unavailable)?;
        let destination_metadata =
            fstat(destination).map_err(|_| ArtifactExposeFailure::Unavailable)?;
        if FileType::from_raw_mode(destination_metadata.st_mode) != FileType::Directory
            || source_metadata.st_dev != destination_metadata.st_dev
            || u64::try_from(source_metadata.st_size) != Ok(carrier.size())
        {
            return Err(ArtifactExposeFailure::Unavailable);
        }
        #[cfg(test)]
        if self.inner.artifact_links_blocked.load(Ordering::Acquire) {
            return Err(ArtifactExposeFailure::Unavailable);
        }
        linkat(
            &self.inner.staging_root,
            carrier.handle.artifact_identity.as_ref(),
            destination,
            destination_name,
            AtFlags::empty(),
        )
        .map_err(|failure| match failure {
            Errno::EXIST => ArtifactExposeFailure::DestinationExists,
            _ => ArtifactExposeFailure::Unavailable,
        })?;
        let exposed = statat(destination, destination_name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| ArtifactExposeFailure::Unavailable)?;
        let expected = carrier.handle.lease.file_identity;
        if FileType::from_raw_mode(exposed.st_mode) != FileType::RegularFile
            || exposed.st_dev != expected.device
            || exposed.st_ino != expected.inode
            || u64::try_from(exposed.st_size) != Ok(carrier.size())
        {
            return Err(ArtifactExposeFailure::Unavailable);
        }
        Ok(())
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
        (budget.files.captured_count, budget.files.captured_bytes)
    }

    #[cfg(test)]
    pub(crate) fn git_budget_usage(&self) -> (usize, u64) {
        let budget = self
            .inner
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (
            budget.git_carriers.captured_count,
            budget.git_carriers.captured_bytes,
        )
    }

    #[cfg(test)]
    pub(crate) fn reservation_usage(&self) -> (usize, u64) {
        let budget = self
            .inner
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (budget.files.reserved_count, budget.files.reserved_bytes)
    }

    #[cfg(test)]
    pub(crate) fn git_reservation_usage(&self) -> (usize, u64) {
        let budget = self
            .inner
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (
            budget.git_carriers.reserved_count,
            budget.git_carriers.reserved_bytes,
        )
    }

    #[cfg(test)]
    pub(crate) fn block_artifact_unlinks(&self) {
        self.inner
            .artifact_unlinks_blocked
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn block_artifact_links(&self) {
        self.inner
            .artifact_links_blocked
            .store(true, Ordering::Release);
    }

    fn rollback_capture_set(
        &self,
        captured: &BTreeMap<String, CapturedValue>,
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

    fn rollback_cancelled_capture_set(&self, captured: &BTreeMap<String, CapturedValue>) {
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
                maximum_bytes: self.inner.file_limits.maximum_bytes.get(),
                overflow_kind: CaptureFailureKind::FileSizeLimitExceeded,
            },
            &CaptureCancellation::default(),
            copier,
        ) {
            Ok(artifact) => Ok(artifact),
            Err(CaptureAttemptFailure::Capture(failure)) => Err(failure),
            Err(CaptureAttemptFailure::Cancelled) => {
                panic!("a private uncancelled capture cannot be cancelled")
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
        let lifecycle = self.active_lifecycle(&output_identity)?;
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
        let capture_result = {
            let mut hashing = HashingCarrierDestination::new(&mut destination);
            copier
                .copy(CopyRequest {
                    source: &mut source,
                    destination: &mut hashing,
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
                    (size == hashing.bytes_written)
                        .then(|| (size, hashing.finish_digest()))
                        .ok_or_else(|| {
                            CaptureAttemptFailure::Capture(CaptureFailure::new(
                                Arc::clone(&output_identity),
                                CaptureFailureKind::StagingUnavailable,
                            ))
                        })
                })
        };
        self.finish_staged_carrier(
            lifecycle,
            cancellation,
            PendingStagedCarrier {
                artifact_identity,
                output_identity,
                destination,
                capture_result,
                profile: (media_type, CarrierBudgetClass::File),
            },
        )
        .map(|carrier| CapturedArtifact { carrier })
    }

    fn stage_git_carrier(
        &self,
        output_identity: Arc<str>,
        producer: &mut dyn CarrierProducer,
        bounds: CaptureBounds,
        cancellation: &CaptureCancellation,
    ) -> Result<GitBranchCarrier, CaptureAttemptFailure> {
        cancellation.check()?;
        let lifecycle = self.active_lifecycle(&output_identity)?;
        let (artifact_identity, mut destination) = self.create_destination().map_err(|kind| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(Arc::clone(&output_identity), kind))
        })?;
        let capture_result = {
            let mut bounded =
                CarrierDestination::new(&mut destination, bounds, &output_identity, cancellation);
            let produced = producer.stream_to(&mut bounded);
            let failure = bounded.failure.take();
            let size = bounded.bytes_written;
            match (failure, produced) {
                (Some(failure), _) => Err(failure),
                (None, Err(_)) => Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                    Arc::clone(&output_identity),
                    CaptureFailureKind::CarrierProducerUnavailable,
                ))),
                (None, Ok(())) => Ok((size, bounded.finish_digest())),
            }
        };
        self.finish_staged_carrier(
            lifecycle,
            cancellation,
            PendingStagedCarrier {
                artifact_identity,
                output_identity,
                destination,
                capture_result,
                profile: (
                    Arc::from("application/vnd.git.bundle"),
                    CarrierBudgetClass::Git,
                ),
            },
        )
        .map(|staged| GitBranchCarrier { staged })
    }

    fn active_lifecycle(
        &self,
        output_identity: &Arc<str>,
    ) -> Result<RwLockReadGuard<'_, StagingLifecycle>, CaptureAttemptFailure> {
        let lifecycle = self.inner.lifecycle.read().map_err(|_| {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::clone(output_identity),
                CaptureFailureKind::StagingUnavailable,
            ))
        })?;
        if *lifecycle != StagingLifecycle::Active {
            return Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::clone(output_identity),
                CaptureFailureKind::StagingUnavailable,
            )));
        }
        Ok(lifecycle)
    }

    fn finish_carrier_destination(
        &self,
        artifact_identity: &Arc<str>,
        destination: &mut File,
        output_identity: &Arc<str>,
        cancellation: &CaptureCancellation,
        capture_result: Result<(u64, Arc<str>), CaptureAttemptFailure>,
    ) -> Result<(u64, Arc<str>, CarrierFileIdentity), CaptureAttemptFailure> {
        capture_result.and_then(|(size, digest)| {
            self.finish_destination(
                artifact_identity,
                destination,
                output_identity,
                cancellation,
            )
            .map(|file_identity| (size, digest, file_identity))
        })
    }

    fn finish_staged_carrier(
        &self,
        lifecycle: RwLockReadGuard<'_, StagingLifecycle>,
        cancellation: &CaptureCancellation,
        pending: PendingStagedCarrier,
    ) -> Result<StagedCarrier, CaptureAttemptFailure> {
        let PendingStagedCarrier {
            artifact_identity,
            output_identity,
            mut destination,
            capture_result,
            profile,
        } = pending;
        let capture_result = self.finish_carrier_destination(
            &artifact_identity,
            &mut destination,
            &output_identity,
            cancellation,
            capture_result,
        );
        drop(destination);
        let (size, sha256, file_identity) = self.finish_staging_result(
            lifecycle,
            &artifact_identity,
            &output_identity,
            capture_result,
        )?;
        Ok(self.staged_carrier(
            artifact_identity,
            file_identity,
            StagedCarrierMetadata {
                output_identity,
                size,
                media_type: profile.0,
                sha256,
                budget_class: profile.1,
            },
        ))
    }

    fn finish_staging_result<Value>(
        &self,
        lifecycle: RwLockReadGuard<'_, StagingLifecycle>,
        artifact_identity: &Arc<str>,
        output_identity: &Arc<str>,
        result: Result<Value, CaptureAttemptFailure>,
    ) -> Result<Value, CaptureAttemptFailure> {
        let failure = match result {
            Ok(value) => return Ok(value),
            Err(failure) => failure,
        };
        drop(lifecycle);
        if self.inner.remove_artifact_while_active(artifact_identity) {
            return Err(failure);
        }
        self.inner.mark_cleanup_failed();
        match failure {
            CaptureAttemptFailure::Cancelled => Err(CaptureAttemptFailure::Cancelled),
            CaptureAttemptFailure::Capture(_) => {
                Err(CaptureAttemptFailure::Capture(CaptureFailure::new(
                    Arc::clone(output_identity),
                    CaptureFailureKind::StagingUnavailable,
                )))
            }
        }
    }

    fn finish_destination(
        &self,
        artifact_identity: &Arc<str>,
        destination: &mut File,
        output_identity: &Arc<str>,
        cancellation: &CaptureCancellation,
    ) -> Result<CarrierFileIdentity, CaptureAttemptFailure> {
        let unavailable = || {
            CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::clone(output_identity),
                CaptureFailureKind::StagingUnavailable,
            ))
        };
        cancellation.check()?;
        destination.flush().map_err(|_| unavailable())?;
        cancellation.check()?;
        fchmod(&*destination, Mode::RUSR).map_err(|_| unavailable())?;
        cancellation.check()?;
        let metadata = fstat(&*destination).map_err(|_| unavailable())?;
        let guard_identity = Arc::<str>::from(format!("guard_{artifact_identity}"));
        let mut identity_guards = self
            .inner
            .identity_guards
            .lock()
            .map_err(|_| unavailable())?;
        linkat(
            &self.inner.staging_root,
            artifact_identity.as_ref(),
            &self.inner.staging_root,
            guard_identity.as_ref(),
            AtFlags::empty(),
        )
        .map_err(|_| unavailable())?;
        identity_guards.insert(Arc::clone(artifact_identity), Arc::clone(&guard_identity));
        let guard_metadata = statat(
            &self.inner.staging_root,
            guard_identity.as_ref(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| unavailable())?;
        if FileType::from_raw_mode(guard_metadata.st_mode) != FileType::RegularFile
            || guard_metadata.st_dev != metadata.st_dev
            || guard_metadata.st_ino != metadata.st_ino
        {
            return Err(unavailable());
        }
        Ok(CarrierFileIdentity {
            device: metadata.st_dev,
            inode: metadata.st_ino,
        })
    }

    fn staged_carrier(
        &self,
        artifact_identity: Arc<str>,
        file_identity: CarrierFileIdentity,
        metadata: StagedCarrierMetadata,
    ) -> StagedCarrier {
        StagedCarrier {
            handle: ArtifactHandle {
                store_identity: Arc::clone(&self.inner.store_identity),
                artifact_identity: Arc::clone(&artifact_identity),
                lease: Arc::new(ArtifactLease {
                    store: Arc::downgrade(&self.inner),
                    artifact_identity,
                    file_identity,
                    metadata,
                    budgeted: AtomicBool::new(false),
                }),
            },
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

    pub(crate) fn open_artifact(
        &self,
        handle: &ArtifactHandle,
    ) -> Result<File, ArtifactReadFailure> {
        let lifecycle = self
            .inner
            .lifecycle
            .read()
            .map_err(|_| ArtifactReadFailure::Unavailable)?;
        if *lifecycle != StagingLifecycle::Active {
            return Err(ArtifactReadFailure::UnknownHandle);
        }
        self.open_artifact_while_active(handle)
    }

    fn open_artifact_while_active(
        &self,
        handle: &ArtifactHandle,
    ) -> Result<File, ArtifactReadFailure> {
        if handle.store_identity != self.inner.store_identity {
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
        let expected = handle.lease.file_identity;
        if metadata.st_dev != expected.device || metadata.st_ino != expected.inode {
            return Err(ArtifactReadFailure::UnknownHandle);
        }
        let guard_identity = self
            .inner
            .identity_guards
            .lock()
            .map_err(|_| ArtifactReadFailure::Unavailable)?
            .get(&handle.artifact_identity)
            .cloned()
            .ok_or(ArtifactReadFailure::UnknownHandle)?;
        let guard_metadata = statat(
            &self.inner.staging_root,
            guard_identity.as_ref(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|failure| match failure {
            Errno::NOENT | Errno::LOOP => ArtifactReadFailure::UnknownHandle,
            _ => ArtifactReadFailure::Unavailable,
        })?;
        if FileType::from_raw_mode(guard_metadata.st_mode) != FileType::RegularFile
            || guard_metadata.st_dev != expected.device
            || guard_metadata.st_ino != expected.inode
        {
            return Err(ArtifactReadFailure::UnknownHandle);
        }
        Ok(File::from(opened))
    }

    pub(super) fn discard(&self, artifact: &CapturedArtifact) {
        self.discard_carrier(artifact.carrier());
    }

    fn discard_carrier(&self, carrier: &StagedCarrier) {
        if carrier.handle.store_identity == self.inner.store_identity
            && self
                .inner
                .remove_artifact(&carrier.handle.artifact_identity)
        {
            carrier.handle.lease.release_budget(&self.inner);
        }
    }
}

impl ArtifactStagingInner {
    fn remove_capture_set(&self, captured: &BTreeMap<String, CapturedValue>) -> bool {
        let mut rollback_complete = true;
        for carrier in captured
            .values()
            .filter_map(CapturedValue::private_capture_carrier)
        {
            rollback_complete &=
                self.remove_artifact_while_active(&carrier.handle.artifact_identity);
        }
        rollback_complete
    }

    fn release_budget(&self, class: CarrierBudgetClass, size: u64) {
        let Ok(mut budget) = self.budget.lock() else {
            return;
        };
        let usage = budget.usage_mut(class);
        usage.captured_count = usage.captured_count.saturating_sub(1);
        usage.captured_bytes = usage.captured_bytes.saturating_sub(size);
    }

    fn remove_artifact(&self, artifact_identity: &str) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.write() else {
            return false;
        };
        if *lifecycle == StagingLifecycle::Released {
            return true;
        }
        let removed = self.remove_artifact_while_active(artifact_identity);
        if !removed && *lifecycle == StagingLifecycle::Active {
            *lifecycle = StagingLifecycle::CleanupFailed;
        }
        removed
    }

    fn remove_artifact_while_active(&self, artifact_identity: &str) -> bool {
        #[cfg(test)]
        if self.artifact_unlinks_blocked.load(Ordering::Acquire) {
            return false;
        }
        let Ok(mut artifacts) = self.artifacts.lock() else {
            return false;
        };
        let Ok(mut identity_guards) = self.identity_guards.lock() else {
            return false;
        };
        if !matches!(
            unlinkat(&self.staging_root, artifact_identity, AtFlags::empty()),
            Ok(()) | Err(Errno::NOENT)
        ) {
            return false;
        }
        if let Some(guard_identity) = identity_guards.get(artifact_identity)
            && !matches!(
                unlinkat(
                    &self.staging_root,
                    guard_identity.as_ref(),
                    AtFlags::empty(),
                ),
                Ok(()) | Err(Errno::NOENT)
            )
        {
            return false;
        }
        artifacts.remove(artifact_identity);
        identity_guards.remove(artifact_identity);
        true
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
        let mut identity_guards = self
            .identity_guards
            .lock()
            .map_err(|_| ArtifactReleaseFailure::CleanupUnavailable)?;
        let identities = artifacts.iter().cloned().collect::<Vec<_>>();
        for identity in identities {
            match unlinkat(&self.staging_root, identity.as_ref(), AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => {}
                Err(_) => return Err(ArtifactReleaseFailure::CleanupUnavailable),
            }
            if let Some(guard_identity) = identity_guards.get(&identity) {
                match unlinkat(
                    &self.staging_root,
                    guard_identity.as_ref(),
                    AtFlags::empty(),
                ) {
                    Ok(()) | Err(Errno::NOENT) => {}
                    Err(_) => return Err(ArtifactReleaseFailure::CleanupUnavailable),
                }
            }
            artifacts.remove(&identity);
            identity_guards.remove(&identity);
        }
        if !identity_guards.is_empty() {
            return Err(ArtifactReleaseFailure::CleanupUnavailable);
        }
        drop(identity_guards);
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

fn finish_sha256(digest: &mut Option<DigestContext>) -> Arc<str> {
    let digest = digest
        .take()
        .unwrap_or_else(|| DigestContext::new(&SHA256))
        .finish();
    Arc::from(lowercase_hex(digest.as_ref()))
}

pub(crate) struct CarrierDestination<'a> {
    destination: &'a mut File,
    bounds: CaptureBounds,
    output_identity: &'a Arc<str>,
    cancellation: &'a CaptureCancellation,
    digest: Option<DigestContext>,
    bytes_written: u64,
    failure: Option<CaptureAttemptFailure>,
}

impl<'a> CarrierDestination<'a> {
    fn new(
        destination: &'a mut File,
        bounds: CaptureBounds,
        output_identity: &'a Arc<str>,
        cancellation: &'a CaptureCancellation,
    ) -> Self {
        Self {
            destination,
            bounds,
            output_identity,
            cancellation,
            digest: Some(DigestContext::new(&SHA256)),
            bytes_written: 0,
            failure: None,
        }
    }

    fn finish_digest(&mut self) -> Arc<str> {
        finish_sha256(&mut self.digest)
    }

    fn record_failure(&mut self, failure: CaptureAttemptFailure) -> io::Error {
        self.failure = Some(failure);
        io::Error::other("private carrier staging rejected the stream")
    }

    fn staging_failure(&self) -> CaptureAttemptFailure {
        CaptureAttemptFailure::Capture(CaptureFailure::new(
            Arc::clone(self.output_identity),
            CaptureFailureKind::StagingUnavailable,
        ))
    }
}

impl Write for CarrierDestination<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failure.is_some() {
            return Err(io::Error::other("private carrier staging is unavailable"));
        }
        if let Err(failure) = self
            .cancellation
            .boundary(self.output_identity, CaptureBoundaryKind::BeforeWrite)
        {
            return Err(self.record_failure(failure));
        }
        let requested = match u64::try_from(bytes.len()) {
            Ok(requested) => requested,
            Err(_) => {
                let failure = CaptureAttemptFailure::Capture(CaptureFailure::new(
                    Arc::clone(self.output_identity),
                    self.bounds.overflow_kind,
                ));
                return Err(self.record_failure(failure));
            }
        };
        let remaining = self.bounds.maximum_bytes.saturating_sub(self.bytes_written);
        if requested > remaining {
            let failure = CaptureAttemptFailure::Capture(CaptureFailure::new(
                Arc::clone(self.output_identity),
                self.bounds.overflow_kind,
            ));
            return Err(self.record_failure(failure));
        }
        let written = match self.destination.write(bytes) {
            Ok(0) if !bytes.is_empty() => {
                let failure = self.staging_failure();
                return Err(self.record_failure(failure));
            }
            Ok(written) => written,
            Err(_) => {
                let failure = self.staging_failure();
                return Err(self.record_failure(failure));
            }
        };
        let written_bytes = match u64::try_from(written) {
            Ok(written) => written,
            Err(_) => {
                let failure = self.staging_failure();
                return Err(self.record_failure(failure));
            }
        };
        self.bytes_written = match self.bytes_written.checked_add(written_bytes) {
            Some(total) => total,
            None => {
                let failure = self.staging_failure();
                return Err(self.record_failure(failure));
            }
        };
        if let Some(digest) = &mut self.digest {
            digest.update(&bytes[..written]);
        }
        if let Err(failure) = self
            .cancellation
            .boundary(self.output_identity, CaptureBoundaryKind::AfterWrite)
        {
            return Err(self.record_failure(failure));
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failure.is_some() {
            return Err(io::Error::other("private carrier staging is unavailable"));
        }
        if let Err(failure) = self.cancellation.check() {
            return Err(self.record_failure(failure));
        }
        if self.destination.flush().is_err() {
            let failure = self.staging_failure();
            return Err(self.record_failure(failure));
        }
        Ok(())
    }
}

struct HashingCarrierDestination<'a> {
    destination: &'a mut File,
    digest: Option<DigestContext>,
    bytes_written: u64,
}

impl<'a> HashingCarrierDestination<'a> {
    fn new(destination: &'a mut File) -> Self {
        Self {
            destination,
            digest: Some(DigestContext::new(&SHA256)),
            bytes_written: 0,
        }
    }

    fn finish_digest(&mut self) -> Arc<str> {
        finish_sha256(&mut self.digest)
    }
}

impl Write for HashingCarrierDestination<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.destination.write(bytes)?;
        self.bytes_written = self
            .bytes_written
            .checked_add(u64::try_from(written).map_err(|_| io::Error::other("carrier size"))?)
            .ok_or_else(|| io::Error::other("carrier size"))?;
        if let Some(digest) = &mut self.digest {
            digest.update(&bytes[..written]);
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.destination.flush()
    }
}

struct CopyRequest<'request, 'destination> {
    source: &'request mut File,
    destination: &'request mut HashingCarrierDestination<'destination>,
    maximum_bytes: u64,
    output_identity: &'request Arc<str>,
    cancellation: &'request CaptureCancellation,
}

trait StreamCopier {
    fn copy(&mut self, request: CopyRequest<'_, '_>) -> Result<u64, CaptureAttemptFailure>;
}

struct PortableCopier;

impl StreamCopier for PortableCopier {
    fn copy(&mut self, request: CopyRequest<'_, '_>) -> Result<u64, CaptureAttemptFailure> {
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

fn expose_read_failure(failure: ArtifactReadFailure) -> ArtifactExposeFailure {
    match failure {
        ArtifactReadFailure::UnknownHandle => ArtifactExposeFailure::UnknownHandle,
        ArtifactReadFailure::Unavailable | ArtifactReadFailure::DestinationWrite => {
            ArtifactExposeFailure::Unavailable
        }
    }
}

fn is_single_path_component(name: &OsStr) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(component)) if component == name)
        && components.next().is_none()
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
