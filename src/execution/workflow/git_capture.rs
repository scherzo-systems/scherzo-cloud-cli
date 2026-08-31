//! Read-only Git workspace capture for semantic branch artifacts.
//!
//! The adapter pins committed object identities and verifies the staged carrier. It never creates
//! a ref or invokes a named remote. Local capture may use the workspace's retained promisor
//! authority; Cloud capture disables implicit lazy fetch and requires its admitted checkout to be
//! self-contained.
//!
//! Capture does not provide snapshot isolation from arbitrary concurrent repository mutation. The
//! final head and cleanliness observations detect changes present at that boundary only.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use ring::digest::{Context as DigestContext, SHA256};

use super::admission::{AdmittedExecutionContext, EnvironmentSnapshot};
use super::artifact::{
    ArtifactReadFailure, ArtifactStaging, CaptureAttemptFailure, CaptureBoundaryKind,
    CaptureCancellation, CaptureCandidateDeclaration, CaptureCandidateSet, CaptureDeclaration,
    CaptureFailure, CaptureFailureKind, CarrierDestination, CarrierProducer,
    GitBranchCaptureDeclaration, GitBranchMetadata, GitObjectFormat,
};
use super::execution_root::{AdmittedExecutionRoot, WorkingDirectorySelectionFailure};
use super::schema_common::{is_lowercase_hex, lowercase_hex};
use crate::process::ManagedProcessGroup;

const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAXIMUM_SMALL_OUTPUT_BYTES: usize = 64 * 1024;
const MAXIMUM_BUNDLE_HEADER_BYTES: usize = 1024 * 1024;
const MAXIMUM_PACK_ENTRIES: usize = 1_000_000;
const MAXIMUM_INFLATED_GIT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_OBJECT_LIST_BYTES: usize = 41 * MAXIMUM_PACK_ENTRIES;
const MAXIMUM_OBJECT_SIZE_LIST_BYTES: usize = 21 * MAXIMUM_PACK_ENTRIES;
const MAXIMUM_TRACKED_ENTRY_MODE_BYTES: usize = 7 * (MAXIMUM_PACK_ENTRIES + 1);
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const BUNDLE_MEDIA_TYPE: &str = "application/vnd.git.bundle";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GitWorkspaceAdmissionFailure {
    Cancelled,
    ExecutionRootRebound,
    GitUnavailable,
    GitTimedOut,
    GitOutputLimitExceeded,
    NotWorkTree,
    ExecutionRootNotWorkTreeRoot,
    UnsupportedObjectFormat,
    BaselineUnavailable,
    InitialWorkspaceDirty,
}

impl fmt::Display for GitWorkspaceAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Git workspace admission failure: {self:?}")
    }
}

impl std::error::Error for GitWorkspaceAdmissionFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GitCommandTimeout {
    command: Arc<str>,
    limit: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GitCaptureFailure {
    Cancelled,
    ExecutionRootRebound,
    StagingMismatch,
    HeadUnavailable,
    BaselineNotAncestor,
    CleanlinessUnavailable,
    WorkspaceDirty,
    TreeUnavailable,
    RequiredObjectsUnavailable,
    SourceAuthorityChanged,
    GitStructureLimitExceeded,
    CommandTimedOut(Box<GitCommandTimeout>),
    BundleGenerationFailed,
    BundleProfileInvalid,
    BundleVerificationFailed,
    WorkspaceChanged,
    TemporaryStorageUnavailable,
    Artifact(CaptureFailure),
}

impl fmt::Display for GitCaptureFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandTimedOut(timeout) => write!(
                formatter,
                "Git branch capture command `{}` exceeded the {:?} timeout",
                timeout.command, timeout.limit
            ),
            _ => write!(formatter, "Git branch capture failure: {self:?}"),
        }
    }
}

impl std::error::Error for GitCaptureFailure {}

#[derive(Clone, Copy)]
pub(crate) enum GitAwareCaptureDeclaration<'a> {
    File(CaptureDeclaration<'a>),
    GitBranch(&'a str),
}

#[derive(Clone)]
pub(crate) struct CloudGitCaptureProjection {
    source_commit: Arc<str>,
    workflow_digest: Arc<str>,
    admission_cancellation: CaptureCancellation,
}

impl fmt::Debug for CloudGitCaptureProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CloudGitCaptureProjection")
            .field("source_commit", &self.source_commit)
            .field("workflow_digest", &self.workflow_digest)
            .finish_non_exhaustive()
    }
}

impl CloudGitCaptureProjection {
    pub(crate) fn new(
        source_commit: Arc<str>,
        workflow_digest: Arc<str>,
        admission_cancellation: CaptureCancellation,
    ) -> Self {
        Self {
            source_commit,
            workflow_digest,
            admission_cancellation,
        }
    }

    pub(crate) fn workflow_digest(&self) -> &str {
        &self.workflow_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GitMetadataIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
}

#[derive(Clone)]
pub(crate) struct GitCaptureContext {
    root: AdmittedExecutionRoot,
    git_metadata: GitMetadataIdentity,
    environment: EnvironmentSnapshot,
    baseline_oid: Arc<str>,
    workflow_digest: Option<Arc<str>>,
    source_authority: Arc<[u8]>,
    object_format: GitObjectFormat,
    carrier_limits: (usize, u64, u64),
    disable_implicit_fetch: bool,
    git_program: Arc<PathBuf>,
    command_timeout: Duration,
}

impl fmt::Debug for GitCaptureContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GitCaptureContext")
            .field("root", &self.root)
            .field("git_metadata", &self.git_metadata)
            .field("baseline_oid", &self.baseline_oid)
            .field("workflow_digest", &self.workflow_digest)
            .field("object_format", &self.object_format)
            .field("carrier_limits", &self.carrier_limits)
            .finish_non_exhaustive()
    }
}

impl GitCaptureContext {
    pub(crate) fn admit(
        execution: &AdmittedExecutionContext,
        cancellation: &CaptureCancellation,
    ) -> Result<Self, GitWorkspaceAdmissionFailure> {
        Self::admit_local(execution, cancellation)
    }

    pub(crate) fn admit_local(
        execution: &AdmittedExecutionContext,
        cancellation: &CaptureCancellation,
    ) -> Result<Self, GitWorkspaceAdmissionFailure> {
        Self::admit_with_program(
            execution,
            cancellation,
            PathBuf::from("git"),
            GIT_COMMAND_TIMEOUT,
        )
    }

    pub(crate) fn admit_cloud(
        execution: &AdmittedExecutionContext,
        projection: &CloudGitCaptureProjection,
    ) -> Result<Self, GitWorkspaceAdmissionFailure> {
        Self::admit_with_program_and_cloud(
            execution,
            &projection.admission_cancellation,
            Some(projection),
            PathBuf::from("git"),
            GIT_COMMAND_TIMEOUT,
        )
    }

    fn admit_with_program(
        execution: &AdmittedExecutionContext,
        cancellation: &CaptureCancellation,
        git_program: PathBuf,
        command_timeout: Duration,
    ) -> Result<Self, GitWorkspaceAdmissionFailure> {
        Self::admit_with_program_and_cloud(
            execution,
            cancellation,
            None,
            git_program,
            command_timeout,
        )
    }

    fn admit_with_program_and_cloud(
        execution: &AdmittedExecutionContext,
        cancellation: &CaptureCancellation,
        cloud: Option<&CloudGitCaptureProjection>,
        git_program: PathBuf,
        command_timeout: Duration,
    ) -> Result<Self, GitWorkspaceAdmissionFailure> {
        cancellation
            .check()
            .map_err(|_| GitWorkspaceAdmissionFailure::Cancelled)?;
        let mut context = Self {
            root: execution.root_identity().clone(),
            git_metadata: GitMetadataIdentity {
                path: PathBuf::new(),
                device: 0,
                inode: 0,
            },
            environment: execution.environment().clone(),
            baseline_oid: Arc::from(""),
            workflow_digest: cloud.map(|projection| Arc::clone(&projection.workflow_digest)),
            source_authority: Arc::from([]),
            object_format: GitObjectFormat::Sha1,
            carrier_limits: (
                execution.limits().maximum_captured_git_carriers().get(),
                execution
                    .limits()
                    .maximum_captured_git_carrier_bytes()
                    .get(),
                execution
                    .limits()
                    .maximum_total_captured_git_carrier_bytes()
                    .get(),
            ),
            disable_implicit_fetch: cloud.is_some(),
            git_program: Arc::new(git_program),
            command_timeout,
        };

        let repository = context
            .run_source(
                &[
                    "rev-parse",
                    "--is-inside-work-tree",
                    "--show-prefix",
                    "--show-object-format",
                ],
                ProcessInput::None,
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
                true,
                None,
            )
            .map_err(admission_process_failure)?;
        if repository.stdout.truncated {
            return Err(GitWorkspaceAdmissionFailure::GitOutputLimitExceeded);
        }
        if !repository.status.success() {
            return Err(GitWorkspaceAdmissionFailure::NotWorkTree);
        }
        let mut facts = repository.stdout.bytes.split(|byte| *byte == b'\n');
        if facts.next() != Some(b"true".as_slice()) {
            return Err(GitWorkspaceAdmissionFailure::NotWorkTree);
        }
        if facts.next() != Some(b"".as_slice()) {
            return Err(GitWorkspaceAdmissionFailure::ExecutionRootNotWorkTreeRoot);
        }
        if facts.next() != Some(b"sha1".as_slice()) || facts.next() != Some(b"".as_slice()) {
            return Err(GitWorkspaceAdmissionFailure::UnsupportedObjectFormat);
        }

        let tracked_modes = context
            .read_tracked_entry_modes(cancellation)
            .map_err(admission_process_failure)?;
        if tracked_modes.stdout.truncated {
            return Err(GitWorkspaceAdmissionFailure::GitOutputLimitExceeded);
        }
        if !tracked_modes.status.success() {
            return Err(GitWorkspaceAdmissionFailure::GitUnavailable);
        }
        if contains_gitlink(&tracked_modes.stdout.bytes) {
            return Err(GitWorkspaceAdmissionFailure::ExecutionRootNotWorkTreeRoot);
        }

        let baseline = context
            .run_source(
                &["rev-parse", "--verify", "HEAD^{commit}"],
                ProcessInput::None,
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
                true,
                None,
            )
            .map_err(admission_process_failure)?;
        let baseline_oid =
            successful_oid(&baseline).ok_or(GitWorkspaceAdmissionFailure::BaselineUnavailable)?;
        let source_authority = context
            .read_source_authority(cancellation)
            .map_err(admission_process_failure)?;
        if source_authority.stdout.truncated {
            return Err(GitWorkspaceAdmissionFailure::GitOutputLimitExceeded);
        }
        let source_authority = source_authority_snapshot(&source_authority)
            .ok_or(GitWorkspaceAdmissionFailure::GitUnavailable)?;
        if let Some(cloud) = cloud
            && baseline_oid.as_ref() != cloud.source_commit.as_ref()
        {
            return Err(GitWorkspaceAdmissionFailure::BaselineUnavailable);
        }
        context.git_metadata = context.read_git_metadata(cancellation)?;
        if cloud.is_some() {
            let status = context
                .run_source(
                    &[
                        "status",
                        "--porcelain=v1",
                        "-z",
                        "--untracked-files=normal",
                        "--ignore-submodules=none",
                    ],
                    ProcessInput::None,
                    1,
                    cancellation,
                    true,
                    None,
                )
                .map_err(admission_process_failure)?;
            if !status.status.success()
                || status.stdout.truncated
                || !status.stdout.bytes.is_empty()
            {
                return Err(GitWorkspaceAdmissionFailure::InitialWorkspaceDirty);
            }
        }
        context.baseline_oid = baseline_oid;
        context.source_authority = source_authority;
        Ok(context)
    }

    pub(crate) fn baseline_oid(&self) -> &str {
        &self.baseline_oid
    }

    pub(crate) fn object_format(&self) -> GitObjectFormat {
        self.object_format
    }

    pub(crate) fn workflow_digest(&self) -> Option<&str> {
        self.workflow_digest.as_deref()
    }

    pub(crate) fn carrier_limits(&self) -> (usize, u64, u64) {
        self.carrier_limits
    }

    /// Stages and verifies one branch candidate. The returned set remains provisional until its
    /// workflow reduction is accepted; dropping it rolls back the carrier and reservation.
    pub(crate) fn capture(
        &self,
        output_identity: &str,
        artifacts: &ArtifactStaging,
        cancellation: &CaptureCancellation,
    ) -> Result<CaptureCandidateSet, GitCaptureFailure> {
        self.capture_step(
            &[GitAwareCaptureDeclaration::GitBranch(output_identity)],
            artifacts,
            cancellation,
        )
    }

    /// Captures one step's file and Git declarations as one physical candidate set.
    pub(crate) fn capture_step(
        &self,
        declarations: &[GitAwareCaptureDeclaration<'_>],
        artifacts: &ArtifactStaging,
        cancellation: &CaptureCancellation,
    ) -> Result<CaptureCandidateSet, GitCaptureFailure> {
        cancellation
            .check()
            .map_err(|_| GitCaptureFailure::Cancelled)?;
        if !artifacts.is_bound_to_root(&self.root) {
            return Err(GitCaptureFailure::StagingMismatch);
        }
        let first_git = declarations
            .iter()
            .find_map(|declaration| match declaration {
                GitAwareCaptureDeclaration::GitBranch(identity) => Some(*identity),
                GitAwareCaptureDeclaration::File(_) => None,
            })
            .ok_or(GitCaptureFailure::BundleProfileInvalid)?;

        let initial = self.observe(cancellation)?;
        // Treating the admitted baseline as the sole shallow boundary keeps revision traversal
        // independent of history before it while exposing missing post-baseline objects to Git's
        // ordinary promisor hydration instead of silently accepting another shallow cutoff.
        let shallow = self.baseline_shallow_file()?;
        self.require_ancestor(&initial.head_oid, shallow.path(), cancellation)?;
        let changed = initial.head_oid != self.baseline_oid;
        let object_count = if changed {
            self.require_capture_objects(&initial.head_oid, shallow.path(), cancellation)?
        } else {
            0
        };
        let metadata = GitBranchMetadata::new(
            Arc::clone(&self.baseline_oid),
            Arc::clone(&initial.head_oid),
            Arc::clone(&initial.tree_oid),
        );
        let mut producers = declarations
            .iter()
            .filter(|declaration| {
                changed && matches!(declaration, GitAwareCaptureDeclaration::GitBranch(_))
            })
            .map(|_| GitBundleProducer {
                context: self,
                baseline_oid: Arc::clone(&self.baseline_oid),
                head_oid: Arc::clone(&initial.head_oid),
                shallow_file: shallow.path(),
                cancellation,
                failure: None,
            })
            .collect::<Vec<_>>();
        let staged = {
            let mut producers = producers.iter_mut();
            let mut candidates = declarations
                .iter()
                .map(|declaration| match declaration {
                    GitAwareCaptureDeclaration::File(declaration) => {
                        Ok(CaptureCandidateDeclaration::File(*declaration))
                    }
                    GitAwareCaptureDeclaration::GitBranch(identity) => {
                        let producer = if changed {
                            Some(
                                producers
                                    .next()
                                    .ok_or(GitCaptureFailure::BundleProfileInvalid)?
                                    as &mut dyn CarrierProducer,
                            )
                        } else {
                            None
                        };
                        Ok(CaptureCandidateDeclaration::GitBranch(
                            GitBranchCaptureDeclaration::new(identity, metadata.clone(), producer),
                        ))
                    }
                })
                .collect::<Result<Vec<_>, GitCaptureFailure>>()?;
            artifacts.capture_candidates(&mut candidates, cancellation)
        };
        let production_failure = producers.into_iter().find_map(|producer| producer.failure);
        let candidates = match staged {
            Ok(candidates) if production_failure.is_none() => candidates,
            Ok(candidates) => {
                candidates.abort();
                return Err(production_failure.unwrap_or(GitCaptureFailure::BundleGenerationFailed));
            }
            Err(failure) => {
                return Err(production_failure.unwrap_or_else(|| map_staging_failure(failure)));
            }
        };

        if changed {
            for output_identity in declarations
                .iter()
                .filter_map(|declaration| match declaration {
                    GitAwareCaptureDeclaration::GitBranch(identity) => Some(*identity),
                    GitAwareCaptureDeclaration::File(_) => None,
                })
            {
                if let Err(failure) = self.verify_staged(
                    output_identity,
                    artifacts,
                    &candidates,
                    &metadata,
                    object_count,
                    cancellation,
                ) {
                    candidates.abort();
                    return Err(failure);
                }
            }
        }

        cancellation
            .boundary(&Arc::from(first_git), CaptureBoundaryKind::BeforeGitRecheck)
            .map_err(|_| GitCaptureFailure::Cancelled)?;
        let final_observation = match self.observe(cancellation) {
            Ok(observation) => observation,
            Err(GitCaptureFailure::WorkspaceDirty) => {
                candidates.abort();
                return Err(GitCaptureFailure::WorkspaceChanged);
            }
            Err(failure) => {
                candidates.abort();
                return Err(failure);
            }
        };
        if final_observation.head_oid != initial.head_oid
            || final_observation.tree_oid != initial.tree_oid
        {
            candidates.abort();
            return Err(GitCaptureFailure::WorkspaceChanged);
        }
        Ok(candidates)
    }

    fn observe(
        &self,
        cancellation: &CaptureCancellation,
    ) -> Result<GitObservation, GitCaptureFailure> {
        let head_oid = self.read_head(cancellation)?;

        let clean = self
            .run_source(
                &[
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--untracked-files=normal",
                    "--ignore-submodules=none",
                ],
                ProcessInput::None,
                1,
                cancellation,
                true,
                None,
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::CleanlinessUnavailable)
            })?;
        if !clean.status.success() {
            return Err(GitCaptureFailure::CleanlinessUnavailable);
        }
        if clean.stdout.truncated || !clean.stdout.bytes.is_empty() {
            return Err(GitCaptureFailure::WorkspaceDirty);
        }
        self.require_single_repository(cancellation)?;

        let tree_expression = format!("{head_oid}^{{tree}}");
        let tree = self
            .run_source(
                &["rev-parse", "--verify", &tree_expression],
                ProcessInput::None,
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
                true,
                None,
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::TreeUnavailable)
            })?;
        let tree_oid = successful_oid(&tree).ok_or(GitCaptureFailure::TreeUnavailable)?;
        if self.read_head(cancellation)? != head_oid {
            return Err(GitCaptureFailure::WorkspaceChanged);
        }
        Ok(GitObservation { head_oid, tree_oid })
    }

    fn read_head(&self, cancellation: &CaptureCancellation) -> Result<Arc<str>, GitCaptureFailure> {
        let head = self
            .run_source(
                &["rev-parse", "--verify", "HEAD^{commit}"],
                ProcessInput::None,
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
                true,
                None,
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::HeadUnavailable)
            })?;
        successful_oid(&head).ok_or(GitCaptureFailure::HeadUnavailable)
    }

    fn require_ancestor(
        &self,
        head_oid: &str,
        shallow_file: &Path,
        cancellation: &CaptureCancellation,
    ) -> Result<(), GitCaptureFailure> {
        let result = self
            .run_source(
                &["merge-base", "--is-ancestor", &self.baseline_oid, head_oid],
                ProcessInput::None,
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
                true,
                Some(shallow_file),
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::RequiredObjectsUnavailable)
            })?;
        match result.status.code() {
            Some(0) => Ok(()),
            Some(1) => Err(GitCaptureFailure::BaselineNotAncestor),
            _ => Err(GitCaptureFailure::RequiredObjectsUnavailable),
        }
    }

    fn require_capture_objects(
        &self,
        head_oid: &str,
        shallow_file: &Path,
        cancellation: &CaptureCancellation,
    ) -> Result<usize, GitCaptureFailure> {
        let exclusion = format!("^{}", self.baseline_oid);
        self.ensure_source_authority(cancellation)?;
        let objects = self
            .run_source(
                &[
                    "rev-list",
                    "--objects",
                    "--no-object-names",
                    head_oid,
                    &exclusion,
                ],
                ProcessInput::None,
                MAXIMUM_OBJECT_LIST_BYTES,
                cancellation,
                self.disable_implicit_fetch,
                Some(shallow_file),
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::RequiredObjectsUnavailable)
            })?;
        if !objects.status.success() {
            return Err(GitCaptureFailure::RequiredObjectsUnavailable);
        }
        if objects.stdout.truncated {
            return Err(GitCaptureFailure::GitStructureLimitExceeded);
        }
        let object_ids = parse_object_list(&objects.stdout.bytes)?;
        if object_ids.is_empty()
            || object_ids.len() > MAXIMUM_PACK_ENTRIES
            || !object_ids.contains(&head_oid)
        {
            return Err(GitCaptureFailure::GitStructureLimitExceeded);
        }

        self.ensure_source_authority(cancellation)?;
        let sizes = self
            .run_source(
                &["cat-file", "--batch-check=%(objectsize)"],
                ProcessInput::Bytes(&objects.stdout.bytes),
                MAXIMUM_OBJECT_SIZE_LIST_BYTES,
                cancellation,
                self.disable_implicit_fetch,
                None,
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::RequiredObjectsUnavailable)
            })?;
        if !sizes.status.success() {
            return Err(GitCaptureFailure::RequiredObjectsUnavailable);
        }
        if sizes.stdout.truncated {
            return Err(GitCaptureFailure::GitStructureLimitExceeded);
        }
        let mut inflated_bytes = 0_u64;
        let mut size_count = 0_usize;
        for line in terminated_lines(&sizes.stdout.bytes)
            .ok_or(GitCaptureFailure::RequiredObjectsUnavailable)?
        {
            let size = std::str::from_utf8(line)
                .ok()
                .and_then(|line| line.parse::<u64>().ok())
                .ok_or(GitCaptureFailure::RequiredObjectsUnavailable)?;
            inflated_bytes = inflated_bytes
                .checked_add(size)
                .ok_or(GitCaptureFailure::GitStructureLimitExceeded)?;
            size_count += 1;
        }
        if size_count != object_ids.len() || inflated_bytes > MAXIMUM_INFLATED_GIT_BYTES {
            return Err(GitCaptureFailure::GitStructureLimitExceeded);
        }
        Ok(object_ids.len())
    }

    fn verify_staged(
        &self,
        output_identity: &str,
        artifacts: &ArtifactStaging,
        candidates: &CaptureCandidateSet,
        expected: &GitBranchMetadata,
        expected_object_count: usize,
        cancellation: &CaptureCancellation,
    ) -> Result<(), GitCaptureFailure> {
        let branch = candidates
            .outputs()
            .get(output_identity)
            .and_then(|value| value.as_git_branch())
            .filter(|branch| branch.metadata() == expected)
            .ok_or(GitCaptureFailure::BundleProfileInvalid)?;
        let carrier = branch
            .carrier()
            .filter(|carrier| carrier.media_type() == BUNDLE_MEDIA_TYPE)
            .ok_or(GitCaptureFailure::BundleProfileInvalid)?;
        let scratch =
            tempfile::tempdir().map_err(|_| GitCaptureFailure::TemporaryStorageUnavailable)?;
        let bundle_path = scratch.path().join("carrier.bundle");
        let bundle = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&bundle_path)
            .map_err(|_| GitCaptureFailure::TemporaryStorageUnavailable)?;
        let mut copy = CancellableHashingWriter::new(bundle, cancellation);
        let copied =
            artifacts
                .copy_to(carrier.handle(), &mut copy)
                .map_err(|failure| match failure {
                    ArtifactReadFailure::DestinationWrite if cancellation.is_cancelled() => {
                        GitCaptureFailure::Cancelled
                    }
                    ArtifactReadFailure::UnknownHandle
                    | ArtifactReadFailure::Unavailable
                    | ArtifactReadFailure::DestinationWrite => {
                        GitCaptureFailure::BundleVerificationFailed
                    }
                })?;
        let (mut bundle, digest, observed_size) = copy.finish()?;
        if copied != carrier.size()
            || observed_size != carrier.size()
            || lowercase_hex(digest.as_ref()) != carrier.sha256()
        {
            return Err(GitCaptureFailure::BundleVerificationFailed);
        }

        let expected_header = bundle_header(expected.base_oid(), expected.head_oid());
        let header = read_bundle_header(&mut bundle)?;
        if header != expected_header {
            return Err(GitCaptureFailure::BundleProfileInvalid);
        }
        let body_offset =
            u64::try_from(header.len()).map_err(|_| GitCaptureFailure::BundleVerificationFailed)?;
        let mut pack_header = [0_u8; 12];
        bundle
            .read_exact(&mut pack_header)
            .map_err(|_| GitCaptureFailure::BundleVerificationFailed)?;
        let version = u32::from_be_bytes(
            pack_header[4..8]
                .try_into()
                .map_err(|_| GitCaptureFailure::BundleVerificationFailed)?,
        );
        let object_count = usize::try_from(u32::from_be_bytes(
            pack_header[8..12]
                .try_into()
                .map_err(|_| GitCaptureFailure::BundleVerificationFailed)?,
        ))
        .map_err(|_| GitCaptureFailure::GitStructureLimitExceeded)?;
        if &pack_header[..4] != b"PACK"
            || !matches!(version, 2 | 3)
            || object_count == 0
            || object_count > MAXIMUM_PACK_ENTRIES
            || object_count != expected_object_count
        {
            return Err(GitCaptureFailure::BundleVerificationFailed);
        }

        let verification_repository = scratch.path().join("verify.git");
        let repository_argument = verification_repository.as_os_str().to_owned();
        let initialized = self
            .run_unbound(
                &[
                    OsString::from("init"),
                    OsString::from("--quiet"),
                    OsString::from("--bare"),
                    OsString::from("--object-format=sha1"),
                    repository_argument,
                ],
                ProcessInput::None,
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::BundleVerificationFailed)
            })?;
        if !initialized.status.success() {
            return Err(GitCaptureFailure::BundleVerificationFailed);
        }

        bundle
            .seek(SeekFrom::Start(body_offset))
            .map_err(|_| GitCaptureFailure::BundleVerificationFailed)?;
        let git_dir = format!("--git-dir={}", verification_repository.display());
        let indexed = self
            .run_unbound(
                &[
                    OsString::from(&git_dir),
                    OsString::from("index-pack"),
                    OsString::from("--stdin"),
                ],
                ProcessInput::File(bundle),
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::BundleVerificationFailed)
            })?;
        if !indexed.status.success() || indexed.stdout.truncated {
            return Err(GitCaptureFailure::BundleVerificationFailed);
        }

        let object_type = self
            .run_unbound(
                &[
                    OsString::from(&git_dir),
                    OsString::from("cat-file"),
                    OsString::from("-t"),
                    OsString::from(expected.head_oid()),
                ],
                ProcessInput::None,
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::BundleVerificationFailed)
            })?;
        if !object_type.status.success() || object_type.stdout.bytes != b"commit\n" {
            return Err(GitCaptureFailure::BundleVerificationFailed);
        }
        let commit = self
            .run_unbound(
                &[
                    OsString::from(&git_dir),
                    OsString::from("cat-file"),
                    OsString::from("commit"),
                    OsString::from(expected.head_oid()),
                ],
                ProcessInput::None,
                4096,
                cancellation,
            )
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::BundleVerificationFailed)
            })?;
        let expected_tree = format!("tree {}", expected.tree_oid());
        if !commit.status.success()
            || commit.stdout.bytes.split(|byte| *byte == b'\n').next()
                != Some(expected_tree.as_bytes())
        {
            return Err(GitCaptureFailure::BundleVerificationFailed);
        }
        Ok(())
    }

    fn baseline_shallow_file(&self) -> Result<tempfile::NamedTempFile, GitCaptureFailure> {
        let mut shallow = tempfile::NamedTempFile::new()
            .map_err(|_| GitCaptureFailure::TemporaryStorageUnavailable)?;
        writeln!(shallow, "{}", self.baseline_oid)
            .and_then(|()| shallow.flush())
            .map_err(|_| GitCaptureFailure::TemporaryStorageUnavailable)?;
        Ok(shallow)
    }

    fn read_git_metadata(
        &self,
        cancellation: &CaptureCancellation,
    ) -> Result<GitMetadataIdentity, GitWorkspaceAdmissionFailure> {
        let output = self
            .run_source(
                &["rev-parse", "--absolute-git-dir"],
                ProcessInput::None,
                MAXIMUM_SMALL_OUTPUT_BYTES,
                cancellation,
                true,
                None,
            )
            .map_err(admission_process_failure)?;
        if !output.status.success() || output.stdout.truncated {
            return Err(GitWorkspaceAdmissionFailure::GitUnavailable);
        }
        let path = output
            .stdout
            .bytes
            .strip_suffix(b"\n")
            .and_then(|value| std::str::from_utf8(value).ok())
            .map(PathBuf::from)
            .ok_or(GitWorkspaceAdmissionFailure::GitUnavailable)?;
        let path = std::fs::canonicalize(path)
            .map_err(|_| GitWorkspaceAdmissionFailure::GitUnavailable)?;
        let metadata =
            std::fs::metadata(&path).map_err(|_| GitWorkspaceAdmissionFailure::GitUnavailable)?;
        if !metadata.is_dir() {
            return Err(GitWorkspaceAdmissionFailure::NotWorkTree);
        }
        Ok(GitMetadataIdentity {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn git_metadata_is_bound(&self) -> bool {
        if self.git_metadata.path.as_os_str().is_empty() {
            return true;
        }
        std::fs::metadata(&self.git_metadata.path).is_ok_and(|metadata| {
            metadata.is_dir()
                && metadata.dev() == self.git_metadata.device
                && metadata.ino() == self.git_metadata.inode
        })
    }

    fn read_tracked_entry_modes(
        &self,
        cancellation: &CaptureCancellation,
    ) -> Result<ProcessOutput, ProcessFailure> {
        self.run_source(
            &["ls-files", "--format=%(objectmode)"],
            ProcessInput::None,
            MAXIMUM_TRACKED_ENTRY_MODE_BYTES,
            cancellation,
            true,
            None,
        )
    }

    fn require_single_repository(
        &self,
        cancellation: &CaptureCancellation,
    ) -> Result<(), GitCaptureFailure> {
        let tracked_modes = self
            .read_tracked_entry_modes(cancellation)
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::CleanlinessUnavailable)
            })?;
        if tracked_modes.stdout.truncated {
            return Err(GitCaptureFailure::GitStructureLimitExceeded);
        }
        if !tracked_modes.status.success() {
            return Err(GitCaptureFailure::CleanlinessUnavailable);
        }
        if contains_gitlink(&tracked_modes.stdout.bytes) {
            return Err(GitCaptureFailure::WorkspaceChanged);
        }
        Ok(())
    }

    fn read_source_authority(
        &self,
        cancellation: &CaptureCancellation,
    ) -> Result<ProcessOutput, ProcessFailure> {
        self.run_source(
            &[
                "config",
                "--null",
                "--get-regexp",
                r"^(extensions\..*|remote\..*|url\..*|credential\..*|http\..*|core\.(askpass|gitproxy|sshcommand))$",
            ],
            ProcessInput::None,
            MAXIMUM_SMALL_OUTPUT_BYTES,
            cancellation,
            true,
            None,
        )
    }

    fn ensure_source_authority(
        &self,
        cancellation: &CaptureCancellation,
    ) -> Result<(), GitCaptureFailure> {
        let authority = self
            .read_source_authority(cancellation)
            .map_err(|failure| {
                capture_process_failure(failure, GitCaptureFailure::RequiredObjectsUnavailable)
            })?;
        if source_authority_snapshot(&authority).as_deref() != Some(self.source_authority.as_ref())
        {
            return Err(GitCaptureFailure::SourceAuthorityChanged);
        }
        Ok(())
    }

    fn run_source(
        &self,
        arguments: &[&str],
        input: ProcessInput<'_>,
        maximum_stdout_bytes: usize,
        cancellation: &CaptureCancellation,
        no_lazy_fetch: bool,
        shallow_file: Option<&Path>,
    ) -> Result<ProcessOutput, ProcessFailure> {
        if !self.git_metadata_is_bound() {
            return Err(ProcessFailure::ExecutionRootRebound);
        }
        let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
        let mut command = self.git_command(&arguments, no_lazy_fetch, shallow_file);
        self.root
            .bind_command_ref(&mut command)
            .map_err(|failure| match failure {
                WorkingDirectorySelectionFailure::ExecutionRootRebound
                | WorkingDirectorySelectionFailure::Unavailable
                | WorkingDirectorySelectionFailure::EscapesExecutionRoot
                | WorkingDirectorySelectionFailure::NotDirectory => {
                    ProcessFailure::ExecutionRootRebound
                }
            })?;
        execute_process(
            command,
            git_command_description(&arguments),
            input,
            maximum_stdout_bytes,
            cancellation,
            self.command_timeout,
        )
    }

    fn run_unbound(
        &self,
        arguments: &[OsString],
        input: ProcessInput<'_>,
        maximum_stdout_bytes: usize,
        cancellation: &CaptureCancellation,
    ) -> Result<ProcessOutput, ProcessFailure> {
        execute_process(
            self.git_command(arguments, true, None),
            git_command_description(arguments),
            input,
            maximum_stdout_bytes,
            cancellation,
            self.command_timeout,
        )
    }

    fn git_command(
        &self,
        arguments: &[OsString],
        no_lazy_fetch: bool,
        shallow_file: Option<&Path>,
    ) -> Command {
        let mut command = Command::new(self.git_program.as_os_str());
        command
            .args([
                "--no-pager",
                "--no-optional-locks",
                "--no-replace-objects",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "gc.auto=0",
                "-c",
                "maintenance.auto=false",
                "-c",
                "fetch.writeCommitGraph=false",
            ])
            .args(arguments)
            .env_clear();
        for (name, value) in self.environment.variables() {
            if !reserved_git_environment(name) {
                command.env(name, value);
            }
        }
        command
            .env("LC_ALL", "C")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env("GIT_NO_REPLACE_OBJECTS", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if no_lazy_fetch {
            command.env("GIT_NO_LAZY_FETCH", "1");
        }
        if let Some(shallow_file) = shallow_file {
            command.env("GIT_SHALLOW_FILE", shallow_file);
        }
        command.process_group(0);
        command
    }

    fn stream_bundle(
        &self,
        baseline_oid: &str,
        head_oid: &str,
        shallow_file: &Path,
        destination: &mut CarrierDestination<'_>,
        cancellation: &CaptureCancellation,
    ) -> Result<(), PackStreamFailure> {
        destination
            .write_all(&bundle_header(baseline_oid, head_oid))
            .map_err(PackStreamFailure::Destination)?;
        let exclusion = format!("^{baseline_oid}");
        let input = format!("{head_oid}\n{exclusion}\n");
        // A handwritten header avoids a temporary ref. Disabling deltas makes every carried
        // object's identity independently checkable in the isolated verification repository.
        self.ensure_source_authority(cancellation)
            .map_err(PackStreamFailure::Capture)?;
        let arguments = [
            OsString::from("pack-objects"),
            OsString::from("--stdout"),
            OsString::from("--revs"),
            // Git's default sparse object walk may add redundant objects for direct tree copies.
            // Keep the pack aligned with the fully walked object set checked before generation.
            OsString::from("--no-sparse"),
            OsString::from("--window=0"),
            OsString::from("--depth=0"),
        ];
        let mut command =
            self.git_command(&arguments, self.disable_implicit_fetch, Some(shallow_file));
        self.root
            .bind_command_ref(&mut command)
            .map_err(|_| PackStreamFailure::Process(ProcessFailure::ExecutionRootRebound))?;
        stream_process_stdout(
            command,
            git_command_description(&arguments),
            input.as_bytes(),
            destination,
            cancellation,
            self.command_timeout,
        )
    }
}

struct GitObservation {
    head_oid: Arc<str>,
    tree_oid: Arc<str>,
}

struct GitBundleProducer<'a> {
    context: &'a GitCaptureContext,
    baseline_oid: Arc<str>,
    head_oid: Arc<str>,
    shallow_file: &'a Path,
    cancellation: &'a CaptureCancellation,
    failure: Option<GitCaptureFailure>,
}

impl CarrierProducer for GitBundleProducer<'_> {
    fn stream_to(&mut self, destination: &mut CarrierDestination<'_>) -> io::Result<()> {
        match self.context.stream_bundle(
            &self.baseline_oid,
            &self.head_oid,
            self.shallow_file,
            destination,
            self.cancellation,
        ) {
            Ok(()) => Ok(()),
            Err(PackStreamFailure::Destination(failure)) => Err(failure),
            Err(PackStreamFailure::Process(failure)) => {
                let capture_failure =
                    capture_process_failure(failure, GitCaptureFailure::BundleGenerationFailed);
                let kind = if capture_failure == GitCaptureFailure::Cancelled {
                    io::ErrorKind::Interrupted
                } else {
                    io::ErrorKind::Other
                };
                self.failure = Some(capture_failure);
                Err(io::Error::new(kind, "Git bundle generation failed"))
            }
            Err(PackStreamFailure::Capture(failure)) => {
                let kind = if failure == GitCaptureFailure::Cancelled {
                    io::ErrorKind::Interrupted
                } else {
                    io::ErrorKind::Other
                };
                self.failure = Some(failure);
                Err(io::Error::new(kind, "Git bundle generation failed"))
            }
        }
    }
}

fn bundle_header(baseline_oid: &str, head_oid: &str) -> Vec<u8> {
    format!("# v2 git bundle\n-{baseline_oid} scherzo baseline\n{head_oid} refs/scherzo/head\n\n")
        .into_bytes()
}

fn read_bundle_header(bundle: &mut File) -> Result<Vec<u8>, GitCaptureFailure> {
    bundle
        .seek(SeekFrom::Start(0))
        .map_err(|_| GitCaptureFailure::BundleVerificationFailed)?;
    let mut header = Vec::new();
    let mut buffer = [0_u8; 4096];
    while header.len() <= MAXIMUM_BUNDLE_HEADER_BYTES {
        let read = bundle
            .read(&mut buffer)
            .map_err(|_| GitCaptureFailure::BundleVerificationFailed)?;
        if read == 0 {
            return Err(GitCaptureFailure::BundleProfileInvalid);
        }
        header.extend_from_slice(&buffer[..read]);
        if let Some(end) = header.windows(2).position(|window| window == b"\n\n") {
            let end = end + 2;
            if end > MAXIMUM_BUNDLE_HEADER_BYTES {
                return Err(GitCaptureFailure::GitStructureLimitExceeded);
            }
            header.truncate(end);
            bundle
                .seek(SeekFrom::Start(
                    u64::try_from(end).map_err(|_| GitCaptureFailure::BundleVerificationFailed)?,
                ))
                .map_err(|_| GitCaptureFailure::BundleVerificationFailed)?;
            return Ok(header);
        }
    }
    Err(GitCaptureFailure::GitStructureLimitExceeded)
}

fn parse_object_list(bytes: &[u8]) -> Result<Vec<&str>, GitCaptureFailure> {
    terminated_lines(bytes)
        .ok_or(GitCaptureFailure::RequiredObjectsUnavailable)?
        .map(|line| {
            std::str::from_utf8(line)
                .ok()
                .filter(|oid| is_lowercase_hex(oid, 40))
                .ok_or(GitCaptureFailure::RequiredObjectsUnavailable)
        })
        .collect()
}

fn terminated_lines(bytes: &[u8]) -> Option<impl Iterator<Item = &[u8]>> {
    bytes
        .is_empty()
        .then_some(())
        .or_else(|| (bytes.last() == Some(&b'\n')).then_some(()))?;
    Some(
        bytes
            .strip_suffix(b"\n")
            .unwrap_or(bytes)
            .split(|byte| *byte == b'\n'),
    )
}

fn successful_oid(output: &ProcessOutput) -> Option<Arc<str>> {
    if !output.status.success() || output.stdout.truncated {
        return None;
    }
    let oid = output.stdout.bytes.strip_suffix(b"\n")?;
    let oid = std::str::from_utf8(oid).ok()?;
    is_lowercase_hex(oid, 40).then(|| Arc::from(oid))
}

fn source_authority_snapshot(output: &ProcessOutput) -> Option<Arc<[u8]>> {
    if output.stdout.truncated {
        return None;
    }
    match output.status.code() {
        Some(0) => Some(Arc::from(output.stdout.bytes.as_slice())),
        Some(1) if output.stdout.bytes.is_empty() => Some(Arc::from([])),
        _ => None,
    }
}

fn contains_gitlink(bytes: &[u8]) -> bool {
    bytes
        .split(|byte| *byte == b'\n')
        .any(|mode| mode == b"160000")
}

fn git_command_description(arguments: &[OsString]) -> Arc<str> {
    let mut command = String::from("git");
    for argument in arguments {
        command.push(' ');
        for character in argument.to_string_lossy().chars() {
            if character == '`' {
                command.push_str("\\`");
            } else {
                command.extend(character.escape_default());
            }
        }
    }
    Arc::from(command)
}

fn reserved_git_environment(name: &OsStr) -> bool {
    let name = name.as_encoded_bytes();
    matches!(
        name,
        b"GIT_DIR"
            | b"GIT_WORK_TREE"
            | b"GIT_COMMON_DIR"
            | b"GIT_INDEX_FILE"
            | b"GIT_OBJECT_DIRECTORY"
            | b"GIT_ALTERNATE_OBJECT_DIRECTORIES"
            | b"GIT_SHALLOW_FILE"
            | b"GIT_QUARANTINE_PATH"
            | b"GIT_NAMESPACE"
            | b"GIT_CEILING_DIRECTORIES"
            | b"GIT_DISCOVERY_ACROSS_FILESYSTEM"
            | b"GIT_NO_LAZY_FETCH"
            | b"GIT_OPTIONAL_LOCKS"
            | b"GIT_NO_REPLACE_OBJECTS"
            | b"GIT_REPLACE_REF_BASE"
            | b"GIT_CONFIG"
            | b"GIT_CONFIG_COUNT"
            | b"GIT_CONFIG_PARAMETERS"
            | b"GIT_CONFIG_GLOBAL"
            | b"GIT_CONFIG_NOSYSTEM"
            | b"GIT_CONFIG_SYSTEM"
            | b"GIT_ASKPASS"
            | b"GIT_ASKPASS_REQUIRE"
            | b"GIT_TERMINAL_PROMPT"
            | b"GIT_SSH"
            | b"GIT_SSH_COMMAND"
            | b"SSH_ASKPASS"
            | b"SSH_ASKPASS_REQUIRE"
    ) || name.starts_with(b"GIT_CONFIG_KEY_")
        || name.starts_with(b"GIT_CONFIG_VALUE_")
        || name.starts_with(b"GIT_TRACE")
}

fn admission_process_failure(failure: ProcessFailure) -> GitWorkspaceAdmissionFailure {
    match failure {
        ProcessFailure::Cancelled => GitWorkspaceAdmissionFailure::Cancelled,
        ProcessFailure::ExecutionRootRebound => GitWorkspaceAdmissionFailure::ExecutionRootRebound,
        ProcessFailure::TimedOut { .. } => GitWorkspaceAdmissionFailure::GitTimedOut,
        ProcessFailure::Spawn
        | ProcessFailure::Wait
        | ProcessFailure::Io
        | ProcessFailure::Input
        | ProcessFailure::StreamClosed => GitWorkspaceAdmissionFailure::GitUnavailable,
    }
}

fn capture_process_failure(
    failure: ProcessFailure,
    unavailable: GitCaptureFailure,
) -> GitCaptureFailure {
    match failure {
        ProcessFailure::Cancelled => GitCaptureFailure::Cancelled,
        ProcessFailure::ExecutionRootRebound => GitCaptureFailure::ExecutionRootRebound,
        ProcessFailure::TimedOut { command, limit } => {
            GitCaptureFailure::CommandTimedOut(Box::new(GitCommandTimeout { command, limit }))
        }
        ProcessFailure::Spawn
        | ProcessFailure::Wait
        | ProcessFailure::Io
        | ProcessFailure::Input
        | ProcessFailure::StreamClosed => unavailable,
    }
}

fn map_staging_failure(failure: CaptureAttemptFailure) -> GitCaptureFailure {
    match failure {
        CaptureAttemptFailure::Cancelled => GitCaptureFailure::Cancelled,
        CaptureAttemptFailure::Capture(failure)
            if failure.kind() == CaptureFailureKind::CarrierProducerUnavailable =>
        {
            GitCaptureFailure::BundleGenerationFailed
        }
        CaptureAttemptFailure::Capture(failure) => GitCaptureFailure::Artifact(failure),
    }
}

enum ProcessInput<'a> {
    None,
    Bytes(&'a [u8]),
    File(File),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProcessFailure {
    Cancelled,
    ExecutionRootRebound,
    TimedOut { command: Arc<str>, limit: Duration },
    Spawn,
    Wait,
    Io,
    Input,
    StreamClosed,
}

struct BoundedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

struct ProcessOutput {
    status: ExitStatus,
    stdout: BoundedOutput,
    #[allow(
        dead_code,
        reason = "stderr is drained and retained for future private diagnostics"
    )]
    stderr: BoundedOutput,
}

fn execute_process(
    mut command: Command,
    command_description: Arc<str>,
    input: ProcessInput<'_>,
    maximum_stdout_bytes: usize,
    cancellation: &CaptureCancellation,
    timeout: Duration,
) -> Result<ProcessOutput, ProcessFailure> {
    match &input {
        ProcessInput::None => {
            command.stdin(Stdio::null());
        }
        ProcessInput::Bytes(_) => {
            command.stdin(Stdio::piped());
        }
        ProcessInput::File(file) => {
            command.stdin(
                file.try_clone()
                    .map(Stdio::from)
                    .map_err(|_| ProcessFailure::Input)?,
            );
        }
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = ManagedProcessGroup::spawn(&mut command).map_err(|_| ProcessFailure::Spawn)?;
    let child_process = child.child_mut();
    let stdout = child_process.stdout.take().ok_or(ProcessFailure::Io)?;
    let stderr = child_process.stderr.take().ok_or(ProcessFailure::Io)?;
    let stdin = match input {
        ProcessInput::Bytes(_) => child_process.stdin.take(),
        ProcessInput::None | ProcessInput::File(_) => None,
    };

    thread::scope(|scope| {
        let stdout = scope.spawn(move || drain_bounded(stdout, maximum_stdout_bytes));
        let stderr = scope.spawn(move || drain_bounded(stderr, MAXIMUM_SMALL_OUTPUT_BYTES));
        let input_writer = match (input, stdin) {
            (ProcessInput::Bytes(bytes), Some(mut stdin)) => {
                Some(scope.spawn(move || stdin.write_all(bytes).and_then(|()| stdin.flush())))
            }
            (ProcessInput::Bytes(_), None) => return Err(ProcessFailure::Input),
            (ProcessInput::None | ProcessInput::File(_), _) => None,
        };
        let status = wait_managed_child(
            &mut child,
            cancellation,
            timeout,
            command_description,
            || false,
            || {
                stdout.is_finished()
                    && stderr.is_finished()
                    && input_writer
                        .as_ref()
                        .is_none_or(thread::ScopedJoinHandle::is_finished)
            },
        );
        let stdout = stdout.join().map_err(|_| ProcessFailure::Io)?;
        let stderr = stderr.join().map_err(|_| ProcessFailure::Io)?;
        let input_result = input_writer
            .map(|writer| writer.join().map_err(|_| ProcessFailure::Input))
            .transpose()?
            .transpose()
            .map_err(|_| ProcessFailure::Input);
        let status = status?;
        input_result?;
        Ok(ProcessOutput {
            status,
            stdout: stdout.map_err(|_| ProcessFailure::Io)?,
            stderr: stderr.map_err(|_| ProcessFailure::Io)?,
        })
    })
}

enum PackStreamFailure {
    Destination(io::Error),
    Process(ProcessFailure),
    Capture(GitCaptureFailure),
}

fn stream_process_stdout(
    mut command: Command,
    command_description: Arc<str>,
    input: &[u8],
    destination: &mut CarrierDestination<'_>,
    cancellation: &CaptureCancellation,
    timeout: Duration,
) -> Result<(), PackStreamFailure> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = ManagedProcessGroup::spawn(&mut command)
        .map_err(|_| PackStreamFailure::Process(ProcessFailure::Spawn))?;
    let child_process = child.child_mut();
    let mut stdin = child_process
        .stdin
        .take()
        .ok_or(PackStreamFailure::Process(ProcessFailure::Input))?;
    let mut stdout = child_process
        .stdout
        .take()
        .ok_or(PackStreamFailure::Process(ProcessFailure::Io))?;
    let stderr = child_process
        .stderr
        .take()
        .ok_or(PackStreamFailure::Process(ProcessFailure::Io))?;

    thread::scope(|scope| {
        let input_writer = scope.spawn(move || stdin.write_all(input).and_then(|()| stdin.flush()));
        let stream = scope.spawn(move || {
            let mut buffer = [0_u8; COPY_BUFFER_BYTES];
            loop {
                let read = stdout.read(&mut buffer)?;
                if read == 0 {
                    return Ok::<(), io::Error>(());
                }
                destination.write_all(&buffer[..read])?;
            }
        });
        let stderr = scope.spawn(move || drain_bounded(stderr, MAXIMUM_SMALL_OUTPUT_BYTES));
        let status = wait_managed_child(
            &mut child,
            cancellation,
            timeout,
            command_description,
            || stream.is_finished(),
            || stream.is_finished() && input_writer.is_finished() && stderr.is_finished(),
        );
        let stream = stream
            .join()
            .map_err(|_| PackStreamFailure::Process(ProcessFailure::Io))?;
        let input = input_writer
            .join()
            .map_err(|_| PackStreamFailure::Process(ProcessFailure::Input))?;
        let stderr = stderr
            .join()
            .map_err(|_| PackStreamFailure::Process(ProcessFailure::Io))?;
        match status {
            Err(ProcessFailure::StreamClosed) => match stream {
                Err(destination_failure) => {
                    Err(PackStreamFailure::Destination(destination_failure))
                }
                Ok(()) => Err(PackStreamFailure::Process(ProcessFailure::StreamClosed)),
            },
            Err(failure) => Err(PackStreamFailure::Process(failure)),
            Ok(status) => {
                stream.map_err(PackStreamFailure::Destination)?;
                input.map_err(|_| PackStreamFailure::Process(ProcessFailure::Input))?;
                stderr.map_err(|_| PackStreamFailure::Process(ProcessFailure::Io))?;
                if status.success() {
                    Ok(())
                } else {
                    Err(PackStreamFailure::Process(ProcessFailure::Io))
                }
            }
        }
    })
}

fn wait_managed_child(
    child: &mut ManagedProcessGroup,
    cancellation: &CaptureCancellation,
    timeout: Duration,
    command: Arc<str>,
    stream_finished: impl Fn() -> bool,
    workers_finished: impl Fn() -> bool,
) -> Result<ExitStatus, ProcessFailure> {
    let started = crate::timing::monotonic_now();
    let mut status = None;
    let mut stream_closed_at = None;
    loop {
        if cancellation.is_cancelled() {
            child.terminate();
            return Err(ProcessFailure::Cancelled);
        }
        if status.is_none() {
            match child.try_wait() {
                Ok(Some(observed)) => status = Some(observed),
                Ok(None) => {}
                Err(_) => {
                    child.terminate();
                    return Err(ProcessFailure::Wait);
                }
            }
        }
        if let Some(status) = status
            && workers_finished()
        {
            child.terminate_process_group();
            return Ok(status);
        }
        if status.is_none() && stream_finished() {
            let closed_at = stream_closed_at.get_or_insert_with(crate::timing::monotonic_now);
            if crate::timing::elapsed(*closed_at) >= Duration::from_millis(100) {
                child.terminate();
                return Err(ProcessFailure::StreamClosed);
            }
        } else {
            stream_closed_at = None;
        }
        if crate::timing::elapsed(started) >= timeout {
            child.terminate();
            return Err(ProcessFailure::TimedOut {
                command,
                limit: timeout,
            });
        }
        crate::timing::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn drain_bounded(mut source: impl Read, maximum_bytes: usize) -> io::Result<BoundedOutput> {
    let mut bytes = Vec::with_capacity(maximum_bytes.min(COPY_BUFFER_BYTES));
    let mut truncated = false;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            return Ok(BoundedOutput { bytes, truncated });
        }
        let retained = maximum_bytes.saturating_sub(bytes.len()).min(read);
        bytes.extend_from_slice(&buffer[..retained]);
        truncated |= retained != read;
    }
}

struct CancellableHashingWriter {
    destination: File,
    cancellation: CaptureCancellation,
    digest: DigestContext,
    bytes: u64,
}

impl CancellableHashingWriter {
    fn new(destination: File, cancellation: &CaptureCancellation) -> Self {
        Self {
            destination,
            cancellation: cancellation.clone(),
            digest: DigestContext::new(&SHA256),
            bytes: 0,
        }
    }

    fn finish(mut self) -> Result<(File, ring::digest::Digest, u64), GitCaptureFailure> {
        self.flush().map_err(|failure| {
            if failure.kind() == io::ErrorKind::Interrupted {
                GitCaptureFailure::Cancelled
            } else {
                GitCaptureFailure::TemporaryStorageUnavailable
            }
        })?;
        Ok((self.destination, self.digest.finish(), self.bytes))
    }
}

impl Write for CancellableHashingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "capture cancelled",
            ));
        }
        let written = self.destination.write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(written).map_err(|_| io::Error::other("carrier size"))?)
            .ok_or_else(|| io::Error::other("carrier size"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "capture cancelled",
            ));
        }
        self.destination.flush()
    }
}

#[cfg(test)]
mod tests;
