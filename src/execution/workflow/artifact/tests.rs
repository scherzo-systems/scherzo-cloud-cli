use std::fs;
use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _, symlink};
use std::path::Path;
use std::sync::{Arc, Barrier};

use super::*;

struct CaptureFixture {
    _temporary: tempfile::TempDir,
    execution_root: PathBuf,
    store: ArtifactStaging,
}

impl CaptureFixture {
    fn new(maximum_file_bytes: u64) -> Self {
        Self::with_limits(64, maximum_file_bytes, u64::MAX)
    }

    fn with_limits(
        maximum_files: usize,
        maximum_file_bytes: u64,
        maximum_total_bytes: u64,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let staging_parent = temporary.path().join("staging");
        fs::create_dir(&execution_root).unwrap();
        fs::create_dir(&staging_parent).unwrap();
        let store = ArtifactStaging::create_for_execution(
            &execution_root,
            &staging_parent,
            NonZeroUsize::new(maximum_files).unwrap(),
            NonZeroU64::new(maximum_file_bytes).unwrap(),
            NonZeroU64::new(maximum_total_bytes).unwrap(),
        )
        .unwrap();
        Self {
            _temporary: temporary,
            execution_root,
            store,
        }
    }

    fn capture(&self, path: &str) -> Result<CapturedArtifact, CaptureFailure> {
        self.capture_set(&[("report", path)])
            .map(|captured| captured.into_values().next().unwrap())
    }

    fn capture_set(
        &self,
        declarations: &[(&str, &str)],
    ) -> Result<BTreeMap<String, CapturedArtifact>, CaptureFailure> {
        let declarations = declarations
            .iter()
            .map(|(identity, path)| {
                CaptureDeclaration::new(identity, Path::new(path), "application/octet-stream")
            })
            .collect::<Vec<_>>();
        self.store.capture_files(&declarations)
    }

    fn read(&self, artifact: &CapturedArtifact) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.store.copy_to(artifact.handle(), &mut bytes).unwrap();
        bytes
    }

    fn staging_path(&self) -> PathBuf {
        self.store.inner.staging_path.clone()
    }
}

#[test]
fn staging_cannot_be_created_inside_the_execution_root() {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    let exposed_parent = execution_root.join("staging");
    fs::create_dir_all(&exposed_parent).unwrap();

    let result = ArtifactStaging::create_for_execution(
        &execution_root,
        &exposed_parent,
        NonZeroUsize::new(64).unwrap(),
        NonZeroU64::new(64).unwrap(),
        NonZeroU64::new(4096).unwrap(),
    );

    assert!(matches!(
        result,
        Err(ArtifactStagingFailure::StagingParentExposed)
    ));
    assert!(fs::read_dir(exposed_parent).unwrap().next().is_none());
}

#[test]
fn captures_a_regular_file_with_path_free_metadata_and_independent_bytes() {
    let fixture = CaptureFixture::new(64);
    fs::create_dir(fixture.execution_root.join("nested")).unwrap();
    let source_path = fixture.execution_root.join("nested/report.bin");
    fs::write(&source_path, b"captured bytes").unwrap();

    let captured = fixture.capture("nested/report.bin").unwrap();

    assert_eq!(captured.output_identity(), "report");
    assert_eq!(captured.size(), 14);
    assert_eq!(captured.media_type(), "application/octet-stream");
    assert!(captured.handle().opaque_id().starts_with("art_"));
    assert!(!format!("{captured:?}").contains(fixture.staging_path().to_str().unwrap()));
    assert!(!captured.handle().opaque_id().contains('/'));
    assert_eq!(fixture.read(&captured), b"captured bytes");

    let source_metadata = fs::metadata(&source_path).unwrap();
    let staged_file = fixture.store.open_artifact(captured.handle()).unwrap();
    let staged_metadata = staged_file.metadata().unwrap();
    assert_ne!(
        (source_metadata.dev(), source_metadata.ino()),
        (staged_metadata.dev(), staged_metadata.ino())
    );
    assert_eq!(staged_metadata.permissions().mode() & 0o222, 0);
    assert_eq!(
        fs::metadata(fixture.staging_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o077,
        0
    );

    fs::write(&source_path, b"changed bytes!").unwrap();
    fs::remove_file(&source_path).unwrap();
    assert_eq!(fixture.read(&captured), b"captured bytes");
}

#[test]
fn rejects_unsafe_paths_symlinks_and_nonregular_sources_with_typed_failures() {
    let fixture = CaptureFixture::new(64);
    fs::create_dir(fixture.execution_root.join("directory")).unwrap();
    fs::write(fixture.execution_root.join("target.bin"), b"target").unwrap();
    symlink("target.bin", fixture.execution_root.join("final-link")).unwrap();
    symlink("directory", fixture.execution_root.join("directory-link")).unwrap();
    let fifo_path = fixture.execution_root.join("fifo");
    nix::unistd::mkfifo(
        &fifo_path,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();

    for (path, expected) in [
        ("/absolute.bin", CaptureFailureKind::AbsolutePath),
        ("../escape.bin", CaptureFailureKind::LexicalEscape),
        (".", CaptureFailureKind::EmptyPath),
        ("missing.bin", CaptureFailureKind::Missing),
        ("final-link", CaptureFailureKind::SymbolicLink),
        ("directory-link/file.bin", CaptureFailureKind::SymbolicLink),
        ("directory", CaptureFailureKind::NotRegularFile),
        ("target.bin/child", CaptureFailureKind::NotDirectory),
        ("fifo", CaptureFailureKind::NotRegularFile),
    ] {
        let failure = fixture.capture(path).unwrap_err();
        assert_eq!(failure.output_identity(), "report");
        assert_eq!(failure.kind(), expected, "unexpected failure for {path}");
    }
    assert!(
        fs::read_dir(fixture.staging_path())
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn captures_through_a_searchable_but_unreadable_directory() {
    let fixture = CaptureFixture::new(64);
    let directory = fixture.execution_root.join("search-only");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("report.bin"), b"captured bytes").unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o111)).unwrap();

    let result = fixture.capture("search-only/report.bin");

    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let captured = result.expect("a readable file behind a searchable directory is valid");
    assert_eq!(fixture.read(&captured), b"captured bytes");
}

#[test]
fn the_exact_limit_succeeds_and_overflow_removes_the_partial_destination() {
    let fixture = CaptureFixture::new(5);
    fs::write(fixture.execution_root.join("exact.bin"), b"12345").unwrap();
    fs::write(fixture.execution_root.join("large.bin"), b"123456").unwrap();

    let exact = fixture.capture("exact.bin").unwrap();
    assert_eq!(fixture.read(&exact), b"12345");
    fixture.store.discard(&exact);
    let failure = fixture.capture("large.bin").unwrap_err();

    assert_eq!(failure.kind(), CaptureFailureKind::FileSizeLimitExceeded);
    assert!(
        fs::read_dir(fixture.staging_path())
            .unwrap()
            .next()
            .is_none()
    );
}

struct GatedCopier {
    source_opened: Arc<Barrier>,
    resume_copy: Arc<Barrier>,
}

impl StreamCopier for GatedCopier {
    fn copy(&mut self, request: CopyRequest<'_>) -> Result<u64, CaptureAttemptFailure> {
        self.source_opened.wait();
        self.resume_copy.wait();
        copy_bounded(
            request.source,
            request.destination,
            request.maximum_bytes,
            request.output_identity,
            request.cancellation,
        )
    }
}

#[test]
fn failed_sets_restore_file_reservations_and_preserve_prior_captures() {
    let fixture = CaptureFixture::with_limits(3, 4, 6);
    fs::write(fixture.execution_root.join("prior.bin"), b"12").unwrap();
    fs::write(fixture.execution_root.join("candidate.bin"), b"34").unwrap();
    fs::write(fixture.execution_root.join("replacement.bin"), b"56").unwrap();

    let prior = fixture.capture("prior.bin").unwrap();
    let failure = fixture
        .capture_set(&[("candidate", "candidate.bin"), ("missing", "missing.bin")])
        .unwrap_err();

    assert_eq!(failure.output_identity(), "missing");
    assert_eq!(failure.kind(), CaptureFailureKind::Missing);
    assert_eq!(fixture.store.budget_usage(), (1, 2));
    assert_eq!(fixture.store.staged_artifact_count(), 1);
    assert_eq!(fixture.read(&prior), b"12");

    let replacement = fixture
        .capture_set(&[
            ("candidate", "candidate.bin"),
            ("replacement", "replacement.bin"),
        ])
        .unwrap();
    assert_eq!(fixture.store.budget_usage(), (3, 6));
    let count_failure = fixture.capture("candidate.bin").unwrap_err();
    assert_eq!(
        count_failure.kind(),
        CaptureFailureKind::FileCountLimitExceeded
    );
    assert_eq!(fixture.store.staged_artifact_count(), 3);

    drop(replacement);
    assert_eq!(fixture.store.budget_usage(), (1, 2));
    assert_eq!(fixture.read(&prior), b"12");
}

#[test]
fn failed_rollback_quarantines_the_store_and_release_retries_cleanup() {
    let fixture = CaptureFixture::with_limits(2, 4, 8);
    fs::write(fixture.execution_root.join("candidate.bin"), b"12").unwrap();
    fixture.store.block_artifact_unlinks();

    let failure = fixture
        .capture_set(&[("candidate", "candidate.bin"), ("missing", "missing.bin")])
        .unwrap_err();

    assert_eq!(failure.output_identity(), "missing");
    assert_eq!(failure.kind(), CaptureFailureKind::StagingUnavailable);
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 1);
    assert_eq!(
        fixture.capture("candidate.bin").unwrap_err().kind(),
        CaptureFailureKind::StagingUnavailable
    );

    let staging_path = fixture.staging_path();
    fixture.store.release().unwrap();
    assert!(!staging_path.exists());
}

#[test]
fn total_byte_overflow_rolls_back_the_set_and_the_exact_budget_is_reusable() {
    let fixture = CaptureFixture::with_limits(3, 4, 5);
    fs::write(fixture.execution_root.join("two.bin"), b"12").unwrap();
    fs::write(fixture.execution_root.join("three.bin"), b"345").unwrap();
    fs::write(fixture.execution_root.join("four.bin"), b"3456").unwrap();
    fs::write(fixture.execution_root.join("one.bin"), b"7").unwrap();

    let failure = fixture
        .capture_set(&[("two", "two.bin"), ("four", "four.bin")])
        .unwrap_err();

    assert_eq!(failure.output_identity(), "four");
    assert_eq!(failure.kind(), CaptureFailureKind::TotalSizeLimitExceeded);
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);

    let exact = fixture
        .capture_set(&[("two", "two.bin"), ("three", "three.bin")])
        .unwrap();
    assert_eq!(fixture.store.budget_usage(), (2, 5));
    let overflow = fixture.capture_set(&[("one", "one.bin")]).unwrap_err();
    assert_eq!(overflow.kind(), CaptureFailureKind::TotalSizeLimitExceeded);
    assert_eq!(fixture.store.staged_artifact_count(), 2);
    assert_eq!(exact.keys().cloned().collect::<Vec<_>>(), ["three", "two"]);
}

#[test]
fn streaming_growth_is_bounded_and_its_partial_destination_is_removed() {
    let fixture = CaptureFixture::new(5);
    let declared_path = fixture.execution_root.join("report.bin");
    fs::write(&declared_path, b"12345").unwrap();
    let source_opened = Arc::new(Barrier::new(2));
    let resume_copy = Arc::new(Barrier::new(2));
    let store = fixture.store.clone();
    let capture_source_opened = Arc::clone(&source_opened);
    let capture_resume_copy = Arc::clone(&resume_copy);

    let capture = std::thread::spawn(move || {
        store.capture_with_copier(
            Arc::from("report"),
            Path::new("report.bin"),
            Arc::from("application/octet-stream"),
            &mut GatedCopier {
                source_opened: capture_source_opened,
                resume_copy: capture_resume_copy,
            },
        )
    });

    source_opened.wait();
    fs::write(&declared_path, b"123456").unwrap();
    resume_copy.wait();
    let failure = capture.join().unwrap().unwrap_err();

    assert_eq!(failure.kind(), CaptureFailureKind::FileSizeLimitExceeded);
    assert!(
        fs::read_dir(fixture.staging_path())
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn an_open_source_cannot_be_replaced_by_a_symlink_before_copy() {
    let fixture = CaptureFixture::new(64);
    let declared_path = fixture.execution_root.join("report.bin");
    let moved_path = fixture.execution_root.join("opened-report.bin");
    let outside_path = fixture._temporary.path().join("outside.bin");
    fs::write(&declared_path, b"opened source").unwrap();
    fs::write(&outside_path, b"substituted source").unwrap();
    let source_opened = Arc::new(Barrier::new(2));
    let resume_copy = Arc::new(Barrier::new(2));
    let store = fixture.store.clone();
    let capture_source_opened = Arc::clone(&source_opened);
    let capture_resume_copy = Arc::clone(&resume_copy);

    let capture = std::thread::spawn(move || {
        store.capture_with_copier(
            Arc::from("report"),
            Path::new("report.bin"),
            Arc::from("application/octet-stream"),
            &mut GatedCopier {
                source_opened: capture_source_opened,
                resume_copy: capture_resume_copy,
            },
        )
    });

    source_opened.wait();
    fs::rename(&declared_path, &moved_path).unwrap();
    symlink(&outside_path, &declared_path).unwrap();
    resume_copy.wait();
    let captured = capture.join().unwrap().unwrap();

    assert_eq!(fixture.read(&captured), b"opened source");
}

#[test]
fn adapter_release_removes_artifacts_after_the_staging_parent_moves() {
    let fixture = CaptureFixture::new(64);
    fs::write(fixture.execution_root.join("report.bin"), b"content").unwrap();
    let _captured = fixture.capture("report.bin").unwrap();
    let staging_path = fixture.staging_path();
    let staging_parent = staging_path.parent().unwrap();
    let staging_name = staging_path.file_name().unwrap();
    let relocated_parent = fixture._temporary.path().join("relocated-staging");
    fs::rename(staging_parent, &relocated_parent).unwrap();
    let relocated_staging = relocated_parent.join(staging_name);
    assert!(relocated_staging.exists());

    fixture.store.release().unwrap();

    assert!(
        !relocated_staging.exists(),
        "release left the opened staging root and captured bytes behind"
    );
}

#[test]
fn dropping_the_last_artifact_handle_removes_an_unreachable_capture() {
    let fixture = CaptureFixture::new(64);
    fs::write(fixture.execution_root.join("report.bin"), b"content").unwrap();
    let captured = fixture.capture("report.bin").unwrap();
    let retained_handle = captured.handle().clone();
    let staged_path = fixture.staging_path().join(retained_handle.opaque_id());
    assert!(staged_path.exists());

    drop(captured);
    assert!(staged_path.exists());
    drop(retained_handle);

    assert!(!staged_path.exists());
}

#[test]
fn adapter_release_removes_artifacts_and_invalidates_handles() {
    let fixture = CaptureFixture::new(64);
    fs::write(fixture.execution_root.join("report.bin"), b"content").unwrap();
    let captured = fixture.capture("report.bin").unwrap();
    let staging_path = fixture.staging_path();
    assert!(staging_path.exists());

    fixture.store.release().unwrap();

    assert!(!staging_path.exists());
    let mut destination = Vec::new();
    assert_eq!(
        fixture.store.copy_to(captured.handle(), &mut destination),
        Err(ArtifactReadFailure::UnknownHandle)
    );
    assert_eq!(
        fixture.capture("report.bin").unwrap_err().kind(),
        CaptureFailureKind::StagingUnavailable
    );
}

#[test]
fn a_destination_write_error_is_typed_without_exposing_a_path() {
    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let fixture = CaptureFixture::new(64);
    fs::write(fixture.execution_root.join("report.bin"), b"content").unwrap();
    let captured = fixture.capture("report.bin").unwrap();

    assert_eq!(
        fixture.store.copy_to(captured.handle(), &mut FailingWriter),
        Err(ArtifactReadFailure::DestinationWrite)
    );
}
