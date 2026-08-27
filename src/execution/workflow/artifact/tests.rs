use std::fs;
use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _, symlink};
use std::path::Path;
use std::sync::{Arc, Barrier};

use super::*;
use crate::execution::workflow::canonical_json;
use crate::execution::workflow::result_validation::RetainedJsonSchema;
use crate::execution::workflow::strict_json;
use crate::execution::workflow::value::{CapturedJson, CapturedText, SemanticCarrierError};

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
        Self::with_all_limits(
            maximum_files,
            maximum_file_bytes,
            maximum_total_bytes,
            64,
            64 * 1024 * 1024,
            256 * 1024 * 1024,
        )
    }

    fn with_all_limits(
        maximum_files: usize,
        maximum_file_bytes: u64,
        maximum_total_bytes: u64,
        maximum_git_carriers: usize,
        maximum_git_carrier_bytes: u64,
        maximum_total_git_carrier_bytes: u64,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let staging_parent = temporary.path().join("staging");
        fs::create_dir(&execution_root).unwrap();
        fs::create_dir(&staging_parent).unwrap();
        let store = ArtifactStaging::create_for_execution(
            &execution_root,
            &staging_parent,
            CarrierLimits {
                maximum_count: NonZeroUsize::new(maximum_files).unwrap(),
                maximum_bytes: NonZeroU64::new(maximum_file_bytes).unwrap(),
                maximum_total_bytes: NonZeroU64::new(maximum_total_bytes).unwrap(),
            },
            CarrierLimits {
                maximum_count: NonZeroUsize::new(maximum_git_carriers).unwrap(),
                maximum_bytes: NonZeroU64::new(maximum_git_carrier_bytes).unwrap(),
                maximum_total_bytes: NonZeroU64::new(maximum_total_git_carrier_bytes).unwrap(),
            },
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
                CaptureDeclaration::file(identity, Path::new(path), "application/octet-stream")
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
        CarrierLimits {
            maximum_count: NonZeroUsize::new(64).unwrap(),
            maximum_bytes: NonZeroU64::new(64).unwrap(),
            maximum_total_bytes: NonZeroU64::new(4096).unwrap(),
        },
        CarrierLimits {
            maximum_count: NonZeroUsize::new(64).unwrap(),
            maximum_bytes: NonZeroU64::new(64).unwrap(),
            maximum_total_bytes: NonZeroU64::new(4096).unwrap(),
        },
    );

    assert!(matches!(
        result,
        Err(ArtifactStagingFailure::StagingParentExposed)
    ));
    assert!(fs::read_dir(exposed_parent).unwrap().next().is_none());
}

#[test]
fn capture_remains_bound_to_the_admitted_directory_after_path_rebinding() {
    let fixture = CaptureFixture::new(64);
    let moved_root = fixture._temporary.path().join("moved-execution");
    fs::write(fixture.execution_root.join("report.bin"), b"admitted bytes").unwrap();
    fs::rename(&fixture.execution_root, &moved_root).unwrap();
    fs::create_dir(&fixture.execution_root).unwrap();
    fs::write(
        fixture.execution_root.join("report.bin"),
        b"replacement bytes",
    )
    .unwrap();

    let captured = fixture.capture("report.bin").unwrap();

    assert_eq!(fixture.read(&captured), b"admitted bytes");
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
    assert_eq!(
        captured.sha256(),
        lowercase_hex(ring::digest::digest(&SHA256, b"captured bytes").as_ref())
    );
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
fn retained_identity_guard_is_a_cleanup_tracked_hard_link() {
    let fixture = CaptureFixture::new(64);
    fs::write(fixture.execution_root.join("report.bin"), b"captured bytes").unwrap();

    let captured = fixture.capture("report.bin").unwrap();
    let staged_path = fixture.staging_path().join(captured.handle().opaque_id());
    let guard_identity = fixture
        .store
        .inner
        .identity_guards
        .lock()
        .unwrap()
        .get(captured.handle().opaque_id())
        .unwrap()
        .clone();
    let guard_path = fixture.staging_path().join(guard_identity.as_ref());
    let staged_metadata = fs::metadata(&staged_path).unwrap();
    let guard_metadata = fs::metadata(&guard_path).unwrap();

    assert_eq!(
        (staged_metadata.dev(), staged_metadata.ino()),
        (guard_metadata.dev(), guard_metadata.ino())
    );
    assert_eq!(staged_metadata.nlink(), 2);

    drop(captured);
    assert!(
        fs::read_dir(fixture.staging_path())
            .unwrap()
            .next()
            .is_none()
    );
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
    fn copy(&mut self, request: CopyRequest<'_, '_>) -> Result<u64, CaptureAttemptFailure> {
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

struct BytesCarrierProducer(Vec<u8>);

impl CarrierProducer for BytesCarrierProducer {
    fn stream_to(&mut self, destination: &mut CarrierDestination<'_>) -> io::Result<()> {
        destination.write_all(&self.0)
    }
}

fn git_metadata(base: u8, head: u8, tree: u8) -> GitBranchMetadata {
    GitBranchMetadata::new(
        Arc::from(format!("{base:040x}")),
        Arc::from(format!("{head:040x}")),
        Arc::from(format!("{tree:040x}")),
    )
}

fn failed_capture(result: Result<CaptureCandidateSet, CaptureAttemptFailure>) -> CaptureFailure {
    match result {
        Err(CaptureAttemptFailure::Capture(failure)) => failure,
        Err(CaptureAttemptFailure::Cancelled) => panic!("capture was unexpectedly cancelled"),
        Ok(_) => panic!("capture unexpectedly succeeded"),
    }
}

fn retained_json_schema() -> RetainedJsonSchema {
    let document = Arc::new(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object"
    }));
    RetainedJsonSchema::compile(
        Arc::from(
            br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#
                .as_slice(),
        ),
        document,
    )
    .unwrap()
}

#[test]
fn path_capture_profiles_are_closed_over_semantic_policy() {
    let schema = retained_json_schema();
    let profiles = [
        PathCaptureProfile::Text,
        PathCaptureProfile::Json { schema: &schema },
        PathCaptureProfile::File {
            media_type: "application/octet-stream",
        },
    ];

    assert_eq!(profiles[0].value_type(), WorkflowValueType::Text);
    assert_eq!(profiles[0].media_type(), "text/plain; charset=utf-8");
    assert_eq!(profiles[1].value_type(), WorkflowValueType::Json);
    assert_eq!(profiles[1].media_type(), "application/json");
    assert_eq!(profiles[1].json_schema(), Some(&schema));
    assert_eq!(profiles[2].value_type(), WorkflowValueType::File);
    assert_eq!(profiles[2].media_type(), "application/octet-stream");
    assert_eq!(profiles[2].json_schema(), None);
}

#[test]
fn semantic_outputs_path_capture_decodes_values_and_classifies_content_failures() {
    let fixture = CaptureFixture::with_limits(3, 256, 768);
    fs::write(
        fixture.execution_root.join("summary.txt"),
        b"\xef\xbb\xbfline one\r\nline two\n",
    )
    .unwrap();
    fs::write(
        fixture.execution_root.join("result.json"),
        br#"{ "z": 2, "a": 1 }"#,
    )
    .unwrap();
    fs::write(fixture.execution_root.join("report.bin"), b"report").unwrap();
    let schema = retained_json_schema();
    let declarations = [
        CaptureDeclaration::text("summary", Path::new("summary.txt")),
        CaptureDeclaration::json("result", Path::new("result.json"), &schema),
        CaptureDeclaration::file(
            "report",
            Path::new("report.bin"),
            "application/octet-stream",
        ),
    ];

    let outputs = fixture
        .store
        .capture_file_candidates(&declarations, &CaptureCancellation::default())
        .unwrap()
        .commit();

    assert!(matches!(
        &outputs["summary"],
        CapturedValue::Text(value)
            if value.carrier() == b"\xef\xbb\xbfline one\r\nline two\n"
    ));
    assert!(matches!(
        &outputs["result"],
        CapturedValue::Json(value) if value.carrier() == br#"{"a":1,"z":2}"#
    ));
    assert!(matches!(&outputs["report"], CapturedValue::File(_)));
    drop(outputs);
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);

    let text_fixture = CaptureFixture::with_limits(1, 64, 64);
    fs::write(text_fixture.execution_root.join("value"), b"\xff").unwrap();
    let text_failure = failed_capture(text_fixture.store.capture_file_candidates(
        &[CaptureDeclaration::text("text", Path::new("value"))],
        &CaptureCancellation::default(),
    ));
    assert_eq!(text_failure.output_identity(), "text");
    assert_eq!(text_failure.kind(), CaptureFailureKind::InvalidTextEncoding);
    assert_eq!(text_fixture.store.reservation_usage(), (0, 0));
    assert_eq!(text_fixture.store.staged_artifact_count(), 0);

    for (bytes, expected) in [
        (b"{".as_slice(), CaptureFailureKind::InvalidJson),
        (
            br#"{"value":1,"value":2}"#.as_slice(),
            CaptureFailureKind::DuplicateJsonMember,
        ),
        (b"[]".as_slice(), CaptureFailureKind::JsonSchemaMismatch),
    ] {
        let json_fixture = CaptureFixture::with_limits(1, 64, 64);
        fs::write(json_fixture.execution_root.join("value"), bytes).unwrap();
        let failure = failed_capture(json_fixture.store.capture_file_candidates(
            &[CaptureDeclaration::json(
                "json",
                Path::new("value"),
                &schema,
            )],
            &CaptureCancellation::default(),
        ));
        assert_eq!(failure.output_identity(), "json");
        assert_eq!(failure.kind(), expected);
        assert_eq!(json_fixture.store.reservation_usage(), (0, 0));
        assert_eq!(json_fixture.store.staged_artifact_count(), 0);
    }
}

#[test]
fn semantic_outputs_text_source_equivalence() {
    let fixture = CaptureFixture::with_limits(2, 128, 256);
    let text_source = fixture.execution_root.join("summary.txt");
    let json_source = fixture.execution_root.join("result.json");
    let text_bytes = b"\xef\xbb\xbfline one\r\nline two\n";
    let json_source_bytes = br#"{ "z": 2, "a": 1 }"#;
    fs::write(&text_source, text_bytes).unwrap();
    fs::write(&json_source, json_source_bytes).unwrap();

    let text_backing = fixture.capture("summary.txt").unwrap();
    let retained_text_bytes = Arc::<[u8]>::from(fixture.read(&text_backing));
    let path_text =
        CapturedText::from_bounded_carrier(retained_text_bytes, text_backing.into_capture_lease())
            .unwrap();

    let json_backing = fixture.capture("result.json").unwrap();
    let retained_json_source = fixture.read(&json_backing);
    let json_value = Arc::new(strict_json::from_slice(&retained_json_source).unwrap());
    let canonical = canonical_json::to_bounded_bytes(&json_value, 128).unwrap();
    let schema = retained_json_schema();
    let path_json = CapturedJson::from_bounded_carrier(
        Arc::clone(&json_value),
        Arc::clone(&canonical),
        schema.clone(),
        json_backing.into_capture_lease(),
    )
    .unwrap();

    assert_eq!(path_text.value_type(), WorkflowValueType::Text);
    assert_eq!(path_text.carrier(), text_bytes);
    assert_eq!(path_text.as_str().as_bytes(), text_bytes);
    assert_eq!(path_json.value_type(), WorkflowValueType::Json);
    assert_eq!(path_json.value(), json_value.as_ref());
    assert_eq!(path_json.carrier(), br#"{"a":1,"z":2}"#);
    assert_eq!(path_json.schema(), &schema);
    assert_eq!(
        fixture.store.budget_usage(),
        (
            2,
            u64::try_from(text_bytes.len() + json_source_bytes.len()).unwrap()
        )
    );

    let native_text = CapturedText::new(Arc::from(path_text.as_str()));
    let native_json = CapturedJson::from_validated(json_value, canonical, schema);
    assert_eq!(path_text, native_text);
    assert_eq!(path_json, native_json);
    for (value, expected_type) in [
        (
            CapturedValue::Text(path_text.clone()),
            WorkflowValueType::Text,
        ),
        (
            CapturedValue::Json(path_json.clone()),
            WorkflowValueType::Json,
        ),
    ] {
        assert_eq!(value.value_type(), expected_type);
        let debug = format!("{value:?}");
        assert!(!debug.contains("art_"));
        assert!(!debug.contains(fixture.staging_path().to_str().unwrap()));
    }

    fs::write(&text_source, b"replacement text").unwrap();
    fs::write(&json_source, br#"{"replacement":true}"#).unwrap();
    assert_eq!(path_text.carrier(), text_bytes);
    assert_eq!(path_json.carrier(), br#"{"a":1,"z":2}"#);

    let last_text_owner = path_text.clone();
    drop(path_text);
    assert_eq!(fixture.store.budget_usage().0, 2);
    drop(last_text_owner);
    assert_eq!(fixture.store.budget_usage().0, 1);
    drop(path_json);
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);
}

#[test]
fn semantic_outputs_json_source_equivalence() {
    let fixture = CaptureFixture::with_limits(1, 128, 128);
    fs::write(
        fixture.execution_root.join("result.json"),
        br#"{ "z": 2, "a": 1 }"#,
    )
    .unwrap();
    let schema = retained_json_schema();
    let outputs = fixture
        .store
        .capture_file_candidates(
            &[CaptureDeclaration::json(
                "result",
                Path::new("result.json"),
                &schema,
            )],
            &CaptureCancellation::default(),
        )
        .unwrap()
        .commit();
    let CapturedValue::Json(path_value) = &outputs["result"] else {
        panic!("path JSON lost its semantic kind");
    };
    let native_value = CapturedJson::from_validated(
        Arc::new(serde_json::json!({"z": 2, "a": 1})),
        Arc::from(br#"{"a":1,"z":2}"#.as_slice()),
        schema,
    );

    assert_eq!(path_value, &native_value);
    assert_eq!(path_value.carrier(), br#"{"a":1,"z":2}"#);
    drop(outputs);
    assert_eq!(fixture.store.budget_usage(), (0, 0));
}

#[test]
fn semantic_carrier_construction_failures_release_the_exact_capture_debit() {
    let fixture = CaptureFixture::with_limits(1, 64, 64);
    fs::write(fixture.execution_root.join("value.bin"), b"\xff").unwrap();
    let invalid_text = fixture.capture("value.bin").unwrap();
    assert_eq!(fixture.store.budget_usage(), (1, 1));

    let failure = CapturedText::from_bounded_carrier(
        Arc::from(b"\xff".as_slice()),
        invalid_text.into_capture_lease(),
    )
    .unwrap_err();

    assert_eq!(failure, SemanticCarrierError::InvalidTextEncoding);
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);

    fs::write(
        fixture.execution_root.join("value.bin"),
        br#"{"z":2,"a":1}"#,
    )
    .unwrap();
    let invalid_json = fixture.capture("value.bin").unwrap();
    assert_eq!(fixture.store.budget_usage(), (1, 13));
    let value = Arc::new(serde_json::json!({"z": 2, "a": 1}));
    let failure = CapturedJson::from_bounded_carrier(
        value,
        Arc::from(br#"{"z":2,"a":1}"#.as_slice()),
        retained_json_schema(),
        invalid_json.into_capture_lease(),
    )
    .unwrap_err();

    assert_eq!(failure, SemanticCarrierError::InvalidCanonicalJson);
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);
}

#[test]
fn successful_cleanup_releases_semantic_budget_after_final_drop_cleanup_failure() {
    let fixture = CaptureFixture::with_limits(1, 64, 64);
    fs::write(fixture.execution_root.join("summary.txt"), b"committed").unwrap();
    let backing = fixture.capture("summary.txt").unwrap();
    let bytes = Arc::<[u8]>::from(fixture.read(&backing));
    let text = CapturedText::from_bounded_carrier(bytes, backing.into_capture_lease()).unwrap();
    assert_eq!(fixture.store.budget_usage(), (1, 9));

    fixture.store.block_artifact_unlinks();
    drop(text);
    assert_eq!(fixture.store.staged_artifact_count(), 1);
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    let failure = fixture.capture("summary.txt").unwrap_err();
    assert_eq!(failure.kind(), CaptureFailureKind::StagingUnavailable);

    fixture.store.release().unwrap();

    assert_eq!(fixture.store.staged_artifact_count(), 0);
    assert_eq!(
        fixture.store.budget_usage(),
        (0, 0),
        "successful centralized cleanup must release the semantic lease debit"
    );
}

#[test]
fn cancelling_a_provisional_semantic_value_releases_its_reservation_and_lease() {
    let fixture = CaptureFixture::with_limits(1, 64, 64);
    fs::write(fixture.execution_root.join("summary.txt"), b"candidate").unwrap();
    let cancellation = CaptureCancellation::default();
    let declarations = [CaptureDeclaration::file(
        "summary",
        Path::new("summary.txt"),
        "text/plain; charset=utf-8",
    )];
    let mut candidates = fixture
        .store
        .capture_file_candidates(&declarations, &cancellation)
        .unwrap();
    let file = candidates
        .outputs
        .remove("summary")
        .unwrap()
        .into_file()
        .unwrap();
    let bytes = Arc::<[u8]>::from(fixture.read(&file));
    let text = CapturedText::from_bounded_carrier(bytes, file.into_capture_lease()).unwrap();
    candidates
        .outputs
        .insert("summary".to_owned(), CapturedValue::Text(text));
    assert_eq!(fixture.store.reservation_usage(), (1, 9));

    cancellation.cancel();
    assert!(matches!(
        cancellation.check(),
        Err(CaptureAttemptFailure::Cancelled)
    ));
    candidates.abort();

    assert_eq!(fixture.store.reservation_usage(), (0, 0));
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);
}

#[test]
fn mixed_candidates_commit_and_release_independent_typed_carriers() {
    let fixture = CaptureFixture::with_all_limits(2, 4, 6, 2, 5, 7);
    fs::write(fixture.execution_root.join("report.bin"), b"file").unwrap();
    let mut producer = BytesCarrierProducer(b"git!!".to_vec());
    let mut declarations = [
        CaptureCandidateDeclaration::File(CaptureDeclaration::file(
            "report",
            Path::new("report.bin"),
            "application/octet-stream",
        )),
        CaptureCandidateDeclaration::GitBranch(GitBranchCaptureDeclaration::new(
            "changes",
            git_metadata(1, 2, 3),
            Some(&mut producer),
        )),
    ];

    let candidates = fixture
        .store
        .capture_candidates(&mut declarations, &CaptureCancellation::default())
        .unwrap();

    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.git_budget_usage(), (0, 0));
    assert_eq!(fixture.store.reservation_usage(), (1, 4));
    assert_eq!(fixture.store.git_reservation_usage(), (1, 5));
    let mut outputs = candidates.commit();
    assert_eq!(fixture.store.budget_usage(), (1, 4));
    assert_eq!(fixture.store.git_budget_usage(), (1, 5));

    let file = outputs["report"].as_file().unwrap();
    assert_eq!(file.carrier().budget_class(), CarrierBudgetClass::File);
    let CapturedValue::GitBranch(branch) = &outputs["changes"] else {
        panic!("Git output lost its semantic type");
    };
    assert_eq!(branch.output_identity(), "changes");
    assert_eq!(branch.metadata().artifact_version(), 1);
    assert_eq!(branch.metadata().object_format().as_str(), "sha1");
    assert_eq!(branch.metadata().base_oid(), format!("{:040x}", 1));
    assert_eq!(branch.metadata().head_oid(), format!("{:040x}", 2));
    assert_eq!(branch.metadata().tree_oid(), format!("{:040x}", 3));
    let carrier = branch.carrier().unwrap();
    assert_eq!(carrier.media_type(), "application/vnd.git.bundle");
    assert_eq!(carrier.identity(), carrier.handle().opaque_id());
    assert_eq!(carrier.budget_class(), CarrierBudgetClass::Git);
    let mut bytes = Vec::new();
    fixture.store.copy_to(carrier.handle(), &mut bytes).unwrap();
    assert_eq!(bytes, b"git!!");

    let exported = fixture._temporary.path().join("exported");
    fs::create_dir(&exported).unwrap();
    let exported_handle = open_directory(&exported).unwrap();
    assert_eq!(
        fixture.store.expose_carrier(
            file.carrier(),
            "wrong-output",
            &exported_handle,
            OsStr::new("wrong"),
        ),
        Err(ArtifactExposeFailure::UnknownHandle)
    );
    assert_eq!(
        fixture.store.expose_carrier(
            file.carrier(),
            "report",
            &exported_handle,
            OsStr::new("../escape"),
        ),
        Err(ArtifactExposeFailure::InvalidDestination)
    );
    assert!(!fixture._temporary.path().join("escape").exists());
    for (name, expected_identity, staged) in [
        ("file", "report", file.carrier()),
        ("git", "changes", carrier.staged()),
    ] {
        let source = fixture.store.open_artifact(staged.handle()).unwrap();
        let source_metadata = source.metadata().unwrap();
        fixture
            .store
            .expose_carrier(
                staged,
                expected_identity,
                &exported_handle,
                OsStr::new(name),
            )
            .unwrap();
        let exposed_metadata = fs::metadata(exported.join(name)).unwrap();
        assert_eq!(
            (exposed_metadata.dev(), exposed_metadata.ino()),
            (source_metadata.dev(), source_metadata.ino()),
            "{name} carrier bytes were copied instead of linked"
        );
        assert_eq!(exposed_metadata.nlink(), 3);
    }

    drop(outputs.remove("report"));
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.git_budget_usage(), (1, 5));
    drop(outputs);
    assert_eq!(fixture.store.git_budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);
}

#[test]
fn aborting_a_mixed_candidate_releases_both_reservations_and_carriers() {
    let fixture = CaptureFixture::with_all_limits(2, 8, 16, 2, 8, 16);
    fs::write(fixture.execution_root.join("report.bin"), b"file").unwrap();
    let mut producer = BytesCarrierProducer(b"git".to_vec());
    let mut declarations = [
        CaptureCandidateDeclaration::File(CaptureDeclaration::file(
            "report",
            Path::new("report.bin"),
            "application/octet-stream",
        )),
        CaptureCandidateDeclaration::GitBranch(GitBranchCaptureDeclaration::new(
            "changes",
            git_metadata(1, 2, 3),
            Some(&mut producer),
        )),
    ];
    let candidates = fixture
        .store
        .capture_candidates(&mut declarations, &CaptureCancellation::default())
        .unwrap();
    assert_eq!(fixture.store.reservation_usage(), (1, 4));
    assert_eq!(fixture.store.git_reservation_usage(), (1, 3));

    candidates.abort();

    assert_eq!(fixture.store.reservation_usage(), (0, 0));
    assert_eq!(fixture.store.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.git_budget_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);
}

#[test]
fn zero_delta_git_value_has_no_carrier_or_budget_charge() {
    let fixture = CaptureFixture::with_all_limits(1, 1, 1, 1, 1, 1);
    let mut declarations = [CaptureCandidateDeclaration::GitBranch(
        GitBranchCaptureDeclaration::new("changes", git_metadata(1, 1, 2), None),
    )];

    let outputs = fixture
        .store
        .capture_candidates(&mut declarations, &CaptureCancellation::default())
        .unwrap()
        .commit();

    let CapturedValue::GitBranch(branch) = &outputs["changes"] else {
        panic!("Git output lost its semantic type");
    };
    assert!(branch.carrier().is_none());
    assert_eq!(fixture.store.git_budget_usage(), (0, 0));
    assert_eq!(fixture.store.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);
}

#[test]
fn git_overflow_rolls_back_both_ledgers_without_borrowing_file_capacity() {
    let fixture = CaptureFixture::with_all_limits(3, 8, 16, 2, 3, 3);
    fs::write(fixture.execution_root.join("report.bin"), b"file").unwrap();
    let mut oversized = BytesCarrierProducer(b"git!".to_vec());
    let mut mixed = [
        CaptureCandidateDeclaration::File(CaptureDeclaration::file(
            "report",
            Path::new("report.bin"),
            "application/octet-stream",
        )),
        CaptureCandidateDeclaration::GitBranch(GitBranchCaptureDeclaration::new(
            "changes",
            git_metadata(1, 2, 3),
            Some(&mut oversized),
        )),
    ];

    let failure = failed_capture(
        fixture
            .store
            .capture_candidates(&mut mixed, &CaptureCancellation::default()),
    );

    assert_eq!(failure.output_identity(), "changes");
    assert_eq!(
        failure.kind(),
        CaptureFailureKind::GitCarrierSizeLimitExceeded
    );
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.git_budget_usage(), (0, 0));
    assert_eq!(fixture.store.reservation_usage(), (0, 0));
    assert_eq!(fixture.store.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);

    let mut exact = BytesCarrierProducer(b"git".to_vec());
    let mut exact_declaration = [CaptureCandidateDeclaration::GitBranch(
        GitBranchCaptureDeclaration::new("prior", git_metadata(1, 2, 3), Some(&mut exact)),
    )];
    let retained = fixture
        .store
        .capture_candidates(&mut exact_declaration, &CaptureCancellation::default())
        .unwrap()
        .commit();
    assert_eq!(fixture.store.git_budget_usage(), (1, 3));

    fs::write(fixture.execution_root.join("next.bin"), b"x").unwrap();
    let mut one_more = BytesCarrierProducer(b"x".to_vec());
    let mut total_overflow = [
        CaptureCandidateDeclaration::File(CaptureDeclaration::file(
            "next",
            Path::new("next.bin"),
            "application/octet-stream",
        )),
        CaptureCandidateDeclaration::GitBranch(GitBranchCaptureDeclaration::new(
            "nextChanges",
            git_metadata(2, 3, 4),
            Some(&mut one_more),
        )),
    ];
    let failure = failed_capture(
        fixture
            .store
            .capture_candidates(&mut total_overflow, &CaptureCancellation::default()),
    );
    assert_eq!(
        failure.kind(),
        CaptureFailureKind::TotalGitCarrierSizeLimitExceeded
    );
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.git_budget_usage(), (1, 3));
    assert_eq!(fixture.store.staged_artifact_count(), 1);
    drop(retained);
}

#[test]
fn git_count_failure_acquires_neither_ledger() {
    let fixture = CaptureFixture::with_all_limits(2, 8, 16, 1, 8, 16);
    let mut prior_producer = BytesCarrierProducer(b"a".to_vec());
    let mut prior = [CaptureCandidateDeclaration::GitBranch(
        GitBranchCaptureDeclaration::new("prior", git_metadata(1, 2, 3), Some(&mut prior_producer)),
    )];
    let retained = fixture
        .store
        .capture_candidates(&mut prior, &CaptureCancellation::default())
        .unwrap()
        .commit();
    fs::write(fixture.execution_root.join("report.bin"), b"file").unwrap();
    let mut next_producer = BytesCarrierProducer(b"b".to_vec());
    let mut mixed = [
        CaptureCandidateDeclaration::File(CaptureDeclaration::file(
            "report",
            Path::new("report.bin"),
            "application/octet-stream",
        )),
        CaptureCandidateDeclaration::GitBranch(GitBranchCaptureDeclaration::new(
            "changes",
            git_metadata(2, 3, 4),
            Some(&mut next_producer),
        )),
    ];

    let failure = failed_capture(
        fixture
            .store
            .capture_candidates(&mut mixed, &CaptureCancellation::default()),
    );

    assert_eq!(
        failure.kind(),
        CaptureFailureKind::GitCarrierCountLimitExceeded
    );
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.reservation_usage(), (0, 0));
    assert_eq!(fixture.store.git_budget_usage(), (1, 1));
    assert_eq!(fixture.store.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 1);
    drop(retained);
}

struct CancellingCarrierProducer {
    cancellation: CaptureCancellation,
}

impl CarrierProducer for CancellingCarrierProducer {
    fn stream_to(&mut self, destination: &mut CarrierDestination<'_>) -> io::Result<()> {
        destination.write_all(b"git")?;
        self.cancellation.cancel();
        destination.write_all(b"!")
    }
}

#[test]
fn carrier_stream_cancellation_rolls_back_a_mixed_candidate_set() {
    let fixture = CaptureFixture::with_all_limits(2, 8, 16, 2, 8, 16);
    fs::write(fixture.execution_root.join("report.bin"), b"file").unwrap();
    let cancellation = CaptureCancellation::default();
    let mut producer = CancellingCarrierProducer {
        cancellation: cancellation.clone(),
    };
    let mut declarations = [
        CaptureCandidateDeclaration::File(CaptureDeclaration::file(
            "report",
            Path::new("report.bin"),
            "application/octet-stream",
        )),
        CaptureCandidateDeclaration::GitBranch(GitBranchCaptureDeclaration::new(
            "changes",
            git_metadata(1, 2, 3),
            Some(&mut producer),
        )),
    ];

    let result = fixture
        .store
        .capture_candidates(&mut declarations, &cancellation);

    assert!(matches!(result, Err(CaptureAttemptFailure::Cancelled)));
    assert_eq!(fixture.store.budget_usage(), (0, 0));
    assert_eq!(fixture.store.git_budget_usage(), (0, 0));
    assert_eq!(fixture.store.reservation_usage(), (0, 0));
    assert_eq!(fixture.store.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);
}

struct GatedCarrierProducer {
    started: Arc<Barrier>,
    resume: Arc<Barrier>,
    bytes: &'static [u8],
}

impl CarrierProducer for GatedCarrierProducer {
    fn stream_to(&mut self, destination: &mut CarrierDestination<'_>) -> io::Result<()> {
        self.started.wait();
        self.resume.wait();
        destination.write_all(self.bytes)
    }
}

#[test]
fn concurrent_git_captures_reserve_in_serial_capture_order() {
    let fixture = CaptureFixture::with_all_limits(1, 1, 1, 2, 3, 3);
    let started = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let first_store = fixture.store.clone();
    let first_started = Arc::clone(&started);
    let first_resume = Arc::clone(&resume);
    let first = std::thread::spawn(move || {
        let mut producer = GatedCarrierProducer {
            started: first_started,
            resume: first_resume,
            bytes: b"one",
        };
        let mut declarations = [CaptureCandidateDeclaration::GitBranch(
            GitBranchCaptureDeclaration::new("first", git_metadata(1, 2, 3), Some(&mut producer)),
        )];
        first_store.capture_candidates(&mut declarations, &CaptureCancellation::default())
    });
    started.wait();
    let second_store = fixture.store.clone();
    let second = std::thread::spawn(move || {
        let mut producer = BytesCarrierProducer(b"two".to_vec());
        let mut declarations = [CaptureCandidateDeclaration::GitBranch(
            GitBranchCaptureDeclaration::new("second", git_metadata(2, 3, 4), Some(&mut producer)),
        )];
        second_store.capture_candidates(&mut declarations, &CaptureCancellation::default())
    });
    resume.wait();

    let retained = first.join().unwrap().unwrap().commit();
    let failure = failed_capture(second.join().unwrap());

    assert_eq!(failure.output_identity(), "second");
    assert_eq!(
        failure.kind(),
        CaptureFailureKind::TotalGitCarrierSizeLimitExceeded
    );
    assert_eq!(fixture.store.git_budget_usage(), (1, 3));
    assert_eq!(fixture.store.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 1);
    drop(retained);
}

#[test]
fn git_carrier_presence_must_match_semantic_delta() {
    let fixture = CaptureFixture::with_all_limits(1, 1, 1, 1, 8, 8);

    let mut missing_carrier = [CaptureCandidateDeclaration::GitBranch(
        GitBranchCaptureDeclaration::new("changed", git_metadata(1, 2, 3), None),
    )];
    let missing_carrier_rejected = fixture
        .store
        .capture_candidates(&mut missing_carrier, &CaptureCancellation::default())
        .is_err();

    let mut producer = BytesCarrierProducer(b"bundle".to_vec());
    let mut forbidden_carrier = [CaptureCandidateDeclaration::GitBranch(
        GitBranchCaptureDeclaration::new("unchanged", git_metadata(1, 1, 3), Some(&mut producer)),
    )];
    let forbidden_carrier_rejected = fixture
        .store
        .capture_candidates(&mut forbidden_carrier, &CaptureCancellation::default())
        .is_err();

    assert_eq!(fixture.store.git_budget_usage(), (0, 0));
    assert_eq!(fixture.store.git_reservation_usage(), (0, 0));
    assert_eq!(fixture.store.staged_artifact_count(), 0);
    assert!(
        missing_carrier_rejected && forbidden_carrier_rejected,
        "carrier presence invariant was not enforced: missing_rejected={missing_carrier_rejected}, forbidden_rejected={forbidden_carrier_rejected}"
    );
}

#[test]
fn git_carrier_handles_reject_cross_execution_replacement_and_release() {
    let fixture = CaptureFixture::with_all_limits(1, 1, 1, 1, 8, 8);
    let other = CaptureFixture::with_all_limits(1, 1, 1, 1, 8, 8);
    let mut producer = BytesCarrierProducer(b"bundle".to_vec());
    let mut declarations = [CaptureCandidateDeclaration::GitBranch(
        GitBranchCaptureDeclaration::new("changes", git_metadata(1, 2, 3), Some(&mut producer)),
    )];
    let outputs = fixture
        .store
        .capture_candidates(&mut declarations, &CaptureCancellation::default())
        .unwrap()
        .commit();
    let CapturedValue::GitBranch(branch) = &outputs["changes"] else {
        panic!("Git output lost its semantic type");
    };
    let handle = branch.carrier().unwrap().handle().clone();

    assert_eq!(
        other.store.copy_to(&handle, &mut Vec::new()),
        Err(ArtifactReadFailure::UnknownHandle)
    );
    let staged_path = fixture.staging_path().join(handle.opaque_id());
    fs::remove_file(&staged_path).unwrap();
    fs::write(&staged_path, b"replacement").unwrap();
    assert_eq!(
        fixture.store.copy_to(&handle, &mut Vec::new()),
        Err(ArtifactReadFailure::UnknownHandle)
    );

    fixture.store.release().unwrap();
    assert_eq!(
        fixture.store.copy_to(&handle, &mut Vec::new()),
        Err(ArtifactReadFailure::UnknownHandle)
    );
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
