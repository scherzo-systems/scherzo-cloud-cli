use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, CaptureLimits, EnvironmentSnapshot, ExecutionContext,
    ExecutionPolicyLimits, InputLimits, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::artifact::{ArtifactStaging, CaptureDeclaration};
use crate::execution::workflow::resolution;

struct Fixture {
    _temporary: tempfile::TempDir,
    execution_root: PathBuf,
    artifacts: ArtifactStaging,
    inputs: InputStaging,
}

impl Fixture {
    fn new(
        maximum_parallel_steps: usize,
        maximum_values: usize,
        maximum_value_bytes: u64,
        maximum_total_bytes: u64,
    ) -> Self {
        Self::with_live_limit(
            maximum_parallel_steps,
            maximum_values,
            maximum_value_bytes,
            maximum_total_bytes,
            maximum_total_bytes,
        )
    }

    fn with_live_limit(
        maximum_parallel_steps: usize,
        maximum_values: usize,
        maximum_value_bytes: u64,
        maximum_total_bytes: u64,
        maximum_live_bytes: u64,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let artifact_parent = temporary.path().join("artifacts");
        let input_parent = temporary.path().join("inputs");
        for directory in [
            &source_root,
            &execution_root,
            &artifact_parent,
            &input_parent,
        ] {
            fs::create_dir(directory).unwrap();
        }
        fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  task:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let admitted = admit_workflow(
            resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
            ResolvedImports::default(),
            ExecutionContext::new(
                execution_root.clone(),
                ExecutionPolicyLimits::new(
                    maximum_parallel_steps,
                    CaptureLimits::new(64, 1024 * 1024, 8 * 1024 * 1024),
                    InputLimits::new(
                        maximum_values,
                        maximum_value_bytes,
                        maximum_total_bytes,
                        maximum_live_bytes,
                    ),
                    1024 * 1024,
                ),
                EnvironmentSnapshot::default(),
                CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
            ),
        )
        .unwrap();
        let artifacts = ArtifactStaging::create(admitted.execution(), &artifact_parent).unwrap();
        let inputs = InputStaging::create(admitted.execution(), &input_parent).unwrap();
        Self {
            _temporary: temporary,
            execution_root,
            artifacts,
            inputs,
        }
    }

    fn capture_file(
        &self,
        name: &str,
        path: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> CapturedValue {
        fs::write(self.execution_root.join(path), bytes).unwrap();
        let mut captured = self
            .artifacts
            .capture_files(&[CaptureDeclaration::file(name, Path::new(path), media_type)])
            .unwrap();
        CapturedValue::file(captured.remove(name).unwrap())
    }
}

#[test]
fn materializes_every_value_kind_with_exact_canonical_layout_and_private_copies() {
    let fixture = Fixture::new(2, 16, 1024, 4096);
    let file = fixture.capture_file(
        "artifact",
        "caller-name-do-not-expose.bin",
        "application/vnd.scherzo.fixture",
        b"captured file",
    );
    fs::write(
        fixture.execution_root.join("caller-name-do-not-expose.bin"),
        b"mutated source",
    )
    .unwrap();
    let response = CapturedValue::text(Arc::from("agent response"));
    let json = CapturedValue::json_fixture(Arc::new(json!({
        "z": 1,
        "a": [3, {"x": true}]
    })));
    let attachments = [
        ResolvedAttachment::new(Arc::from("image/png"), Arc::from([0_u8, 0xff, 3])),
        ResolvedAttachment::new(Arc::from("application/empty"), Arc::from([])),
    ];
    let values = BTreeMap::from([
        (
            "attachments".to_owned(),
            InputValue::Attachments(attachments.as_slice()),
        ),
        (
            "fileValue".to_owned(),
            InputValue::Captured {
                expected_type: WorkflowValueType::File,
                value: &file,
            },
        ),
        (
            "jsonValue".to_owned(),
            InputValue::Captured {
                expected_type: WorkflowValueType::Json,
                value: &json,
            },
        ),
        ("prompt".to_owned(), InputValue::Prompt("héllo")),
        (
            "response".to_owned(),
            InputValue::Captured {
                expected_type: WorkflowValueType::Text,
                value: &response,
            },
        ),
    ]);

    let first = fixture
        .inputs
        .materialize(&values, &fixture.artifacts)
        .unwrap();
    let second = fixture
        .inputs
        .materialize(&values, &fixture.artifacts)
        .unwrap();

    assert_ne!(first.path(), second.path());
    assert_eq!(fixture.inputs.active_view_count(), 2);
    assert_eq!(fixture.inputs.reservation_usage(), (2, 14, 124));
    let expected_entries = [
        "collections/attachments",
        "collections/attachments/000000",
        "collections/attachments/000001",
        "manifest.json",
        "values/fileValue",
        "values/jsonValue",
        "values/prompt",
        "values/response",
    ];
    for view in [&first, &second] {
        assert_eq!(relative_tree(view.path()), expected_entries);
        assert_eq!(
            fs::read(view.path().join("values/prompt")).unwrap(),
            "héllo".as_bytes()
        );
        assert_eq!(
            fs::read(view.path().join("values/response")).unwrap(),
            b"agent response"
        );
        assert_eq!(
            fs::read(view.path().join("values/jsonValue")).unwrap(),
            br#"{"a":[3,{"x":true}],"z":1}"#
        );
        assert_eq!(
            fs::read(view.path().join("values/fileValue")).unwrap(),
            b"captured file"
        );
        assert_eq!(
            fs::read(view.path().join("collections/attachments/000000")).unwrap(),
            [0, 0xff, 3]
        );
        assert_eq!(
            fs::read(view.path().join("collections/attachments/000001")).unwrap(),
            Vec::<u8>::new()
        );
        assert_eq!(
            fs::read_to_string(view.path().join("manifest.json")).unwrap(),
            concat!(
                "{\"schemaVersion\":1,\"inputs\":{",
                "\"attachments\":{\"kind\":\"attachment_collection\",",
                "\"path\":\"collections/attachments\",\"items\":[",
                "{\"index\":0,\"mediaType\":\"image/png\",",
                "\"path\":\"collections/attachments/000000\"},",
                "{\"index\":1,\"mediaType\":\"application/empty\",",
                "\"path\":\"collections/attachments/000001\"}]},",
                "\"fileValue\":{\"kind\":\"file\",",
                "\"mediaType\":\"application/vnd.scherzo.fixture\",",
                "\"path\":\"values/fileValue\"},",
                "\"jsonValue\":{\"kind\":\"json\",",
                "\"mediaType\":\"application/json\",",
                "\"path\":\"values/jsonValue\"},",
                "\"prompt\":{\"kind\":\"text\",",
                "\"mediaType\":\"text/plain; charset=utf-8\",",
                "\"path\":\"values/prompt\"},",
                "\"response\":{\"kind\":\"text\",",
                "\"mediaType\":\"text/plain; charset=utf-8\",",
                "\"path\":\"values/response\"}}}"
            )
        );
        for entry in relative_tree(view.path()) {
            let mode = fs::symlink_metadata(view.path().join(entry))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o222, 0);
        }
    }

    let source_metadata =
        fs::metadata(fixture.execution_root.join("caller-name-do-not-expose.bin")).unwrap();
    let first_file = first.path().join("values/fileValue");
    let second_file = second.path().join("values/fileValue");
    let first_metadata = fs::metadata(&first_file).unwrap();
    let second_metadata = fs::metadata(&second_file).unwrap();
    assert_ne!(first_metadata.ino(), second_metadata.ino());
    assert_ne!(source_metadata.ino(), first_metadata.ino());
    assert_ne!(source_metadata.ino(), second_metadata.ino());

    fs::set_permissions(&first_file, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&first_file, b"changed consumer").unwrap();
    assert_eq!(fs::read(&second_file).unwrap(), b"captured file");
    let captured = file.as_file().unwrap();
    let mut captured_bytes = Vec::new();
    fixture
        .artifacts
        .copy_to(captured.handle(), &mut captured_bytes)
        .unwrap();
    assert_eq!(captured_bytes, b"captured file");
    assert_eq!(
        fs::read(fixture.execution_root.join("caller-name-do-not-expose.bin")).unwrap(),
        b"mutated source"
    );

    let first_path = first.path().to_owned();
    let second_path = second.path().to_owned();
    drop(first);
    assert!(!first_path.exists());
    assert_eq!(fixture.inputs.reservation_usage(), (1, 7, 62));
    drop(second);
    assert!(!second_path.exists());
    assert_eq!(fixture.inputs.reservation_usage(), (0, 0, 0));
}

#[test]
fn canonical_json_bytes_are_materialized_without_reserialization() {
    let fixture = Fixture::new(1, 8, 1024, 2048);
    let exact = br#"{"schemaVersion":1,"trigger":"succeeded","primaryIssueStepId":null,"cancellationReason":null,"ordinaryIssues":[]}"#;
    let view = fixture
        .inputs
        .materialize(
            &BTreeMap::from([(
                "context".to_owned(),
                InputValue::CanonicalJson(exact.as_slice()),
            )]),
            &fixture.artifacts,
        )
        .unwrap();

    assert_eq!(fs::read(view.path().join("values/context")).unwrap(), exact);
    assert_eq!(
        fs::read_to_string(view.path().join("manifest.json")).unwrap(),
        "{\"schemaVersion\":1,\"inputs\":{\"context\":{\"kind\":\"json\",\"mediaType\":\"application/json\",\"path\":\"values/context\"}}}"
    );
}

#[test]
fn live_byte_reservations_are_run_wide_and_reusable_at_high_parallelism() {
    const MAXIMUM_LIVE_BYTES: u64 = 256 * 1024 * 1024;
    let fixture = Fixture::with_live_limit(
        256,
        1,
        MAXIMUM_LIVE_BYTES,
        MAXIMUM_LIVE_BYTES,
        MAXIMUM_LIVE_BYTES,
    );
    let larger_half = MAXIMUM_LIVE_BYTES / 2 + 1;
    let smaller_half = MAXIMUM_LIVE_BYTES / 2 - 1;

    let first = reserve_view(&fixture.inputs, larger_half).unwrap();
    let second = reserve_view(&fixture.inputs, smaller_half).unwrap();
    assert_eq!(
        fixture.inputs.reservation_usage(),
        (2, 2, MAXIMUM_LIVE_BYTES)
    );
    assert!(fs::read_dir(first.path()).unwrap().next().is_none());
    assert!(fs::read_dir(second.path()).unwrap().next().is_none());
    let staging_root = first.path().parent().unwrap().to_owned();
    assert_eq!(fs::read_dir(&staging_root).unwrap().count(), 2);

    assert_preparation_failure(
        reserve_view(&fixture.inputs, larger_half),
        InputPreparationFailureKind::LiveLimitExceeded,
        None,
        None,
    );
    assert_eq!(
        fixture.inputs.reservation_usage(),
        (2, 2, MAXIMUM_LIVE_BYTES)
    );
    assert_eq!(fs::read_dir(&staging_root).unwrap().count(), 2);

    drop(first);
    assert_eq!(fixture.inputs.reservation_usage(), (1, 1, smaller_half));
    let replacement = reserve_view(&fixture.inputs, larger_half).unwrap();
    assert_eq!(
        fixture.inputs.reservation_usage(),
        (2, 2, MAXIMUM_LIVE_BYTES)
    );

    drop(second);
    drop(replacement);
    assert_eq!(fixture.inputs.reservation_usage(), (0, 0, 0));
}

#[test]
fn dropping_a_view_cleans_it_after_the_staging_parent_moves() {
    let fixture = Fixture::new(1, 8, 8, 16);
    let view = fixture
        .inputs
        .materialize(
            &BTreeMap::from([("prompt".to_owned(), InputValue::Prompt("held"))]),
            &fixture.artifacts,
        )
        .unwrap();
    let staging_root = view.path().parent().unwrap();
    let staging_parent = staging_root.parent().unwrap();
    let moved_parent = fixture._temporary.path().join("moved-inputs");
    let leaked_view = moved_parent
        .join(staging_root.file_name().unwrap())
        .join(view.path().file_name().unwrap());
    fs::rename(staging_parent, &moved_parent).unwrap();

    drop(view);

    assert!(!leaked_view.exists());
}

#[test]
fn preparation_rejects_names_limits_types_live_capacity_and_unavailable_sources() {
    let name_fixture = Fixture::new(1, 8, 8, 16);
    assert_preparation_failure(
        name_fixture.inputs.materialize(
            &BTreeMap::from([("../escape".to_owned(), InputValue::Prompt("x"))]),
            &name_fixture.artifacts,
        ),
        InputPreparationFailureKind::InvalidInputName,
        Some("../escape"),
        None,
    );

    let count_fixture = Fixture::new(1, 1, 8, 16);
    let one_attachment = [ResolvedAttachment::new(
        Arc::from("application/octet-stream"),
        Arc::from([1_u8]),
    )];
    assert_preparation_failure(
        count_fixture.inputs.materialize(
            &BTreeMap::from([(
                "attachments".to_owned(),
                InputValue::Attachments(one_attachment.as_slice()),
            )]),
            &count_fixture.artifacts,
        ),
        InputPreparationFailureKind::ValueCountLimitExceeded,
        Some("attachments"),
        None,
    );

    let size_fixture = Fixture::new(1, 8, 3, 16);
    assert_preparation_failure(
        size_fixture.inputs.materialize(
            &BTreeMap::from([("prompt".to_owned(), InputValue::Prompt("four"))]),
            &size_fixture.artifacts,
        ),
        InputPreparationFailureKind::ValueSizeLimitExceeded,
        Some("prompt"),
        None,
    );

    let total_fixture = Fixture::new(1, 8, 4, 5);
    let response = CapturedValue::text(Arc::from("def"));
    assert_preparation_failure(
        total_fixture.inputs.materialize(
            &BTreeMap::from([
                ("prompt".to_owned(), InputValue::Prompt("abc")),
                (
                    "response".to_owned(),
                    InputValue::Captured {
                        expected_type: WorkflowValueType::Text,
                        value: &response,
                    },
                ),
            ]),
            &total_fixture.artifacts,
        ),
        InputPreparationFailureKind::TotalSizeLimitExceeded,
        Some("response"),
        None,
    );

    let mismatch_fixture = Fixture::new(1, 8, 8, 16);
    let json = CapturedValue::json_fixture(Arc::new(json!({"ok": true})));
    assert_preparation_failure(
        mismatch_fixture.inputs.materialize(
            &BTreeMap::from([(
                "result".to_owned(),
                InputValue::Captured {
                    expected_type: WorkflowValueType::Text,
                    value: &json,
                },
            )]),
            &mismatch_fixture.artifacts,
        ),
        InputPreparationFailureKind::ValueTypeMismatch,
        Some("result"),
        None,
    );

    let source_fixture = Fixture::new(1, 8, 64, 128);
    let foreign_fixture = Fixture::new(1, 8, 64, 128);
    let foreign_file = foreign_fixture.capture_file(
        "artifact",
        "foreign.bin",
        "application/octet-stream",
        b"foreign",
    );
    assert_preparation_failure(
        source_fixture.inputs.materialize(
            &BTreeMap::from([(
                "artifact".to_owned(),
                InputValue::Captured {
                    expected_type: WorkflowValueType::File,
                    value: &foreign_file,
                },
            )]),
            &source_fixture.artifacts,
        ),
        InputPreparationFailureKind::SourceUnavailable,
        Some("artifact"),
        None,
    );
    assert_eq!(source_fixture.inputs.reservation_usage(), (0, 0, 0));

    let live_fixture = Fixture::new(1, 8, 8, 16);
    let values = BTreeMap::from([("prompt".to_owned(), InputValue::Prompt("held"))]);
    let held = live_fixture
        .inputs
        .materialize(&values, &live_fixture.artifacts)
        .unwrap();
    assert_preparation_failure(
        live_fixture
            .inputs
            .materialize(&values, &live_fixture.artifacts),
        InputPreparationFailureKind::LiveLimitExceeded,
        None,
        None,
    );
    drop(held);
    assert_eq!(live_fixture.inputs.reservation_usage(), (0, 0, 0));
}

fn reserve_view(staging: &InputStaging, bytes: u64) -> Result<InputView, InputPreparationFailure> {
    let (identity, path) = staging.reserve(ReservationUsage {
        views: 1,
        values: 1,
        bytes,
    })?;
    Ok(InputView {
        inner: Arc::clone(&staging.inner),
        identity,
        path,
        released: false,
    })
}

fn assert_preparation_failure(
    result: Result<InputView, InputPreparationFailure>,
    kind: InputPreparationFailureKind,
    input_identity: Option<&str>,
    collection_index: Option<usize>,
) {
    let failure = result.err().unwrap();
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.input_identity(), input_identity);
    assert_eq!(failure.collection_index(), collection_index);
}

fn relative_tree(root: &Path) -> Vec<String> {
    fn collect(root: &Path, directory: &Path, entries: &mut Vec<String>) {
        let mut children = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        children.sort();
        for child in children {
            let relative = child
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            if child.is_dir() {
                if relative != "collections" && relative != "values" {
                    entries.push(relative);
                }
                collect(root, &child, entries);
            } else {
                entries.push(relative);
            }
        }
    }

    let mut entries = Vec::new();
    collect(root, root, &mut entries);
    entries.sort();
    entries
}
