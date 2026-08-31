use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, CaptureLimits, EnvironmentSnapshot, ExecutionContext,
    ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::agent::{AgentOutcome, AgentValueKind};
use crate::execution::workflow::artifact::{
    ArtifactReadFailure, CaptureDeclaration, CapturedArtifact,
};
use crate::execution::workflow::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
use crate::execution::workflow::evidence::{
    CancellationDetail, PrimaryIssue, failure_detail as canonical_failure_detail,
};
use crate::execution::workflow::pi_json_v1::{
    PiJsonV1Parser, PiJsonV1ProcessCompletion, PiJsonV1ProtocolLimits,
};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::OutputSet;
use crate::execution::workflow::validated::{WorkflowNode, WorkflowNodeRole};

struct PublicationFixture {
    _temporary: tempfile::TempDir,
    source_root: PathBuf,
    execution_root: PathBuf,
    results_parent: PathBuf,
    content_digest: WorkflowContentDigest,
    artifacts: ArtifactStaging,
}

impl PublicationFixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let artifact_parent = temporary.path().join("artifact-staging");
        let results_parent = temporary.path().join("results");
        for directory in [
            &source_root,
            &execution_root,
            &artifact_parent,
            &results_parent,
        ] {
            fs::create_dir(directory).unwrap();
        }
        fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  terminal:\n    kind: cmd\n    dependsOn: [produce]\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let resolved = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let content_digest = resolved.content_digest.clone();
        let admitted = admit_workflow(
            resolved,
            ResolvedImports::default(),
            ExecutionContext::new(
                execution_root.clone(),
                ExecutionRootLifecycle::CallerOwnedRetained,
                ExecutionPolicyLimits::new(
                    2,
                    CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
                    InputLimits::new(16, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
                    super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM,
                ),
                EnvironmentSnapshot::default(),
                CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
            ),
        )
        .unwrap();
        let artifacts = ArtifactStaging::create(admitted.execution(), &artifact_parent).unwrap();
        Self {
            _temporary: temporary,
            source_root,
            execution_root,
            results_parent,
            content_digest,
            artifacts,
        }
    }

    fn capture(
        &self,
        identity: &str,
        relative_path: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> CapturedArtifact {
        let path = self.execution_root.join(relative_path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        self.artifacts
            .capture_files(&[CaptureDeclaration::file(
                identity,
                Path::new(relative_path),
                media_type,
            )])
            .unwrap()
            .remove(identity)
            .unwrap()
    }

    fn destination(&self, name: &str) -> PathBuf {
        self.results_parent.join(name)
    }
}

fn timestamp_fixture(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).unwrap()
}

fn diagnostic(fully_drained: bool) -> StepDiagnostic {
    StepDiagnostic::from_streams(
        CapturedDiagnosticStream::from_parts(Arc::<[u8]>::from(*b"ok\n"), 0, fully_drained),
        CapturedDiagnosticStream::from_parts(Arc::<[u8]>::from(*b"warn"), 0, true),
    )
}

fn export_source(step: &str, output: &str, value_type: WorkflowValueType) -> ResolvedOutputSource {
    ResolvedOutputSource {
        node: WorkflowNode {
            id: step.to_owned(),
            role: WorkflowNodeRole::Step,
        },
        output: output.to_owned(),
        value_type,
    }
}

fn succeeded_step(id: &str, outputs: OutputSet<CapturedValue>) -> WorkflowRunStep {
    WorkflowRunStep {
        id: id.to_owned(),
        role: WorkflowNodeRole::Step,
        kind: WorkflowRunStepKind::Command,
        failure_policy: FailurePolicy::Required,
        state: StepState::Succeeded { outputs },
        timing: Some(WorkflowStepTiming {
            started_at: timestamp_fixture("2026-08-02T12:01:44.01Z"),
            duration: Duration::from_millis(240),
        }),
        command_output: Some(diagnostic(true)),
        recovery: None,
        invocations: Vec::new(),
    }
}

fn run_fixture(fixture: &PublicationFixture) -> WorkflowRunResult {
    let report_upper = fixture.capture(
        "upper",
        "odd names/report?.txt",
        "application/junit+xml",
        b"upper report bytes",
    );
    let report_lower = fixture.capture(
        "lower",
        "other odd names/report?.txt",
        "text/plain",
        b"lower report bytes",
    );
    let outputs = BTreeMap::from([
        (
            "upper".to_owned(),
            CapturedValue::file(report_upper.clone()),
        ),
        (
            "lower".to_owned(),
            CapturedValue::file(report_lower.clone()),
        ),
    ]);
    WorkflowRunResult {
        run_directory: fixture.results_parent.clone(),
        attempt_number: 1,
        workflow_path: "workflow.yaml".to_owned(),
        source_root: fixture.source_root.clone(),
        content_digest: fixture.content_digest.clone(),
        execution_root: fixture.execution_root.clone(),
        maximum_parallel_steps: NonZeroUsize::new(2).unwrap(),
        cloud_capacity: None,
        timing: WorkflowRunTiming {
            started_at: timestamp_fixture("2026-08-02T12:01:44Z"),
            finished_at: timestamp_fixture("2026-08-02T12:01:45.25Z"),
            duration: Duration::from_millis(1250),
        },
        outcome: RunOutcome::Succeeded,
        cancellation: None,
        steps: vec![
            succeeded_step("produce", outputs),
            succeeded_step("terminal", BTreeMap::new()),
        ],
        finalization: None,
        exports: BTreeMap::from([
            (
                "reportA".to_owned(),
                ExportValue::Available {
                    output: CapturedValue::file(report_upper),
                },
            ),
            (
                "reporta".to_owned(),
                ExportValue::Available {
                    output: CapturedValue::file(report_lower),
                },
            ),
        ]),
        export_sources: BTreeMap::from([
            (
                "reportA".to_owned(),
                export_source("produce", "upper", WorkflowValueType::File),
            ),
            (
                "reporta".to_owned(),
                export_source("produce", "lower", WorkflowValueType::File),
            ),
        ]),
    }
}

fn make_failed(run: &mut WorkflowRunResult) {
    let cause = StepFailureCause::Execution(StepExecutionFailure::Command(
        CommandExecutionFailure::UnsuccessfulExit { code: Some(23) },
    ));
    let detail = canonical_failure_detail(FailurePhase::Execution, &cause).unwrap();
    run.steps[1].state = StepState::Failed {
        detail: detail.clone(),
    };
    run.outcome = RunOutcome::Failed {
        primary_issue: PrimaryIssue::failed(
            WorkflowNode {
                id: "terminal".to_owned(),
                role: WorkflowNodeRole::Step,
            },
            detail,
        ),
        later_cancellation: None,
    };
    run.exports.insert(
        "reportZ".to_owned(),
        ExportValue::Unavailable {
            reason: ExportUnavailableReason::Blocked,
        },
    );
    run.exports.insert(
        "sourceFailed".to_owned(),
        ExportValue::Unavailable {
            reason: ExportUnavailableReason::Failed,
        },
    );
    run.export_sources.insert(
        "reportZ".to_owned(),
        export_source("terminal", "blocked", WorkflowValueType::File),
    );
    run.export_sources.insert(
        "sourceFailed".to_owned(),
        export_source("terminal", "failed", WorkflowValueType::File),
    );
}

fn make_cancelled(run: &mut WorkflowRunResult) {
    run.steps[1].state = StepState::Cancelled {
        detail: CancellationDetail::new(CancellationReason::UserRequest),
    };
    run.outcome = RunOutcome::Cancelled {
        reason: CancellationReason::UserRequest,
    };
    run.cancellation = Some(WorkflowRunCancellation {
        reason: CancellationReason::UserRequest,
        force_stop_deadline: timestamp_fixture("2026-08-02T12:01:55Z"),
    });
    run.exports.insert(
        "sourceCancelled".to_owned(),
        ExportValue::Unavailable {
            reason: ExportUnavailableReason::Cancelled,
        },
    );
    run.export_sources.insert(
        "sourceCancelled".to_owned(),
        export_source("terminal", "cancelled", WorkflowValueType::File),
    );
}

fn read_result(destination: &Path) -> (Vec<u8>, Value) {
    let bytes = fs::read(destination.join(RESULT_FILE)).unwrap();
    let value = serde_json::from_slice(&bytes).unwrap();
    (bytes, value)
}

fn staging_paths(parent: &Path) -> Vec<PathBuf> {
    fs::read_dir(parent)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.starts_with(".result-"))
        })
        .collect()
}

#[test]
fn prepares_metadata_only_and_carrier_cloud_results() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    run.cloud_capacity = Some(CloudExecutionCapacityV1 {
        execution_contract: "workflow_v1_cloud_inputs_artifacts@1".to_owned(),
        source_closure_digest: DigestV1 {
            algorithm: run.content_digest.algorithm.as_str().to_owned(),
            value: run.content_digest.value.clone(),
        },
        general_maximum_transitions: 8,
        selected_maximum_transitions: 7,
        maximum_invocations: 1,
        maximum_retained_bytes_per_invocation: 4_194_304,
        diagnostic_retention_bytes: 8_388_608,
        native_session_retention_bytes: 4_194_304,
        aggregate_retention_bytes: 12_582_912,
        encoded_outbox_bytes: 85_458_944,
    });
    run.exports.insert(
        "agentResponse".to_owned(),
        ExportValue::Available {
            output: CapturedValue::text(Arc::from("response")),
        },
    );
    run.export_sources.insert(
        "agentResponse".to_owned(),
        export_source("produce", "response", WorkflowValueType::Text),
    );
    run.exports.insert(
        "agentResult".to_owned(),
        ExportValue::Available {
            output: CapturedValue::json_fixture(Arc::new(serde_json::json!({ "ok": true }))),
        },
    );
    run.export_sources.insert(
        "agentResult".to_owned(),
        export_source("produce", "result", WorkflowValueType::Json),
    );
    let captured = prepare_cloud_workflow_result(
        &run,
        "prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "sha1".to_owned(),
        "0123456789abcdef0123456789abcdef01234567".to_owned(),
    )
    .unwrap();
    assert_eq!(captured.carriers.len(), 4);
    let document: serde_json::Value = serde_json::from_slice(&captured.result_json).unwrap();
    assert_eq!(document["workflow"]["provenance"]["kind"], "cloud");
    assert!(document["execution"].get("executionRoot").is_none());

    let mut metadata_only = run;
    metadata_only.exports.clear();
    metadata_only.export_sources.clear();
    let prepared = prepare_cloud_workflow_result(
        &metadata_only,
        "prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "sha1".to_owned(),
        "0123456789abcdef0123456789abcdef01234567".to_owned(),
    )
    .unwrap();
    assert!(prepared.carriers.is_empty());
    let document: serde_json::Value = serde_json::from_slice(&prepared.result_json).unwrap();
    assert_eq!(document["exports"], serde_json::json!({}));
}

#[test]
fn publishes_streams_within_live_tui_run_budget() {
    const STEP_COUNT: usize = 12;
    const STREAM_BYTES: usize = 4 * 1024 * 1024;

    let fixture = PublicationFixture::new();
    let retained = Arc::<[u8]>::from(vec![b'x'; STREAM_BYTES]);
    let empty = Arc::<[u8]>::from([]);
    let mut run = run_fixture(&fixture);
    run.exports.clear();
    run.export_sources.clear();
    run.steps = (0..STEP_COUNT)
        .map(|index| {
            let mut step = succeeded_step(&format!("emit{index}"), BTreeMap::new());
            step.command_output = Some(StepDiagnostic::from_streams(
                CapturedDiagnosticStream::from_parts(Arc::clone(&retained), 0, true),
                CapturedDiagnosticStream::from_parts(Arc::clone(&empty), 0, true),
            ));
            step
        })
        .collect();

    publish_workflow_result(
        &fixture.destination("tui-budgeted-streams"),
        &fixture.artifacts,
        &run,
    )
    .expect("all streams retained within the live TUI run budget must be publishable");
}

#[test]
fn publishes_each_terminal_outcome_as_the_same_self_contained_v1_value() {
    let fixture = PublicationFixture::new();
    let base = run_fixture(&fixture);
    let mut failed = base.clone();
    make_failed(&mut failed);
    let mut cancelled = base.clone();
    make_cancelled(&mut cancelled);

    for (name, run, expected_outcome, expected_status) in [
        ("succeeded", base.clone(), "succeeded", 0_u64),
        ("failed", failed, "failed", 1),
        ("cancelled", cancelled, "cancelled", 130),
    ] {
        let destination = fixture.destination(name);
        let normalized_destination = fs::canonicalize(destination.parent().unwrap())
            .unwrap()
            .join(destination.file_name().unwrap());
        let terminal = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
        let terminal_value = serde_json::to_value(&terminal).unwrap();
        let (bytes, result_value) = read_result(&destination);

        assert!(bytes.ends_with(b"\n"));
        assert_eq!(result_value, terminal_value["result"]);
        assert_eq!(terminal_value["outcome"], expected_outcome);
        assert_eq!(terminal_value["exitStatus"], expected_status);
        assert_eq!(
            terminal.result_directory(),
            normalized_destination.to_str().unwrap()
        );
        assert_eq!(
            serde_json::to_value(terminal.result()).unwrap(),
            result_value
        );
        let lower_ordinal = if expected_outcome == "failed" {
            "0003"
        } else {
            "0002"
        };
        assert_eq!(
            fs::read(destination.join("exports/0001")).unwrap(),
            b"upper report bytes"
        );
        assert_eq!(
            fs::read(destination.join(format!("exports/{lower_ordinal}"))).unwrap(),
            b"lower report bytes"
        );
        assert_eq!(result_value["exports"]["reportA"]["path"], "exports/0001");
        assert_eq!(
            result_value["exports"]["reporta"]["path"],
            format!("exports/{lower_ordinal}")
        );
        assert_eq!(
            result_value["exports"]["reportA"]["mediaType"],
            "application/junit+xml"
        );
        assert_eq!(result_value["exports"]["reportA"]["sizeBytes"], 18);
        assert_eq!(
            result_value["exports"]["reportA"]["digest"]["value"],
            lowercase_hex(ring::digest::digest(&SHA256, b"upper report bytes").as_ref())
        );
        assert_eq!(
            result_value["steps"][0]["commandOutput"]["stdout"]["data"],
            "b2sK"
        );
        assert_eq!(
            result_value["steps"][0]["commandOutput"]["stdout"]["discardedBytes"],
            0
        );
    }

    let (_, failed_result) = read_result(&fixture.destination("failed"));
    assert_eq!(
        failed_result["exports"]["reportZ"],
        serde_json::json!({"state": "unavailable", "reason": "source_blocked"})
    );
    assert_eq!(
        failed_result["exports"]["sourceFailed"],
        serde_json::json!({"state": "unavailable", "reason": "source_failed"})
    );
    assert!(!fixture.destination("failed").join("exports/0002").exists());
    assert!(!fixture.destination("failed").join("exports/0004").exists());
    assert!(staging_paths(&fixture.results_parent).is_empty());
}

#[test]
fn publishes_text_json_and_file_exports_with_typed_canonical_metadata() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    let ExportValue::Available { output: file } = run.exports.remove("reportA").unwrap() else {
        panic!("reportA must be available");
    };
    run.exports = BTreeMap::from([
        ("file".to_owned(), ExportValue::Available { output: file }),
        (
            "response".to_owned(),
            ExportValue::Available {
                output: CapturedValue::text(Arc::from("agent response\n")),
            },
        ),
        (
            "result".to_owned(),
            ExportValue::Available {
                output: CapturedValue::json_fixture(Arc::new(serde_json::json!({"z": 2, "a": 1}))),
            },
        ),
    ]);
    run.export_sources = BTreeMap::from([
        (
            "file".to_owned(),
            export_source("produce", "upper", WorkflowValueType::File),
        ),
        (
            "response".to_owned(),
            export_source("produce", "response", WorkflowValueType::Text),
        ),
        (
            "result".to_owned(),
            export_source("produce", "result", WorkflowValueType::Json),
        ),
    ]);
    let destination = fixture.destination("typed-exports");

    publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    let (_, result) = read_result(&destination);

    assert_eq!(
        fs::read(destination.join("exports/0001")).unwrap(),
        b"upper report bytes"
    );
    assert_eq!(
        fs::read(destination.join("exports/0002")).unwrap(),
        b"agent response\n"
    );
    assert_eq!(
        fs::read(destination.join("exports/0003")).unwrap(),
        br#"{"a":1,"z":2}"#
    );
    assert_eq!(result["exports"]["file"]["kind"], "file");
    assert_eq!(
        result["exports"]["file"]["mediaType"],
        "application/junit+xml"
    );
    assert_eq!(result["exports"]["response"]["kind"], "text");
    assert_eq!(
        result["exports"]["response"]["mediaType"],
        "text/plain; charset=utf-8"
    );
    assert_eq!(result["exports"]["result"]["kind"], "json");
    assert_eq!(result["exports"]["result"]["mediaType"], "application/json");
}

#[test]
fn aliases_share_one_carrier_for_each_existing_kind() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    let ExportValue::Available { output: file } = run.exports.remove("reportA").unwrap() else {
        panic!("reportA must be available");
    };
    let text = CapturedValue::text(Arc::from("shared response\n"));
    let json = CapturedValue::json_fixture(Arc::new(serde_json::json!({"answer": 42})));
    run.exports = BTreeMap::from([
        (
            "aFile".to_owned(),
            ExportValue::Available {
                output: file.clone(),
            },
        ),
        ("bFile".to_owned(), ExportValue::Available { output: file }),
        (
            "cUnavailable".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Failed,
            },
        ),
        (
            "cUnavailableCopy".to_owned(),
            ExportValue::Unavailable {
                reason: ExportUnavailableReason::Failed,
            },
        ),
        (
            "dText".to_owned(),
            ExportValue::Available {
                output: text.clone(),
            },
        ),
        ("eText".to_owned(), ExportValue::Available { output: text }),
        (
            "fJson".to_owned(),
            ExportValue::Available {
                output: json.clone(),
            },
        ),
        ("gJson".to_owned(), ExportValue::Available { output: json }),
    ]);
    run.export_sources = BTreeMap::from([
        (
            "aFile".to_owned(),
            export_source("produce", "upper", WorkflowValueType::File),
        ),
        (
            "bFile".to_owned(),
            export_source("produce", "upper", WorkflowValueType::File),
        ),
        (
            "cUnavailable".to_owned(),
            export_source("terminal", "failed", WorkflowValueType::File),
        ),
        (
            "cUnavailableCopy".to_owned(),
            export_source("terminal", "failed", WorkflowValueType::File),
        ),
        (
            "dText".to_owned(),
            export_source("produce", "response", WorkflowValueType::Text),
        ),
        (
            "eText".to_owned(),
            export_source("produce", "response", WorkflowValueType::Text),
        ),
        (
            "fJson".to_owned(),
            export_source("produce", "result", WorkflowValueType::Json),
        ),
        (
            "gJson".to_owned(),
            export_source("produce", "result", WorkflowValueType::Json),
        ),
    ]);
    let destination = fixture.destination("aliases");

    publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    let (_, result) = read_result(&destination);

    assert_eq!(result["exports"]["aFile"], result["exports"]["bFile"]);
    assert_eq!(result["exports"]["dText"], result["exports"]["eText"]);
    assert_eq!(result["exports"]["fJson"], result["exports"]["gJson"]);
    assert_eq!(result["exports"]["aFile"]["path"], "exports/0001");
    assert_eq!(
        result["exports"]["cUnavailable"],
        result["exports"]["cUnavailableCopy"]
    );
    assert_eq!(result["exports"]["dText"]["path"], "exports/0005");
    assert_eq!(result["exports"]["fJson"]["path"], "exports/0007");
    assert_eq!(
        fs::read_dir(destination.join(EXPORT_DIRECTORY))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["0001".into(), "0005".into(), "0007".into()])
    );
}

#[test]
fn conflicting_values_for_one_captured_identity_are_rejected() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    run.export_sources.insert(
        "reporta".to_owned(),
        export_source("produce", "upper", WorkflowValueType::File),
    );
    let destination = fixture.destination("conflicting-alias");

    let failure = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap_err();

    assert_eq!(failure.phase(), LocalPublicationPhase::Serialization);
    assert_eq!(
        failure.kind(),
        LocalPublicationFailureKind::InvalidRunResult
    );
    assert!(!destination.exists());
}

#[test]
fn distinct_equal_captures_keep_distinct_carriers() {
    let fixture = PublicationFixture::new();
    let first = fixture.capture("first", "first.bin", "application/octet-stream", b"same");
    let second = fixture.capture("second", "second.bin", "application/octet-stream", b"same");
    let mut run = run_fixture(&fixture);
    run.exports = BTreeMap::from([
        (
            "first".to_owned(),
            ExportValue::Available {
                output: CapturedValue::file(first),
            },
        ),
        (
            "second".to_owned(),
            ExportValue::Available {
                output: CapturedValue::file(second),
            },
        ),
    ]);
    run.export_sources = BTreeMap::from([
        (
            "first".to_owned(),
            export_source("produce", "first", WorkflowValueType::File),
        ),
        (
            "second".to_owned(),
            export_source("produce", "second", WorkflowValueType::File),
        ),
    ]);
    let destination = fixture.destination("equal-captures");

    publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    let (_, result) = read_result(&destination);

    assert_eq!(result["exports"]["first"]["path"], "exports/0001");
    assert_eq!(result["exports"]["second"]["path"], "exports/0002");
    assert_eq!(
        result["exports"]["first"]["digest"],
        result["exports"]["second"]["digest"]
    );
    assert_eq!(fs::read(destination.join("exports/0001")).unwrap(), b"same");
    assert_eq!(fs::read(destination.join("exports/0002")).unwrap(), b"same");
    let first_metadata = fs::metadata(destination.join("exports/0001")).unwrap();
    let second_metadata = fs::metadata(destination.join("exports/0002")).unwrap();
    assert_ne!(
        (first_metadata.dev(), first_metadata.ino()),
        (second_metadata.dev(), second_metadata.ino()),
        "independent captures were coalesced by equal bytes"
    );
}

#[test]
fn equivalent_results_have_deterministic_export_mappings_and_serialization_semantics() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    make_failed(&mut run);
    let first = fixture.destination("first");
    let second = fixture.destination("second");

    let first_terminal = publish_workflow_result(&first, &fixture.artifacts, &run).unwrap();
    let second_terminal = publish_workflow_result(&second, &fixture.artifacts, &run).unwrap();
    let (first_bytes, first_result) = read_result(&first);
    let (second_bytes, second_result) = read_result(&second);

    assert_eq!(first_terminal.result(), second_terminal.result());
    assert_eq!(first_result, second_result);
    assert_eq!(first_bytes, second_bytes);
    let mut export_files = fs::read_dir(first.join(EXPORT_DIRECTORY))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    export_files.sort();
    assert_eq!(export_files, ["0001", "0003"]);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InjectedFailure {
    AfterFirstMaterialization,
    Serialization,
    Commit,
}

struct BoundaryObserver {
    failure: InjectedFailure,
    parent: PathBuf,
    destination: PathBuf,
    observed_complete_staging: bool,
}

impl PublicationObserver for BoundaryObserver {
    fn observe(&mut self, boundary: &PublicationBoundary) -> Result<(), ()> {
        assert!(!self.destination.exists());
        let stages = staging_paths(&self.parent);
        assert_eq!(stages.len(), 1);
        match boundary {
            PublicationBoundary::AfterExportMaterialization { export }
                if self.failure == InjectedFailure::AfterFirstMaterialization
                    && export == "reportA" =>
            {
                Err(())
            }
            PublicationBoundary::BeforeSerialization
                if self.failure == InjectedFailure::Serialization =>
            {
                Err(())
            }
            PublicationBoundary::StagingComplete => {
                let stage = &stages[0];
                assert!(stage.join(RESULT_FILE).is_file());
                assert_eq!(
                    fs::read(stage.join("exports/0001")).unwrap(),
                    b"upper report bytes"
                );
                self.observed_complete_staging = true;
                if self.failure == InjectedFailure::Commit {
                    Err(())
                } else {
                    Ok(())
                }
            }
            PublicationBoundary::StagingCreated
            | PublicationBoundary::BeforeExportMaterialization { .. }
            | PublicationBoundary::AfterExportMaterialization { .. }
            | PublicationBoundary::BeforeSerialization => Ok(()),
        }
    }
}

#[test]
fn injected_handoff_serialization_and_commit_failures_remove_result_staging() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);

    for (name, injected, expected_phase) in [
        (
            "handoff-failure",
            InjectedFailure::AfterFirstMaterialization,
            LocalPublicationPhase::ExportCopy,
        ),
        (
            "serialization-failure",
            InjectedFailure::Serialization,
            LocalPublicationPhase::Serialization,
        ),
        (
            "commit-failure",
            InjectedFailure::Commit,
            LocalPublicationPhase::Commit,
        ),
    ] {
        let destination = fixture.destination(name);
        let mut observer = BoundaryObserver {
            failure: injected,
            parent: fixture.results_parent.clone(),
            destination: destination.clone(),
            observed_complete_staging: false,
        };
        let failure = publish_with_observer(&destination, &fixture.artifacts, &run, &mut observer)
            .unwrap_err();

        assert_eq!(failure.phase(), expected_phase);
        assert!(!destination.exists());
        assert!(staging_paths(&fixture.results_parent).is_empty());
        assert_eq!(
            observer.observed_complete_staging,
            injected == InjectedFailure::Commit
        );
    }

    let ExportValue::Available { output } = &run.exports["reportA"] else {
        panic!("reportA must remain available");
    };
    let mut bytes = Vec::new();
    fixture
        .artifacts
        .copy_to(output.as_file().unwrap().handle(), &mut bytes)
        .unwrap();
    assert_eq!(bytes, b"upper report bytes");
    assert!(fixture.artifacts.staged_artifact_count() > 0);
    drop(run);
    assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
}

#[test]
fn failed_carrier_handoff_does_not_copy_or_destroy_private_staging() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);
    let destination = fixture.destination("handoff-failure");
    let ExportValue::Available { output } = &run.exports["reportA"] else {
        panic!("reportA must remain available");
    };
    let handle = output.as_file().unwrap().handle().clone();
    fixture.artifacts.block_artifact_links();

    let failure = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap_err();

    assert_eq!(failure.phase(), LocalPublicationPhase::ExportCopy);
    assert_eq!(
        failure.kind(),
        LocalPublicationFailureKind::CarrierHandoffUnavailable
    );
    assert_eq!(failure.export(), Some("reportA"));
    assert!(!destination.exists());
    assert!(staging_paths(&fixture.results_parent).is_empty());
    let mut retained = Vec::new();
    fixture.artifacts.copy_to(&handle, &mut retained).unwrap();
    assert_eq!(retained, b"upper report bytes");
    fixture.artifacts.release().unwrap();
    assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
    assert!(matches!(
        fixture.artifacts.copy_to(&handle, &mut Vec::new()),
        Err(ArtifactReadFailure::Unavailable | ArtifactReadFailure::UnknownHandle)
    ));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InjectedCloseFailure {
    Export,
    Result,
}

struct CloseFailureObserver {
    failure: InjectedCloseFailure,
}

impl PublicationObserver for CloseFailureObserver {
    fn close_staged_file(&mut self, file: File, staged_file: &StagedFile) -> io::Result<()> {
        close_file(file)?;
        let fail = match staged_file {
            StagedFile::Export { export } => {
                self.failure == InjectedCloseFailure::Export && export == "reportA"
            }
            StagedFile::Result => self.failure == InjectedCloseFailure::Result,
        };
        if fail {
            Err(io::Error::other("injected close failure"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn close_failures_retain_a_distinct_publication_phase_and_remove_staging() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    run.exports.insert(
        "reportA".to_owned(),
        ExportValue::Available {
            output: CapturedValue::text(Arc::from("in-memory text")),
        },
    );
    run.export_sources.insert(
        "reportA".to_owned(),
        export_source("produce", "response", WorkflowValueType::Text),
    );

    for (name, injected, expected_phase, expected_kind, expected_export) in [
        (
            "export-close-failure",
            InjectedCloseFailure::Export,
            LocalPublicationPhase::Close,
            LocalPublicationFailureKind::ExportWriteUnavailable,
            Some("reportA"),
        ),
        (
            "result-close-failure",
            InjectedCloseFailure::Result,
            LocalPublicationPhase::Close,
            LocalPublicationFailureKind::SerializationUnavailable,
            None,
        ),
    ] {
        let destination = fixture.destination(name);
        let mut observer = CloseFailureObserver { failure: injected };
        let failure = publish_with_observer(&destination, &fixture.artifacts, &run, &mut observer)
            .unwrap_err();

        assert_eq!(failure.phase(), expected_phase);
        assert_eq!(failure.kind(), expected_kind);
        assert_eq!(failure.export(), expected_export);
        assert!(!destination.exists());
        assert!(staging_paths(&fixture.results_parent).is_empty());
    }
}

struct DestinationRaceObserver {
    destination: PathBuf,
}

impl PublicationObserver for DestinationRaceObserver {
    fn observe(&mut self, boundary: &PublicationBoundary) -> Result<(), ()> {
        if matches!(boundary, PublicationBoundary::StagingComplete) {
            fs::create_dir(&self.destination).unwrap();
            fs::write(self.destination.join("owned.txt"), b"preexisting").unwrap();
        }
        Ok(())
    }
}

struct ExportDirectorySwapObserver {
    parent: PathBuf,
}

impl PublicationObserver for ExportDirectorySwapObserver {
    fn observe(&mut self, boundary: &PublicationBoundary) -> Result<(), ()> {
        if matches!(boundary, PublicationBoundary::StagingComplete) {
            let stages = staging_paths(&self.parent);
            assert_eq!(stages.len(), 1);
            let displaced = self.parent.join("displaced-exports");
            fs::rename(stages[0].join(EXPORT_DIRECTORY), &displaced).unwrap();
            fs::create_dir(stages[0].join(EXPORT_DIRECTORY)).unwrap();
        }
        Ok(())
    }
}

struct UnreferencedCarrierObserver {
    parent: PathBuf,
}

impl PublicationObserver for UnreferencedCarrierObserver {
    fn observe(&mut self, boundary: &PublicationBoundary) -> Result<(), ()> {
        if matches!(boundary, PublicationBoundary::StagingComplete) {
            let stages = staging_paths(&self.parent);
            assert_eq!(stages.len(), 1);
            fs::write(stages[0].join("exports/9999"), b"unreferenced").unwrap();
        }
        Ok(())
    }
}

#[test]
fn unreferenced_staged_carriers_are_rejected() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);
    let destination = fixture.destination("unreferenced-carrier");
    let mut observer = UnreferencedCarrierObserver {
        parent: fixture.results_parent.clone(),
    };

    let failure = publish_with_observer(&destination, &fixture.artifacts, &run, &mut observer)
        .expect_err("publication must reject a carrier absent from result metadata");

    assert_eq!(failure.phase(), LocalPublicationPhase::Verification);
    assert_eq!(
        failure.kind(),
        LocalPublicationFailureKind::VerificationUnavailable
    );
    assert!(!destination.exists());
}

#[test]
fn replaced_staged_exports_directory_is_rejected() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);
    let destination = fixture.destination("swapped-exports");
    let mut observer = ExportDirectorySwapObserver {
        parent: fixture.results_parent.clone(),
    };

    let failure = publish_with_observer(&destination, &fixture.artifacts, &run, &mut observer)
        .expect_err("publication must reject a replacement for the opened exports directory");

    assert_eq!(failure.phase(), LocalPublicationPhase::Verification);
    assert_eq!(
        failure.kind(),
        LocalPublicationFailureKind::VerificationUnavailable
    );
    assert!(!destination.exists());
    assert!(staging_paths(&fixture.results_parent).is_empty());
}

#[test]
fn semantic_outputs_publication_idempotence_accepts_identical_replay() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);
    let destination = fixture.destination("identical-republication");

    let first = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    let first_result = fs::read(destination.join("result.json")).unwrap();
    let first_export = fs::read(destination.join("exports/0001")).unwrap();
    let first_entries = fs::read_dir(&destination).unwrap().count();

    let replay = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();

    assert_eq!(replay, first);
    assert_eq!(
        fs::read(destination.join("result.json")).unwrap(),
        first_result
    );
    assert_eq!(
        fs::read(destination.join("exports/0001")).unwrap(),
        first_export
    );
    assert_eq!(fs::read_dir(&destination).unwrap().count(), first_entries);
    assert!(staging_paths(&fixture.results_parent).is_empty());
}

#[test]
fn semantic_outputs_publication_idempotence_preserves_first_conflict() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    let destination = fixture.destination("conflicting-republication");
    publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    let first_result = fs::read(destination.join("result.json")).unwrap();
    let first_export = fs::read(destination.join("exports/0001")).unwrap();

    run.attempt_number += 1;
    let failure = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap_err();
    assert_eq!(failure.phase(), LocalPublicationPhase::Commit);
    assert_eq!(failure.kind(), LocalPublicationFailureKind::ResultConflict);
    assert_eq!(
        fs::read(destination.join("result.json")).unwrap(),
        first_result
    );
    assert_eq!(
        fs::read(destination.join("exports/0001")).unwrap(),
        first_export
    );

    let racing = fixture.destination("racing");
    let mut observer = DestinationRaceObserver {
        destination: racing.clone(),
    };
    let failure =
        publish_with_observer(&racing, &fixture.artifacts, &run, &mut observer).unwrap_err();
    assert_eq!(failure.phase(), LocalPublicationPhase::Commit);
    assert_eq!(failure.kind(), LocalPublicationFailureKind::ResultConflict);
    assert_eq!(fs::read(racing.join("owned.txt")).unwrap(), b"preexisting");
    assert_eq!(fs::read_dir(&racing).unwrap().count(), 1);
    assert!(staging_paths(&fixture.results_parent).is_empty());
}

#[test]
fn publication_links_carriers_until_successful_private_cleanup() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);
    let destination = fixture.destination("released");
    let ExportValue::Available { output } = &run.exports["reportA"] else {
        panic!("reportA must be available");
    };
    let handle = output.as_file().unwrap().handle().clone();

    publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    let published_path = destination.join("exports/0001");
    assert_eq!(fs::read(&published_path).unwrap(), b"upper report bytes");
    assert_eq!(
        fs::metadata(&published_path).unwrap().nlink(),
        3,
        "publication allocated a copied carrier instead of linking private staging"
    );
    let mut retained = Vec::new();
    fixture.artifacts.copy_to(&handle, &mut retained).unwrap();
    assert_eq!(retained, b"upper report bytes");

    fixture.artifacts.release().unwrap();
    assert_eq!(
        fs::metadata(&published_path).unwrap().nlink(),
        1,
        "private carrier links remained after successful cleanup"
    );
    assert!(matches!(
        fixture.artifacts.copy_to(&handle, &mut Vec::new()),
        Err(ArtifactReadFailure::Unavailable | ArtifactReadFailure::UnknownHandle)
    ));
}

#[test]
fn parser_response_limit_failure_remains_valid_result_metadata() {
    let mut parser = PiJsonV1Parser::new(
        Arc::from("/execution/worktree"),
        AgentValueKind::Response,
        NonZeroU64::new(1).unwrap(),
        PiJsonV1ProtocolLimits::profile(),
        None,
    );
    let assistant = json!({
        "role": "assistant",
        "content": [{"type": "text", "text": "xx"}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {
                "input": 0,
                "output": 0,
                "cacheRead": 0,
                "cacheWrite": 0,
                "total": 0
            }
        },
        "stopReason": "pending",
        "timestamp": 2
    });
    for event in [
        json!({"type": "session", "version": 3, "id": "00000000-0000-4000-8000-00000000000d", "timestamp": "2026-07-30T12:00:00Z", "cwd": "/execution/worktree"}),
        json!({"type": "agent_start"}),
        json!({"type": "turn_start"}),
        json!({"type": "message_start", "message": assistant}),
    ] {
        let mut frame = serde_json::to_vec(&event).unwrap();
        frame.push(b'\n');
        if parser.push_stdout(&frame, drop).is_err() {
            break;
        }
    }
    let AgentOutcome::Failed(failure) = parser.finish(PiJsonV1ProcessCompletion::exited(false))
    else {
        panic!("an oversized response must fail");
    };
    assert_eq!(failure.cause(), &AgentFailureCause::CapturedValueTooLarge);
    assert!(failure.protocol_rejection().is_none());

    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    let cause = StepFailureCause::Execution(StepExecutionFailure::Agent(failure));
    let detail = canonical_failure_detail(FailurePhase::Execution, &cause).unwrap();
    run.steps[1].kind = WorkflowRunStepKind::Agent;
    run.steps[1].state = StepState::Failed {
        detail: detail.clone(),
    };
    run.steps[1].command_output = None;
    run.outcome = RunOutcome::Failed {
        primary_issue: PrimaryIssue::failed(
            WorkflowNode {
                id: "terminal".to_owned(),
                role: WorkflowNodeRole::Step,
            },
            detail,
        ),
        later_cancellation: None,
    };

    let destination = fixture.destination("captured-value-too-large");
    publish_workflow_result(&destination, &fixture.artifacts, &run)
        .expect("a response-limit failure must remain publishable");
    let (_, published) = read_result(&destination);
    let detail = published["steps"][1]["detail"].as_object().unwrap();
    assert_eq!(detail["code"], "captured_value_too_large");
    assert!(!detail.contains_key("protocolRejection"));
}

#[test]
fn structured_agent_failure_keeps_protocol_rejection_out_of_node_detail() {
    let mut parser =
        PiJsonV1Parser::profile(Arc::from("/execution/worktree"), AgentValueKind::None);
    let frames = concat!(
        "{\"type\":\"session\",\"version\":3,",
        "\"id\":\"00000000-0000-4000-8000-00000000000c\",",
        "\"timestamp\":\"2026-07-30T12:00:00Z\",",
        "\"cwd\":\"/execution/worktree\"}\n",
        "{\"type\":\"agent_start\"}\n",
        "{\"type\":\"agent_start\",\"content\":\"sensitive sentinel\"}\n",
    );
    assert!(parser.push_stdout(frames.as_bytes(), drop).is_err());
    let AgentOutcome::Failed(failure) = parser.finish(PiJsonV1ProcessCompletion::exited(false))
    else {
        panic!("invalid agent lifecycle must fail");
    };

    let source = StepFailureCause::Execution(StepExecutionFailure::Agent(failure));
    let projected = canonical_failure_detail(FailurePhase::Execution, &source).unwrap();
    let projected = serde_json::to_value(projected).unwrap();
    assert_eq!(projected["code"], "harness_protocol_failed");
    assert!(projected.get("protocolRejection").is_none());
    assert!(!projected.to_string().contains("sensitive sentinel"));
}

#[test]
fn incomplete_command_output_selects_local_failure_status_without_changing_outcome() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    run.steps[0].command_output = Some(diagnostic(false));
    let destination = fixture.destination("incomplete-output");

    let terminal = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    let value = serde_json::to_value(terminal).unwrap();

    assert_eq!(value["outcome"], "succeeded");
    assert_eq!(value["exitStatus"], 1);
    assert_eq!(
        value["result"]["steps"][0]["commandOutput"]["stdout"]["fullyDrained"],
        false
    );
}

#[test]
fn recovered_invocation_with_incomplete_diagnostics_selects_local_failure_status() {
    let fixture = PublicationFixture::new();
    let mut run = run_fixture(&fixture);
    let failure = FailureV1 {
        phase: FailurePhaseV1::Execution,
        cause: FailureCauseV1 {
            code: FailureCodeV1::CommandExit,
            input: None,
            collection_index: None,
            output: None,
            exit_code: Some(75),
        },
    };
    let incomplete = command_output_v1(&diagnostic(false)).unwrap();
    let complete = command_output_v1(&diagnostic(true)).unwrap();
    run.steps[0].recovery = Some(StepRecoverySummaryV1 {
        schema_version: 1,
        configured_retries: 1,
        handler_kind: None,
        rounds: vec![RecoveryRoundSummaryV1 {
            number: 1,
            failed_execution: RecoveryFailedExecutionV1 {
                execution_number: 1,
                invocation_id: 1,
                failure,
            },
            handler: None,
        }],
        termination: RecoveryTerminationV1::Recovered {
            execution_number: 2,
        },
    });
    run.steps[0].invocations = vec![
        RecoveryInvocationV1 {
            invocation_id: 1,
            role: RecoveryInvocationRoleV1::Target,
            target_execution: Some(1),
            recovery_round: None,
            state: RecoveryInvocationStateV1::Settled,
            started_at: "2026-08-02T12:01:44.01Z".to_owned(),
            finished_at: "2026-08-02T12:01:44.02Z".to_owned(),
            duration_milliseconds: 10,
            usage: RecoveryInvocationUsageV1::default(),
            diagnostics: vec![RecoveryInvocationDiagnosticV1 {
                kind: RecoveryDiagnosticKindV1::CommandStdout,
                reference: "attempts/000001/invocations/00000000000000000001/stdout.bin".to_owned(),
                stream: incomplete.stdout,
            }],
            diagnostic_reference: None,
        },
        RecoveryInvocationV1 {
            invocation_id: 2,
            role: RecoveryInvocationRoleV1::Target,
            target_execution: Some(2),
            recovery_round: None,
            state: RecoveryInvocationStateV1::Settled,
            started_at: "2026-08-02T12:01:44.021Z".to_owned(),
            finished_at: "2026-08-02T12:01:44.031Z".to_owned(),
            duration_milliseconds: 10,
            usage: RecoveryInvocationUsageV1::default(),
            diagnostics: vec![RecoveryInvocationDiagnosticV1 {
                kind: RecoveryDiagnosticKindV1::CommandStdout,
                reference: "attempts/000001/invocations/00000000000000000002/stdout.bin".to_owned(),
                stream: complete.stdout,
            }],
            diagnostic_reference: None,
        },
    ];
    let destination = fixture.destination("incomplete-recovery-invocation");

    let terminal = publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    let value = serde_json::to_value(terminal).unwrap();

    assert_eq!(value["outcome"], "succeeded");
    assert_eq!(
        value["result"]["steps"][0]["invocations"][0]["diagnostics"][0]["stream"]["fullyDrained"],
        false
    );
    assert_eq!(
        value["exitStatus"], 1,
        "an incomplete provisional invocation stream is a contracted local integrity failure"
    );
}
