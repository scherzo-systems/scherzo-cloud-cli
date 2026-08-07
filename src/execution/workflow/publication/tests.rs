use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, CaptureLimits, EnvironmentSnapshot, ExecutionContext,
    ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::artifact::{CaptureDeclaration, CapturedArtifact};
use crate::execution::workflow::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::OutputSet;

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
                    MAXIMUM_RETAINED_BYTES_PER_STREAM,
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
            .capture_files(&[CaptureDeclaration::new(
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
        step: step.to_owned(),
        output: output.to_owned(),
        value_type,
    }
}

fn succeeded_step(id: &str, outputs: OutputSet<CapturedValue>) -> WorkflowRunStep {
    WorkflowRunStep {
        id: id.to_owned(),
        kind: WorkflowRunStepKind::Command,
        state: StepState::Succeeded { outputs },
        timing: Some(WorkflowStepTiming {
            started_at: timestamp_fixture("2026-08-02T12:01:44.01Z"),
            duration: Duration::from_millis(240),
        }),
        command_output: Some(diagnostic(true)),
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
    run.steps[1].state = StepState::Failed {
        phase: FailurePhase::Execution,
        cause: cause.clone(),
    };
    run.outcome = RunOutcome::Failed {
        primary_failure: StepFailure {
            step: "terminal".to_owned(),
            phase: FailurePhase::Execution,
            cause,
        },
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
        reason: CancellationReason::UserRequest,
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
                output: CapturedValue::Text(Arc::from("agent response\n")),
            },
        ),
        (
            "result".to_owned(),
            ExportValue::Available {
                output: CapturedValue::Json(Arc::new(serde_json::json!({"z": 2, "a": 1}))),
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
    let text = Arc::<str>::from("shared response\n");
    let json = Arc::new(serde_json::json!({"answer": 42}));
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
                output: CapturedValue::Text(Arc::clone(&text)),
            },
        ),
        (
            "eText".to_owned(),
            ExportValue::Available {
                output: CapturedValue::Text(text),
            },
        ),
        (
            "fJson".to_owned(),
            ExportValue::Available {
                output: CapturedValue::Json(Arc::clone(&json)),
            },
        ),
        (
            "gJson".to_owned(),
            ExportValue::Available {
                output: CapturedValue::Json(json),
            },
        ),
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
    CopyAfterFirstFile,
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
            PublicationBoundary::AfterExportCopy { export }
                if self.failure == InjectedFailure::CopyAfterFirstFile && export == "reportA" =>
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
            | PublicationBoundary::BeforeExportCopy { .. }
            | PublicationBoundary::AfterExportCopy { .. }
            | PublicationBoundary::BeforeSerialization => Ok(()),
        }
    }
}

#[test]
fn injected_copy_serialization_and_commit_failures_remove_private_staging() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);

    for (name, injected, expected_phase) in [
        (
            "copy-failure",
            InjectedFailure::CopyAfterFirstFile,
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
    let run = run_fixture(&fixture);

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
fn existing_and_racing_destinations_are_never_replaced_or_merged() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);
    let existing = fixture.destination("existing");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("owned.txt"), b"preexisting").unwrap();

    let failure = publish_workflow_result(&existing, &fixture.artifacts, &run).unwrap_err();
    assert_eq!(failure.phase(), LocalPublicationPhase::TargetValidation);
    assert_eq!(
        failure.kind(),
        LocalPublicationFailureKind::DestinationExists
    );
    assert_eq!(
        fs::read(existing.join("owned.txt")).unwrap(),
        b"preexisting"
    );
    assert_eq!(fs::read_dir(&existing).unwrap().count(), 1);

    let racing = fixture.destination("racing");
    let mut observer = DestinationRaceObserver {
        destination: racing.clone(),
    };
    let failure =
        publish_with_observer(&racing, &fixture.artifacts, &run, &mut observer).unwrap_err();
    assert_eq!(failure.phase(), LocalPublicationPhase::Commit);
    assert_eq!(
        failure.kind(),
        LocalPublicationFailureKind::DestinationExists
    );
    assert_eq!(fs::read(racing.join("owned.txt")).unwrap(), b"preexisting");
    assert_eq!(fs::read_dir(&racing).unwrap().count(), 1);
    assert!(staging_paths(&fixture.results_parent).is_empty());
}

#[test]
fn caller_releases_artifacts_only_after_publication_has_copied_them() {
    let fixture = PublicationFixture::new();
    let run = run_fixture(&fixture);
    let destination = fixture.destination("released");
    let ExportValue::Available { output } = &run.exports["reportA"] else {
        panic!("reportA must be available");
    };
    let handle = output.as_file().unwrap().handle().clone();

    publish_workflow_result(&destination, &fixture.artifacts, &run).unwrap();
    assert_eq!(
        fs::read(destination.join("exports/0001")).unwrap(),
        b"upper report bytes"
    );
    let mut retained = Vec::new();
    fixture.artifacts.copy_to(&handle, &mut retained).unwrap();
    assert_eq!(retained, b"upper report bytes");

    fixture.artifacts.release().unwrap();
    assert!(matches!(
        fixture.artifacts.copy_to(&handle, &mut Vec::new()),
        Err(ArtifactReadFailure::Unavailable | ArtifactReadFailure::UnknownHandle)
    ));
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
