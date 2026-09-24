use std::collections::BTreeMap;
use std::fs::{self, Permissions};
use std::num::NonZeroU64;
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::json;
use std::time::Duration;

use super::*;
use crate::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, CaptureLimits, EnvironmentSnapshot,
    ExecutionContext, ExecutionPolicyLimits, InputLimits, ResolvedAttachment, ResolvedFile,
    ResolvedInput, ResolvedInputs, ResolvedJsonInput, admit_workflow,
};
use crate::workflow::archived_attempt::{
    ArchivedAttemptIneligibilityReason, ArchivedAttemptLoadError,
    ArchivedAttemptOperationalErrorCode, ArchivedAttemptState, ArchivedCancellationReason,
    ArchivedStepDetail, ArchivedWorkflowOutcome, load_local_archived_attempt,
    load_local_archived_attempt_observed,
};
use crate::workflow::resolution;

struct AdmittedFixture {
    _temporary: tempfile::TempDir,
    admitted: AdmittedWorkflow,
    execution_root: PathBuf,
    run_parent: PathBuf,
}

impl AdmittedFixture {
    fn new() -> Self {
        Self::new_with_environment(EnvironmentSnapshot::default())
    }

    fn new_with_environment(environment: EnvironmentSnapshot) -> Self {
        Self::from_source_with_inputs_and_environment(
            "schemaVersion: 1\ninputs:\n  request: {kind: text}\n  settings: {kind: json}\n  evidence: {kind: attachments}\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\n",
            ResolvedInputs::new(BTreeMap::from([
                (
                    "request".to_owned(),
                    ResolvedInput::Text(Arc::from("durable request\n")),
                ),
                (
                    "evidence".to_owned(),
                    ResolvedInput::Attachments(Arc::from([ResolvedAttachment::new(
                        Arc::from("application/octet-stream"),
                        Arc::from([0_u8, 1, 0xff]),
                    )])),
                ),
                (
                    "settings".to_owned(),
                    ResolvedInput::Json(
                        ResolvedJsonInput::from_source(Arc::from(
                            b"{ \"z\": null, \"n\": 1.2300 }\n".as_slice(),
                        ))
                        .unwrap(),
                    ),
                ),
            ])),
            1024,
            environment,
        )
    }

    fn from_source(source: &str) -> Self {
        Self::from_source_with_inputs(source, ResolvedInputs::default(), 1024)
    }

    fn from_source_with_maximum_step_log_bytes(source: &str, maximum_step_log_bytes: u64) -> Self {
        Self::from_source_with_inputs(source, ResolvedInputs::default(), maximum_step_log_bytes)
    }

    fn from_source_with_inputs(
        source: &str,
        inputs: ResolvedInputs,
        maximum_step_log_bytes: u64,
    ) -> Self {
        Self::from_source_with_inputs_and_environment(
            source,
            inputs,
            maximum_step_log_bytes,
            EnvironmentSnapshot::default(),
        )
    }

    fn from_source_with_inputs_and_environment(
        source: &str,
        inputs: ResolvedInputs,
        maximum_step_log_bytes: u64,
        environment: EnvironmentSnapshot,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let run_parent = temporary.path().join("runs");
        for directory in [&source_root, &execution_root, &run_parent] {
            fs::create_dir(directory).unwrap();
        }
        fs::write(source_root.join("workflow.yaml"), source).unwrap();
        let workflow = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let admitted = admit_workflow(
            workflow,
            inputs,
            ExecutionContext::new(
                execution_root.clone(),
                ExecutionPolicyLimits::new(
                    2,
                    CaptureLimits::new(16, 1024, 4096),
                    InputLimits::new(16, 1024, 4096, 4096),
                    maximum_step_log_bytes,
                ),
                environment,
                CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
            ),
        )
        .unwrap();
        Self {
            _temporary: temporary,
            admitted,
            execution_root,
            run_parent,
        }
    }

    fn run_path(&self, name: &str) -> PathBuf {
        self.run_parent.join(name)
    }
}

struct AssertLockedPublication {
    observed: bool,
}

impl InitialPublicationObserver for AssertLockedPublication {
    fn published(&mut self, path: &Path, owner_lock: &File) -> Result<(), LocalRunDirectoryError> {
        assert!(path.join(LOCK_FILE).is_file());
        assert!(owner_lock.metadata().unwrap().is_file());
        self.observed = true;
        Ok(())
    }
}

#[test]
fn retry_ineligibility_uses_the_closed_precedence_order() {
    let ownership_unproven = LocalRecoveryStatus::OwnershipUnproven {
        guard_ids: vec!["11111111-1111-4111-8111-111111111111".to_owned()],
        reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
    };
    assert_eq!(
        retry_eligibility(AttemptStateV1::Succeeded, &ownership_unproven, true),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::RunLocked)
    );
    assert_eq!(
        retry_eligibility(AttemptStateV1::Succeeded, &ownership_unproven, false),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::OwnershipUnproven)
    );
    assert_eq!(
        retry_eligibility(
            AttemptStateV1::Succeeded,
            &LocalRecoveryStatus::Settled,
            false,
        ),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::LatestAttemptSucceeded)
    );
    assert_eq!(
        retry_eligibility(
            AttemptStateV1::Rejected,
            &LocalRecoveryStatus::Settled,
            false,
        ),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::LatestAttemptRejected)
    );
    for state in [
        AttemptStateV1::WorkflowFailed,
        AttemptStateV1::Cancelled,
        AttemptStateV1::Interrupted,
    ] {
        assert_eq!(
            retry_eligibility(state, &LocalRecoveryStatus::Settled, false),
            LocalRetryEligibility::Eligible
        );
    }
}

struct FixtureRecoveryAuthority {
    host: Result<ExecutionHostV1, ()>,
    observation: ProcessIdentityObservation,
}

impl LocalRecoveryAuthority for FixtureRecoveryAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        self.host.clone()
    }

    fn observe_process(&self, _guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        self.observation
    }
}

fn fixture_guarded_attempt() -> LocalAttemptV1 {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("guarded"), &fixture.admitted).unwrap();
    let mut attempt = read_state(run.root_handle()).unwrap().attempts.remove(0);
    attempt.process_guards.push(ProcessGuardV1 {
        guard_id: "11111111-1111-4111-8111-111111111111".to_owned(),
        action_id: 1,
        step_id: "first".to_owned(),
        node_role: AttemptNodeRoleV1::Step,
        state: ProcessGuardStateV1::Released,
        execution_host: attempt.owner.execution_host.clone(),
        process_group_id: 41,
        liveness: ProcessLivenessV1 {
            kind: ProcessLivenessKindV1::LeaderStartIdentity,
            value: "9001".to_owned(),
        },
    });
    attempt
}

#[test]
fn deterministic_recovery_fixtures_classify_exact_absent_and_lost_inspection() {
    let attempt = fixture_guarded_attempt();
    let host = attempt.owner.execution_host.clone();
    let guard_ids = vec!["11111111-1111-4111-8111-111111111111".to_owned()];

    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Ok(host.clone()),
                observation: ProcessIdentityObservation::Exact {
                    leader: crate::workflow::process_group::LeaderState::Running,
                },
            },
        ),
        LocalRecoveryStatus::Abandoned
    );
    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Ok(host.clone()),
                observation: ProcessIdentityObservation::Absent,
            },
        ),
        LocalRecoveryStatus::Abandoned
    );
    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Ok(host),
                observation: ProcessIdentityObservation::Unavailable,
            },
        ),
        LocalRecoveryStatus::OwnershipUnproven {
            guard_ids,
            reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
        }
    );
}

#[test]
fn host_restart_proves_old_work_absent_without_process_inspection() {
    let attempt = fixture_guarded_attempt();
    let restarted_host = ExecutionHostV1 {
        kind: ExecutionHostKindV1::HostBoot,
        value: "22222222-2222-4222-8222-222222222222".to_owned(),
    };

    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Ok(restarted_host),
                observation: ProcessIdentityObservation::Unavailable,
            },
        ),
        LocalRecoveryStatus::Abandoned
    );
    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Err(()),
                observation: ProcessIdentityObservation::Absent,
            },
        ),
        LocalRecoveryStatus::OwnershipUnproven {
            guard_ids: vec!["11111111-1111-4111-8111-111111111111".to_owned()],
            reason: OwnershipUnprovenReason::ExecutionHostIdentityUnavailable,
        }
    );
}

#[test]
fn attempt_directory_names_are_exact_at_the_six_digit_boundary() {
    assert_eq!(attempt_directory_name(0), None);
    assert_eq!(attempt_directory_name(1).as_deref(), Some("000001"));
    assert_eq!(attempt_directory_name(42).as_deref(), Some("000042"));
    assert_eq!(attempt_directory_name(999_999).as_deref(), Some("999999"));
    assert_eq!(
        attempt_directory_name(1_000_000).as_deref(),
        Some("1000000")
    );
    assert_eq!(
        attempt_directory_name(u64::MAX).as_deref(),
        Some("18446744073709551615")
    );
}

#[test]
fn retained_read_budget_enforces_the_component_derivation() {
    assert_eq!(
        MAXIMUM_RETAINED_TOTAL_BYTES,
        MAXIMUM_RETAINED_CAPTURED_FILE_BYTES
            + crate::workflow::result_metadata::MAXIMUM_ENCODED_RETAINED_STREAM_BYTES
            + MAXIMUM_RETAINED_RUN_JSON_BYTES
    );

    let mut captured_file_bytes = MAXIMUM_RETAINED_CAPTURED_FILE_BYTES - 1;
    account_retained_bytes(
        &mut captured_file_bytes,
        1,
        MAXIMUM_RETAINED_CAPTURED_FILE_BYTES,
    )
    .unwrap();
    assert_eq!(
        account_retained_bytes(
            &mut captured_file_bytes,
            1,
            MAXIMUM_RETAINED_CAPTURED_FILE_BYTES,
        ),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut budget = RetainedReadBudget::with_bytes(MAXIMUM_RETAINED_TOTAL_BYTES - 1).unwrap();
    budget.account(&[0]).unwrap();
    assert_eq!(
        budget.account(&[0]),
        Err(LocalRunDirectoryError::StateInvalid)
    );
}

#[test]
fn retained_manifest_rejects_source_files_out_of_canonical_order() {
    let source_file = |path: &str, ordinal: u64| ManifestSourceFileV1 {
        path: path.to_owned(),
        file: ManifestFileV1 {
            ordinal,
            relative_file: format!("files/{ordinal:04}"),
            size_bytes: 0,
            digest: DigestV1::sha256(&[]),
        },
    };
    let manifest = WorkflowManifestV1 {
        schema_version: 1,
        workflow_path: "workflow.yaml".to_owned(),
        source_root: "/source".to_owned(),
        maximum_parallel_steps: 1,
        source_files: vec![
            source_file("workflow.yaml", 1),
            source_file("auxiliary.txt", 2),
        ],
        inputs: BTreeMap::new(),
    };

    assert_eq!(
        validate_manifest(&manifest),
        Err(LocalRunDirectoryError::SerializationUnavailable)
    );
}

#[test]
fn retained_manifest_rejects_duplicate_named_input_keys() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("duplicate-input-key");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    drop(run);

    let manifest_path = run_path.join("workflow/manifest.json");
    let manifest_bytes = fs::read(&manifest_path).unwrap();
    let manifest: WorkflowManifestV1 = decode_schema_one(&manifest_bytes).unwrap();
    let request = serde_json::to_string(&manifest.inputs["request"]).unwrap();
    let original_entry = format!("\"request\":{request}");
    let mut duplicate_manifest = serde_json::to_string(&manifest).unwrap();
    assert_eq!(duplicate_manifest.matches(&original_entry).count(), 1);
    duplicate_manifest = duplicate_manifest.replacen(
        &original_entry,
        &format!("{original_entry},{original_entry}"),
        1,
    );
    duplicate_manifest.push('\n');
    fs::set_permissions(&manifest_path, Permissions::from_mode(0o600)).unwrap();
    fs::write(&manifest_path, duplicate_manifest.as_bytes()).unwrap();

    let run_file = run_path.join(RUN_FILE);
    let mut run_document: LocalRunV1 = decode_schema_one(&fs::read(&run_file).unwrap()).unwrap();
    run_document.workflow_manifest_digest = DigestV1::sha256(duplicate_manifest.as_bytes());
    fs::set_permissions(&run_file, Permissions::from_mode(0o600)).unwrap();
    fs::write(&run_file, encode_json(&run_document).unwrap()).unwrap();

    match acquire_local_retry(&run_path) {
        Err(LocalRunDirectoryError::StateInvalid) => {}
        Err(other) => panic!("duplicate named input produced the wrong failure: {other:?}"),
        Ok(_) => panic!("retry accepted a retained manifest with a duplicate named input key"),
    }
    assert!(!run_path.join("attempts/000002").exists());
}

#[test]
fn initial_publication_retains_the_staging_lock_and_immutable_execution_bytes() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retained");
    let mut observer = AssertLockedPublication { observed: false };

    let run = create_with_observer(&run_path, &fixture.admitted, &mut observer).unwrap();

    assert!(observer.observed);
    assert_eq!(run.run_directory(), fs::canonicalize(&run_path).unwrap());
    assert_eq!(
        fs::metadata(&run_path).unwrap().permissions().mode() & 0o7777,
        0o700
    );
    assert!(run_path.join(RUN_FILE).is_file());
    assert!(run_path.join(STATE_FILE).is_file());
    assert!(run_path.join(LOCK_FILE).is_file());
    assert!(run_path.join("attempts/000001").is_dir());
    assert!(!run.result_directory().exists());

    let manifest: Value =
        serde_json::from_slice(&fs::read(run_path.join("workflow/manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["schemaVersion"], 1);
    assert_eq!(manifest["workflowPath"], "workflow.yaml");
    assert_eq!(manifest["maximumParallelSteps"], 2);
    assert_eq!(manifest["sourceFiles"][0]["relativeFile"], "files/0001");
    assert_eq!(
        manifest["inputs"]["evidence"]["items"][0]["relativeFile"],
        "files/0002"
    );
    assert_eq!(manifest["inputs"]["request"]["relativeFile"], "files/0003");
    assert_eq!(manifest["inputs"]["settings"]["relativeFile"], "files/0004");
    assert_eq!(
        fs::read(run_path.join("workflow/files/0001")).unwrap(),
        fixture.admitted.workflow().source_closure["workflow.yaml"].as_ref()
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0002")).unwrap(),
        [0_u8, 1, 0xff]
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0003")).unwrap(),
        b"durable request\n"
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0004")).unwrap(),
        b"{ \"z\": null, \"n\": 1.2300 }\n"
    );

    let state = read_state(run.root_handle()).unwrap();
    assert_eq!(state.revision, 1);
    assert_eq!(state.attempts[0].state, AttemptStateV1::Created);
    assert_eq!(state.attempts[0].progress.steps[0].id, "first");
    assert_eq!(state.attempts[0].progress.steps[1].id, "second");

    drop(run);
}

#[test]
fn singular_file_input_is_retained_and_reused_for_retry() {
    let fixture = AdmittedFixture::from_source_with_inputs(
        "schemaVersion: 1\ninputs:\n  payload: {kind: file, mediaType: application/octet-stream}\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"true\"]}\n",
        ResolvedInputs::new(BTreeMap::from([(
            "payload".to_owned(),
            ResolvedInput::File(ResolvedFile::new(
                Arc::from("application/octet-stream"),
                Arc::from([0_u8, 0xff, 7]),
            )),
        )])),
        1024,
    );
    let run_path = fixture.run_path("retained-file");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    let manifest: Value =
        serde_json::from_slice(&fs::read(run_path.join("workflow/manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["inputs"]["payload"]["kind"], "file");
    assert_eq!(
        manifest["inputs"]["payload"]["mediaType"],
        "application/octet-stream"
    );
    assert_eq!(manifest["inputs"]["payload"]["relativeFile"], "files/0002");
    assert_eq!(
        fs::read(run_path.join("workflow/files/0002")).unwrap(),
        [0_u8, 0xff, 7]
    );

    settle_as_workflow_failed(&initial);
    drop(initial);
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed File-input attempt should be retryable");
    };
    let Some(ResolvedInput::File(file)) = pending.execution_specification().1.get("payload") else {
        panic!("retained File input is missing");
    };
    assert_eq!(file.media_type(), "application/octet-stream");
    assert_eq!(file.bytes(), [0_u8, 0xff, 7]);
}

#[test]
fn run_target_validation_rejects_overlap_in_both_path_directions() {
    assert!(paths_overlap(
        Path::new("/workspace/run"),
        Path::new("/workspace/run/execution")
    ));
    assert!(paths_overlap(
        Path::new("/workspace/execution/run"),
        Path::new("/workspace/execution")
    ));
    assert!(!paths_overlap(
        Path::new("/workspace/runs/run"),
        Path::new("/workspace/execution")
    ));

    let fixture = AdmittedFixture::new();
    let disjoint_lexical_run = fixture.run_path("aliased");
    let aliased_parent = open_directory_path(&fixture.execution_root).unwrap();
    assert!(!paths_overlap(
        &disjoint_lexical_run,
        &fixture.execution_root
    ));
    assert!(
        run_directory_overlaps_execution_root(
            &disjoint_lexical_run,
            &fixture.execution_root,
            fixture.admitted.execution().root_identity(),
            &aliased_parent,
        )
        .unwrap(),
        "directory identity must reject an alias that lexical paths miss"
    );

    let nested_run = fixture.execution_root.join("run");
    let failure = match InitialLocalRun::create(&nested_run, &fixture.admitted) {
        Ok(_) => panic!("an overlapping run directory must be rejected"),
        Err(failure) => failure,
    };
    assert_eq!(failure, LocalRunDirectoryError::ExecutionRootOverlap);
    assert!(!nested_run.exists());
}

#[test]
fn closed_durable_documents_reject_versions_fields_nulls_and_corruption() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("closed"), &fixture.admitted).unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let state = read_state(run.root_handle()).unwrap();

    let mut value = serde_json::to_value(&run_document).unwrap();
    value["schemaVersion"] = Value::from(2);
    assert_eq!(
        decode_run(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );
    let mut value = serde_json::to_value(&run_document).unwrap();
    value["unknown"] = Value::Bool(true);
    assert_eq!(
        decode_run(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );

    let mut value = serde_json::to_value(&state).unwrap();
    value["schemaVersion"] = Value::from(99);
    assert_eq!(
        decode_state(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );
    let mut value = serde_json::to_value(&state).unwrap();
    value["attempts"][0]["owner"]["unknown"] = Value::Bool(true);
    assert_eq!(
        decode_state(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );
    let mut value = serde_json::to_value(&state).unwrap();
    value["attempts"][0]["startedAt"] = Value::Null;
    assert_eq!(
        decode_state(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );
    let mut value = serde_json::to_value(&state).unwrap();
    value["attempts"][0]["executionRoot"] = Value::String("/tmp/../tmp".to_owned());
    assert_eq!(
        decode_state(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );
    let mut value = serde_json::to_value(&state).unwrap();
    value["attempts"][0]["state"] = Value::String("future_state".to_owned());
    assert_eq!(
        decode_state(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );
    assert_eq!(
        decode_state(b"{\"schemaVersion\":1").unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );
}

struct PartialTemporaryWrite;

impl StateCommitObserver for PartialTemporaryWrite {
    fn write_temporary(&mut self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(&bytes[..bytes.len() / 2])?;
        Err(io::Error::other("injected process loss"))
    }
}

struct FailBeforeReplace;

impl StateCommitObserver for FailBeforeReplace {
    fn temporary_complete(&mut self) -> Result<(), LocalRunDirectoryError> {
        Err(LocalRunDirectoryError::StateWriteUnavailable)
    }
}

struct CorruptBeforeReplace {
    state_path: PathBuf,
}

impl StateCommitObserver for CorruptBeforeReplace {
    fn temporary_complete(&mut self) -> Result<(), LocalRunDirectoryError> {
        fs::write(&self.state_path, b"partial")
            .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)
    }
}

struct FailAfterReplace;

impl StateCommitObserver for FailAfterReplace {
    fn replaced(&mut self) -> Result<(), LocalRunDirectoryError> {
        Err(LocalRunDirectoryError::StateWriteUnavailable)
    }
}

#[test]
fn invocation_evidence_republication_preserves_first_authoritative_bytes() {
    let temporary = tempfile::tempdir().unwrap();
    let directory = open_directory_path(temporary.path()).unwrap();
    let original = b"first authoritative diagnostic";

    write_or_verify_immutable_file(&directory, "diagnostic.bin", original).unwrap();
    let first = fs::read(temporary.path().join("diagnostic.bin")).unwrap();
    write_or_verify_immutable_file(&directory, "diagnostic.bin", original).unwrap();
    assert_eq!(
        fs::read(temporary.path().join("diagnostic.bin")).unwrap(),
        first
    );

    assert_eq!(
        write_or_verify_immutable_file(&directory, "diagnostic.bin", b"changed"),
        Err(LocalRunDirectoryError::StateConflict)
    );
    assert_eq!(
        fs::read(temporary.path().join("diagnostic.bin")).unwrap(),
        first
    );
}

#[test]
fn atomic_state_crash_boundaries_expose_only_complete_snapshots() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("atomic"), &fixture.admitted).unwrap();
    let before = read_state(run.root_handle()).unwrap();
    let mutate = |state: &mut LocalRunStateV1| {
        append_diagnostic(
            state,
            INITIAL_ATTEMPT_NUMBER,
            DiagnosticCodeV1::StaleOccurrence,
        )
    };

    let failure = run
        .state
        .update_with_observer(mutate, &mut PartialTemporaryWrite)
        .unwrap_err();
    assert_eq!(failure, LocalRunDirectoryError::StateWriteUnavailable);
    assert_eq!(read_state(run.root_handle()).unwrap(), before);

    let failure = run
        .state
        .update_with_observer(mutate, &mut FailBeforeReplace)
        .unwrap_err();
    assert_eq!(failure, LocalRunDirectoryError::StateWriteUnavailable);
    assert_eq!(read_state(run.root_handle()).unwrap(), before);

    let failure = run
        .state
        .update_with_observer(mutate, &mut FailAfterReplace)
        .unwrap_err();
    assert_eq!(failure, LocalRunDirectoryError::StateWriteUnavailable);
    let after = read_state(run.root_handle()).unwrap();
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(after.diagnostics.len(), 1);
    assert_eq!(*lock_state(&run.state.current).unwrap(), after);
    assert!(decode_state(&encode_json(&after).unwrap()).is_ok());

    fs::set_permissions(
        run.run_directory().join(STATE_FILE),
        Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::write(run.run_directory().join(STATE_FILE), b"{partial").unwrap();
    assert_eq!(
        read_state(run.root_handle()).unwrap_err(),
        LocalRunDirectoryError::StateInvalid
    );
}

#[test]
fn locked_state_store_replaces_without_reloading_disk() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("concurrent"), &fixture.admitted).unwrap();
    let mut observer = CorruptBeforeReplace {
        state_path: run.run_directory().join(STATE_FILE),
    };

    run.state
        .update_with_observer(
            |state| {
                append_diagnostic(
                    state,
                    INITIAL_ATTEMPT_NUMBER,
                    DiagnosticCodeV1::StaleOccurrence,
                )
            },
            &mut observer,
        )
        .unwrap();
    let state = read_state(run.root_handle()).unwrap();
    assert_eq!(state.diagnostics.len(), 1);
    assert_eq!(*lock_state(&run.state.current).unwrap(), state);
}

#[test]
fn oversized_state_update_preserves_the_last_readable_snapshot() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("oversized"), &fixture.admitted).unwrap();
    let before = read_state(run.root_handle()).unwrap();
    let oversized_id = "x".repeat(usize::try_from(MAXIMUM_DURABLE_JSON_BYTES).unwrap());
    let failure = run.state.update(|state| {
        state.attempts[0].progress.steps[0].id = oversized_id;
        // The document is otherwise valid; only its encoded size exceeds the reader limit.
        validate_state(state)
    });

    assert_eq!(failure, Err(LocalRunDirectoryError::StateInvalid));
    assert_eq!(read_state(run.root_handle()).unwrap(), before);
    assert_eq!(*lock_state(&run.state.current).unwrap(), before);
}

struct NoExchange;

impl StateCommitObserver for NoExchange {
    fn exchange(
        &mut self,
        _private: &OwnedFd,
        _temporary_name: &str,
        _root: &OwnedFd,
    ) -> rustix::io::Result<()> {
        // Filesystems without exchange support return EOPNOTSUPP.
        Err(Errno::OPNOTSUPP)
    }
}

#[test]
fn unsupported_atomic_exchange_leaves_the_last_snapshot_authoritative() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("no-exchange"), &fixture.admitted).unwrap();
    let before = read_state(run.root_handle()).unwrap();
    assert_eq!(
        run.state.update_with_observer(
            |state| append_diagnostic(
                state,
                INITIAL_ATTEMPT_NUMBER,
                DiagnosticCodeV1::StaleOccurrence
            ),
            &mut NoExchange,
        ),
        Err(LocalRunDirectoryError::AtomicCommitUnavailable)
    );
    assert_eq!(read_state(run.root_handle()).unwrap(), before);
    assert_eq!(*lock_state(&run.state.current).unwrap(), before);
}

#[test]
fn guard_preparation_is_durable_before_release_and_quiescence_is_one_later_commit() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("guard-batch"), &fixture.admitted).unwrap();
    let initial_revision = read_state(run.root_handle()).unwrap().revision;
    let store = run.process_guard_registry();
    for (action_id, step) in [(1, "first"), (2, "second")] {
        run.state
            .update(|state| {
                let attempt = current_attempt_mut(state)?;
                attempt.progress.last_transition_sequence = action_id;
                attempt
                    .progress
                    .outstanding_actions
                    .push(OutstandingActionV1 {
                        action_id,
                        kind: OutstandingActionKindV1::StartStep,
                        step_id: Some(step.to_owned()),
                        node_role: Some(AttemptNodeRoleV1::Step),
                        target_execution: Some(1),
                        recovery_round: None,
                    });
                Ok(())
            })
            .unwrap();
        let identity = AuthenticatedProcessGroup::new(
            rustix::process::Pid::from_raw(41 + i32::try_from(action_id).unwrap()).unwrap(),
            "9001".to_owned(),
        )
        .unwrap();
        let mut registration = store.register(step, action_id, &identity).unwrap();
        let prepared = read_state(run.root_handle()).unwrap();
        assert_eq!(prepared.revision, initial_revision + 3 * action_id - 1);
        assert_eq!(
            prepared.attempts[0].process_guards.last().unwrap().state,
            ProcessGuardStateV1::Prepared
        );
        registration.mark_released().unwrap();
        let during = read_state(run.root_handle()).unwrap();
        assert_eq!(during, prepared);
        let guard = during.attempts[0].process_guards.last().unwrap();
        assert_eq!(
            recovery_status_with(
                &during.attempts[0],
                false,
                &FixtureRecoveryAuthority {
                    host: Ok(guard.execution_host.clone()),
                    observation: ProcessIdentityObservation::Unavailable,
                }
            ),
            LocalRecoveryStatus::OwnershipUnproven {
                guard_ids: vec![guard.guard_id.clone()],
                reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
            }
        );
        if action_id == 2 {
            let failure = run.state.update_with_observer(
                |state| {
                    current_attempt_mut(state)?
                        .process_guards
                        .last_mut()
                        .unwrap()
                        .state = ProcessGuardStateV1::Quiesced;
                    Ok(())
                },
                &mut FailBeforeReplace,
            );
            assert_eq!(failure, Err(LocalRunDirectoryError::StateWriteUnavailable));
            assert_eq!(read_state(run.root_handle()).unwrap(), prepared);
        }
        registration.mark_quiesced().unwrap();
        let after = read_state(run.root_handle()).unwrap();
        assert_eq!(after.revision, prepared.revision + 1);
        assert_eq!(
            after.attempts[0].process_guards.last().unwrap().state,
            ProcessGuardStateV1::Quiesced
        );
    }
}

fn fixture_settlement_snapshot() -> WorkspaceSnapshotV1 {
    capture_settlement_snapshot(
        Path::new("/fixture-workspace-is-unavailable"),
        WorkspaceSnapshotSettlementV1::Engine,
    )
}

fn settle_as_workflow_failed(run: &InitialLocalRun) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::WorkflowFailed;
            attempt.progress.steps[0].state = AttemptStepStateV1::Failed;
            attempt.progress.steps[0].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(23),
                )
                .unwrap(),
            ));
            attempt.progress.steps[1].state = AttemptStepStateV1::Blocked;
            attempt.progress.steps[1].detail =
                Some(crate::workflow::evidence::NodeDetail::Blocked(
                    crate::workflow::evidence::BlockedDetail::new([
                        crate::workflow::evidence::Prerequisite::control("first").unwrap(),
                    ])
                    .unwrap(),
                ));
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

#[test]
fn schema_one_documents_without_continuation_foundation_fields_remain_readable() {
    let fixture = AdmittedFixture::new();
    let run =
        InitialLocalRun::create(&fixture.run_path("schema-one-old"), &fixture.admitted).unwrap();
    settle_as_succeeded(&run);

    let mut run_document: serde_json::Value =
        serde_json::from_slice(&read_regular_file(run.root_handle(), RUN_FILE).unwrap()).unwrap();
    run_document.as_object_mut().unwrap().remove("gitBaseline");
    assert!(decode_run(&json_bytes(run_document)).is_ok());

    let mut state_document: serde_json::Value =
        serde_json::from_slice(&read_regular_file(run.root_handle(), STATE_FILE).unwrap()).unwrap();
    let attempt = state_document["attempts"][0].as_object_mut().unwrap();
    attempt.remove("definition");
    attempt.remove("settlementSnapshot");
    for step in attempt["progress"]["steps"].as_array_mut().unwrap() {
        step.as_object_mut().unwrap().remove("outputs");
    }
    assert!(decode_state(&json_bytes(state_document)).is_ok());
}

#[test]
fn archived_attempt_loads_schema_one_state_without_retained_output_foundation() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("schema-one-old-archive");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    publish_result_fixture(&fixture, &run);

    let mut state: Value =
        serde_json::from_slice(&read_regular_file(run.root_handle(), STATE_FILE).unwrap()).unwrap();
    let attempt = state["attempts"][0].as_object_mut().unwrap();
    attempt.remove("definition");
    attempt.remove("settlementSnapshot");
    for step in attempt["progress"]["steps"].as_array_mut().unwrap() {
        step.as_object_mut().unwrap().remove("outputs");
    }
    fs::write(run_path.join(STATE_FILE), json_bytes(state)).unwrap();

    let archived = load_local_archived_attempt(&run_path, None).unwrap();
    assert_eq!(archived.attempt_number, 1);
    assert_eq!(archived.state, ArchivedAttemptState::WorkflowFailed);
}

#[test]
fn run_retains_original_git_baseline_for_descendant_retries() {
    let fixture = AdmittedFixture::new_with_environment(EnvironmentSnapshot::new([(
        "PATH",
        std::env::var_os("PATH").unwrap(),
    )]));
    let git = |arguments: &[&str]| {
        let output = std::process::Command::new("git")
            .args(arguments)
            .current_dir(&fixture.execution_root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    };
    git(&["init", "--quiet"]);
    git(&["config", "user.name", "Baseline Fixture"]);
    git(&["config", "user.email", "baseline@example.invalid"]);
    fs::write(fixture.execution_root.join("tracked"), b"original\n").unwrap();
    git(&["add", "tracked"]);
    git(&["commit", "--quiet", "-m", "original"]);
    let original = String::from_utf8(git(&["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();

    let run_path = fixture.run_path("git-baseline");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    let retained = read_run(run.root_handle()).unwrap();
    assert_eq!(
        retained.git_baseline,
        Some(GitBaselineV1::Available {
            object_format: "sha1".to_owned(),
            commit_oid: original.clone(),
        })
    );
    settle_as_workflow_failed(&run);
    drop(run);
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let baseline = pending.git_baseline().unwrap().clone();

    fs::write(fixture.execution_root.join("tracked"), b"descendant\n").unwrap();
    git(&["add", "tracked"]);
    git(&["commit", "--quiet", "-m", "descendant"]);
    let capture = GitCaptureContext::admit_local_with_baseline(
        fixture.admitted.execution(),
        &baseline,
        &super::super::artifact::CaptureCancellation::default(),
    )
    .unwrap();
    assert_eq!(capture.baseline().commit_oid(), original);

    git(&["checkout", "--quiet", "--orphan", "rewritten"]);
    fs::write(fixture.execution_root.join("tracked"), b"rewritten\n").unwrap();
    git(&["add", "-A"]);
    git(&["commit", "--quiet", "-m", "rewritten"]);
    assert_eq!(
        GitCaptureContext::admit_local_with_baseline(
            fixture.admitted.execution(),
            &baseline,
            &super::super::artifact::CaptureCancellation::default(),
        )
        .unwrap_err(),
        GitWorkspaceAdmissionFailure::BaselineUnavailable
    );
    let missing = LocalGitBaseline::new(GitObjectFormat::Sha1, Arc::from("0".repeat(40))).unwrap();
    assert_eq!(
        GitCaptureContext::admit_local_with_baseline(
            fixture.admitted.execution(),
            &missing,
            &super::super::artifact::CaptureCancellation::default(),
        )
        .unwrap_err(),
        GitWorkspaceAdmissionFailure::BaselineUnavailable
    );
}

#[test]
fn retained_output_carriers_are_verified_and_orphans_are_removed() {
    let fixture = AdmittedFixture::new();
    let run =
        InitialLocalRun::create(&fixture.run_path("retained-output"), &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let private = run.create_private_staging().unwrap();
    let artifacts = ArtifactStaging::create_bound(
        fixture.admitted.execution(),
        private.path(),
        private.root_handle(),
    )
    .unwrap();
    let attempts = open_directory_at(run.root_handle(), ATTEMPTS_DIRECTORY).unwrap();
    let attempt = open_directory_at(&attempts, "000001").unwrap();
    let values = create_or_open_directory(&attempt, VALUES_DIRECTORY).unwrap();
    let steps = create_or_open_directory(&values, "steps").unwrap();
    let first = create_or_open_directory(&steps, "first").unwrap();
    let retained = retain_output_value(
        &artifacts,
        &first,
        AttemptNodeRoleV1::Step,
        "first",
        "message",
        &crate::workflow::value::CapturedValue::text(Arc::from("evidence\n")),
    )
    .unwrap();
    sync_directory(&first).unwrap();
    sync_directory(&steps).unwrap();
    sync_directory(&values).unwrap();
    sync_directory(&attempt).unwrap();
    run.state
        .update(|state| {
            state.attempts[0].progress.steps[0].outputs = Some(vec![retained.clone()]);
            Ok(())
        })
        .unwrap();
    let state = read_state(run.root_handle()).unwrap();
    verify_retained_output_evidence(run.root_handle(), &state, 1).unwrap();

    let carrier = retained.carrier().unwrap();
    let carrier_path = run
        .run_directory()
        .join("attempts/000001")
        .join(&carrier.relative_path);
    let orphan = carrier_path.parent().unwrap().join("orphan");
    fs::write(&orphan, b"not committed").unwrap();
    cleanup_unreferenced_retained_values(run.root_handle(), &state, 1).unwrap();
    assert!(!orphan.exists());
    assert!(carrier_path.is_file());

    let mut permissions = fs::metadata(&carrier_path).unwrap().permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(&carrier_path, permissions).unwrap();
    fs::write(&carrier_path, b"tampered\n").unwrap();
    assert!(verify_retained_output_evidence(run.root_handle(), &state, 1).is_err());
}

#[test]
fn inherited_seed_loads_required_values_and_preserves_full_durable_references() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n      unused:\n        kind: text\n        from: path\n        path: unused.txt\n",
    );
    let run = InitialLocalRun::create(
        &fixture.run_path("inherited-output-chain"),
        &fixture.admitted,
    )
    .unwrap();
    let private = run.create_private_staging().unwrap();
    let artifacts = ArtifactStaging::create_bound(
        fixture.admitted.execution(),
        private.path(),
        private.root_handle(),
    )
    .unwrap();
    let mut state = read_state(run.root_handle()).unwrap();
    let producer_attempt_id = state.attempts[0].attempt_id.clone();
    let producer = crate::workflow::runtime::OutputProducer {
        attempt_id: producer_attempt_id.clone(),
        attempt_number: 1,
        node: "first".to_owned(),
        output: "message".to_owned(),
    };
    let unused_producer = crate::workflow::runtime::OutputProducer {
        output: "unused".to_owned(),
        ..producer.clone()
    };
    let producers = BTreeMap::from([
        (("first".to_owned(), "message".to_owned()), producer.clone()),
        (("first".to_owned(), "unused".to_owned()), unused_producer),
    ]);
    let attempts = open_directory_at(run.root_handle(), ATTEMPTS_DIRECTORY).unwrap();
    let attempt = open_directory_at(&attempts, "000001").unwrap();
    let values = create_or_open_directory(&attempt, VALUES_DIRECTORY).unwrap();
    let steps = create_or_open_directory(&values, "steps").unwrap();
    let first = create_or_open_directory(&steps, "first").unwrap();
    let source_message = retain_output_value_with_producer(
        &artifacts,
        &first,
        AttemptNodeRoleV1::Step,
        "first",
        "message",
        &crate::workflow::value::CapturedValue::text(Arc::from("evidence\n")),
        None,
    )
    .unwrap();
    let source_unused = retain_output_value_with_producer(
        &artifacts,
        &first,
        AttemptNodeRoleV1::Step,
        "first",
        "unused",
        &crate::workflow::value::CapturedValue::text(Arc::from("unused\n")),
        None,
    )
    .unwrap();
    sync_directory(&first).unwrap();
    sync_directory(&steps).unwrap();
    sync_directory(&values).unwrap();
    sync_directory(&attempt).unwrap();
    state.attempts[0].progress.steps[0].state = AttemptStepStateV1::Succeeded;
    state.attempts[0].progress.steps[0].outputs = Some(vec![source_message, source_unused]);
    let retained = inherited_retained_outputs(
        &state,
        2,
        "first",
        &BTreeMap::from([
            (
                "message".to_owned(),
                crate::workflow::value::CapturedValue::text(Arc::from("evidence\n")),
            ),
            (
                "unused".to_owned(),
                crate::workflow::value::CapturedValue::text(Arc::from("unused\n")),
            ),
        ]),
        &producers,
    )
    .unwrap();
    let retained_message = retained
        .iter()
        .find(|output| output.name() == "message")
        .unwrap()
        .clone();

    let run_metadata = read_run(run.root_handle()).unwrap();
    let mut second = fresh_attempt(
        &fixture.admitted,
        2,
        AttemptTriggerV1::Continuation,
        Some(1),
        attempt_definition_for_run(&run_metadata),
        timestamp(scherzo_cloud_support::utc_now()).unwrap(),
    )
    .unwrap();
    second.progress.steps[0].state = AttemptStepStateV1::Inherited;
    second.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: producer_attempt_id,
            prior_attempt_number: 1,
            prior_state: crate::workflow::evidence::InheritedPriorState::Succeeded,
            definition_changed: false,
        },
    ));
    second.progress.steps[0].outputs = Some(retained.clone());
    let second_attempt_id = second.attempt_id.clone();
    state.attempts.push(second);

    let mut third = fresh_attempt(
        &fixture.admitted,
        3,
        AttemptTriggerV1::Continuation,
        Some(2),
        attempt_definition_for_run(&run_metadata),
        timestamp(scherzo_cloud_support::utc_now()).unwrap(),
    )
    .unwrap();
    third.progress.steps[0].state = AttemptStepStateV1::Inherited;
    third.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: second_attempt_id,
            prior_attempt_number: 2,
            prior_state: crate::workflow::evidence::InheritedPriorState::Inherited,
            definition_changed: true,
        },
    ));
    third.progress.steps[0].outputs = Some(retained.clone());
    state.attempts.push(third);
    state.current_attempt_number = 3;

    let replacement_source = "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n      added:\n        kind: text\n        from: path\n        path: added.txt\nexports:\n  message:\n    ref: outputs.first.message\n";
    fs::write(
        fixture
            .admitted
            .workflow()
            .source
            .source_root
            .join("workflow.yaml"),
        replacement_source,
    )
    .unwrap();
    let replacement = admit_workflow(
        resolution::resolve(
            &fixture.admitted.workflow().source.source_root,
            Path::new("workflow.yaml"),
        )
        .unwrap(),
        ResolvedInputs::default(),
        ExecutionContext::new(
            fixture.execution_root.clone(),
            ExecutionPolicyLimits::new(
                2,
                CaptureLimits::new(16, 1024, 4096),
                InputLimits::new(16, 1024, 4096, 4096),
                1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
        ),
    )
    .unwrap();
    let current = Mutex::new(state);
    let seed =
        load_execution_seed(run.root_handle(), &current, 3, &replacement, &artifacts).unwrap();
    let inherited = seed.inherited_step("first").unwrap();
    assert_eq!(inherited.disposition, InheritedDisposition::Succeeded);
    assert_eq!(inherited.producers["message"], producer);
    assert!(matches!(
        &inherited.outputs["message"],
        CapturedValue::Text(value) if value.as_str() == "evidence\n"
    ));
    assert!(!inherited.outputs.contains_key("unused"));

    let reduction = super::super::runtime::initialize_seeded_with_operation::<
        (),
        StepFailureCause,
        CapturedValue,
        (),
    >(&replacement, seed.clone(), None);
    let mut durable_nodes = lock_state(&current).unwrap().attempts[2]
        .progress
        .steps
        .clone();
    update_progress_nodes(
        &mut durable_nodes,
        &reduction.state.steps,
        &BTreeMap::from([(
            (AttemptNodeRoleV1::Step, "first".to_owned()),
            vec![retained_message],
        )]),
    )
    .unwrap();
    assert_eq!(durable_nodes[0].outputs.as_ref(), Some(&retained));
    assert!(
        !run.run_directory()
            .join("attempts/000002/values/steps/first/message")
            .exists()
    );

    let staged_carrier = inherited.outputs["message"]
        .private_capture_carrier()
        .unwrap();
    let staged = fstat(artifacts.open_artifact(staged_carrier.handle()).unwrap()).unwrap();
    let original_path = run
        .run_directory()
        .join("attempts/000001/values/steps/first/message");
    let original = rustix::fs::stat(&original_path).unwrap();
    assert_eq!(staged.st_dev, original.st_dev);
    assert_eq!(staged.st_ino, original.st_ino);
}

#[test]
fn continuation_validation_binds_inheritance_to_the_immediate_prior_step() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1
steps:
  first:
    kind: cmd
    command: {argv: [\"true\"]}
    outputs:
      message:
        kind: text
        from: path
        path: message.txt
  second:
    kind: cmd
    dependsOn: [first]
    command: {argv: [\"true\"]}
",
    );
    let run = InitialLocalRun::create(
        &fixture.run_path("continuation-prior-step-binding"),
        &fixture.admitted,
    )
    .unwrap();
    settle_as_succeeded(&run);
    let mut state = read_state(run.root_handle()).unwrap();
    let run_metadata = read_run(run.root_handle()).unwrap();
    let producer_attempt_id = state.attempts[0].attempt_id.clone();
    let source = RetainedOutputV1::Text {
        name: "message".to_owned(),
        producer: None,
        carrier: RetainedCarrierV1 {
            relative_path: retained_value_relative_path(
                AttemptNodeRoleV1::Step,
                "first",
                "message",
            ),
            media_type: "text/plain; charset=utf-8".to_owned(),
            size_bytes: 9,
            digest: DigestV1::sha256(b"evidence\n"),
        },
    };
    state.attempts[0].progress.steps[0].outputs = Some(vec![source.clone()]);

    let mut second = fresh_attempt(
        &fixture.admitted,
        2,
        AttemptTriggerV1::ExplicitRetry,
        Some(1),
        attempt_definition_for_run(&run_metadata),
        timestamp(scherzo_cloud_support::utc_now()).unwrap(),
    )
    .unwrap();
    let second_settled = second.created_at.clone();
    second.started_at = Some(second_settled.clone());
    second.settled_at = Some(second_settled);
    second.settlement_snapshot = Some(fixture_settlement_snapshot());
    let second_settlement_snapshot = second.settlement_snapshot.clone().unwrap();
    second.state = AttemptStateV1::Succeeded;
    second.progress.steps[0].state = AttemptStepStateV1::Skipped;
    second.progress.steps[0].detail = Some(NodeDetail::Skipped(
        crate::workflow::evidence::ConditionFalseDetail::new([
            crate::workflow::evidence::EvaluatedPredicateEvidence {
                path: String::new(),
                result: false,
            },
        ])
        .unwrap(),
    ));
    second.progress.steps[1].state = AttemptStepStateV1::Succeeded;
    second.result = AttemptResultV1::NotPublished {
        reason: ResultAbsentReasonV1::PublicationPending,
    };
    let second_attempt_id = second.attempt_id.clone();
    let second_execution_root = second.execution_root.clone();
    state.attempts.push(second);

    let mut third = fresh_attempt(
        &fixture.admitted,
        3,
        AttemptTriggerV1::Continuation,
        Some(2),
        attempt_definition_for_run(&run_metadata),
        timestamp(scherzo_cloud_support::utc_now()).unwrap(),
    )
    .unwrap();
    third.progress.steps[0].state = AttemptStepStateV1::Inherited;
    third.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: second_attempt_id.clone(),
            prior_attempt_number: 2,
            prior_state: crate::workflow::evidence::InheritedPriorState::Skipped,
            definition_changed: false,
        },
    ));
    let execution_root = third.execution_root.clone();
    third.continuation = Some(
        serde_json::from_value(json!({
            "request": {
                "fromSteps": ["second"],
                "definition": "inherited"
            },
            "fromSteps": ["second"],
            "reexecutedSteps": ["second"],
            "inheritedSteps": [{
                "id": "first",
                "priorState": "skipped",
                "definitionChanged": false
            }],
            "definitionSource": {
                "kind": "inherited",
                "manifestDigest": {"algorithm": "sha256", "value": "2".repeat(64)},
                "priorManifestDigest": {"algorithm": "sha256", "value": "2".repeat(64)}
            },
            "workspace": {
                "executionRoot": execution_root,
                "priorExecutionRoot": second_execution_root,
                "startSnapshot": {
                    "algorithm": "git_worktree_sha256_v1",
                    "unavailable": "not_work_tree"
                },
                "priorSettlementSnapshot": second_settlement_snapshot,
                "modified": "unknown",
                "quiescence": {
                    "groupsRecorded": 0,
                    "groupsTerminated": 0,
                    "groupsAbsent": 0,
                    "provenAt": "2026-08-02T12:01:42Z"
                }
            }
        }))
        .unwrap(),
    );
    state.current_attempt_number = 3;
    state.attempts.push(third);
    assert_eq!(validate_state(&state), Ok(()));

    let mut missing_full_reexecution_record = state.clone();
    let current = missing_full_reexecution_record.attempts.last_mut().unwrap();
    current.continuation = None;
    current.progress.steps[0].state = AttemptStepStateV1::Pending;
    current.progress.steps[0].detail = None;
    current.progress.steps[0].outputs = None;
    assert_eq!(
        validate_state(&missing_full_reexecution_record),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut explicit_request_root = state.clone();
    let execution_root = explicit_request_root.attempts[2]
        .continuation
        .as_ref()
        .unwrap()
        .workspace
        .execution_root
        .clone();
    explicit_request_root.attempts[2]
        .continuation
        .as_mut()
        .unwrap()
        .request
        .execution_root = Some(execution_root);
    assert_eq!(validate_state(&explicit_request_root), Ok(()));

    let mut mismatched_request_root = state.clone();
    mismatched_request_root
        .attempts
        .last_mut()
        .unwrap()
        .continuation
        .as_mut()
        .unwrap()
        .request
        .execution_root = Some("/different-execution-root".to_owned());
    assert_eq!(
        validate_state(&mismatched_request_root),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut mismatched_prior_workspace = state.clone();
    mismatched_prior_workspace
        .attempts
        .last_mut()
        .unwrap()
        .continuation
        .as_mut()
        .unwrap()
        .workspace
        .prior_execution_root = "/different-prior-root".to_owned();
    assert_eq!(
        validate_state(&mismatched_prior_workspace),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut missing_prior_snapshot = state.clone();
    missing_prior_snapshot
        .attempts
        .last_mut()
        .unwrap()
        .continuation
        .as_mut()
        .unwrap()
        .workspace
        .prior_settlement_snapshot = None;
    assert_eq!(
        validate_state(&missing_prior_snapshot),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut mismatched_prior_state = state.clone();
    let current = mismatched_prior_state.attempts.last_mut().unwrap();
    let Some(NodeDetail::Inherited(detail)) = &mut current.progress.steps[0].detail else {
        panic!("fixture must contain inherited detail");
    };
    detail.prior_state = crate::workflow::evidence::InheritedPriorState::Succeeded;
    current.continuation.as_mut().unwrap().inherited_steps[0].prior_state =
        crate::workflow::evidence::InheritedPriorState::Succeeded;
    assert_eq!(
        validate_state(&mismatched_prior_state),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut impossible_skipped_output = state.clone();
    let mut inherited = source.clone();
    inherited.set_producer(crate::workflow::runtime::OutputProducer {
        attempt_id: producer_attempt_id.clone(),
        attempt_number: 1,
        node: "first".to_owned(),
        output: "message".to_owned(),
    });
    impossible_skipped_output.attempts[2].progress.steps[0].outputs = Some(vec![inherited]);
    assert_eq!(
        validate_state(&impossible_skipped_output),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut flattened_chain = state.clone();
    let initial_attempt_id = flattened_chain.attempts[0].attempt_id.clone();
    let initial_execution_root = flattened_chain.attempts[0].execution_root.clone();
    let initial_settlement_snapshot = flattened_chain.attempts[0].settlement_snapshot.clone();
    let mut flattened = source.clone();
    flattened.set_producer(crate::workflow::runtime::OutputProducer {
        attempt_id: initial_attempt_id.clone(),
        attempt_number: 1,
        node: "first".to_owned(),
        output: "message".to_owned(),
    });
    let mut second_record = flattened_chain.attempts[2].continuation.clone().unwrap();
    let second = &mut flattened_chain.attempts[1];
    second.trigger = AttemptTriggerV1::Continuation;
    second.progress.steps[0].state = AttemptStepStateV1::Inherited;
    second.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: initial_attempt_id,
            prior_attempt_number: 1,
            prior_state: crate::workflow::evidence::InheritedPriorState::Succeeded,
            definition_changed: false,
        },
    ));
    second.progress.steps[0].outputs = Some(vec![flattened.clone()]);
    second_record.inherited_steps[0].prior_state =
        crate::workflow::evidence::InheritedPriorState::Succeeded;
    second_record.workspace.prior_execution_root = initial_execution_root;
    second_record.workspace.prior_settlement_snapshot = initial_settlement_snapshot;
    second.continuation = Some(second_record);
    let third = &mut flattened_chain.attempts[2];
    let Some(NodeDetail::Inherited(detail)) = &mut third.progress.steps[0].detail else {
        panic!("fixture must contain inherited detail");
    };
    detail.prior_state = crate::workflow::evidence::InheritedPriorState::Inherited;
    third.continuation.as_mut().unwrap().inherited_steps[0].prior_state =
        crate::workflow::evidence::InheritedPriorState::Inherited;
    third.progress.steps[0].outputs = Some(vec![flattened]);
    assert_eq!(validate_state(&flattened_chain), Ok(()));

    let mut false_flattening = flattened_chain;
    false_flattening.attempts[2].progress.steps[0]
        .outputs
        .as_mut()
        .unwrap()[0]
        .set_producer(crate::workflow::runtime::OutputProducer {
            attempt_id: second_attempt_id.clone(),
            attempt_number: 2,
            node: "first".to_owned(),
            output: "message".to_owned(),
        });
    assert_eq!(
        validate_state(&false_flattening),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut direct_prior_producer = state;
    direct_prior_producer.attempts[1].progress.steps[0].state = AttemptStepStateV1::Succeeded;
    direct_prior_producer.attempts[1].progress.steps[0].detail = None;
    direct_prior_producer.attempts[1].progress.steps[0].outputs = Some(vec![source.clone()]);
    let current = &mut direct_prior_producer.attempts[2];
    let Some(NodeDetail::Inherited(detail)) = &mut current.progress.steps[0].detail else {
        panic!("fixture must contain inherited detail");
    };
    detail.prior_state = crate::workflow::evidence::InheritedPriorState::Succeeded;
    current.continuation.as_mut().unwrap().inherited_steps[0].prior_state =
        crate::workflow::evidence::InheritedPriorState::Succeeded;
    let mut inherited = source;
    inherited.set_producer(crate::workflow::runtime::OutputProducer {
        attempt_id: second_attempt_id,
        attempt_number: 2,
        node: "first".to_owned(),
        output: "message".to_owned(),
    });
    current.progress.steps[0].outputs = Some(vec![inherited]);
    assert_eq!(validate_state(&direct_prior_producer), Ok(()));

    let mut stale_producer = direct_prior_producer;
    stale_producer.attempts[2].progress.steps[0]
        .outputs
        .as_mut()
        .unwrap()[0]
        .set_producer(crate::workflow::runtime::OutputProducer {
            attempt_id: producer_attempt_id,
            attempt_number: 1,
            node: "first".to_owned(),
            output: "message".to_owned(),
        });
    assert_eq!(
        validate_state(&stale_producer),
        Err(LocalRunDirectoryError::StateInvalid)
    );
}

#[test]
fn retained_output_verification_rejects_a_fifo_without_waiting_for_a_writer() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n",
    );
    let run =
        InitialLocalRun::create(&fixture.run_path("retained-fifo"), &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    retain_text_output_for_step(&run, "first", "message", b"evidence\n");
    let state = read_state(run.root_handle()).unwrap();
    let carrier = run
        .run_directory()
        .join("attempts/000001/values/steps/first/message");
    fs::remove_file(&carrier).unwrap();
    nix::unistd::mkfifo(
        &carrier,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();

    assert_eq!(
        verify_retained_output_evidence(run.root_handle(), &state, 1),
        Err(LocalRunDirectoryError::StateInvalid)
    );
}

#[test]
fn retry_rejects_incomplete_succeeded_outputs_before_orphan_cleanup() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"false\"]}\n",
    );
    let run_path = fixture.run_path("retry-incomplete-output-set");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    run.state
        .update(|state| {
            let steps = &mut current_attempt_mut(state)?.progress.steps;
            let failed_detail = steps[0].detail.take();
            steps[0].state = AttemptStepStateV1::Succeeded;
            steps[0].outputs = Some(Vec::new());
            steps[1].state = AttemptStepStateV1::Failed;
            steps[1].detail = failed_detail;
            Ok(())
        })
        .unwrap();
    let carrier = run_path.join("attempts/000001/values/steps/first/message");
    fs::create_dir_all(carrier.parent().unwrap()).unwrap();
    fs::write(&carrier, b"producer evidence\n").unwrap();
    drop(run);

    assert!(matches!(
        acquire_local_retry(&run_path),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
    assert_eq!(fs::read(carrier).unwrap(), b"producer evidence\n");
    assert!(!run_path.join("attempts/000002").exists());
}

#[test]
fn retry_commits_only_fresh_attempt_state_and_retained_inputs() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retry-fresh");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    let documents = read_state(initial.root_handle()).unwrap();
    let predecessor = documents.attempts[0].clone();
    assert_eq!(
        predecessor.definition.as_ref().unwrap().locator,
        AttemptDefinitionLocatorV1::Run
    );
    assert_eq!(
        predecessor.settlement_snapshot.as_ref().unwrap().settled_by,
        Some(WorkspaceSnapshotSettlementV1::Engine)
    );
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let pending = *pending;
    let (_, inputs, maximum_parallel_steps) = pending.execution_specification();
    assert_eq!(maximum_parallel_steps, 2);
    assert!(matches!(
        inputs.get("request"),
        Some(ResolvedInput::Text(value)) if value.as_ref() == "durable request\n"
    ));
    let Some(ResolvedInput::Attachments(attachments)) = inputs.get("evidence") else {
        panic!("retained named attachment collection is missing");
    };
    assert_eq!(attachments[0].bytes(), [0_u8, 1, 0xff]);
    let Some(ResolvedInput::Json(settings)) = inputs.get("settings") else {
        panic!("retained named JSON input is missing");
    };
    assert_eq!(settings.source(), b"{ \"z\": null, \"n\": 1.2300 }\n");
    assert_eq!(settings.canonical(), b"{\"n\":1.2300,\"z\":null}");
    assert!(settings.value()["z"].is_null());
    let retry = pending.begin(&fixture.admitted).unwrap_or_else(|_| {
        panic!("eligible retry should commit");
    });

    assert_eq!(retry.attempt_number(), 2);
    let state = read_state(retry.root_handle()).unwrap();
    assert_eq!(state.current_attempt_number, 2);
    assert_eq!(state.attempts[0], predecessor);
    let attempt = &state.attempts[1];
    assert_eq!(attempt.trigger, AttemptTriggerV1::ExplicitRetry);
    assert_eq!(attempt.prior_attempt_number, Some(1));
    assert_eq!(
        attempt.definition.as_ref().unwrap().locator,
        AttemptDefinitionLocatorV1::Run
    );
    assert_eq!(attempt.state, AttemptStateV1::Created);
    assert_eq!(attempt.progress.accepted_occurrence_ordinal, 0);
    assert_eq!(attempt.progress.last_transition_sequence, 0);
    assert!(attempt.progress.outstanding_actions.is_empty());
    assert!(attempt.process_guards.is_empty());
    assert!(
        attempt
            .progress
            .steps
            .iter()
            .all(|step| step.state == AttemptStepStateV1::Pending)
    );
    assert!(run_path.join("attempts/000002").is_dir());
    assert!(!run_path.join("attempts/000002/result").exists());
}

#[test]
fn attempt_definition_locator_resolves_attempt_retention_without_run_fallback() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("attempt-definition");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    drop(initial);
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let retry = pending.begin(&fixture.admitted).unwrap_or_else(|_| {
        panic!("retry should commit");
    });
    settle_as_workflow_failed(&retry);

    let attempts = open_directory_at(retry.root_handle(), ATTEMPTS_DIRECTORY).unwrap();
    let attempt = open_directory_at(&attempts, "000002").unwrap();
    mkdir(&attempt, WORKFLOW_DIRECTORY).unwrap();
    let workflow = open_directory_at(&attempt, WORKFLOW_DIRECTORY).unwrap();
    mkdir(&workflow, WORKFLOW_FILES_DIRECTORY).unwrap();
    let files = open_directory_at(&workflow, WORKFLOW_FILES_DIRECTORY).unwrap();
    let manifest = retain_execution_specification(&files, &fixture.admitted).unwrap();
    let manifest_bytes = encode_json(&manifest).unwrap();
    write_new_immutable_file(&workflow, WORKFLOW_MANIFEST_FILE, &manifest_bytes).unwrap();
    sync_directory(&files).unwrap();
    sync_directory(&workflow).unwrap();
    sync_directory(&attempt).unwrap();
    retry
        .state
        .update(|state| {
            let prior = state.attempts[0].clone();
            let current = current_attempt_mut(state)?;
            let manifest_digest = DigestV1::sha256(&manifest_bytes);
            current.trigger = AttemptTriggerV1::Continuation;
            current.definition = Some(AttemptDefinitionV1 {
                digest: DigestV1 {
                    algorithm: fixture
                        .admitted
                        .workflow()
                        .content_digest
                        .algorithm
                        .as_str()
                        .to_owned(),
                    value: fixture.admitted.workflow().content_digest.value.clone(),
                },
                manifest_digest: manifest_digest.clone(),
                locator: AttemptDefinitionLocatorV1::Attempt { attempt_number: 2 },
            });
            current.continuation = Some(
                serde_json::from_value(json!({
                    "request": {
                        "fromSteps": ["first"],
                        "definition": {
                            "replaced": {
                                "path": fixture.admitted.workflow().source.source_root.join("workflow.yaml")
                            }
                        }
                    },
                    "fromSteps": ["first"],
                    "reexecutedSteps": ["first", "second"],
                    "inheritedSteps": [],
                    "definitionSource": {
                        "kind": "replaced",
                        "manifestDigest": manifest_digest,
                        "priorManifestDigest": prior.definition.as_ref().unwrap().manifest_digest
                    },
                    "workspace": {
                        "executionRoot": current.execution_root,
                        "priorExecutionRoot": prior.execution_root,
                        "startSnapshot": {
                            "algorithm": "git_worktree_sha256_v1",
                            "unavailable": "not_work_tree"
                        },
                        "priorSettlementSnapshot": prior.settlement_snapshot,
                        "modified": "unknown",
                        "quiescence": {
                            "groupsRecorded": 0,
                            "groupsTerminated": 0,
                            "groupsAbsent": 0,
                            "provenAt": "2026-08-02T12:01:42Z"
                        }
                    }
                }))
                .unwrap(),
            );
            Ok(())
        })
        .unwrap();

    let retained_run = read_run(retry.root_handle()).unwrap();
    let retained_state = read_state(retry.root_handle()).unwrap();
    validate_run_state_pair(&retained_run, &retained_state).unwrap();
    let run_source = run_path.join("workflow/files/0001");
    let mut permissions = fs::metadata(&run_source).unwrap().permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(&run_source, permissions).unwrap();
    fs::write(run_source, b"corrupted run-level closure\n").unwrap();

    let mut budget = RetainedReadBudget::with_bytes(0).unwrap();
    let (resolved, _, maximum_parallel_steps) = load_attempt_retained_execution_with_budget(
        retry.root_handle(),
        &retained_run,
        &retained_state,
        &retained_state.attempts[1],
        &mut budget,
    )
    .unwrap();
    assert_eq!(
        resolved.content_digest,
        fixture.admitted.workflow().content_digest
    );
    assert_eq!(maximum_parallel_steps, 2);
}

#[test]
fn finalizer_retry_uses_fresh_identity_and_omits_prior_finalization_bytes() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: [\"false\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let run_path = fixture.run_path("finalizer-retry-fresh");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    assert!(
        read_state(initial.root_handle()).unwrap().attempts[0]
            .finalization
            .is_none(),
        "an untriggered finalizer graph must not synthesize progress"
    );
    assert_eq!(initial.finalizers.len(), 1);

    initial
        .state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::WorkflowFailed;
            attempt.progress.steps[0].state = AttemptStepStateV1::Failed;
            attempt.progress.steps[0].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(1),
                )
                .unwrap(),
            ));
            attempt.finalization = Some(AttemptFinalizationV1::Complete(
                AttemptFinalizationCompleteV1 {
                    complete: true,
                    trigger: FinalizationTriggerV1::Failed,
                    finalizers: vec![DurableFinalizerV1 {
                        id: "cleanup".to_owned(),
                        role: AttemptNodeRoleV1::Finalizer,
                        failure_policy: FailurePolicy::Required,
                        state: AttemptStepStateV1::Succeeded,
                        outputs: Some(Vec::new()),
                        detail: None,
                    }],
                    issues: Vec::new(),
                    cancellation: None,
                    force_abort: false,
                },
            ));
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    let predecessor = read_state(initial.root_handle()).unwrap().attempts[0].clone();
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed finalizer attempt should be retryable");
    };
    let retry = (*pending)
        .begin(&fixture.admitted)
        .unwrap_or_else(|_| panic!("eligible finalizer retry should commit"));
    let state = read_state(retry.root_handle()).unwrap();
    let attempt = &state.attempts[1];

    assert_eq!(state.attempts[0], predecessor);
    assert_ne!(attempt.attempt_id, predecessor.attempt_id);
    assert_ne!(attempt.owner.owner_nonce, predecessor.owner.owner_nonce);
    assert_eq!(attempt.progress.accepted_occurrence_ordinal, 0);
    assert_eq!(attempt.progress.last_transition_sequence, 0);
    assert!(attempt.progress.outstanding_actions.is_empty());
    assert!(attempt.finalization.is_none());
    assert_eq!(retry.finalizers.len(), 1);
    assert_eq!(retry.finalizers[0].id, "cleanup");
    assert_eq!(retry.finalizers[0].state, AttemptStepStateV1::Pending);
}

#[test]
fn owner_loss_after_retry_commit_consumes_the_attempt_number() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retry-crash");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let retry = (*pending).begin(&fixture.admitted).unwrap_or_else(|_| {
        panic!("first retry should commit");
    });
    assert_eq!(retry.attempt_number(), 2);
    drop(retry);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("abandoned retry should itself be retryable");
    };
    let next = (*pending).begin(&fixture.admitted).unwrap_or_else(|_| {
        panic!("abandoned retry should settle and advance");
    });
    assert_eq!(next.attempt_number(), 3);
    let state = read_state(next.root_handle()).unwrap();
    assert_eq!(state.attempts.len(), 3);
    assert_eq!(state.attempts[1].state, AttemptStateV1::Interrupted);
    assert_eq!(
        state.attempts[1].interruption,
        Some(AttemptInterruptionV1 {
            cause: InterruptionCauseV1::ExecutionOwnerLost,
            execution_may_have_started: false,
            cancellation_requested: false,
        })
    );
    assert_eq!(state.attempts[2].attempt_number, 3);
    assert!(run_path.join("attempts/000002").is_dir());
    assert!(run_path.join("attempts/000003").is_dir());
}

struct ExactThenAbsentAuthority {
    host: ExecutionHostV1,
    observations: std::cell::RefCell<VecDeque<ProcessIdentityObservation>>,
    terminations: std::cell::Cell<usize>,
}

impl LocalRecoveryAuthority for ExactThenAbsentAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        Ok(self.host.clone())
    }

    fn observe_process(&self, _guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        self.observations.borrow_mut().pop_front().unwrap()
    }
}

impl LocalQuiescenceAuthority for ExactThenAbsentAuthority {
    fn terminate_process(&self, _guard: &ProcessGuardV1) -> AuthenticatedSignalResult {
        self.terminations.set(self.terminations.get() + 1);
        AuthenticatedSignalResult::Signalled
    }

    fn wait_for_process_change(&self) {}
}

#[test]
fn abandoned_exact_group_is_authenticated_terminated_and_proven_absent() {
    let attempt = fixture_guarded_attempt();
    let authority = ExactThenAbsentAuthority {
        host: attempt.owner.execution_host.clone(),
        observations: std::cell::RefCell::new(VecDeque::from([
            ProcessIdentityObservation::Exact {
                leader: crate::workflow::process_group::LeaderState::Running,
            },
            ProcessIdentityObservation::Absent,
        ])),
        terminations: std::cell::Cell::new(0),
    };

    assert_eq!(quiesce_attempt(&attempt, &authority), Ok(()));
    assert_eq!(authority.terminations.get(), 1);
}

struct DelayedAbsentAuthority {
    host: ExecutionHostV1,
    observations: std::cell::Cell<usize>,
    waits: std::cell::Cell<usize>,
}

impl LocalRecoveryAuthority for DelayedAbsentAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        Ok(self.host.clone())
    }

    fn observe_process(&self, _guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        let observation = self.observations.get() + 1;
        self.observations.set(observation);
        if observation <= 2_002 {
            ProcessIdentityObservation::Exact {
                leader: crate::workflow::process_group::LeaderState::Running,
            }
        } else {
            ProcessIdentityObservation::Absent
        }
    }
}

impl LocalQuiescenceAuthority for DelayedAbsentAuthority {
    fn terminate_process(&self, _guard: &ProcessGuardV1) -> AuthenticatedSignalResult {
        AuthenticatedSignalResult::Signalled
    }

    fn wait_for_process_change(&self) {
        self.waits.set(self.waits.get() + 1);
    }
}

#[test]
fn quiescence_wait_allows_exit_after_ten_seconds() {
    let attempt = fixture_guarded_attempt();
    let authority = DelayedAbsentAuthority {
        host: attempt.owner.execution_host.clone(),
        observations: std::cell::Cell::new(0),
        waits: std::cell::Cell::new(0),
    };

    assert_eq!(quiesce_attempt(&attempt, &authority), Ok(()));
    assert_eq!(authority.waits.get(), 2_001);
    assert!(
        authority.waits.get() * usize::try_from(QUIESCENCE_POLL_INTERVAL.as_millis()).unwrap()
            > 10_000
    );
}

#[test]
fn retained_named_input_corruption_rejects_retry_without_fallback() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("corrupt-retained-input");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    drop(run);

    let retained_json = run_path.join("workflow/files/0004");
    fs::set_permissions(&retained_json, Permissions::from_mode(0o600)).unwrap();
    fs::write(&retained_json, br#"{"key":1,"key":2}"#).unwrap();

    assert!(matches!(
        acquire_local_retry(&run_path),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
    assert!(!run_path.join("attempts/000002").exists());
}

#[test]
fn retry_execution_setup_rejects_a_rebound_run_path() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retry-rebound");
    let moved_path = fixture.run_path("retry-original");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let retry = (*pending)
        .begin(&fixture.admitted)
        .unwrap_or_else(|_| panic!("eligible retry should commit"));

    fs::rename(&run_path, moved_path).unwrap();
    fs::create_dir(&run_path).unwrap();
    fs::create_dir(run_path.join(PRIVATE_DIRECTORY)).unwrap();
    fs::create_dir_all(run_path.join("attempts/000002")).unwrap();

    let prepared = crate::workflow::publication::prepare_attempt_result_destination(
        retry.result_directory(),
        retry.private_directory(),
        retry.attempt_directory_handle(),
        retry.private_directory_handle(),
    );
    assert!(
        prepared.is_err(),
        "execution setup must not adopt a replacement at the run path"
    );
}

fn json_bytes(value: Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(&value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn settle_as_succeeded(run: &LocalAttemptOwner) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::Succeeded;
            for step in &mut attempt.progress.steps {
                step.state = AttemptStepStateV1::Succeeded;
            }
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

fn retain_text_output_for_step(
    run: &LocalAttemptOwner,
    step_id: &str,
    output_name: &str,
    bytes: &[u8],
) {
    let state = read_state(run.root_handle()).unwrap();
    let attempt_name = attempt_directory_name(state.current_attempt_number).unwrap();
    let relative_path = retained_value_relative_path(AttemptNodeRoleV1::Step, step_id, output_name);
    let path = run
        .run_directory()
        .join(ATTEMPTS_DIRECTORY)
        .join(attempt_name)
        .join(&relative_path);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
    let retained = RetainedOutputV1::Text {
        name: output_name.to_owned(),
        producer: None,
        carrier: RetainedCarrierV1 {
            relative_path,
            media_type: "text/plain; charset=utf-8".to_owned(),
            size_bytes: u64::try_from(bytes.len()).unwrap(),
            digest: DigestV1::sha256(bytes),
        },
    };
    run.state
        .update(|state| {
            let step = current_attempt_mut(state)?
                .progress
                .steps
                .iter_mut()
                .find(|step| step.id == step_id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            step.outputs = Some(vec![retained.clone()]);
            Ok(())
        })
        .unwrap();
}

fn retain_file_outputs_for_step(
    run: &LocalAttemptOwner,
    step_id: &str,
    outputs: &[(&str, &str, &[u8])],
) {
    let state = read_state(run.root_handle()).unwrap();
    let attempt_number = state.current_attempt_number;
    let attempt_name = attempt_directory_name(attempt_number).unwrap();
    let mut retained = Vec::with_capacity(outputs.len());
    for (name, media_type, bytes) in outputs {
        let relative_path = retained_value_relative_path(AttemptNodeRoleV1::Step, step_id, name);
        let path = run
            .run_directory()
            .join(ATTEMPTS_DIRECTORY)
            .join(&attempt_name)
            .join(&relative_path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
        retained.push(RetainedOutputV1::File {
            name: (*name).to_owned(),
            producer: None,
            media_type: (*media_type).to_owned(),
            carrier: RetainedCarrierV1 {
                relative_path,
                media_type: (*media_type).to_owned(),
                size_bytes: u64::try_from(bytes.len()).unwrap(),
                digest: DigestV1::sha256(bytes),
            },
        });
    }
    run.state
        .update(|state| {
            let step = current_attempt_mut(state)?
                .progress
                .steps
                .iter_mut()
                .find(|step| step.id == step_id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            step.outputs = Some(retained.clone());
            Ok(())
        })
        .unwrap();
}

fn settle_as_cancelled(run: &LocalAttemptOwner) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled.clone());
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::Cancelled;
            attempt.cancellation = Some(AttemptCancellationV1 {
                reason: CancellationReasonV1::UserRequest,
                requested_at: settled.clone(),
                force_stop_deadline: settled,
                workflow_confirmed: true,
            });
            for step in &mut attempt.progress.steps {
                step.state = AttemptStepStateV1::Cancelled;
                step.detail = Some(crate::workflow::evidence::NodeDetail::Cancellation(
                    crate::workflow::evidence::CancellationDetail::new(
                        crate::workflow::admission::CancellationReason::UserRequest,
                    ),
                ));
            }
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

fn settle_as_force_cancelled_with_finalizer(
    run: &LocalAttemptOwner,
    phase: super::super::runtime::RunCancellationPhase,
) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled.clone());
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::Cancelled;
            attempt.force_abort = Some(super::super::runtime::ForceAbortEvidence {
                reason: CancellationReason::ForceAbort,
                phase,
            });
            let ordinary_reason = match phase {
                super::super::runtime::RunCancellationPhase::Ordinary => {
                    attempt.cancellation = None;
                    CancellationReason::ForceAbort
                }
                super::super::runtime::RunCancellationPhase::Finalization => {
                    attempt.cancellation = Some(AttemptCancellationV1 {
                        reason: CancellationReasonV1::UserRequest,
                        requested_at: settled.clone(),
                        force_stop_deadline: settled,
                        workflow_confirmed: true,
                    });
                    CancellationReason::UserRequest
                }
            };
            for step in &mut attempt.progress.steps {
                step.state = AttemptStepStateV1::Cancelled;
                step.detail = Some(NodeDetail::Cancellation(
                    super::super::evidence::CancellationDetail::new(ordinary_reason),
                ));
            }
            attempt.finalization = Some(AttemptFinalizationV1::Complete(
                AttemptFinalizationCompleteV1 {
                    complete: true,
                    trigger: FinalizationTriggerV1::Cancelled,
                    finalizers: vec![DurableFinalizerV1 {
                        id: "cleanup".to_owned(),
                        role: AttemptNodeRoleV1::Finalizer,
                        failure_policy: FailurePolicy::Required,
                        state: AttemptStepStateV1::Cancelled,
                        outputs: Some(Vec::new()),
                        detail: Some(NodeDetail::Cancellation(
                            super::super::evidence::CancellationDetail::new(
                                CancellationReason::ForceAbort,
                            ),
                        )),
                    }],
                    issues: Vec::new(),
                    cancellation: Some(DurableFinalizationCancellationV1 {
                        reason: CancellationReasonV1::ForceAbort,
                        force_stop_deadline: None,
                    }),
                    force_abort: true,
                },
            ));
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

#[test]
fn retained_attempt_rejects_force_evidence_on_success() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: { argv: [\"true\"] }\n",
    );
    let run =
        InitialLocalRun::create(&fixture.run_path("forced-success"), &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let state = read_state(run.root_handle()).unwrap();
    assert!(decode_state(&encode_json(&state).unwrap()).is_ok());
    let mut fabricated_force = serde_json::to_value(state).unwrap();
    fabricated_force["attempts"][0]["forceAbort"] =
        json!({ "reason": "force_abort", "phase": "ordinary" });
    assert_eq!(
        decode_state(&json_bytes(fabricated_force)),
        Err(LocalRunDirectoryError::StateInvalid)
    );
}

#[test]
fn retained_attempt_rejects_phase_impossible_force_evidence() {
    let workflow = "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: { argv: [\"true\"] }\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: { argv: [\"true\"] }\n";

    let ordinary_fixture = AdmittedFixture::from_source(workflow);
    let ordinary_run = InitialLocalRun::create(
        &ordinary_fixture.run_path("ordinary-force"),
        &ordinary_fixture.admitted,
    )
    .unwrap();
    settle_as_force_cancelled_with_finalizer(
        &ordinary_run,
        super::super::runtime::RunCancellationPhase::Ordinary,
    );
    let ordinary_state = read_state(ordinary_run.root_handle()).unwrap();
    assert!(decode_state(&encode_json(&ordinary_state).unwrap()).is_ok());
    let mut graceful_finalization = serde_json::to_value(ordinary_state).unwrap();
    let deadline = graceful_finalization["attempts"][0]["createdAt"].clone();
    graceful_finalization["attempts"][0]["finalization"]["cancellation"] = json!({
        "reason": "runner_shutdown",
        "forceStopDeadline": deadline
    });
    assert_eq!(
        decode_state(&json_bytes(graceful_finalization)),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let finalization_fixture = AdmittedFixture::from_source(workflow);
    let finalization_run = InitialLocalRun::create(
        &finalization_fixture.run_path("finalization-force"),
        &finalization_fixture.admitted,
    )
    .unwrap();
    settle_as_force_cancelled_with_finalizer(
        &finalization_run,
        super::super::runtime::RunCancellationPhase::Finalization,
    );
    let finalization_state = read_state(finalization_run.root_handle()).unwrap();
    assert!(decode_state(&encode_json(&finalization_state).unwrap()).is_ok());
    let mut rewritten_ordinary = serde_json::to_value(finalization_state).unwrap();
    rewritten_ordinary["attempts"][0]["progress"]["steps"][0]["detail"] =
        json!({ "code": "force_abort" });
    assert_eq!(
        decode_state(&json_bytes(rewritten_ordinary)),
        Err(LocalRunDirectoryError::StateInvalid)
    );
}

fn publish_result_fixture(fixture: &AdmittedFixture, run: &LocalAttemptOwner) -> PathBuf {
    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let command_output = serde_json::json!({
        "stdout": {
            "encoding": "base64",
            "data": BASE64_STANDARD.encode([0_u8, 0xff, b'\n']),
            "retainedBytes": 3,
            "discardedBytes": 0,
            "truncated": false,
            "fullyDrained": false
        },
        "stderr": {
            "encoding": "base64",
            "data": BASE64_STANDARD.encode(b"warning\n"),
            "retainedBytes": 8,
            "discardedBytes": 0,
            "truncated": false,
            "fullyDrained": true
        }
    });
    let (outcome, mut steps, primary_issue) = match attempt.state {
        AttemptStateV1::Succeeded => (
            "succeeded",
            vec![
                serde_json::json!({
                    "id": "first",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "succeeded",
                    "startedAt": "2026-08-02T12:01:44Z",
                    "durationMilliseconds": 100,
                    "commandOutput": command_output.clone()
                }),
                serde_json::json!({
                    "id": "second",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "succeeded",
                    "startedAt": "2026-08-02T12:01:44.1Z",
                    "durationMilliseconds": 200,
                    "commandOutput": command_output.clone()
                }),
            ],
            None,
        ),
        AttemptStateV1::WorkflowFailed => (
            "failed",
            vec![
                serde_json::json!({
                    "id": "first",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "failed",
                    "startedAt": "2026-08-02T12:01:44Z",
                    "durationMilliseconds": 100,
                    "detail": {
                        "phase": "execution",
                        "code": "command_exit",
                        "exitCode": 23
                    },
                    "commandOutput": command_output
                }),
                serde_json::json!({
                    "id": "second",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "blocked",
                    "detail": {
                        "code": "prerequisites_unsatisfied",
                        "prerequisites": [{"kind": "control", "node": "first"}]
                    }
                }),
            ],
            Some(serde_json::json!({
                "node": { "id": "first", "role": "step" },
                "state": "failed",
                "detail": { "phase": "execution", "code": "command_exit", "exitCode": 23 }
            })),
        ),
        AttemptStateV1::Cancelled => (
            "cancelled",
            vec![
                serde_json::json!({
                    "id": "first",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "cancelled",
                    "detail": { "code": "user_request" }
                }),
                serde_json::json!({
                    "id": "second",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "cancelled",
                    "detail": { "code": "user_request" }
                }),
            ],
            None,
        ),
        state => panic!("unsupported fixture state: {state:?}"),
    };
    for step in &mut steps {
        step["role"] = Value::String("step".to_owned());
    }
    let mut result = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": attempt.attempt_number,
        "workflow": {
            "path": fixture.admitted.workflow().source.workflow_path,
            "provenance": {
                "kind": "local",
                "sourceRoot": fixture.admitted.workflow().source.source_root
            },
            "digest": {
                "algorithm": run_document.workflow_digest.algorithm,
                "value": run_document.workflow_digest.value
            }
        },
        "execution": {
            "executionRoot": attempt.execution_root,
            "maximumParallelSteps": 2,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45.25Z",
            "durationMilliseconds": 1250
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": crate::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": outcome,
        "forceAbort": attempt.force_abort,
        "steps": steps,
        "exports": {}
    });
    if let Some(primary_issue) = primary_issue {
        result["primaryIssue"] = primary_issue;
    }
    if let Some(cancellation) = &attempt.cancellation {
        result["cancellation"] = serde_json::json!({
            "reason": cancellation.reason,
            "forceStopDeadline": cancellation.force_stop_deadline,
        });
    }
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    fs::write(result_directory.join("result.json"), json_bytes(result)).unwrap();
    run.record_result_published().unwrap();
    result_directory
}

#[test]
fn archived_attempt_preserves_advisory_issues_on_a_succeeded_attempt() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    failurePolicy: advisory\n    command:\n      argv: [\"true\"]\n    outputs:\n      report:\n        kind: file\n        from: path\n        path: report.txt\n        mediaType: text/plain\n  second:\n    kind: cmd\n    failurePolicy: advisory\n    inputs:\n      report:\n        ref: outputs.first.report\n    command:\n      argv: [\"true\"]\n",
    );
    let run_path = fixture.run_path("archive-advisory-success");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut result = result_value(&result_directory);
    result["steps"][0]["failurePolicy"] = Value::String("advisory".to_owned());
    result["steps"][0]["state"] = Value::String("failed".to_owned());
    result["steps"][0]["detail"] = serde_json::json!({
        "phase": "execution",
        "code": "command_exit",
        "exitCode": 9
    });
    result["steps"][1] = serde_json::json!({
        "id": "second",
        "role": "step",
        "kind": "cmd",
        "failurePolicy": "advisory",
        "state": "blocked",
        "detail": {
            "code": "prerequisites_unsatisfied",
            "prerequisites": [{"kind": "body", "ref": "outputs.first.report"}]
        }
    });
    overwrite_result(&result_directory, result);
    run.state
        .update(|state| {
            let progress = &mut current_attempt_mut(state)?.progress.steps;
            progress[0].state = AttemptStepStateV1::Failed;
            progress[0].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(9),
                )
                .unwrap(),
            ));
            progress[1].state = AttemptStepStateV1::Blocked;
            progress[1].detail = Some(crate::workflow::evidence::NodeDetail::Blocked(
                crate::workflow::evidence::BlockedDetail::new([
                    crate::workflow::evidence::Prerequisite::body("outputs.first.report").unwrap(),
                ])
                .unwrap(),
            ));
            Ok(())
        })
        .unwrap();

    let archived = load_local_archived_attempt(&run_path, None).unwrap();

    assert_eq!(archived.state, ArchivedAttemptState::Succeeded);
    assert_eq!(archived.outcome, ArchivedWorkflowOutcome::Succeeded);
    assert!(archived.primary_issue.is_none());
    assert!(matches!(
        archived.steps[0].detail,
        ArchivedStepDetail::Evidence(crate::workflow::evidence::NodeDetail::Failed(_))
    ));
    assert!(matches!(
        archived.steps[1].detail,
        ArchivedStepDetail::Evidence(crate::workflow::evidence::NodeDetail::Blocked(_))
    ));
    assert!(
        archived
            .steps
            .iter()
            .all(|step| step.failure_policy == super::super::document::FailurePolicy::Advisory)
    );
}

fn result_value(result_directory: &Path) -> Value {
    serde_json::from_slice(&fs::read(result_directory.join("result.json")).unwrap()).unwrap()
}

fn overwrite_result(result_directory: &Path, value: Value) {
    fs::write(result_directory.join("result.json"), json_bytes(value)).unwrap();
}

fn assert_archive_ineligible(
    failure: ArchivedAttemptLoadError,
    reason: ArchivedAttemptIneligibilityReason,
) {
    let ArchivedAttemptLoadError::Ineligible(failure) = failure else {
        panic!("expected attempt ineligibility, got {failure:?}");
    };
    assert_eq!(failure.reason, reason);
}

fn assert_archive_operational(
    failure: ArchivedAttemptLoadError,
    code: ArchivedAttemptOperationalErrorCode,
) {
    let ArchivedAttemptLoadError::Operational(failure) = failure else {
        panic!("expected archive operational failure, got {failure:?}");
    };
    assert_eq!(failure.code, code);
}

fn durable_tree(path: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        let mut children = fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        children.sort();
        for child in children {
            let relative = child.strip_prefix(root).unwrap().to_owned();
            if child.is_dir() {
                entries.insert(relative, None);
                visit(root, &child, entries);
            } else {
                entries.insert(relative, Some(fs::read(&child).unwrap()));
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(path, path, &mut entries);
    entries
}

#[test]
fn archived_attempt_loads_failed_current_result_and_raw_stream_prefixes_read_only() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-current");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let expected_result = result_value(&result_directory);
    let before = durable_tree(&run_path);
    let result_open_count = std::cell::Cell::new(0_u8);

    let archived = load_local_archived_attempt_observed(
        &run_path,
        None,
        |_| {},
        |_| result_open_count.set(result_open_count.get().saturating_add(1)),
    )
    .unwrap();

    assert_eq!(archived.current_attempt_number, 1);
    assert_eq!(archived.attempt_number, 1);
    assert_eq!(archived.state, ArchivedAttemptState::WorkflowFailed);
    assert_eq!(archived.outcome, ArchivedWorkflowOutcome::Failed);
    assert_eq!(archived.result_directory, result_directory);
    assert_eq!(
        serde_json::to_value(&archived.result).unwrap(),
        expected_result,
        "the loader must expose the complete value from its validated immutable read"
    );
    assert_eq!(result_open_count.get(), 1);
    assert_eq!(archived.workflow.presentation_order, ["first", "second"]);
    assert_eq!(archived.steps.len(), 2);
    assert!(matches!(
        archived.steps[0].detail,
        ArchivedStepDetail::Evidence(crate::workflow::evidence::NodeDetail::Failed(_))
    ));
    let output = archived.steps[0].command_output.as_ref().unwrap();
    assert_eq!(output.stdout.bytes.as_ref(), [0_u8, 0xff, b'\n']);
    assert_eq!(output.stdout.retained_bytes, 3);
    assert_eq!(output.stdout.discarded_bytes, 0);
    assert!(!output.stdout.truncated);
    assert!(!output.stdout.fully_drained);
    assert_eq!(durable_tree(&run_path), before);
    assert_eq!(read_state(run.root_handle()).unwrap().attempts.len(), 1);
}

#[test]
fn archived_attempt_selects_current_and_explicit_historical_publications() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-history");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    publish_result_fixture(&fixture, &initial);
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed initial attempt should be retryable");
    };
    let retry = (*pending)
        .begin(&fixture.admitted)
        .unwrap_or_else(|_| panic!("retry should begin"));
    settle_as_succeeded(&retry);
    publish_result_fixture(&fixture, &retry);

    let current = load_local_archived_attempt(&run_path, None).unwrap();
    assert_eq!(current.current_attempt_number, 2);
    assert_eq!(current.attempt_number, 2);
    assert_eq!(current.outcome, ArchivedWorkflowOutcome::Succeeded);

    let historical =
        load_local_archived_attempt(&run_path, Some(NonZeroU64::new(1).unwrap())).unwrap();
    assert_eq!(historical.current_attempt_number, 2);
    assert_eq!(historical.attempt_number, 1);
    assert_eq!(historical.outcome, ArchivedWorkflowOutcome::Failed);

    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, Some(NonZeroU64::new(3).unwrap())).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Unknown,
    );
}

#[test]
fn archived_attempt_reports_each_nonpublished_disposition_without_fallback() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-nonterminal");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Nonterminal,
    );

    run.record_executor_fault_before_execution().unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Interrupted,
    );

    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-rejected");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            attempt.state = AttemptStateV1::Rejected;
            attempt.settled_at = Some(attempt.created_at.clone());
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.rejection = Some(AttemptRejectionV1 {
                code: RejectionCodeV1::ImmutableSpecificationUnusable,
            });
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::Rejected,
            };
            Ok(())
        })
        .unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Rejected,
    );

    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-pending");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Unpublished,
    );

    run.record_result_publication_failed(PublicationFailurePhaseV1::Serialization, None)
        .unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::PublicationFailed,
    );
}

#[test]
fn publication_failure_persists_only_the_closed_result_invariant() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("publication-invariant");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);

    run.record_result_publication_failed(
        PublicationFailurePhaseV1::Serialization,
        Some(RunResultInvariant::ExportValues),
    )
    .unwrap();

    let state = read_state(run.root_handle()).unwrap();
    assert!(matches!(
        &state.attempts[0].result,
        AttemptResultV1::PublicationFailed {
            phase: PublicationFailurePhaseV1::Serialization,
            result_invariant: Some(RunResultInvariant::ExportValues),
        }
    ));
    let status = read_local_run_status(&run_path).unwrap();
    assert_eq!(
        status.state["attempts"][0]["result"],
        serde_json::json!({
            "status": "publication_failed",
            "phase": "serialization",
            "resultInvariant": "export_values"
        })
    );

    let diagnostic = state.diagnostics.last().unwrap();
    assert_eq!(diagnostic.code, DiagnosticCodeV1::ResultPublicationFailure);
    assert_eq!(diagnostic.step_id, None);
    assert_eq!(diagnostic.action_id, None);
    assert_eq!(diagnostic.guard_id, None);

    let mut inconsistent = state;
    inconsistent.attempts[0].result = AttemptResultV1::PublicationFailed {
        phase: PublicationFailurePhaseV1::Rename,
        result_invariant: Some(RunResultInvariant::ExportValues),
    };
    assert_eq!(
        decode_state(&encode_json(&inconsistent).unwrap()),
        Err(LocalRunDirectoryError::StateInvalid)
    );
}

#[test]
fn archived_attempt_rejects_malformed_and_cross_document_mismatched_results() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-invalid-result");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let valid = result_value(&result_directory);

    fs::write(
        result_directory.join("result.json"),
        b"{\"schemaVersion\":1}",
    )
    .unwrap();
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    let mut invalid_values = Vec::new();
    let mut value = valid.clone();
    value["unknown"] = Value::Bool(true);
    invalid_values.push(value);
    let mut value = valid.clone();
    value["attemptNumber"] = Value::from(2);
    invalid_values.push(value);
    let mut value = valid.clone();
    value["workflow"]["digest"]["value"] = Value::String("0".repeat(64));
    invalid_values.push(value);
    let mut value = valid.clone();
    value["execution"]["executionRoot"] = Value::String("/different".to_owned());
    invalid_values.push(value);
    let mut value = valid.clone();
    value["outcome"] = Value::String("succeeded".to_owned());
    value.as_object_mut().unwrap().remove("primaryIssue");
    invalid_values.push(value);
    let mut value = valid.clone();
    value["steps"].as_array_mut().unwrap().swap(0, 1);
    invalid_values.push(value);
    let mut value = valid.clone();
    value["steps"][0]["commandOutput"]["stdout"]["retainedBytes"] = Value::from(2);
    invalid_values.push(value);
    let mut value = valid.clone();
    value["steps"][0]["detail"]["code"] = Value::String("future_code".to_owned());
    value["primaryIssue"]["detail"]["code"] = Value::String("future_code".to_owned());
    invalid_values.push(value);

    for value in invalid_values {
        overwrite_result(&result_directory, value);
        assert_archive_operational(
            load_local_archived_attempt(&run_path, None).unwrap_err(),
            ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
        );
    }
}

#[test]
fn archived_attempt_uses_only_the_authoritative_recorded_result_location() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-result-location");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    publish_result_fixture(&fixture, &run);
    let mut state: Value =
        serde_json::from_slice(&fs::read(run_path.join(STATE_FILE)).unwrap()).unwrap();
    state["attempts"][0]["result"]["relativeDirectory"] =
        Value::String("attempts/000001/other".to_owned());
    fs::write(run_path.join(STATE_FILE), json_bytes(state)).unwrap();

    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::RunDirectoryInvalid,
    );
}

#[test]
fn archived_attempt_rejects_a_broken_retained_workflow_closure() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-broken-closure");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    publish_result_fixture(&fixture, &run);
    let retained_source = run_path.join("workflow/files/0001");
    fs::set_permissions(&retained_source, Permissions::from_mode(0o600)).unwrap();
    fs::write(retained_source, b"changed\n").unwrap();

    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid,
    );
}

#[test]
fn archived_attempt_loads_cancelled_commands_that_never_started() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-pending-cancellation");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_cancelled(&run);
    publish_result_fixture(&fixture, &run);

    let archived = load_local_archived_attempt(&run_path, None).unwrap();

    assert_eq!(archived.state, ArchivedAttemptState::Cancelled);
    assert_eq!(archived.outcome, ArchivedWorkflowOutcome::Cancelled);
    assert!(archived.steps.iter().all(|step| {
        matches!(
            step.detail,
            ArchivedStepDetail::Evidence(crate::workflow::evidence::NodeDetail::Cancellation(_))
        ) && step.started_at.is_none()
            && step.duration.is_none()
            && step.command_output.is_none()
    }));
}

#[test]
fn archived_attempt_loads_ordinary_force_with_suppressed_finalization() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: { argv: [\"true\"] }\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: { argv: [\"true\"] }\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: { argv: [\"true\"] }\n",
    );
    let run_path = fixture.run_path("archive-ordinary-force");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_force_cancelled_with_finalizer(
        &run,
        super::super::runtime::RunCancellationPhase::Ordinary,
    );
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut result = result_value(&result_directory);
    for step in result["steps"].as_array_mut().unwrap() {
        step["detail"] = json!({ "code": "force_abort" });
    }
    result["finalization"] = json!({
        "trigger": "cancelled",
        "finalizers": [{
            "id": "cleanup",
            "role": "finalizer",
            "kind": "cmd",
            "failurePolicy": "required",
            "state": "cancelled",
            "detail": { "code": "force_abort" }
        }],
        "issues": [],
        "cancellation": { "reason": "force_abort" },
        "forceAbort": true
    });
    overwrite_result(&result_directory, result);

    let archived = load_local_archived_attempt(&run_path, None).unwrap();

    assert_eq!(
        archived.force_abort.map(|force_abort| force_abort.phase),
        Some(super::super::publication::ForceAbortPhaseV1::Ordinary)
    );
    assert_eq!(
        archived
            .finalization
            .as_ref()
            .and_then(|finalization| finalization.cancellation.as_ref())
            .map(|cancellation| cancellation.reason),
        Some(ArchivedCancellationReason::ForceAbort)
    );
}

#[test]
fn archived_attempt_loads_valid_result_larger_than_state_document_limit() {
    let maximum_retained_bytes_per_stream = 131_072_u64;
    let mut source = String::from("schemaVersion: 1\nsteps:\n");
    for index in 0..256 {
        source.push_str(&format!(
            "  step{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
        ));
    }
    let fixture = AdmittedFixture::from_source_with_maximum_step_log_bytes(
        &source,
        maximum_retained_bytes_per_stream,
    );
    let run_path = fixture.run_path("archive-large-result");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);

    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let stream = serde_json::json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(vec![
            b'x';
            usize::try_from(maximum_retained_bytes_per_stream).unwrap()
        ]),
        "retainedBytes": maximum_retained_bytes_per_stream,
        "discardedBytes": 0,
        "truncated": false,
        "fullyDrained": true
    });
    let steps = attempt
        .progress
        .steps
        .iter()
        .map(|step| {
            serde_json::json!({
                "id": step.id,
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "succeeded",
                "startedAt": "2026-08-02T12:01:44Z",
                "durationMilliseconds": 1,
                "commandOutput": {
                    "stdout": stream.clone(),
                    "stderr": stream.clone()
                }
            })
        })
        .collect::<Vec<_>>();
    let result = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": attempt.attempt_number,
        "workflow": {
            "path": fixture.admitted.workflow().source.workflow_path,
            "provenance": {
                "kind": "local",
                "sourceRoot": fixture.admitted.workflow().source.source_root
            },
            "digest": {
                "algorithm": run_document.workflow_digest.algorithm,
                "value": run_document.workflow_digest.value
            }
        },
        "execution": {
            "executionRoot": attempt.execution_root,
            "maximumParallelSteps": 2,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": fixture
                .admitted
                .execution()
                .limits()
                .maximum_step_log_bytes()
                .get()
        },
        "outcome": "succeeded",
        "forceAbort": null,
        "steps": steps,
        "exports": {}
    });
    let result_bytes = json_bytes(result);
    assert!(u64::try_from(result_bytes.len()).unwrap() > MAXIMUM_DURABLE_JSON_BYTES);
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    fs::write(result_directory.join("result.json"), result_bytes).unwrap();
    run.record_result_published().unwrap();

    let archived = load_local_archived_attempt(&run_path, None)
        .expect("the result schema bounds streams independently, not the whole document");
    assert_eq!(archived.steps.len(), 256);
}

#[test]
fn archived_attempt_accepts_results_within_the_artifact_set_metadata_limit() {
    let prefix = "a/b;x=";
    let value_count = 128 - prefix.chars().count();
    let media_type = format!("{prefix}{}", "\u{1f600}".repeat(value_count));
    let source_media_type = media_type.clone();
    let mut source = format!(
        "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      payload:\n        kind: file\n        from: path\n        path: payload.bin\n        mediaType: \"{source_media_type}\"\nexports:\n"
    );
    for index in 0..4_096 {
        let name = format!("e{}{index:04}", "a".repeat(59));
        source.push_str(&format!("  {name}:\n    ref: outputs.produce.payload\n"));
    }
    let fixture = AdmittedFixture::from_source(&source);
    let run_path = fixture.run_path("large-result");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    retain_file_outputs_for_step(&run, "produce", &[("payload", &media_type, b"x")]);

    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let metadata = serde_json::json!({
        "state": "available",
        "kind": "file",
        "mediaType": media_type,
        "path": "exports/0001",
        "sizeBytes": 1,
        "digest": {
            "algorithm": "sha256",
            "value": "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
        }
    });
    let exports = (0..4_096)
        .map(|index| (format!("e{}{index:04}", "a".repeat(59)), metadata.clone()))
        .collect::<serde_json::Map<_, _>>();
    let result = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": attempt.attempt_number,
        "workflow": {
            "path": fixture.admitted.workflow().source.workflow_path,
            "provenance": {
                "kind": "local",
                "sourceRoot": fixture.admitted.workflow().source.source_root
            },
            "digest": {
                "algorithm": run_document.workflow_digest.algorithm,
                "value": run_document.workflow_digest.value
            }
        },
        "execution": {
            "executionRoot": attempt.execution_root,
            "maximumParallelSteps": 2,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": crate::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "succeeded",
        "forceAbort": null,
        "steps": [{
            "id": "produce",
            "role": "step",
            "kind": "cmd",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-02T12:01:44Z",
            "durationMilliseconds": 1000,
            "commandOutput": {
                "stdout": {
                    "encoding": "base64",
                    "data": "",
                    "retainedBytes": 0,
                    "discardedBytes": 0,
                    "truncated": false,
                    "fullyDrained": true
                },
                "stderr": {
                    "encoding": "base64",
                    "data": "",
                    "retainedBytes": 0,
                    "discardedBytes": 0,
                    "truncated": false,
                    "fullyDrained": true
                }
            }
        }],
        "exports": exports
    });
    let result_bytes = json_bytes(result);
    assert!(
        u64::try_from(result_bytes.len()).unwrap()
            <= crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES
    );
    assert!(
        u64::try_from(result_bytes.len()).unwrap() <= MAXIMUM_DURABLE_JSON_BYTES,
        "maximal artifact metadata must fit the unified durable document budget"
    );
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    fs::write(result_directory.join("exports/0001"), b"x").unwrap();
    fs::write(result_directory.join("result.json"), &result_bytes).unwrap();
    let result_root = open_directory_path(&result_directory).unwrap();
    crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("fixture must be valid Artifact Set V1");
    run.record_result_published().unwrap();

    load_local_archived_attempt(&run_path, None)
        .expect("a valid published Artifact Set V1 result must remain inspectable");
}

#[test]
fn archived_attempt_binds_retained_output_kind_and_export_metadata() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"true\"]}\nexports:\n  message:\n    ref: outputs.first.message\n",
    );
    let run_path = fixture.run_path("archive-output-binding");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let retained_bytes = b"\"evidence\"";
    retain_text_output_for_step(&run, "first", "message", retained_bytes);
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut valid = result_value(&result_directory);
    valid["exports"] = json!({
        "message": {
            "state": "available",
            "kind": "text",
            "mediaType": "text/plain; charset=utf-8",
            "path": "exports/0001",
            "sizeBytes": retained_bytes.len(),
            "digest": {
                "algorithm": "sha256",
                "value": DigestV1::sha256(retained_bytes).value
            }
        }
    });
    fs::write(result_directory.join("exports/0001"), retained_bytes).unwrap();
    overwrite_result(&result_directory, valid.clone());
    load_local_archived_attempt(&run_path, None).unwrap();

    let substituted = b"different";
    let mut substituted_result = valid.clone();
    substituted_result["exports"]["message"]["sizeBytes"] = substituted.len().into();
    substituted_result["exports"]["message"]["digest"]["value"] =
        DigestV1::sha256(substituted).value.into();
    fs::write(result_directory.join("exports/0001"), substituted).unwrap();
    overwrite_result(&result_directory, substituted_result);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    fs::write(result_directory.join("exports/0001"), retained_bytes).unwrap();
    overwrite_result(&result_directory, valid);
    run.state
        .update(|state| {
            let outputs = current_attempt_mut(state)?.progress.steps[0]
                .outputs
                .as_mut()
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let carrier = match outputs.first() {
                Some(RetainedOutputV1::Text { carrier, .. }) => carrier.clone(),
                _ => return Err(LocalRunDirectoryError::StateInvalid),
            };
            outputs[0] = RetainedOutputV1::Json {
                name: "message".to_owned(),
                producer: None,
                carrier: RetainedCarrierV1 {
                    media_type: "application/json".to_owned(),
                    ..carrier
                },
            };
            Ok(())
        })
        .unwrap();
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid,
    );
}

#[test]
fn archived_attempt_enforces_alias_source_identity() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      one:\n        kind: file\n        from: path\n        path: one.bin\n        mediaType: application/octet-stream\n      two:\n        kind: file\n        from: path\n        path: two.bin\n        mediaType: application/octet-stream\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\nexports:\n  a:\n    ref: outputs.first.one\n  b:\n    ref: outputs.first.one\n  c:\n    ref: outputs.first.two\n",
    );
    let run_path = fixture.run_path("archive-alias-identity");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    retain_file_outputs_for_step(
        &run,
        "first",
        &[
            ("one", "application/octet-stream", b"x"),
            ("two", "application/octet-stream", b"x"),
        ],
    );
    let result_directory = publish_result_fixture(&fixture, &run);
    let metadata = serde_json::json!({
        "state": "available",
        "kind": "file",
        "mediaType": "application/octet-stream",
        "path": "exports/0001",
        "sizeBytes": 1,
        "digest": {
            "algorithm": "sha256",
            "value": "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
        }
    });
    let mut valid = result_value(&result_directory);
    valid["exports"] = serde_json::json!({
        "a": metadata.clone(),
        "b": metadata.clone(),
        "c": {
            "state": "available",
            "kind": "file",
            "mediaType": "application/octet-stream",
            "path": "exports/0003",
            "sizeBytes": 1,
            "digest": {
                "algorithm": "sha256",
                "value": "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
            }
        }
    });
    overwrite_result(&result_directory, valid.clone());
    fs::write(result_directory.join("exports/0001"), b"x").unwrap();
    fs::write(result_directory.join("exports/0003"), b"x").unwrap();
    let result_root = open_directory_path(&result_directory).unwrap();
    crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("the alias and equal-content carriers form a valid Artifact Set V1 result");
    load_local_archived_attempt(&run_path, None)
        .expect("aliases of one retained output must share their owner carrier");

    let mut non_owner = valid.clone();
    non_owner["exports"]["b"]["path"] = Value::String("exports/0002".to_owned());
    overwrite_result(&result_directory, non_owner);
    fs::write(result_directory.join("exports/0002"), b"x").unwrap();
    crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("separate carriers are portable without the retained source identities");
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    let mut shared_by_distinct_sources = valid;
    shared_by_distinct_sources["exports"]["c"] = metadata;
    overwrite_result(&result_directory, shared_by_distinct_sources);
    fs::remove_file(result_directory.join("exports/0002")).unwrap();
    fs::remove_file(result_directory.join("exports/0003")).unwrap();
    crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("equal bytes are portable without the retained source identities");
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );
}

#[test]
fn archived_attempt_enforces_stream_prefix_retention_invariants() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-stream-retention");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let valid = result_value(&result_directory);

    let maximum = crate::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM;
    let retained = vec![b'x'; usize::try_from(maximum).unwrap()];
    let mut full_prefix = valid.clone();
    full_prefix["steps"][0]["commandOutput"]["stdout"] = serde_json::json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(&retained),
        "retainedBytes": maximum,
        "discardedBytes": 1,
        "truncated": true,
        "fullyDrained": true
    });
    overwrite_result(&result_directory, full_prefix);
    let archived = load_local_archived_attempt(&run_path, None).unwrap();
    assert_eq!(
        archived.steps[0]
            .command_output
            .as_ref()
            .unwrap()
            .stdout
            .bytes
            .as_ref(),
        retained
    );

    let mut impossible = valid;
    impossible["steps"][0]["commandOutput"]["stdout"]["discardedBytes"] = Value::from(1);
    impossible["steps"][0]["commandOutput"]["stdout"]["truncated"] = Value::Bool(true);
    overwrite_result(&result_directory, impossible);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );
}

#[test]
fn archived_attempt_validates_failure_identities_against_the_retained_step() {
    let fixture = AdmittedFixture::from_source_with_inputs(
        "schemaVersion: 1\ninputs:\n  request: {kind: text}\nsteps:\n  first:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: inputs.request\n    command:\n      argv: [\"true\"]\n    outputs:\n      artifact:\n        kind: file\n        from: path\n        path: artifact.txt\n        mediaType: text/plain\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\n",
        ResolvedInputs::new(BTreeMap::from([(
            "request".to_owned(),
            ResolvedInput::Text(Arc::from("retained request")),
        )])),
        1024,
    );
    let run_path = fixture.run_path("archive-failure-identities");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let original = result_value(&result_directory);
    let set_failure_detail = |detail: crate::workflow::evidence::FailureDetail| {
        run.state
            .update(|state| {
                current_attempt_mut(state)?.progress.steps[0].detail = Some(
                    crate::workflow::evidence::NodeDetail::Failed(detail.clone()),
                );
                Ok(())
            })
            .unwrap();
    };

    let mut invalid_name = original.clone();
    let invalid_name_detail = serde_json::json!({
        "phase": "start",
        "code": "input_invalid_name",
        "input": "../escape"
    });
    invalid_name["steps"][0]["detail"] = invalid_name_detail.clone();
    invalid_name["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    invalid_name["primaryIssue"] = serde_json::json!({
        "node": { "id": "first", "role": "step" },
        "state": "failed",
        "detail": invalid_name_detail
    });
    overwrite_result(&result_directory, invalid_name);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    let mut input_failure = original.clone();
    let declared_input_detail = serde_json::json!({
        "phase": "start",
        "code": "input_value_size_limit",
        "input": "prompt"
    });
    input_failure["steps"][0]["detail"] = declared_input_detail.clone();
    input_failure["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    input_failure["primaryIssue"] = serde_json::json!({
        "node": { "id": "first", "role": "step" },
        "state": "failed",
        "detail": declared_input_detail
    });
    overwrite_result(&result_directory, input_failure.clone());
    set_failure_detail(
        crate::workflow::evidence::FailureDetail::new(
            crate::workflow::evidence::FailurePhase::Start,
            crate::workflow::evidence::FailureCode::InputValueSizeLimit,
            Some("prompt".to_owned()),
            None,
            None,
            None,
        )
        .unwrap(),
    );
    load_local_archived_attempt(&run_path, None).unwrap();

    input_failure["steps"][0]["detail"]["input"] = Value::String("fabricated".to_owned());
    input_failure["primaryIssue"]["detail"]["input"] = Value::String("fabricated".to_owned());
    overwrite_result(&result_directory, input_failure);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    let mut indexed_scalar = original.clone();
    let indexed_detail = serde_json::json!({
        "phase": "start",
        "code": "input_collection_ordinal_limit",
        "input": "prompt",
        "collectionIndex": 0
    });
    indexed_scalar["steps"][0]["detail"] = indexed_detail.clone();
    indexed_scalar["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    indexed_scalar["primaryIssue"] = serde_json::json!({
        "node": { "id": "first", "role": "step" },
        "state": "failed",
        "detail": indexed_detail
    });
    overwrite_result(&result_directory, indexed_scalar);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    let mut output_failure = original;
    let declared_output_detail = serde_json::json!({
        "phase": "output_capture",
        "code": "output_missing",
        "output": "artifact"
    });
    output_failure["steps"][0]["detail"] = declared_output_detail.clone();
    output_failure["primaryIssue"] = serde_json::json!({
        "node": { "id": "first", "role": "step" },
        "state": "failed",
        "detail": declared_output_detail
    });
    overwrite_result(&result_directory, output_failure.clone());
    set_failure_detail(
        crate::workflow::evidence::FailureDetail::new(
            crate::workflow::evidence::FailurePhase::OutputCapture,
            crate::workflow::evidence::FailureCode::OutputMissing,
            None,
            None,
            Some("artifact".to_owned()),
            None,
        )
        .unwrap(),
    );
    load_local_archived_attempt(&run_path, None).unwrap();

    output_failure["steps"][0]["detail"]["output"] = Value::String("fabricated".to_owned());
    output_failure["primaryIssue"]["detail"]["output"] = Value::String("fabricated".to_owned());
    overwrite_result(&result_directory, output_failure);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );
}

#[test]
fn archived_attempt_rejects_impossible_outcomes_and_blocking_causes() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  second:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  third:\n    kind: cmd\n    dependsOn: [first, second]\n    command:\n      argv: [\"true\"]\n",
    );
    let run_path = fixture.run_path("archive-terminal-invariants");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::WorkflowFailed;
            attempt.progress.steps[0].state = AttemptStepStateV1::Failed;
            attempt.progress.steps[0].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(23),
                )
                .unwrap(),
            ));
            attempt.progress.steps[1].state = AttemptStepStateV1::Succeeded;
            attempt.progress.steps[2].state = AttemptStepStateV1::Blocked;
            attempt.progress.steps[2].detail =
                Some(crate::workflow::evidence::NodeDetail::Blocked(
                    crate::workflow::evidence::BlockedDetail::new([
                        crate::workflow::evidence::Prerequisite::control("first").unwrap(),
                    ])
                    .unwrap(),
                ));
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let stream = serde_json::json!({
        "encoding": "base64",
        "data": "",
        "retainedBytes": 0,
        "discardedBytes": 0,
        "truncated": false,
        "fullyDrained": true
    });
    let command_output = serde_json::json!({
        "stdout": stream.clone(),
        "stderr": stream
    });
    let valid = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": attempt.attempt_number,
        "workflow": {
            "path": fixture.admitted.workflow().source.workflow_path,
            "provenance": {
                "kind": "local",
                "sourceRoot": fixture.admitted.workflow().source.source_root
            },
            "digest": {
                "algorithm": run_document.workflow_digest.algorithm,
                "value": run_document.workflow_digest.value
            }
        },
        "execution": {
            "executionRoot": attempt.execution_root,
            "maximumParallelSteps": 2,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": crate::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "failed",
        "forceAbort": null,
        "primaryIssue": {
            "node": { "id": "first", "role": "step" },
            "state": "failed",
            "detail": { "phase": "execution", "code": "command_exit", "exitCode": 23 }
        },
        "steps": [
            {
                "id": "first",
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "failed",
                "startedAt": "2026-08-02T12:01:44Z",
                "durationMilliseconds": 100,
                "detail": {
                    "phase": "execution",
                    "code": "command_exit",
                    "exitCode": 23
                },
                "commandOutput": command_output.clone()
            },
            {
                "id": "second",
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "succeeded",
                "startedAt": "2026-08-02T12:01:44Z",
                "durationMilliseconds": 100,
                "commandOutput": command_output
            },
            {
                "id": "third",
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "blocked",
                "detail": {
                    "code": "prerequisites_unsatisfied",
                    "prerequisites": [{"kind": "control", "node": "first"}]
                }
            }
        ],
        "exports": {}
    });
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    overwrite_result(&result_directory, valid.clone());
    run.record_result_published().unwrap();
    load_local_archived_attempt(&run_path, None).unwrap();

    // Both roots can already be active when `second` fails first. The consumer records
    // that then-terminal prerequisite even if lexicographically lower `first` fails later.
    let mut historical_blocker = valid.clone();
    historical_blocker["primaryIssue"] = serde_json::json!({
        "node": { "id": "second", "role": "step" },
        "state": "failed",
        "detail": { "phase": "execution", "code": "command_exit", "exitCode": 29 }
    });
    historical_blocker["steps"][1]["state"] = Value::String("failed".to_owned());
    historical_blocker["steps"][1]["detail"] = serde_json::json!({
        "phase": "execution",
        "code": "command_exit",
        "exitCode": 29
    });
    historical_blocker["steps"][2]["detail"]["prerequisites"] =
        serde_json::json!([{"kind": "control", "node": "second"}]);
    run.state
        .update(|state| {
            let progress = &mut current_attempt_mut(state)?.progress.steps;
            progress[1].state = AttemptStepStateV1::Failed;
            progress[1].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(29),
                )
                .unwrap(),
            ));
            progress[2].detail = Some(crate::workflow::evidence::NodeDetail::Blocked(
                crate::workflow::evidence::BlockedDetail::new([
                    crate::workflow::evidence::Prerequisite::control("second").unwrap(),
                ])
                .unwrap(),
            ));
            Ok(())
        })
        .unwrap();
    overwrite_result(&result_directory, historical_blocker);
    load_local_archived_attempt(&run_path, None).unwrap();

    run.state
        .update(|state| {
            let progress = &mut current_attempt_mut(state)?.progress.steps;
            progress[1].state = AttemptStepStateV1::Succeeded;
            progress[1].detail = None;
            progress[2].detail = Some(crate::workflow::evidence::NodeDetail::Blocked(
                crate::workflow::evidence::BlockedDetail::new([
                    crate::workflow::evidence::Prerequisite::control("first").unwrap(),
                ])
                .unwrap(),
            ));
            Ok(())
        })
        .unwrap();

    let mut false_blocker = valid.clone();
    false_blocker["steps"][2]["detail"]["prerequisites"] =
        serde_json::json!([{"kind": "control", "node": "second"}]);
    overwrite_result(&result_directory, false_blocker);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    let mut impossible_success = valid;
    impossible_success["outcome"] = Value::String("succeeded".to_owned());
    impossible_success
        .as_object_mut()
        .unwrap()
        .remove("primaryIssue");
    overwrite_result(&result_directory, impossible_success);
    run.state
        .update(|state| {
            current_attempt_mut(state)?.state = AttemptStateV1::Succeeded;
            Ok(())
        })
        .unwrap();
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );
}

#[test]
fn archived_attempt_rejects_not_run_step_with_failed_dependency() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-impossible-not-run");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut result = result_value(&result_directory);
    result["steps"][1] = serde_json::json!({
        "id": "second",
        "role": "step",
        "kind": "cmd",
        "failurePolicy": "required",
        "state": "not_run",
        "detail": { "code": "failure_stop" }
    });
    overwrite_result(&result_directory, result);
    run.state
        .update(|state| {
            let step = &mut current_attempt_mut(state)?.progress.steps[1];
            step.state = AttemptStepStateV1::NotRun;
            step.detail = Some(crate::workflow::evidence::NodeDetail::NotRun(
                crate::workflow::evidence::NonExecutionDetail::failure_stop(),
            ));
            Ok(())
        })
        .unwrap();

    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );
}

#[test]
fn archived_attempt_rejects_symlinks_and_does_not_adopt_replacements() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-symlink");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let result = result_directory.join("result.json");
    fs::rename(&result, result_directory.join("result-real.json")).unwrap();
    symlink("result-real.json", &result).unwrap();
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable,
    );

    fs::remove_file(&result).unwrap();
    fs::rename(result_directory.join("result-real.json"), &result).unwrap();
    let replacement = fs::read(&result).unwrap();
    assert_archive_operational(
        load_local_archived_attempt_observed(
            &run_path,
            None,
            |_| {},
            |result_directory| {
                let result = result_directory.join("result.json");
                fs::rename(
                    &result,
                    result_directory.join("result-before-replacement.json"),
                )
                .unwrap();
                fs::write(result, &replacement).unwrap();
            },
        )
        .unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable,
    );

    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-path-replacement");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    publish_result_fixture(&fixture, &run);
    let archived = load_local_archived_attempt_observed(
        &run_path,
        None,
        |run_directory| {
            let moved = run_directory.with_file_name("archive-path-original");
            fs::rename(run_directory, moved).unwrap();
            fs::create_dir(run_directory).unwrap();
        },
        |_| {},
    )
    .unwrap();
    assert_eq!(archived.attempt_number, 1);
    assert!(durable_tree(&run_path).is_empty());
}
