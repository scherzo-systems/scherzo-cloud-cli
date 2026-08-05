use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::time::Duration;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, CaptureLimits, EnvironmentSnapshot, ExecutionContext,
    ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits, ResolvedAttachment,
    ResolvedImports, admit_workflow,
};
use crate::execution::workflow::resolution;

struct AdmittedFixture {
    _temporary: tempfile::TempDir,
    admitted: AdmittedWorkflow,
    execution_root: PathBuf,
    run_parent: PathBuf,
}

impl AdmittedFixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let run_parent = temporary.path().join("runs");
        for directory in [&source_root, &execution_root, &run_parent] {
            fs::create_dir(directory).unwrap();
        }
        fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let workflow = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let admitted = admit_workflow(
            workflow,
            ResolvedImports::new(
                Some(Arc::from("durable prompt\n")),
                Arc::from([ResolvedAttachment::new(
                    Arc::from("application/octet-stream"),
                    Arc::from([0_u8, 1, 0xff]),
                )]),
            ),
            ExecutionContext::new(
                execution_root.clone(),
                ExecutionRootLifecycle::CallerOwnedRetained,
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
                    leader: crate::execution::workflow::process_group::LeaderState::Running,
                },
            },
        ),
        LocalRecoveryStatus::OwnershipUnproven {
            guard_ids: guard_ids.clone(),
            reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
        }
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
    assert_eq!(manifest["sourceFiles"][0]["relativeFile"], "files/0001");
    assert_eq!(manifest["imports"]["prompt"]["relativeFile"], "files/0002");
    assert_eq!(
        manifest["imports"]["attachments"][0]["relativeFile"],
        "files/0003"
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0001")).unwrap(),
        fixture.admitted.workflow().source_closure["workflow.yaml"].as_ref()
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0002")).unwrap(),
        b"durable prompt\n"
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0003")).unwrap(),
        [0_u8, 1, 0xff]
    );

    let state = read_state(run.root_handle()).unwrap();
    assert_eq!(state.revision, 1);
    assert_eq!(state.attempts[0].state, AttemptStateV1::Created);
    assert_eq!(state.attempts[0].progress.steps[0].id, "first");
    assert_eq!(state.attempts[0].progress.steps[1].id, "second");

    drop(run);
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
fn atomic_state_replace_rejects_a_concurrent_authoritative_change() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("concurrent"), &fixture.admitted).unwrap();
    let mut observer = CorruptBeforeReplace {
        state_path: run.run_directory().join(STATE_FILE),
    };

    let failure = run
        .state
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
        .unwrap_err();

    assert_eq!(failure, LocalRunDirectoryError::StateConflict);
    assert_eq!(
        fs::read(run.run_directory().join(STATE_FILE)).unwrap(),
        b"partial"
    );
}

fn json_bytes(value: Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(&value).unwrap();
    bytes.push(b'\n');
    bytes
}
