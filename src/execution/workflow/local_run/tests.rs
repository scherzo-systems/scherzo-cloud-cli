use std::collections::BTreeMap;
use std::fs::{self, Permissions};
use std::num::NonZeroU64;
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use std::time::Duration;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, CaptureLimits, EnvironmentSnapshot, ExecutionContext,
    ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits, ResolvedAttachment,
    ResolvedImports, admit_workflow,
};
use crate::execution::workflow::archived_attempt::{
    ArchivedAttemptIneligibilityReason, ArchivedAttemptLoadError,
    ArchivedAttemptOperationalErrorCode, ArchivedAttemptState, ArchivedStepDetail,
    ArchivedWorkflowOutcome, load_local_archived_attempt, load_local_archived_attempt_observed,
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
        Self::from_source(
            "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\n",
        )
    }

    fn from_source(source: &str) -> Self {
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
            + crate::execution::workflow::result_metadata::MAXIMUM_ENCODED_RETAINED_STREAM_BYTES
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
            source_file("prompt.txt", 2),
        ],
        imports: ManifestImportsV1 {
            prompt: None,
            attachments: Vec::new(),
        },
    };

    assert_eq!(
        validate_manifest(&manifest),
        Err(LocalRunDirectoryError::SerializationUnavailable)
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
    assert_eq!(manifest["maximumParallelSteps"], 2);
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

fn settle_as_workflow_failed(run: &InitialLocalRun) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.state = AttemptStateV1::WorkflowFailed;
            attempt.progress.steps[0].state = AttemptStepStateV1::Failed;
            attempt.progress.steps[1].state = AttemptStepStateV1::Blocked;
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

#[test]
fn retry_commits_only_fresh_attempt_state_and_retained_inputs() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retry-fresh");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    let predecessor = read_state(initial.root_handle()).unwrap().attempts[0].clone();
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let pending = *pending;
    let (_, imports, maximum_parallel_steps) = pending.execution_specification();
    assert_eq!(maximum_parallel_steps, 2);
    assert_eq!(imports.prompt(), Some("durable prompt\n"));
    assert_eq!(imports.attachments()[0].bytes(), [0_u8, 1, 0xff]);
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
                leader: crate::execution::workflow::process_group::LeaderState::Running,
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
                leader: crate::execution::workflow::process_group::LeaderState::Running,
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

    let prepared = crate::execution::workflow::publication::prepare_attempt_result_destination(
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

fn settle_as_cancelled(run: &LocalAttemptOwner) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled.clone());
            attempt.state = AttemptStateV1::Cancelled;
            attempt.cancellation = Some(AttemptCancellationV1 {
                reason: CancellationReasonV1::UserRequest,
                requested_at: settled.clone(),
                force_stop_deadline: settled,
                workflow_confirmed: true,
            });
            for step in &mut attempt.progress.steps {
                step.state = AttemptStepStateV1::Cancelled;
            }
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
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
    let (outcome, steps, primary_failure) = match attempt.state {
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
                    "failure": {
                        "phase": "execution",
                        "cause": { "code": "command_exit", "exitCode": 23 }
                    },
                    "commandOutput": command_output
                }),
                serde_json::json!({
                    "id": "second",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "blocked",
                    "dependency": "first"
                }),
            ],
            Some(serde_json::json!({
                "step": "first",
                "phase": "execution",
                "cause": { "code": "command_exit", "exitCode": 23 }
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
                    "reason": "user_request"
                }),
                serde_json::json!({
                    "id": "second",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "cancelled",
                    "reason": "user_request"
                }),
            ],
            None,
        ),
        state => panic!("unsupported fixture state: {state:?}"),
    };
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
            "maximumRetainedBytesPerStream": crate::execution::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": outcome,
        "steps": steps,
        "exports": {}
    });
    if let Some(primary_failure) = primary_failure {
        result["primaryFailure"] = primary_failure;
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
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    failurePolicy: advisory\n    command:\n      argv: [\"true\"]\n    outputs:\n      report:\n        kind: file\n        path: report.txt\n        mediaType: text/plain\n  second:\n    kind: cmd\n    failurePolicy: advisory\n    inputs:\n      report:\n        ref: outputs.first.report\n    command:\n      argv: [\"true\"]\n",
    );
    let run_path = fixture.run_path("archive-advisory-success");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut result = result_value(&result_directory);
    result["steps"][0]["failurePolicy"] = Value::String("advisory".to_owned());
    result["steps"][0]["state"] = Value::String("failed".to_owned());
    result["steps"][0]["failure"] = serde_json::json!({
        "phase": "execution",
        "cause": { "code": "command_exit", "exitCode": 9 }
    });
    result["steps"][1] = serde_json::json!({
        "id": "second",
        "kind": "cmd",
        "failurePolicy": "advisory",
        "state": "blocked",
        "dependency": "first"
    });
    overwrite_result(&result_directory, result);
    run.state
        .update(|state| {
            let progress = &mut current_attempt_mut(state)?.progress.steps;
            progress[0].state = AttemptStepStateV1::Failed;
            progress[1].state = AttemptStepStateV1::Blocked;
            Ok(())
        })
        .unwrap();

    let archived = load_local_archived_attempt(&run_path, None).unwrap();

    assert_eq!(archived.state, ArchivedAttemptState::Succeeded);
    assert_eq!(archived.outcome, ArchivedWorkflowOutcome::Succeeded);
    assert!(archived.primary_failure.is_none());
    assert!(matches!(
        archived.steps[0].detail,
        ArchivedStepDetail::Failed(_)
    ));
    assert!(matches!(
        archived.steps[1].detail,
        ArchivedStepDetail::Blocked { ref dependency } if dependency == "first"
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
    let before = durable_tree(&run_path);

    let archived = load_local_archived_attempt(&run_path, None).unwrap();

    assert_eq!(archived.current_attempt_number, 1);
    assert_eq!(archived.attempt_number, 1);
    assert_eq!(archived.state, ArchivedAttemptState::WorkflowFailed);
    assert_eq!(archived.outcome, ArchivedWorkflowOutcome::Failed);
    assert_eq!(archived.result_directory, result_directory);
    assert_eq!(archived.workflow.presentation_order, ["first", "second"]);
    assert_eq!(archived.steps.len(), 2);
    assert!(matches!(
        archived.steps[0].detail,
        ArchivedStepDetail::Failed(_)
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

    run.record_result_publication_failed(PublicationFailurePhaseV1::Serialization)
        .unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::PublicationFailed,
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
    value.as_object_mut().unwrap().remove("primaryFailure");
    invalid_values.push(value);
    let mut value = valid.clone();
    value["steps"].as_array_mut().unwrap().swap(0, 1);
    invalid_values.push(value);
    let mut value = valid.clone();
    value["steps"][0]["commandOutput"]["stdout"]["retainedBytes"] = Value::from(2);
    invalid_values.push(value);
    let mut value = valid.clone();
    value["steps"][0]["failure"]["cause"]["code"] = Value::String("future_code".to_owned());
    value["primaryFailure"]["cause"]["code"] = Value::String("future_code".to_owned());
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
        matches!(step.detail, ArchivedStepDetail::Cancelled { .. })
            && step.started_at.is_none()
            && step.duration.is_none()
            && step.command_output.is_none()
    }));
}

#[test]
fn archived_attempt_loads_valid_result_larger_than_state_document_limit() {
    let mut source = String::from("schemaVersion: 1\nsteps:\n");
    for index in 0..25 {
        source.push_str(&format!(
            "  step{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
        ));
    }
    let fixture = AdmittedFixture::from_source(&source);
    let run_path = fixture.run_path("archive-large-result");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);

    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let stream = serde_json::json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(vec![b'x'; 65_536]),
        "retainedBytes": 65_536,
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
            "maximumRetainedBytesPerStream": crate::execution::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "succeeded",
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
    assert_eq!(archived.steps.len(), 25);
}

#[test]
fn archived_attempt_accepts_results_within_the_artifact_set_metadata_limit() {
    let prefix = "a/b;x=";
    let control_count = 128 - prefix.chars().count();
    let media_type = format!("{prefix}{}", "\u{1}".repeat(control_count));
    let source_media_type = format!("{prefix}{}", "\\u0001".repeat(control_count));
    let mut source = format!(
        "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      payload:\n        kind: file\n        path: payload.bin\n        mediaType: \"{source_media_type}\"\nexports:\n"
    );
    for index in 0..4_096 {
        let name = format!("e{}{index:04}", "a".repeat(59));
        source.push_str(&format!("  {name}:\n    ref: outputs.produce.payload\n"));
    }
    let fixture = AdmittedFixture::from_source(&source);
    let run_path = fixture.run_path("large-result");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);

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
            "maximumRetainedBytesPerStream": crate::execution::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "succeeded",
        "steps": [{
            "id": "produce",
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
            <= crate::execution::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES
    );
    assert!(
        u64::try_from(result_bytes.len()).unwrap() > MAXIMUM_DURABLE_JSON_BYTES,
        "the artifact metadata fixture must exceed the smaller run/state JSON budget"
    );
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    fs::write(result_directory.join("exports/0001"), b"x").unwrap();
    fs::write(result_directory.join("result.json"), &result_bytes).unwrap();
    let result_root = open_directory_path(&result_directory).unwrap();
    crate::execution::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::execution::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("fixture must be valid Artifact Set V1");
    run.record_result_published().unwrap();

    load_local_archived_attempt(&run_path, None)
        .expect("a valid published Artifact Set V1 result must remain inspectable");
}

#[test]
fn archived_attempt_enforces_alias_source_identity() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      one:\n        kind: file\n        path: one.bin\n        mediaType: application/octet-stream\n      two:\n        kind: file\n        path: two.bin\n        mediaType: application/octet-stream\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\nexports:\n  a:\n    ref: outputs.first.one\n  b:\n    ref: outputs.first.one\n  c:\n    ref: outputs.first.two\n",
    );
    let run_path = fixture.run_path("archive-alias-identity");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
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
    crate::execution::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::execution::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("the alias and equal-content carriers form a valid Artifact Set V1 result");
    load_local_archived_attempt(&run_path, None)
        .expect("aliases of one retained output must share their owner carrier");

    let mut non_owner = valid.clone();
    non_owner["exports"]["b"]["path"] = Value::String("exports/0002".to_owned());
    overwrite_result(&result_directory, non_owner);
    fs::write(result_directory.join("exports/0002"), b"x").unwrap();
    crate::execution::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::execution::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
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
    crate::execution::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::execution::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
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

    let maximum = crate::execution::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM;
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
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: imports.prompt\n    command:\n      argv: [\"true\"]\n    outputs:\n      artifact:\n        kind: file\n        path: artifact.txt\n        mediaType: text/plain\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\n",
    );
    let run_path = fixture.run_path("archive-failure-identities");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let original = result_value(&result_directory);

    let mut invalid_name = original.clone();
    let invalid_name_cause = serde_json::json!({
        "code": "input_invalid_name",
        "input": "../escape"
    });
    invalid_name["steps"][0]["failure"] = serde_json::json!({
        "phase": "start",
        "cause": invalid_name_cause.clone()
    });
    invalid_name["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    invalid_name["primaryFailure"] = serde_json::json!({
        "step": "first",
        "phase": "start",
        "cause": invalid_name_cause
    });
    overwrite_result(&result_directory, invalid_name);
    load_local_archived_attempt(&run_path, None)
        .expect("an invalid-name failure preserves the offending producer identity");

    let mut input_failure = original.clone();
    let declared_input_cause = serde_json::json!({
        "code": "input_value_size_limit",
        "input": "prompt"
    });
    input_failure["steps"][0]["failure"] = serde_json::json!({
        "phase": "start",
        "cause": declared_input_cause.clone()
    });
    input_failure["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    input_failure["primaryFailure"] = serde_json::json!({
        "step": "first",
        "phase": "start",
        "cause": declared_input_cause
    });
    overwrite_result(&result_directory, input_failure.clone());
    load_local_archived_attempt(&run_path, None).unwrap();

    input_failure["steps"][0]["failure"]["cause"]["input"] = Value::String("fabricated".to_owned());
    input_failure["primaryFailure"]["cause"]["input"] = Value::String("fabricated".to_owned());
    overwrite_result(&result_directory, input_failure);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    let mut indexed_scalar = original.clone();
    let indexed_cause = serde_json::json!({
        "code": "input_collection_ordinal_limit",
        "input": "prompt",
        "collectionIndex": 0
    });
    indexed_scalar["steps"][0]["failure"] = serde_json::json!({
        "phase": "start",
        "cause": indexed_cause.clone()
    });
    indexed_scalar["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    indexed_scalar["primaryFailure"] = serde_json::json!({
        "step": "first",
        "phase": "start",
        "cause": indexed_cause
    });
    overwrite_result(&result_directory, indexed_scalar);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultInvalid,
    );

    let mut output_failure = original;
    let declared_output_cause = serde_json::json!({
        "code": "output_missing",
        "output": "artifact"
    });
    output_failure["steps"][0]["failure"] = serde_json::json!({
        "phase": "output_capture",
        "cause": declared_output_cause.clone()
    });
    output_failure["primaryFailure"] = serde_json::json!({
        "step": "first",
        "phase": "output_capture",
        "cause": declared_output_cause
    });
    overwrite_result(&result_directory, output_failure.clone());
    load_local_archived_attempt(&run_path, None).unwrap();

    output_failure["steps"][0]["failure"]["cause"]["output"] =
        Value::String("fabricated".to_owned());
    output_failure["primaryFailure"]["cause"]["output"] = Value::String("fabricated".to_owned());
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
            attempt.state = AttemptStateV1::WorkflowFailed;
            attempt.progress.steps[0].state = AttemptStepStateV1::Failed;
            attempt.progress.steps[1].state = AttemptStepStateV1::Succeeded;
            attempt.progress.steps[2].state = AttemptStepStateV1::Blocked;
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
            "maximumRetainedBytesPerStream": crate::execution::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "failed",
        "primaryFailure": {
            "step": "first",
            "phase": "execution",
            "cause": { "code": "command_exit", "exitCode": 23 }
        },
        "steps": [
            {
                "id": "first",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "failed",
                "startedAt": "2026-08-02T12:01:44Z",
                "durationMilliseconds": 100,
                "failure": {
                    "phase": "execution",
                    "cause": { "code": "command_exit", "exitCode": 23 }
                },
                "commandOutput": command_output.clone()
            },
            {
                "id": "second",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "succeeded",
                "startedAt": "2026-08-02T12:01:44Z",
                "durationMilliseconds": 100,
                "commandOutput": command_output
            },
            {
                "id": "third",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "blocked",
                "dependency": "first"
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
    historical_blocker["primaryFailure"] = serde_json::json!({
        "step": "second",
        "phase": "execution",
        "cause": { "code": "command_exit", "exitCode": 29 }
    });
    historical_blocker["steps"][1]["state"] = Value::String("failed".to_owned());
    historical_blocker["steps"][1]["failure"] = serde_json::json!({
        "phase": "execution",
        "cause": { "code": "command_exit", "exitCode": 29 }
    });
    historical_blocker["steps"][2]["dependency"] = Value::String("second".to_owned());
    run.state
        .update(|state| {
            current_attempt_mut(state)?.progress.steps[1].state = AttemptStepStateV1::Failed;
            Ok(())
        })
        .unwrap();
    overwrite_result(&result_directory, historical_blocker);
    load_local_archived_attempt(&run_path, None).unwrap();

    run.state
        .update(|state| {
            current_attempt_mut(state)?.progress.steps[1].state = AttemptStepStateV1::Succeeded;
            Ok(())
        })
        .unwrap();

    let mut false_blocker = valid.clone();
    false_blocker["steps"][2]["dependency"] = Value::String("second".to_owned());
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
        .remove("primaryFailure");
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
        "kind": "cmd",
        "failurePolicy": "required",
        "state": "not_run",
        "reason": "failure_stop"
    });
    overwrite_result(&result_directory, result);
    run.state
        .update(|state| {
            current_attempt_mut(state)?.progress.steps[1].state = AttemptStepStateV1::NotRun;
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
