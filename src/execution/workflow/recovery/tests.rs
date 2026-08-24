use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustix::fs::{Mode, OFlags, mkdirat, openat};
use serde_json::json;

use super::*;
use crate::execution::workflow::admission::{
    CancellationPolicy, CancellationSource, CaptureLimits, EnvironmentSnapshot, ExecutionContext,
    ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits, ResolvedImports, admit_workflow,
};
use crate::execution::workflow::agent::AgentFailureCause;
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{
    ProvisionalTargetFailure, RecoveryHandlerKind, RecoveryHandlerRecord, TargetExecutionNumber,
    TransitionSequence,
};

fn valid_decision() -> Vec<u8> {
    br#"{"schemaVersion":1,"decision":"recheck","summary":"repaired","reason":"verify unchanged"}"#
        .to_vec()
}

#[test]
fn decision_parser_rejects_each_closed_contract_failure_separately() {
    let overlong_summary = "s".repeat(MAXIMUM_RECOVERY_DECISION_TEXT_BYTES + 1);
    let overlong_reason = "r".repeat(MAXIMUM_RECOVERY_DECISION_TEXT_BYTES + 1);
    let cases = [
        (
            vec![b' '; MAXIMUM_RECOVERY_DECISION_BYTES + 1],
            RecoveryDecisionFailureKind::InputTooLarge,
        ),
        (vec![0xff], RecoveryDecisionFailureKind::InvalidUtf8),
        (b"{".to_vec(), RecoveryDecisionFailureKind::InvalidJson),
        (
            br#"{"schemaVersion":1,"schemaVersion":1,"decision":"recheck","summary":"s","reason":"r"}"#.to_vec(),
            RecoveryDecisionFailureKind::DuplicateKey,
        ),
        (
            br#"{"schemaVersion":1,"decision":"recheck","summary":"s","reason":"r","authority":"extra"}"#.to_vec(),
            RecoveryDecisionFailureKind::UnknownField,
        ),
        (
            br#"{"schemaVersion":2,"decision":"recheck","summary":"s","reason":"r"}"#.to_vec(),
            RecoveryDecisionFailureKind::UnsupportedSchemaVersion,
        ),
        (
            br#"{"schemaVersion":1,"decision":"continue","summary":"s","reason":"r"}"#.to_vec(),
            RecoveryDecisionFailureKind::UnknownDecision,
        ),
        (
            br#"{"schemaVersion":1,"decision":"recheck","summary":"","reason":"r"}"#.to_vec(),
            RecoveryDecisionFailureKind::EmptySummary,
        ),
        (
            serde_json::to_vec(&json!({
                "schemaVersion": 1,
                "decision": "recheck",
                "summary": overlong_summary,
                "reason": "r"
            }))
            .unwrap(),
            RecoveryDecisionFailureKind::SummaryTooLong,
        ),
        (
            br#"{"schemaVersion":1,"decision":"recheck","summary":"s","reason":""}"#.to_vec(),
            RecoveryDecisionFailureKind::EmptyReason,
        ),
        (
            serde_json::to_vec(&json!({
                "schemaVersion": 1,
                "decision": "recheck",
                "summary": "s",
                "reason": overlong_reason
            }))
            .unwrap(),
            RecoveryDecisionFailureKind::ReasonTooLong,
        ),
    ];

    for (bytes, expected) in cases {
        assert_eq!(parse_recovery_decision(&bytes), Err(expected));
    }
    assert_eq!(
        parse_recovery_decision(&valid_decision()).unwrap(),
        RecoveryDecision::recheck("repaired", "verify unchanged")
    );
    assert_eq!(
        parse_recovery_decision(
            br#"{"schemaVersion":1,"decision":"gave_up","summary":"inspected","reason":"unsafe to repair"}"#
        )
        .unwrap(),
        RecoveryDecision::gave_up("inspected", "unsafe to repair")
    );
}

#[test]
fn context_schema_one_materializes_every_required_current_and_prior_round_fact() {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(
        source_root.join("workflow.yaml"),
        "schemaVersion: 1\nsteps:\n  verify:\n    kind: cmd\n    failurePolicy: advisory\n    recovery:\n      retries: 2\n      handler:\n        kind: cmd\n        command:\n          argv: [/bin/true]\n    command:\n      argv: [/bin/false]\n",
    )
    .unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedImports::default(),
        ExecutionContext::new(
            execution_root,
            ExecutionRootLifecycle::CallerOwnedRetained,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(4, 1024, 4096),
                InputLimits::new(4, 1024, 4096, 4096),
                1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();
    let command_failure = |execution_number, invocation| ProvisionalTargetFailure {
        execution_number: TargetExecutionNumber::fixture(execution_number),
        invocation: ActionId {
            transition_sequence: TransitionSequence(invocation),
        },
        phase: FailurePhase::Execution,
        cause: StepFailureCause::Execution(StepExecutionFailure::Command(
            CommandExecutionFailure::UnsuccessfulExit { code: Some(75) },
        )),
    };
    let history = vec![
        RecoveryRoundRecord {
            number: RecoveryRoundNumber::fixture(1),
            failed_execution: command_failure(1, 11),
            handler: Some(RecoveryHandlerRecord {
                kind: RecoveryHandlerKind::Command,
                invocation: ActionId {
                    transition_sequence: TransitionSequence(12),
                },
                outcome: RecoveryHandlerOutcome::Recheck {
                    summary: "repaired first condition".to_owned(),
                    reason: "verify again".to_owned(),
                },
            }),
        },
        RecoveryRoundRecord {
            number: RecoveryRoundNumber::fixture(2),
            failed_execution: command_failure(2, 19),
            handler: Some(RecoveryHandlerRecord {
                kind: RecoveryHandlerKind::Command,
                invocation: ActionId {
                    transition_sequence: TransitionSequence(20),
                },
                outcome: RecoveryHandlerOutcome::Starting,
            }),
        },
    ];
    let context = build_context(
        &admitted,
        "verify",
        RecoveryRoundNumber::fixture(2),
        &history,
        vec![RecoveryDiagnostic {
            kind: "command_stderr".to_owned(),
            media_type: "application/octet-stream".to_owned(),
            byte_count: 8,
            trust: "untrusted".to_owned(),
            truncation: Some(RecoveryDiagnosticTruncation { discarded_bytes: 3 }),
            path: "target-stderr.bin".to_owned(),
        }],
    )
    .unwrap();

    assert_eq!(
        serde_json::to_value(context).unwrap(),
        json!({
            "schemaVersion": 1,
            "target": {
                "id": "verify",
                "kind": "cmd",
                "failurePolicy": "advisory"
            },
            "recoveryRound": 2,
            "maxRecoveryRounds": 2,
            "failedExecution": {
                "executionNumber": 2,
                "invocationId": 19,
                "phase": "execution",
                "cause": {
                    "kind": "command_exit",
                    "exitCode": 75
                }
            },
            "priorRounds": [{
                "recoveryRound": 1,
                "failedExecution": {
                    "executionNumber": 1,
                    "invocationId": 11,
                    "phase": "execution",
                    "cause": {
                        "kind": "command_exit",
                        "exitCode": 75
                    }
                },
                "handlerDecision": {
                    "schemaVersion": 1,
                    "decision": "recheck",
                    "summary": "repaired first condition",
                    "reason": "verify again"
                }
            }],
            "diagnostics": [{
                "kind": "command_stderr",
                "mediaType": "application/octet-stream",
                "byteCount": 8,
                "trust": "untrusted",
                "truncation": {"discardedBytes": 3},
                "path": "target-stderr.bin"
            }]
        })
    );
}

#[test]
fn lossy_projection_classifies_agent_adapter_start_failure_without_internal_detail() {
    let cause = project_cause(
        FailurePhase::Start,
        &StepFailureCause::Start(StepStartFailure::Agent(
            AgentFailureCause::HarnessStartFailed.into(),
        )),
    );
    assert_eq!(
        cause,
        RecoveryCause {
            kind: "agent_failure".to_owned(),
            exit_code: None,
            detail: None,
        }
    );
}

#[test]
fn context_reader_tolerates_unknown_nested_fields_tokens_and_absent_optionals() {
    let context = json!({
        "schemaVersion": 1,
        "target": {
            "id": "verify",
            "kind": "future_target_kind",
            "failurePolicy": "required",
            "compatibleAddition": {"nested": true}
        },
        "recoveryRound": 1,
        "maxRecoveryRounds": 2,
        "failedExecution": {
            "executionNumber": 1,
            "invocationId": 41,
            "phase": "future_phase",
            "cause": {
                "kind": "future_cause",
                "unknownCauseFact": 7
            },
            "unknownFailureFact": "opaque"
        },
        "priorRounds": [],
        "diagnostics": [{
            "kind": "future_diagnostic",
            "mediaType": "application/octet-stream",
            "byteCount": 3,
            "trust": "untrusted",
            "path": "opaque.bin",
            "unknownDiagnosticFact": [1, 2, 3]
        }],
        "unknownTopLevel": {"ignored": true}
    });
    let parsed = read_recovery_context(&serde_json::to_vec(&context).unwrap()).unwrap();
    assert_eq!(parsed.recovery_round, 1);
    assert_eq!(parsed.failed_execution.execution_number, 1);
    assert_eq!(parsed.failed_execution.phase, "future_phase");
    assert_eq!(parsed.failed_execution.cause.kind, "future_cause");
    assert_eq!(parsed.failed_execution.cause.exit_code, None);
    assert_eq!(parsed.failed_execution.cause.detail, None);
    assert_eq!(parsed.diagnostics[0].kind, "future_diagnostic");
    assert_eq!(parsed.diagnostics[0].trust, "untrusted");
    assert_eq!(parsed.diagnostics[0].truncation, None);
}

fn result_staging() -> (tempfile::TempDir, RecoveryInvocationStaging) {
    let temporary = tempfile::tempdir().unwrap();
    let root = open_directory_path(temporary.path()).unwrap();
    let identity: Arc<str> = Arc::from("invocation-fixture");
    mkdirat(&root, identity.as_ref(), Mode::RWXU).unwrap();
    let directory = openat(
        &root,
        identity.as_ref(),
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    mkdirat(&directory, RECOVERY_RESULT_DIRECTORY, Mode::RWXU).unwrap();
    let result_directory = openat(
        &directory,
        RECOVERY_RESULT_DIRECTORY,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let path = temporary.path().to_owned();
    let owner = Arc::new(RecoveryStagingInner {
        _temporary: tempfile::tempdir().unwrap(),
        root,
        path: path.clone(),
    });
    let result_path = path
        .join(identity.as_ref())
        .join(RECOVERY_RESULT_DIRECTORY)
        .join(RECOVERY_RESULT_FILE);
    (
        temporary,
        RecoveryInvocationStaging {
            owner,
            identity,
            directory,
            result_directory,
            context_path: path.join("unused-context.json"),
            result_path,
            released: false,
        },
    )
}

#[test]
fn descriptor_bound_result_reader_rejects_missing_symlink_nonregular_and_oversized() {
    let (_temporary, staging) = result_staging();
    assert_eq!(
        staging.read_decision(),
        Err(RecoveryResultReadFailure::Missing)
    );

    symlink("elsewhere", staging.result_path()).unwrap();
    assert_eq!(
        staging.read_decision(),
        Err(RecoveryResultReadFailure::SymbolicLink)
    );
    fs::remove_file(staging.result_path()).unwrap();

    fs::create_dir(staging.result_path()).unwrap();
    assert_eq!(
        staging.read_decision(),
        Err(RecoveryResultReadFailure::NotRegular)
    );
    fs::remove_dir(staging.result_path()).unwrap();

    fs::write(
        staging.result_path(),
        vec![b'x'; MAXIMUM_RECOVERY_DECISION_BYTES + 1],
    )
    .unwrap();
    assert_eq!(
        staging.read_decision(),
        Err(RecoveryResultReadFailure::TooLarge)
    );
}

#[test]
fn descriptor_bound_result_reader_returns_one_bounded_regular_file() {
    let (_temporary, staging) = result_staging();
    let bytes = valid_decision();
    fs::write(staging.result_path(), &bytes).unwrap();
    assert_eq!(staging.read_decision().unwrap(), bytes);
}
