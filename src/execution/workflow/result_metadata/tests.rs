use serde_json::{Value, json};

use super::*;

fn result_fixture() -> Value {
    let export = json!({
        "state": "available",
        "kind": "file",
        "mediaType": "application/octet-stream",
        "path": "exports/0001",
        "sizeBytes": 4,
        "digest": {
            "algorithm": "sha256",
            "value": "0".repeat(64)
        }
    });
    json!({
        "schemaVersion": 1,
        "attemptNumber": 1,
        "workflow": {
            "path": "workflow.yaml",
            "provenance": {
                "kind": "local",
                "sourceRoot": "/tmp/source"
            },
            "digest": {
                "algorithm": "sha256",
                "value": "1".repeat(64)
            }
        },
        "execution": {
            "executionRoot": "/tmp/execution",
            "maximumParallelSteps": 1,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "succeeded",
        "steps": [{
            "id": "produce",
            "role": "step",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-02T12:01:44Z",
            "durationMilliseconds": 1000
        }],
        "exports": {
            "first": export.clone(),
            "second": export
        }
    })
}

fn finalized_result_fixture() -> Value {
    let mut result = result_fixture();
    result["exports"] = json!({});
    result["finalization"] = json!({
        "trigger": "succeeded",
        "finalizers": [{
            "id": "cleanup",
            "role": "finalizer",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 100
        }],
        "issues": [],
        "forceAbort": false
    });
    result
}

fn cloud_result_fixture() -> Value {
    let mut result = result_fixture();
    result["workflow"]["provenance"] = json!({
        "kind": "cloud",
        "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
        "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "objectFormat": "sha1",
        "commitOid": "0123456789abcdef0123456789abcdef01234567"
    });
    result["execution"]
        .as_object_mut()
        .unwrap()
        .remove("executionRoot");
    result["execution"]["capacity"] = json!({
        "executionContract": "workflow_v1_cloud_inputs_artifacts@1",
        "sourceClosureDigest": { "algorithm": "sha256", "value": "1".repeat(64) },
        "generalMaximumTransitions": 8,
        "selectedMaximumTransitions": 7,
        "maximumInvocations": 1,
        "maximumRetainedBytesPerInvocation": 4_194_304,
        "diagnosticRetentionBytes": 8_388_608,
        "nativeSessionRetentionBytes": 4_194_304,
        "aggregateRetentionBytes": 12_582_912,
        "encodedOutboxBytes": 85_458_944
    });
    result
}

fn cloud_metadata_only_result_fixture() -> Value {
    let mut result = cloud_result_fixture();
    result["exports"] = json!({});
    result
}

fn failed_result_fixture(phase: &str, cause: Value) -> Value {
    let mut result = result_fixture();
    let mut detail = cause;
    detail["phase"] = Value::String(phase.to_owned());
    result["outcome"] = Value::String("failed".to_owned());
    result["primaryIssue"] = json!({
        "node": { "id": "produce", "role": "step" },
        "state": "failed",
        "detail": detail.clone()
    });
    result["steps"][0]["state"] = Value::String("failed".to_owned());
    result["steps"][0]["detail"] = detail;
    result
}

fn encode(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    bytes
}

#[test]
fn finalization_metadata_rejects_role_issue_and_force_mismatches() {
    let valid = finalized_result_fixture();
    assert!(decode(&encode(&valid)).is_ok());

    let mut wrong_role = valid.clone();
    wrong_role["finalization"]["finalizers"][0]["role"] = Value::String("step".to_owned());
    assert_eq!(decode(&encode(&wrong_role)), Err(ResultMetadataError));

    let mut false_issue = valid.clone();
    false_issue["finalization"]["issues"] = json!([{
        "node": { "id": "cleanup", "role": "finalizer" },
        "impact": "required"
    }]);
    assert_eq!(decode(&encode(&false_issue)), Err(ResultMetadataError));

    let mut impossible_force_abort = valid;
    impossible_force_abort["finalization"]["forceAbort"] = Value::Bool(true);
    assert_eq!(
        decode(&encode(&impossible_force_abort)),
        Err(ResultMetadataError)
    );
}

#[test]
fn force_abort_after_graceful_cancellation_accepts_authoritative_terminal_reason() {
    let mut result = finalized_result_fixture();
    result["outcome"] = json!("cancelled");
    result["finalization"]["cancellation"] = json!({
        "reason": "runner_shutdown",
        "forceStopDeadline": "2026-08-02T12:01:46Z"
    });
    result["finalization"]["forceAbort"] = json!(true);
    result["finalization"]["finalizers"][0]["state"] = json!("cancelled");
    result["finalization"]["finalizers"][0]["detail"] = json!({
        "code": "finalization_force_abort"
    });

    assert!(decode(&encode(&result)).is_ok());
}

#[test]
fn admits_exact_local_and_cloud_origin_profiles() {
    assert!(decode(&encode(&result_fixture())).is_ok());
    assert!(decode(&encode(&cloud_result_fixture())).is_ok());
}

#[test]
fn rejects_unknown_mixed_or_malformed_cloud_origin_profiles() {
    let mut invalid_results = Vec::new();

    let mut unknown_kind = cloud_metadata_only_result_fixture();
    unknown_kind["workflow"]["provenance"]["kind"] = Value::String("remote".to_owned());
    invalid_results.push(unknown_kind);

    let mut mixed_cloud = cloud_metadata_only_result_fixture();
    mixed_cloud["workflow"]["provenance"]["sourceRoot"] = Value::String("/runner".to_owned());
    invalid_results.push(mixed_cloud);

    let mut mixed_local = result_fixture();
    mixed_local["workflow"]["provenance"]["projectId"] =
        Value::String("prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
    invalid_results.push(mixed_local);

    let mut malformed_project = cloud_metadata_only_result_fixture();
    malformed_project["workflow"]["provenance"]["projectId"] =
        Value::String("prj_81k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
    invalid_results.push(malformed_project);

    let mut malformed_connection = cloud_metadata_only_result_fixture();
    malformed_connection["workflow"]["provenance"]["repositoryConnectionId"] =
        Value::String("rpc_01K0z6r1w8f4jy2m7q9v3x5abc".to_owned());
    invalid_results.push(malformed_connection);

    let mut malformed_commit = cloud_metadata_only_result_fixture();
    malformed_commit["workflow"]["provenance"]["commitOid"] =
        Value::String("0123456789abcdef0123456789abcdef0123456G".to_owned());
    invalid_results.push(malformed_commit);

    let mut runner_execution_root = cloud_metadata_only_result_fixture();
    runner_execution_root["execution"]["executionRoot"] = Value::String("/runner/work".to_owned());
    invalid_results.push(runner_execution_root);

    for field in [
        "runnerPath",
        "runnerId",
        "organizationName",
        "repositoryUrl",
        "bucket",
        "objectKey",
        "credential",
        "destinationPublicationState",
    ] {
        let mut extra_origin = cloud_metadata_only_result_fixture();
        extra_origin["workflow"]["provenance"][field] = Value::String("forbidden".to_owned());
        invalid_results.push(extra_origin);
    }

    for result in invalid_results {
        assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
    }
}

#[test]
fn local_and_cloud_profiles_share_result_invariants() {
    for mut result in [result_fixture(), cloud_result_fixture()] {
        result["exports"]["second"]["digest"]["value"] = Value::String("2".repeat(64));
        assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
    }

    for mut result in [result_fixture(), cloud_metadata_only_result_fixture()] {
        result["steps"][0]["state"] = Value::String("failed".to_owned());
        assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
    }
}

#[test]
fn accepts_stream_at_shared_retention_cap_and_rejects_larger_claim() {
    let maximum = super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM;
    let bytes = vec![b'x'; usize::try_from(maximum).unwrap()];
    let stream = json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(bytes),
        "retainedBytes": maximum,
        "discardedBytes": 0,
        "truncated": false,
        "fullyDrained": true
    });
    let mut result = result_fixture();
    result["steps"][0]["kind"] = Value::String("cmd".to_owned());
    result["steps"][0]["commandOutput"] = json!({
        "stdout": stream,
        "stderr": {
            "encoding": "base64",
            "data": "",
            "retainedBytes": 0,
            "discardedBytes": 0,
            "truncated": false,
            "fullyDrained": true
        }
    });

    assert!(decode(&encode(&result)).is_ok());

    let oversized = vec![b'x'; usize::try_from(maximum + 1).unwrap()];
    result["steps"][0]["commandOutput"]["stdout"]["data"] =
        Value::String(BASE64_STANDARD.encode(&oversized));
    result["steps"][0]["commandOutput"]["stdout"]["retainedBytes"] = Value::from(maximum + 1);
    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

#[test]
fn partitions_the_durable_stream_budget_across_maximum_step_count() {
    let step_count = 256;
    let maximum = super::super::maximum_retained_bytes_per_stream(step_count);
    let retained = vec![b'x'; usize::try_from(maximum).unwrap()];
    let stream = json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(&retained),
        "retainedBytes": maximum,
        "discardedBytes": 1,
        "truncated": true,
        "fullyDrained": true
    });
    let mut result = result_fixture();
    let command = json!({
        "id": "step0",
        "role": "step",
        "kind": "cmd",
        "failurePolicy": "required",
        "state": "succeeded",
        "startedAt": "2026-08-02T12:01:44Z",
        "durationMilliseconds": 1000,
        "commandOutput": {
            "stdout": stream,
            "stderr": {
                "encoding": "base64",
                "data": "",
                "retainedBytes": 0,
                "discardedBytes": 0,
                "truncated": false,
                "fullyDrained": true
            }
        }
    });
    let agents = (1..step_count).map(|index| {
        json!({
            "id": format!("step{index}"),
            "role": "step",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-02T12:01:44Z",
            "durationMilliseconds": 1000
        })
    });
    result["steps"] = Value::Array(std::iter::once(command).chain(agents).collect());

    assert!(decode(&encode(&result)).is_ok());

    let oversized = vec![b'x'; usize::try_from(maximum + 1).unwrap()];
    result["steps"][0]["commandOutput"]["stdout"] = json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(oversized),
        "retainedBytes": maximum + 1,
        "discardedBytes": 0,
        "truncated": false,
        "fullyDrained": true
    });
    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

#[test]
fn recovery_version_dispatch_precedes_nested_interpretation() {
    let mut result = result_fixture();
    result["steps"][0]["recovery"] = json!({
        "schemaVersion": 2,
        "futureNestedShape": {"not": "schema one"}
    });

    assert_eq!(
        dispatch_recovery_summary_versions(&result),
        Err(RecoverySummaryVersionError::Unsupported)
    );
    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

fn recovered_result_fixture() -> Value {
    let mut result = result_fixture();
    result["steps"][0]["recovery"] = json!({
        "schemaVersion": 1,
        "configuredRetries": 1,
        "rounds": [{
            "number": 1,
            "failedExecution": {
                "executionNumber": 1,
                "invocationId": 1,
                "failure": {
                    "phase": "execution",
                    "cause": { "code": "harness_failed" }
                }
            }
        }],
        "termination": {
            "kind": "recovered",
            "executionNumber": 2
        }
    });
    result["steps"][0]["invocations"] = json!([
        {
            "invocationId": 1,
            "role": "target",
            "targetExecution": 1,
            "state": "settled",
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:44.1Z",
            "durationMilliseconds": 100,
            "usage": { "inputTokens": 1, "outputTokens": 1 }
        },
        {
            "invocationId": 3,
            "role": "target",
            "targetExecution": 2,
            "state": "settled",
            "startedAt": "2026-08-02T12:01:44.2Z",
            "finishedAt": "2026-08-02T12:01:44.3Z",
            "durationMilliseconds": 100,
            "usage": { "inputTokens": 1, "outputTokens": 1 }
        }
    ]);
    result
}

fn handler_invocation_fixture() -> Value {
    json!({
        "invocationId": 2,
        "role": "recovery_handler",
        "recoveryRound": 1,
        "state": "settled",
        "startedAt": "2026-08-02T12:01:44.1Z",
        "finishedAt": "2026-08-02T12:01:44.2Z",
        "durationMilliseconds": 100,
        "usage": { "inputTokens": 0, "outputTokens": 0 }
    })
}

#[test]
fn recovered_summary_accepts_schema_length_non_ascii_text() {
    let mut result = recovered_result_fixture();
    result["steps"][0]["recovery"]["handlerKind"] = json!("cmd");
    result["steps"][0]["recovery"]["rounds"][0]["handler"] = json!({
        "kind": "cmd",
        "invocationId": 2,
        "outcome": "recheck",
        "summary": "é".repeat(3_000),
        "reason": "Verify the repair."
    });
    result["steps"][0]["invocations"]
        .as_array_mut()
        .unwrap()
        .insert(1, handler_invocation_fixture());

    assert!(decode(&encode(&result)).is_ok());
}

#[test]
fn recovered_summary_rejects_a_handler_that_gave_up() {
    let mut result = recovered_result_fixture();
    result["steps"][0]["recovery"]["handlerKind"] = json!("cmd");
    result["steps"][0]["recovery"]["rounds"][0]["handler"] = json!({
        "kind": "cmd",
        "invocationId": 2,
        "outcome": "gave_up",
        "summary": "No repair was made.",
        "reason": "The handler refused to recheck."
    });
    result["steps"][0]["invocations"]
        .as_array_mut()
        .unwrap()
        .insert(1, handler_invocation_fixture());

    assert_eq!(
        decode(&encode(&result)),
        Err(ResultMetadataError),
        "gave_up is terminal and cannot authorize the target execution claimed to recover"
    );
}

#[test]
fn handlerless_summary_rejects_a_phantom_handler_invocation() {
    let mut result = recovered_result_fixture();
    result["steps"][0]["invocations"]
        .as_array_mut()
        .unwrap()
        .insert(1, handler_invocation_fixture());

    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

#[test]
fn accepts_consistent_alias_metadata_owned_by_the_lowest_ordinal() {
    let decoded = decode(&encode(&result_fixture())).unwrap();

    assert_eq!(decoded.exports["first"], decoded.exports["second"]);
}

#[test]
fn rejects_removed_step_fields_and_duplicate_object_members() {
    let mut removed_field = result_fixture();
    removed_field["steps"][0]["committedOutputCount"] = Value::from(0);
    assert_eq!(decode(&encode(&removed_field)), Err(ResultMetadataError));

    let duplicate = String::from_utf8(encode(&result_fixture()))
        .unwrap()
        .replacen(
            "\"schemaVersion\": 1,",
            "\"schemaVersion\": 1,\n  \"schemaVersion\": 1,",
            1,
        );
    assert_eq!(decode(duplicate.as_bytes()), Err(ResultMetadataError));
}

#[test]
fn rejects_failures_with_impossible_phases_or_cause_fields() {
    let valid = failed_result_fixture(
        "execution",
        json!({ "code": "command_exit", "exitCode": 23 }),
    );
    assert!(decode(&encode(&valid)).is_ok());

    for invalid in [
        failed_result_fixture("start", json!({ "code": "command_exit", "exitCode": 23 })),
        failed_result_fixture(
            "execution",
            json!({ "code": "command_exit", "exitCode": 0 }),
        ),
        failed_result_fixture(
            "execution",
            json!({ "code": "command_exit", "input": "payload" }),
        ),
        failed_result_fixture(
            "execution",
            json!({ "code": "input_invalid_name", "input": "payload" }),
        ),
        failed_result_fixture(
            "output_capture",
            json!({ "code": "output_missing", "output": "" }),
        ),
    ] {
        assert_eq!(decode(&encode(&invalid)), Err(ResultMetadataError));
    }
}

#[test]
fn advisory_issue_is_valid_on_success_but_cannot_be_primary() {
    let mut advisory = result_fixture();
    advisory["steps"][0]["failurePolicy"] = Value::String("advisory".to_owned());
    advisory["steps"][0]["state"] = Value::String("failed".to_owned());
    advisory["steps"][0]["detail"] = json!({
        "phase": "execution",
        "code": "harness_failed"
    });
    assert!(decode(&encode(&advisory)).is_ok());

    let mut required = advisory.clone();
    required["steps"][0]["failurePolicy"] = Value::String("required".to_owned());
    assert_eq!(decode(&encode(&required)), Err(ResultMetadataError));

    advisory["outcome"] = Value::String("failed".to_owned());
    advisory["primaryIssue"] = json!({
        "node": { "id": "produce", "role": "step" },
        "state": "failed",
        "detail": { "phase": "execution", "code": "harness_failed" }
    });
    assert_eq!(decode(&encode(&advisory)), Err(ResultMetadataError));
}

#[test]
fn rejects_inconsistent_or_multiply_owned_alias_metadata() {
    let mut mismatch = result_fixture();
    mismatch["exports"]["second"]["digest"]["value"] = Value::String("2".repeat(64));
    assert_eq!(decode(&encode(&mismatch)), Err(ResultMetadataError));

    let mut non_owner = result_fixture();
    non_owner["exports"]["first"]["path"] = Value::String("exports/0002".to_owned());
    non_owner["exports"]["second"]["path"] = Value::String("exports/0002".to_owned());
    assert_eq!(decode(&encode(&non_owner)), Err(ResultMetadataError));
}
