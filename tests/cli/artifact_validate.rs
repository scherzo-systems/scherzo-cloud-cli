use std::collections::BTreeMap;
#[cfg(target_os = "linux")]
use std::ffi::OsStr;
use std::fs;
use std::io::Write as _;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use flate2::{Compression, write::ZlibEncoder};
use ring::digest::{SHA1_FOR_LEGACY_USE_ONLY, SHA256, digest};
use serde_json::{Value, json};
use tempfile::TempDir;

use super::run;

struct ArtifactSet {
    _temporary: TempDir,
    root: PathBuf,
}

impl ArtifactSet {
    fn valid() -> Self {
        let temporary = tempfile::tempdir().expect("temporary artifact directory should exist");
        let root = temporary.path().join("downloaded-artifact");
        let exports = root.join("exports");
        fs::create_dir_all(&exports).expect("artifact exports directory should exist");

        let json_bytes = br#"{"a":1,"z":[true]}"#;
        let text_bytes = "portable text α\n".as_bytes();
        let file_bytes = b"\0portable-file\xff";
        fs::write(exports.join("0001"), json_bytes).unwrap();
        fs::write(exports.join("0002"), text_bytes).unwrap();
        fs::write(exports.join("0003"), file_bytes).unwrap();

        let file = available_export(
            "file",
            "application/octet-stream",
            "exports/0003",
            file_bytes,
        );
        let result = json!({
            "schemaVersion": 1,
            "attemptNumber": 1,
            "workflow": {
                "path": "workflows/portable.yaml",
                "provenance": {
                    "kind": "local",
                    "sourceRoot": "/original/source/does-not-need-to-exist"
                },
                "digest": {
                    "algorithm": "sha256",
                    "value": "1".repeat(64)
                }
            },
            "execution": {
                "executionRoot": "/original/execution/does-not-need-to-exist",
                "maximumParallelSteps": 2,
                "startedAt": "2026-08-06T10:00:00Z",
                "finishedAt": "2026-08-06T10:00:01Z",
                "durationMilliseconds": 1000
            },
            "commandOutputPolicy": {
                "encoding": "base64",
                "maximumRetainedBytesPerStream": 4194304
            },
            "outcome": "succeeded",
            "steps": [{
                "id": "produce",
                "role": "step",
                "kind": "agent",
                "failurePolicy": "required",
                "state": "succeeded",
                "startedAt": "2026-08-06T10:00:00Z",
                "durationMilliseconds": 1000
            }],
            "exports": {
                "data": available_export(
                    "json",
                    "application/json",
                    "exports/0001",
                    json_bytes,
                ),
                "document": available_export(
                    "text",
                    "text/plain; charset=utf-8",
                    "exports/0002",
                    text_bytes,
                ),
                "payload": file.clone(),
                "payloadCopy": file,
                "unavailable": {
                    "state": "unavailable",
                    "reason": "source_blocked"
                }
            }
        });
        write_result(&root, &result);

        Self {
            _temporary: temporary,
            root,
        }
    }

    fn argument(&self) -> &str {
        self.root.to_str().expect("artifact path should be UTF-8")
    }

    fn result(&self) -> Value {
        serde_json::from_slice(&fs::read(self.root.join("result.json")).unwrap()).unwrap()
    }

    fn replace_result(&self, result: &Value) {
        write_result(&self.root, result);
    }

    fn use_cloud_profile(&self) {
        let mut result = self.result();
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
        self.replace_result(&result);
    }
}

fn available_export(kind: &str, media_type: &str, path: &str, bytes: &[u8]) -> Value {
    json!({
        "state": "available",
        "kind": kind,
        "mediaType": media_type,
        "path": path,
        "sizeBytes": bytes.len(),
        "digest": {
            "algorithm": "sha256",
            "value": hex(digest(&SHA256, bytes).as_ref())
        }
    })
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::new();
    for byte in bytes {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    result
}

fn write_result(root: &Path, result: &Value) {
    let mut bytes = serde_json::to_vec_pretty(result).unwrap();
    bytes.push(b'\n');
    fs::write(root.join("result.json"), bytes).unwrap();
}

fn compress_pack_entry(pack: &mut Vec<u8>, bytes: &[u8]) {
    let mut compressed = ZlibEncoder::new(Vec::new(), Compression::default());
    compressed.write_all(bytes).unwrap();
    pack.extend_from_slice(&compressed.finish().unwrap());
}

fn finish_external_delta_bundle(base: &str, head: &str, mut pack: Vec<u8>) -> Vec<u8> {
    let checksum = digest(&SHA1_FOR_LEGACY_USE_ONLY, &pack);
    pack.extend_from_slice(checksum.as_ref());

    let mut bundle =
        format!("# v2 git bundle\n-{base} prerequisite\n{head} refs/scherzo/head\n\n").into_bytes();
    bundle.extend_from_slice(&pack);
    bundle
}

fn malformed_external_delta_bundle(base: &str, head: &str) -> Vec<u8> {
    let mut pack = Vec::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2_u32.to_be_bytes());
    pack.extend_from_slice(&1_u32.to_be_bytes());
    pack.push(0x73); // REF_DELTA with three inflated delta bytes.
    pack.extend_from_slice(&[0xaa_u8; 20]);
    compress_pack_entry(&mut pack, &[0, 0, 0]); // Base size, result size, invalid opcode.
    finish_external_delta_bundle(base, head, pack)
}

fn overdeep_unresolved_delta_bundle(base: &str, head: &str) -> Vec<u8> {
    const OFS_DELTA_COUNT: u32 = 65;

    let mut pack = Vec::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2_u32.to_be_bytes());
    pack.extend_from_slice(&(OFS_DELTA_COUNT + 1).to_be_bytes());

    let mut previous_offset = pack.len();
    pack.push(0x72); // REF_DELTA with a valid empty-to-empty delta program.
    pack.extend_from_slice(&[0xaa_u8; 20]);
    compress_pack_entry(&mut pack, &[0, 0]);
    for _ in 0..OFS_DELTA_COUNT {
        let offset = pack.len();
        let distance = offset - previous_offset;
        assert!(distance < 128, "fixture OFS distance must fit one byte");
        pack.push(0x62); // OFS_DELTA with a valid empty-to-empty delta program.
        pack.push(u8::try_from(distance).unwrap());
        compress_pack_entry(&mut pack, &[0, 0]);
        previous_offset = offset;
    }
    finish_external_delta_bundle(base, head, pack)
}

fn byte_snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut snapshot = BTreeMap::new();
    for path in [
        PathBuf::from("result.json"),
        PathBuf::from("exports/0001"),
        PathBuf::from("exports/0002"),
        PathBuf::from("exports/0003"),
    ] {
        snapshot.insert(path.clone(), fs::read(root.join(path)).unwrap());
    }
    snapshot
}

fn diagnostic_codes(report: &Value) -> Vec<&str> {
    report["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|diagnostic| diagnostic["code"].as_str().unwrap())
        .collect()
}

#[test]
fn missing_artifact_directory_is_operational_for_humans_and_structured_for_json() {
    let temporary = tempfile::tempdir().unwrap();
    let missing = temporary.path().join("missing-artifact");
    let argument = missing.to_str().unwrap();

    let human = run(&["artifact", "validate", argument]);

    assert_eq!(human.status.code(), Some(1));
    assert!(human.stdout.is_empty());
    let structured = run(&["artifact", "validate", "--json", argument]);

    assert_eq!(structured.status.code(), Some(1));
    assert!(structured.stderr.is_empty());
    let report: Value = serde_json::from_slice(&structured.stdout).unwrap();
    assert_eq!(report["outcome"], "invalid");
    assert_eq!(
        report["diagnostics"][0]["code"],
        "artifact_directory_unavailable"
    );
}

#[test]
fn copied_local_profile_set_validates_in_human_and_json_modes_without_mutation() {
    let artifact = ArtifactSet::valid();
    let before = byte_snapshot(&artifact.root);

    let human = run(&["artifact", "validate", artifact.argument()]);
    assert!(human.status.success());
    assert!(!human.stdout.is_empty());
    assert!(human.stderr.is_empty());

    let structured = run(&["artifact", "validate", "--json", artifact.argument()]);
    assert!(structured.status.success());
    let report: Value = serde_json::from_slice(&structured.stdout).unwrap();
    assert_eq!(
        report,
        json!({
            "schemaVersion": 1,
            "command": "scherzo-cloud artifact validate",
            "outcome": "valid",
            "exitStatus": 0,
            "artifactSetVersion": 1,
            "artifactDirectory": fs::canonicalize(&artifact.root).unwrap(),
            "summary": {
                "declaredExports": 5,
                "availableExports": 4,
                "unavailableExports": 1,
                "referencedCarriers": 3,
                "carrierBytes": 50
            }
        })
    );
    assert!(structured.stdout.ends_with(b"\n"));
    assert!(structured.stderr.is_empty());
    assert_eq!(byte_snapshot(&artifact.root), before);
}

#[test]
fn finalization_summary_is_validated_without_mutating_portable_artifact() {
    let artifact = ArtifactSet::valid();
    let mut result = artifact.result();
    result["finalization"] = json!({
        "trigger": "succeeded",
        "finalizers": [{
            "id": "cleanup",
            "role": "finalizer",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-06T10:00:01Z",
            "durationMilliseconds": 0
        }],
        "issues": [],
        "forceAbort": false
    });
    artifact.replace_result(&result);
    let before = fs::read(artifact.root.join("result.json")).unwrap();

    let valid = run(&["artifact", "validate", "--json", artifact.argument()]);

    assert!(
        valid.status.success(),
        "{}",
        String::from_utf8_lossy(&valid.stdout)
    );
    assert_eq!(fs::read(artifact.root.join("result.json")).unwrap(), before);

    result["finalization"]["issues"] = json!([{
        "node": { "id": "cleanup", "role": "finalizer" },
        "impact": "required"
    }]);
    artifact.replace_result(&result);
    let invalid = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&invalid.stdout).unwrap();

    assert_eq!(invalid.status.code(), Some(1));
    assert!(diagnostic_codes(&report).contains(&"result_schema_invalid"));
}

#[test]
fn finalization_cancellation_reason_must_match_cancelled_finalizers() {
    let artifact = ArtifactSet::valid();
    let mut result = artifact.result();
    result["outcome"] = json!("cancelled");
    result["finalization"] = json!({
        "trigger": "succeeded",
        "finalizers": [{
            "id": "cleanup",
            "role": "finalizer",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "cancelled",
            "reason": "termination_request"
        }],
        "issues": [],
        "cancellation": {
            "reason": "user_request",
            "forceStopDeadline": "2026-08-06T10:00:02Z"
        },
        "forceAbort": false
    });
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);

    assert_eq!(
        output.status.code(),
        Some(1),
        "a summary cannot claim user-request cancellation while its cancelled finalizer records termination-request: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(diagnostic_codes(&report).contains(&"result_schema_invalid"));
}

#[test]
fn cancelled_outcome_rejects_required_finalization_issues() {
    let artifact = ArtifactSet::valid();
    let mut result = artifact.result();
    result["outcome"] = json!("cancelled");
    result["finalization"] = json!({
        "trigger": "succeeded",
        "finalizers": [{
            "id": "release",
            "role": "finalizer",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "failed",
            "startedAt": "2026-08-06T10:00:01Z",
            "durationMilliseconds": 0,
            "failure": {
                "phase": "execution",
                "cause": { "code": "harness_failed" }
            }
        }, {
            "id": "notify",
            "role": "finalizer",
            "kind": "agent",
            "failurePolicy": "advisory",
            "state": "cancelled",
            "reason": "user_request"
        }],
        "issues": [{
            "node": { "id": "release", "role": "finalizer" },
            "impact": "required"
        }],
        "cancellation": {
            "reason": "user_request",
            "forceStopDeadline": "2026-08-06T10:00:02Z"
        },
        "forceAbort": false
    });
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);

    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(diagnostic_codes(&report).contains(&"result_schema_invalid"));
}

#[test]
fn cloud_metadata_only_and_carrier_bearing_sets_validate() {
    let carrier_bearing = ArtifactSet::valid();
    carrier_bearing.use_cloud_profile();

    let metadata_only = ArtifactSet::valid();
    for entry in fs::read_dir(metadata_only.root.join("exports")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    let mut metadata_result = metadata_only.result();
    metadata_result["exports"] = json!({});
    metadata_only.replace_result(&metadata_result);
    metadata_only.use_cloud_profile();

    for (artifact, declared_exports, referenced_carriers) in
        [(carrier_bearing, 5, 3), (metadata_only, 0, 0)]
    {
        let output = run(&["artifact", "validate", "--json", artifact.argument()]);
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();

        assert!(output.status.success(), "{report:#}");
        assert_eq!(report["outcome"], "valid");
        assert_eq!(report["summary"]["declaredExports"], declared_exports);
        assert_eq!(report["summary"]["referencedCarriers"], referenced_carriers);
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn missing_carrier_remedy_names_the_resolved_path() {
    let artifact = ArtifactSet::valid();
    let missing = artifact.root.join("exports/0002");
    fs::remove_file(&missing).unwrap();
    let resolved_missing = fs::canonicalize(&artifact.root)
        .unwrap()
        .join("exports/0002");

    let output = run(&["artifact", "validate", artifact.argument()]);

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&resolved_missing.display().to_string()));
    assert!(output.stderr.is_empty());
}

#[test]
fn corruption_is_reported_completely_in_normative_order() {
    let artifact = ArtifactSet::valid();
    fs::write(artifact.root.join("unexpected"), b"root extra").unwrap();
    fs::write(artifact.root.join("exports/0001"), br#"{ "a":1}"#).unwrap();
    fs::remove_file(artifact.root.join("exports/0002")).unwrap();
    fs::write(artifact.root.join("exports/extra"), b"carrier extra").unwrap();
    let mut result = artifact.result();
    result["exports"]["document"]["mediaType"] = Value::String("text/plain".to_owned());
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);

    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "invalid");
    assert_eq!(report["exitStatus"], 1);
    assert!(report.get("summary").is_none());
    assert_eq!(
        diagnostic_codes(&report),
        [
            "root_entry_unexpected",
            "export_media_type_invalid",
            "carrier_size_mismatch",
            "carrier_digest_mismatch",
            "json_content_noncanonical",
            "carrier_missing",
            "carrier_unreferenced",
        ]
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn alias_and_current_kind_content_failures_are_reported_together() {
    let artifact = ArtifactSet::valid();
    let json_bytes = br#"{"a":1,"a":2}"#;
    let text_bytes = [0xff_u8];
    fs::write(artifact.root.join("exports/0001"), json_bytes).unwrap();
    fs::write(artifact.root.join("exports/0002"), text_bytes).unwrap();
    let mut result = artifact.result();
    result["exports"]["data"] =
        available_export("json", "application/json", "exports/0001", json_bytes);
    result["exports"]["document"] = available_export(
        "text",
        "text/plain; charset=utf-8",
        "exports/0002",
        &text_bytes,
    );
    result["exports"]["payloadCopy"]["mediaType"] =
        Value::String("application/x-portable".to_owned());
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        diagnostic_codes(&report),
        [
            "alias_metadata_mismatch",
            "json_content_noncanonical",
            "text_encoding_invalid",
        ]
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn carrier_symlinks_and_unsafe_metadata_paths_are_never_followed() {
    let linked = ArtifactSet::valid();
    let outside = linked.root.parent().unwrap().join("outside-secret");
    fs::write(&outside, b"outside bytes").unwrap();
    fs::remove_file(linked.root.join("exports/0003")).unwrap();
    symlink(&outside, linked.root.join("exports/0003")).unwrap();

    let linked_output = run(&["artifact", "validate", "--json", linked.argument()]);
    let linked_report: Value = serde_json::from_slice(&linked_output.stdout).unwrap();
    assert_eq!(linked_output.status.code(), Some(1));
    assert!(diagnostic_codes(&linked_report).contains(&"carrier_symbolic_link"));

    let unsafe_set = ArtifactSet::valid();
    let mut result = unsafe_set.result();
    result["exports"]["payload"]["path"] = Value::String("../outside-secret".to_owned());
    result["exports"]["payloadCopy"]["path"] = Value::String("../outside-secret".to_owned());
    unsafe_set.replace_result(&result);

    let unsafe_output = run(&["artifact", "validate", "--json", unsafe_set.argument()]);
    let unsafe_report: Value = serde_json::from_slice(&unsafe_output.stdout).unwrap();
    assert_eq!(unsafe_output.status.code(), Some(1));
    assert_eq!(
        diagnostic_codes(&unsafe_report),
        [
            "export_path_invalid",
            "export_path_invalid",
            "carrier_unreferenced",
        ]
    );
    assert_eq!(fs::read(outside).unwrap(), b"outside bytes");
}

#[test]
fn unsupported_artifact_kinds_and_usage_errors_are_closed() {
    let artifact = ArtifactSet::valid();
    let mut result = artifact.result();
    result["exports"]["data"]["kind"] = Value::String("git_branch".to_owned());
    artifact.replace_result(&result);

    let unsupported = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&unsupported.stdout).unwrap();
    assert_eq!(unsupported.status.code(), Some(1));
    assert!(diagnostic_codes(&report).contains(&"export_entry_invalid"));

    for args in [
        vec!["artifact", "validate"],
        vec!["artifact", "validate", artifact.argument(), "extra"],
        vec![
            "artifact",
            "validate",
            "--json",
            "--json",
            artifact.argument(),
        ],
        vec!["artifact", "validate", "--plain", artifact.argument()],
    ] {
        let usage = run(&args);
        assert_eq!(usage.status.code(), Some(2));
        assert!(usage.stdout.is_empty());
        assert!(!usage.stderr.is_empty());
    }
}

#[test]
fn successful_command_without_required_output_metadata_is_rejected() {
    let artifact = ArtifactSet::valid();
    let mut result = artifact.result();
    result["steps"][0]["kind"] = Value::String("cmd".to_owned());
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(report["outcome"], "invalid");
    assert!(diagnostic_codes(&report).contains(&"result_schema_invalid"));
}

#[test]
fn contradictory_workflow_outcome_and_step_state_is_rejected() {
    let artifact = ArtifactSet::valid();
    let mut result = artifact.result();
    result["steps"][0]["state"] = Value::String("failed".to_owned());
    result["steps"][0]["failure"] = json!({
        "phase": "start",
        "cause": { "code": "preparation_task_unavailable" }
    });
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(report["outcome"], "invalid");
    assert!(diagnostic_codes(&report).contains(&"result_schema_invalid"));
}

#[test]
fn canonical_json_with_a_large_rfc_8259_number_is_valid() {
    let artifact = ArtifactSet::valid();
    let json_bytes = b"1e400";
    fs::write(artifact.root.join("exports/0001"), json_bytes).unwrap();
    let mut result = artifact.result();
    result["exports"]["data"] =
        available_export("json", "application/json", "exports/0001", json_bytes);
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);

    assert!(
        output.status.success(),
        "canonical RFC 8259 JSON should validate: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn canonical_json_at_the_nesting_limit_is_valid() {
    let artifact = ArtifactSet::valid();
    let json = format!("{}0{}", "[".repeat(128), "]".repeat(128));
    let json_bytes = json.as_bytes();
    fs::write(artifact.root.join("exports/0001"), json_bytes).unwrap();
    let mut result = artifact.result();
    result["exports"]["data"] =
        available_export("json", "application/json", "exports/0001", json_bytes);
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);

    assert!(
        output.status.success(),
        "JSON at the 128-container limit should validate: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn malformed_export_does_not_hide_its_usable_carrier_reference() {
    let artifact = ArtifactSet::valid();
    let mut result = artifact.result();
    result["exports"]["data"] = json!({
        "state": "available",
        "kind": "git_branch",
        "artifactVersion": 1,
        "objectFormat": "sha1",
        "baseOid": "0".repeat(40),
        "headOid": "1".repeat(40),
        "treeOid": "2".repeat(40),
        "carrier": {
            "path": "exports/0001",
            "mediaType": "application/vnd.git.bundle",
            "sizeBytes": 18,
            "digest": {
                "algorithm": "sha256",
                "value": "0".repeat(64)
            }
        },
        "path": "../ignored-extra-path"
    });
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(diagnostic_codes(&report).contains(&"carrier_digest_mismatch"));
}

#[test]
fn malformed_unresolved_reference_delta_is_rejected() {
    let artifact = ArtifactSet::valid();
    let base = "a".repeat(40);
    let head = "b".repeat(40);
    let tree = "c".repeat(40);
    let bundle = malformed_external_delta_bundle(&base, &head);
    fs::write(artifact.root.join("exports/0001"), &bundle).unwrap();
    let mut result = artifact.result();
    result["exports"]["data"] = json!({
        "state": "available",
        "kind": "git_branch",
        "artifactVersion": 1,
        "objectFormat": "sha1",
        "baseOid": base,
        "headOid": head,
        "treeOid": tree,
        "carrier": {
            "path": "exports/0001",
            "mediaType": "application/vnd.git.bundle",
            "sizeBytes": bundle.len(),
            "digest": {
                "algorithm": "sha256",
                "value": hex(digest(&SHA256, &bundle).as_ref())
            }
        }
    });
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(output.status.code(), Some(1), "{report:#}");
    assert!(diagnostic_codes(&report).contains(&"git_pack_invalid"));
}

#[test]
fn unresolved_delta_chain_over_the_profile_limit_is_rejected() {
    let artifact = ArtifactSet::valid();
    let base = "a".repeat(40);
    let head = "b".repeat(40);
    let tree = "c".repeat(40);
    let bundle = overdeep_unresolved_delta_bundle(&base, &head);
    fs::write(artifact.root.join("exports/0001"), &bundle).unwrap();
    let mut result = artifact.result();
    result["exports"]["data"] = json!({
        "state": "available",
        "kind": "git_branch",
        "artifactVersion": 1,
        "objectFormat": "sha1",
        "baseOid": base,
        "headOid": head,
        "treeOid": tree,
        "carrier": {
            "path": "exports/0001",
            "mediaType": "application/vnd.git.bundle",
            "sizeBytes": bundle.len(),
            "digest": {
                "algorithm": "sha256",
                "value": hex(digest(&SHA256, &bundle).as_ref())
            }
        }
    });
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(output.status.code(), Some(1), "{report:#}");
    assert!(
        diagnostic_codes(&report).contains(&"git_structure_limit_exceeded"),
        "{report:#}"
    );
}

#[test]
fn zero_delta_carrier_diagnostic_does_not_hide_other_shape_errors() {
    let artifact = ArtifactSet::valid();
    let bytes = fs::read(artifact.root.join("exports/0001")).unwrap();
    let mut result = artifact.result();
    result["exports"]["data"] = json!({
        "state": "available",
        "kind": "git_branch",
        "artifactVersion": 1,
        "objectFormat": "sha1",
        "baseOid": "0".repeat(40),
        "headOid": "0".repeat(40),
        "treeOid": "1".repeat(40),
        "carrier": {
            "path": "exports/0001",
            "mediaType": "application/vnd.git.bundle",
            "sizeBytes": bytes.len(),
            "digest": {
                "algorithm": "sha256",
                "value": hex(digest(&SHA256, &bytes).as_ref())
            }
        },
        "unexpected": true
    });
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    let codes = diagnostic_codes(&report);

    assert_eq!(output.status.code(), Some(1));
    assert!(codes.contains(&"export_entry_invalid"));
    assert!(codes.contains(&"git_zero_delta_invalid"));
}

#[cfg(target_os = "linux")]
#[test]
fn duplicate_code_and_location_diagnostics_are_deduplicated() {
    let artifact = ArtifactSet::valid();
    fs::write(
        artifact.root.join(OsStr::from_bytes(b"root-\xff")),
        b"extra",
    )
    .unwrap();
    fs::write(
        artifact
            .root
            .join("exports")
            .join(OsStr::from_bytes(b"carrier-\xff")),
        b"extra",
    )
    .unwrap();

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    let boundary_name_diagnostics = report["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|diagnostic| diagnostic["code"] == "boundary_name_invalid")
        .collect::<Vec<_>>();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(boundary_name_diagnostics.len(), 1);
    assert_eq!(
        boundary_name_diagnostics[0]["location"]["kind"],
        "artifact_directory"
    );
}

#[test]
fn export_limit_does_not_skip_later_authoritative_references() {
    let artifact = ArtifactSet::valid();
    let exports_directory = artifact.root.join("exports");
    for entry in fs::read_dir(&exports_directory).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }

    let mut exports = serde_json::Map::new();
    for ordinal in 1..=4_096 {
        exports.insert(
            format!("export{ordinal:04}"),
            json!({
                "state": "unavailable",
                "reason": "source_blocked"
            }),
        );
    }
    exports.insert(
        "export4097".to_owned(),
        available_export(
            "file",
            "application/octet-stream",
            "exports/4097",
            b"missing",
        ),
    );
    let mut result = artifact.result();
    result["exports"] = Value::Object(exports);
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(diagnostic_codes(&report).contains(&"export_limit_exceeded"));
    assert!(
        report["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| {
                diagnostic["code"] == "carrier_missing"
                    && diagnostic["location"]["path"] == "exports/4097"
            }),
        "the export count limit must not hide a later authoritative carrier: {report}"
    );
}

#[test]
fn carrier_limit_does_not_skip_later_authoritative_references() {
    let artifact = ArtifactSet::valid();
    let exports_directory = artifact.root.join("exports");
    for entry in fs::read_dir(&exports_directory).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }

    let mut exports = serde_json::Map::new();
    for ordinal in 1..=4_097 {
        let bytes = [u8::try_from(ordinal % 251).unwrap()];
        let path = format!("exports/{ordinal:04}");
        exports.insert(
            format!("export{ordinal:04}"),
            available_export("file", "application/octet-stream", &path, &bytes),
        );
        if ordinal <= 4_096 {
            fs::write(artifact.root.join(path), bytes).unwrap();
        }
    }
    let mut result = artifact.result();
    result["exports"] = Value::Object(exports);
    artifact.replace_result(&result);

    let output = run(&["artifact", "validate", "--json", artifact.argument()]);
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(diagnostic_codes(&report).contains(&"carrier_limit_exceeded"));
    assert!(
        report["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| {
                diagnostic["code"] == "carrier_missing"
                    && diagnostic["location"]["path"] == "exports/4097"
            })
    );
}
