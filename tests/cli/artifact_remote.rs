use super::*;

use ring::digest::{SHA256, digest};

const TOKEN: &str = "unique-artifact-list-token-sentinel";
const ORGANIZATION: &str = "acme-research";
const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const ARTIFACT_SET_ID: &str = "ats_01k0z6r1w8f4jy2m7q9v3x5abc";
const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn prepared_artifact(response: Vec<u8>) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(vec![response]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let credential_path = credential_path.to_str().unwrap().to_owned();
    (server, credential_directory, credential_path)
}

fn inventory_body() -> serde_json::Value {
    serde_json::json!({
        "artifactSetId": ARTIFACT_SET_ID,
        "sealedAt": "2026-08-17T12:00:00Z",
        "expiresAt": "2026-09-17T12:00:00Z",
        "memberCount": 2,
        "totalSizeBytes": 1240,
        "members": [{
            "path": "exports/0001",
            "mediaType": "text/plain",
            "sizeBytes": 6,
            "digest": {"algorithm": "sha256", "value": DIGEST}
        }],
        "nextCursor": "next-page"
    })
}

fn inventory_response(body: serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "no-store")],
        &serde_json::to_vec(&body).unwrap(),
    )
}

fn problem_response(status: &str, code: u16) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": format!("https://api.scherzo.dev/problems/fixture-{code}"),
            "title": "Fixture problem",
            "status": code
        }),
    )
}

fn list_args(json: bool) -> Vec<&'static str> {
    let mut args = vec![
        "artifact",
        "list",
        ORGANIZATION,
        RUN_ID,
        "--limit",
        "1",
        "--cursor",
        "current-page",
    ];
    if json {
        args.push("--json");
    }
    args.push("--allow-insecure-http");
    args
}

fn artifact_digest(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn portable_result_bytes() -> Vec<u8> {
    // Keep this black-box wire fixture independent of the production assembly unit fixture so
    // the public command cannot pass by sharing the builder that it is expected to validate.
    // jscpd:ignore-start
    let document = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": 1,
        "workflow": {
            "path": "workflow.yaml",
            "provenance": {
                "kind": "cloud",
                "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
                "objectFormat": "sha1",
                "commitOid": "0123456789abcdef0123456789abcdef01234567"
            },
            "digest": {"algorithm": "sha256", "value": "1".repeat(64)}
        },
        "execution": {
            "maximumParallelSteps": 1,
            "capacity": {
                "executionContract": "workflow_v1_cloud_inputs_artifacts@1",
                "sourceClosureDigest": {"algorithm": "sha256", "value": "1".repeat(64)},
                "generalMaximumTransitions": 8,
                "selectedMaximumTransitions": 7,
                "maximumInvocations": 1,
                "maximumRetainedBytesPerInvocation": 4_194_304,
                "diagnosticRetentionBytes": 8_388_608,
                "nativeSessionRetentionBytes": 4_194_304,
                "aggregateRetentionBytes": 12_582_912,
                "conditionTransitionCount": 0,
                "aggregateConditionTransitionBytes": 0,
                "terminalResultStructureBytes": 67_108_864,
                "portableResultBytes": 202_027_692,
                "encodedOutboxBytes": 85_458_944
            },
            "startedAt": "2026-08-17T12:00:00Z",
            "finishedAt": "2026-08-17T12:00:01Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": 4_194_304
        },
        "outcome": "succeeded",
        "steps": [{
            "id": "produce",
            "role": "step",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-17T12:00:00Z",
            "durationMilliseconds": 1000
        }],
        "exports": {}
    });
    // jscpd:ignore-end
    let mut bytes = serde_json::to_vec_pretty(&document).unwrap();
    bytes.push(b'\n');
    bytes
}

fn complete_inventory_response(result: &[u8]) -> Vec<u8> {
    inventory_response(serde_json::json!({
        "artifactSetId": ARTIFACT_SET_ID,
        "sealedAt": "2998-08-17T12:00:00Z",
        "expiresAt": "2999-09-17T12:00:00Z",
        "memberCount": 1,
        "totalSizeBytes": result.len(),
        "members": [{
            "path": "result.json",
            "mediaType": "application/json",
            "sizeBytes": result.len(),
            "digest": {"algorithm": "sha256", "value": artifact_digest(result)}
        }]
    }))
}

fn download_capability_response(result: &[u8], url: &str) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        &serde_json::to_vec(&serde_json::json!({
            "artifactSetId": ARTIFACT_SET_ID,
            "expiresAt": "2999-09-17T12:00:00Z",
            "capabilityExpiresAt": "2998-08-17T12:05:00Z",
            "members": [{
                "path": "result.json",
                "mediaType": "application/json",
                "sizeBytes": result.len(),
                "digest": {"algorithm": "sha256", "value": artifact_digest(result)},
                "url": url
            }]
        }))
        .unwrap(),
    )
}

#[test]
fn artifact_download_json_reports_only_the_verified_committed_set() {
    let result = portable_result_bytes();
    let storage = OneShotServer::respond("200 OK", Some("application/json"), &result);
    let signed_url = format!(
        "{}/result.json?signature=unique-artifact-capability-sentinel",
        storage.api_url
    );
    let server = ScriptedServer::respond(vec![
        complete_inventory_response(&result),
        download_capability_response(&result, &signed_url),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&server.api_url, credential_path.to_str().unwrap());
    let output_root = tempfile::tempdir().unwrap();
    let destination = output_root.path().join("downloaded");

    let output = run_with_env(
        &[
            "artifact",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(destination.join("result.json")).unwrap(), result);
    assert!(destination.join("exports").is_dir());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "downloaded",
            "runId": RUN_ID,
            "artifactSetId": ARTIFACT_SET_ID,
            "verifiedMemberCount": 1,
            "verifiedTotalBytes": result.len(),
            "destination": fs::canonicalize(&destination).unwrap()
        })
    );
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("unique-artifact-capability-sentinel")
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("/artifact-set?limit=200 HTTP/1.1"));
    assert!(requests[1].contains("/download-capabilities HTTP/1.1"));
    let download = storage.finish();
    assert!(download.starts_with("GET "));
    assert!(!download.contains("authorization:"));
}

#[test]
fn artifact_download_verification_failure_emits_no_receipt_or_destination() {
    let expected = portable_result_bytes();
    let mut corrupted = expected.clone();
    let index = corrupted.len() / 3;
    corrupted[index] = if corrupted[index] == b'x' { b'y' } else { b'x' };
    let storage = OneShotServer::respond("200 OK", Some("application/json"), &corrupted);
    let signed_url = format!("{}/result.json?signature=private", storage.api_url);
    let server = ScriptedServer::respond(vec![
        complete_inventory_response(&expected),
        download_capability_response(&expected, &signed_url),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment(&server.api_url, credential_path.to_str().unwrap());
    let output_root = tempfile::tempdir().unwrap();
    let destination = output_root.path().join("downloaded");

    let output = run_with_env(
        &[
            "artifact",
            "download",
            ORGANIZATION,
            RUN_ID,
            "--output",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
    assert!(!destination.exists());
    assert_eq!(server.finish().len(), 2);
    storage.finish();
}

#[test]
fn artifact_list_reports_one_inventory_page_without_download_authority_or_files() {
    for json in [false, true] {
        let (server, _credential_directory, credential_path) =
            prepared_artifact(inventory_response(inventory_body()));
        let environment = deployment_environment(&server.api_url, &credential_path);
        let working_directory = tempfile::tempdir().unwrap();

        let output = run_with_env_in(&list_args(json), &environment, working_directory.path());

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        if json {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
                serde_json::json!({
                    "schemaVersion": 1,
                    "deployment": server.api_url,
                    "outcome": "listed",
                    "organizationRef": ORGANIZATION,
                    "runId": RUN_ID,
                    "artifactSet": {
                        "artifactSetId": ARTIFACT_SET_ID,
                        "state": "sealed",
                        "sealedAt": "2026-08-17T12:00:00Z",
                        "expiresAt": "2026-09-17T12:00:00Z",
                        "memberCount": 2,
                        "totalSizeBytes": 1240,
                        "members": [{
                            "path": "exports/0001",
                            "mediaType": "text/plain",
                            "sizeBytes": 6,
                            "digest": {"algorithm": "sha256", "value": DIGEST}
                        }],
                        "nextCursor": "next-page"
                    }
                })
            );
        } else {
            let stdout = String::from_utf8(output.stdout).unwrap();
            for field in [
                format!("artifact set: {ARTIFACT_SET_ID}"),
                "state: sealed".to_owned(),
                "members: 2".to_owned(),
                "member: exports/0001".to_owned(),
                format!("sha256: {DIGEST}"),
                "next cursor: next-page".to_owned(),
                format!("run: {RUN_ID}"),
            ] {
                assert!(
                    stdout.lines().any(|line| line == field),
                    "missing {field:?} in {stdout:?}"
                );
            }
        }
        assert_eq!(working_directory.path().read_dir().unwrap().count(), 0);
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/runs/{RUN_ID}/artifact-set?limit=1&cursor=current-page HTTP/1.1\r\n"
        )));
        assert_eq!(
            header_value(&requests[0], "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert!(!requests[0].contains("download-capabilities"));
        assert!(!requests[0].contains("idempotency-key:"));
    }
}

#[test]
fn artifact_list_accepts_a_short_final_continuation_page() {
    let response = inventory_response(serde_json::json!({
        "artifactSetId": ARTIFACT_SET_ID,
        "sealedAt": "2026-08-17T12:00:00Z",
        "expiresAt": "2026-09-17T12:00:00Z",
        "memberCount": 2,
        "totalSizeBytes": 1240,
        "members": [{
            "path": "result.json",
            "mediaType": "application/json",
            "sizeBytes": 6,
            "digest": {"algorithm": "sha256", "value": DIGEST}
        }]
    }));
    let (server, _credential_directory, credential_path) = prepared_artifact(response);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(&list_args(true), &environment);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "listed");
    assert_eq!(
        result["artifactSet"]["members"].as_array().unwrap().len(),
        1
    );
    assert!(result["artifactSet"].get("nextCursor").is_none());
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("cursor=current-page"));
}

#[test]
fn artifact_list_reports_unsealed_or_missing_sets_as_unavailable_in_human_mode() {
    let (server, _credential_directory, credential_path) =
        prepared_artifact(problem_response("404 Not Found", 404));
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "artifact",
            "list",
            ORGANIZATION,
            RUN_ID,
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(&format!("Artifact Set unavailable for run {RUN_ID}")));
    assert!(stderr.contains("may not be sealed"));
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET "));
    assert!(!requests[0].contains("download-capabilities"));
}

#[test]
fn artifact_list_rejects_inventory_pages_that_ignore_request_bounds() {
    let mut body = inventory_body();
    body.as_object_mut().unwrap().remove("nextCursor");
    let mut second = body["members"][0].clone();
    second["path"] = serde_json::json!("exports/0002");
    body["members"].as_array_mut().unwrap().push(second);
    body["totalSizeBytes"] = serde_json::json!(12);

    let mut repeated_cursor = inventory_body();
    repeated_cursor["nextCursor"] = serde_json::json!("current-page");
    for (body, pagination) in [
        (body.clone(), vec!["--limit", "1"]),
        (body, vec!["--limit", "2", "--cursor", "current-page"]),
        (
            repeated_cursor,
            vec!["--limit", "1", "--cursor", "current-page"],
        ),
    ] {
        let (server, _credential_directory, credential_path) =
            prepared_artifact(inventory_response(body));
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec![
            "artifact",
            "list",
            ORGANIZATION,
            RUN_ID,
            "--json",
            "--allow-insecure-http",
        ];
        args.extend(pagination);
        let output = run_with_env(&args, &environment);
        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "invalid_response");
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn artifact_list_escapes_untrusted_remote_text_only_in_human_output() {
    let media_type = "text/plain\nforged: value\u{1b}[2J";
    let cursor = "next\nforged: cursor\u{1b}[2J";
    for json in [false, true] {
        let mut body = inventory_body();
        body["members"][0]["mediaType"] = serde_json::json!(media_type);
        body["nextCursor"] = serde_json::json!(cursor);
        let (server, _credential_directory, credential_path) =
            prepared_artifact(inventory_response(body));
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(&list_args(json), &environment);
        assert!(output.status.success());
        if json {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["artifactSet"]["members"][0]["mediaType"], media_type);
            assert_eq!(result["artifactSet"]["nextCursor"], cursor);
        } else {
            let report = String::from_utf8(output.stdout).unwrap();
            assert!(!report.contains('\u{1b}'));
            assert!(!report.lines().any(|line| line.starts_with("forged:")));
            assert!(report.contains("text/plain\\nforged: value\\u{1b}[2J"));
            assert!(report.contains("next\\nforged: cursor\\u{1b}[2J"));
        }
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn artifact_list_json_preserves_meaningful_api_failure_classes() {
    let unauthenticated = run(&["artifact", "list", ORGANIZATION, RUN_ID, "--json"]);
    assert_eq!(unauthenticated.status.code(), Some(3));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unauthenticated.stdout).unwrap()["outcome"],
        "unauthenticated"
    );
    assert!(unauthenticated.stderr.is_empty());

    let cases = [
        (
            problem_response("400 Bad Request", 400),
            "invalid_input",
            1,
            None,
        ),
        (problem_response("403 Forbidden", 403), "forbidden", 1, None),
        (
            problem_response("404 Not Found", 404),
            "unavailable",
            1,
            None,
        ),
        (problem_response("410 Gone", 410), "expired", 1, None),
        (
            problem_response("500 Internal Server Error", 500),
            "unreachable",
            4,
            Some("server"),
        ),
        (
            problem_http_response(
                "404 Not Found",
                serde_json::json!({
                    "type": "https://api.scherzo.dev/problems/not-found",
                    "title": "Not found",
                    "status": 400
                }),
            ),
            "invalid_response",
            1,
            None,
        ),
        (
            inventory_response(serde_json::json!({
                "artifactSetId": "invalid",
                "sealedAt": "2026-08-17T12:00:00Z",
                "expiresAt": "2026-09-17T12:00:00Z",
                "memberCount": 1,
                "totalSizeBytes": 6,
                "members": [{
                    "path": "result.json",
                    "mediaType": "application/json",
                    "sizeBytes": 6,
                    "digest": {"algorithm": "sha256", "value": DIGEST}
                }]
            })),
            "invalid_response",
            1,
            None,
        ),
        (
            inventory_response(serde_json::json!({
                "artifactSetId": ARTIFACT_SET_ID,
                "sealedAt": "2026-08-17T12:00:00Z",
                "expiresAt": "2026-09-17T12:00:00Z",
                "memberCount": 2,
                "totalSizeBytes": 12,
                "members": [{
                    "path": "exports/0001",
                    "mediaType": "text/plain",
                    "sizeBytes": 6,
                    "digest": {"algorithm": "sha256", "value": DIGEST}
                }]
            })),
            "invalid_response",
            1,
            None,
        ),
    ];

    for (response, expected_outcome, expected_exit, expected_category) in cases {
        let (server, _credential_directory, credential_path) = prepared_artifact(response);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &[
                "artifact",
                "list",
                ORGANIZATION,
                RUN_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(expected_exit));
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["schemaVersion"], 1);
        assert_eq!(result["outcome"], expected_outcome);
        assert_eq!(result["organizationRef"], ORGANIZATION);
        assert_eq!(result["runId"], RUN_ID);
        assert_eq!(
            result.get("category").and_then(serde_json::Value::as_str),
            expected_category
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains(TOKEN));
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("GET "));
        assert!(!requests[0].contains("download-capabilities"));
    }
}
