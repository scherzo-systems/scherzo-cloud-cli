use super::*;
use crate::api::test_support::ScriptedHttpServer;

const TOKEN: &str = "runner-api-token-sentinel";
const ORGANIZATION: &str = "acme";
const POOL_ID: &str = "rpl_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";

fn response(status: &str, body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

fn deletion_response(status: &str, key: Option<&str>, body: &[u8]) -> Vec<u8> {
    let key = key
        .map(|key| format!("Idempotency-Key: {key}\r\n"))
        .unwrap_or_default();
    let mut response = format!(
        "HTTP/1.1 {status}\r\n{key}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

fn api(server: &ScriptedHttpServer) -> RunnerApi {
    RunnerApi::new(
        &server.api_url,
        TOKEN,
        HttpTransportPolicy::AllowInsecureHttp,
    )
    .expect("runner API should build")
}

fn registration_body() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": RUNNER_ID,
        "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
        "runnerPool": {"id": POOL_ID, "name": "builders"},
        "name": "builder-one",
        "administration": {
            "mode": "draining",
            "createdAt": "2026-08-09T12:00:00Z",
            "updatedAt": "2026-08-09T12:01:00Z"
        },
        "enrollment": {
            "state": "credentialed",
            "firstEnrolledAt": "2026-08-09T12:02:00Z",
            "validCredentialCount": 1
        },
        "connectivity": {
            "state": "online",
            "connectedAt": "2026-08-09T12:03:00Z",
            "lastSeenAt": "2026-08-09T12:04:00Z"
        },
        "activity": {"state": "assigned", "currentAssignmentCount": 1},
        "advertisedMetadata": {
            "runnerVersion": "1.2.3",
            "protocolVersion": 1
        }
    }))
    .unwrap()
}

#[test]
fn generated_runner_client_decodes_independent_cloud_and_advertised_projections() {
    let server = ScriptedHttpServer::respond(response("200 OK", &registration_body()));

    let runner = api(&server)
        .get_registration(ORGANIZATION, RUNNER_ID)
        .expect("runner should decode");

    assert_eq!(
        runner.administration.mode,
        models::runner_administration::Mode::Draining
    );
    assert_eq!(runner.enrollment.valid_credential_count, 1);
    assert_eq!(
        runner.connectivity.last_seen_at.as_deref(),
        Some("2026-08-09T12:04:00Z")
    );
    assert_eq!(runner.activity.current_assignment_count, 1);
    assert_eq!(
        runner
            .advertised_metadata
            .as_ref()
            .map(|metadata| metadata.runner_version.as_str()),
        Some("1.2.3")
    );
    let request = server.finish_one();
    assert!(request.starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID} HTTP/1.1\r\n"
    )));
}

#[test]
fn generated_runner_rename_uses_merge_patch_and_idempotency() {
    let pool = serde_json::to_vec(&serde_json::json!({
        "id": POOL_ID,
        "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
        "name": "release-builders",
        "createdAt": "2026-08-09T12:00:00Z",
        "updatedAt": "2026-08-09T12:01:00Z"
    }))
    .unwrap();
    let server = ScriptedHttpServer::respond(response("200 OK", &pool));

    let renamed = runners_api::rename_runner_pool(
        &api(&server).configuration,
        ORGANIZATION,
        POOL_ID,
        "runner-rename-key",
        models::RenameRunnerPoolPatch::new("release-builders".to_owned()),
    )
    .expect("rename should decode");

    assert_eq!(renamed.name, "release-builders");
    let request = server.finish_one();
    assert!(
        request
            .lines()
            .any(|line| line == "content-type: application/merge-patch+json")
    );
    assert!(
        request
            .lines()
            .any(|line| line == "idempotency-key: runner-rename-key")
    );
}

#[test]
fn runner_deletion_requires_exact_bodyless_success_and_echoed_key() {
    let server =
        ScriptedHttpServer::respond(deletion_response("204 No Content", Some("delete-key"), &[]));

    assert_eq!(
        api(&server).delete_registration(ORGANIZATION, RUNNER_ID, "delete-key"),
        Ok(())
    );
    let request = server.finish_one();
    assert!(request.starts_with(&format!(
        "DELETE /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID} HTTP/1.1\r\n"
    )));
    assert!(
        request
            .lines()
            .any(|line| line == "idempotency-key: delete-key")
    );
    assert!(request.ends_with("\r\n\r\n"));

    for malformed in [
        deletion_response("204 No Content", None, &[]),
        deletion_response("200 OK", Some("delete-key"), &[]),
    ] {
        let server = ScriptedHttpServer::respond(malformed);
        assert_eq!(
            api(&server).delete_pool(ORGANIZATION, POOL_ID, "delete-key"),
            Err(RunnerFailure::Protocol)
        );
        server.finish_one();
    }
}

#[test]
fn runner_deletion_accepts_only_ordered_resource_specific_blockers() {
    let blocked = serde_json::to_vec(&serde_json::json!({
        "type": "https://api.scherzo.dev/problems/runner-pool-delete-unavailable",
        "title": "Runner pool deletion unavailable",
        "status": 409,
        "blockers": ["runner_registrations_present", "nonterminal_runs_present"]
    }))
    .unwrap();
    let server = ScriptedHttpServer::respond(response("409 Conflict", &blocked));
    assert_eq!(
        api(&server).delete_pool(ORGANIZATION, POOL_ID, "delete-key"),
        Err(RunnerFailure::DeletionUnavailable(vec![
            RunnerDeletionBlocker::RunnerRegistrationsPresent,
            RunnerDeletionBlocker::NonterminalRunsPresent,
        ]))
    );
    server.finish_one();

    let malformed = serde_json::to_vec(&serde_json::json!({
        "type": "https://api.scherzo.dev/problems/runner-registration-delete-unavailable",
        "title": "Runner registration deletion unavailable",
        "status": 409,
        "blockers": ["nonterminal_assignment", "capacity_reserved"]
    }))
    .unwrap();
    let server = ScriptedHttpServer::respond(response("409 Conflict", &malformed));
    assert_eq!(
        api(&server).delete_registration(ORGANIZATION, RUNNER_ID, "delete-key"),
        Err(RunnerFailure::Protocol)
    );
    server.finish_one();
}

#[test]
fn runner_client_requires_contracted_problem_types() {
    let body = serde_json::to_vec(&serde_json::json!({
        "type": "https://api.scherzo.dev/problems/not-found",
        "title": "Not found",
        "status": 404
    }))
    .unwrap();
    let server = ScriptedHttpServer::respond(response("404 Not Found", &body));
    assert_eq!(
        api(&server).get_pool(ORGANIZATION, POOL_ID),
        Err(RunnerFailure::NotFound)
    );
    server.finish_one();
}
