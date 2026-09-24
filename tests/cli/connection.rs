use super::*;

const TOKEN: &str = "private-linear-token-sentinel";
const SECRET: &str = "private-linear-verifier-sentinel";
const ORG: &str = "acme-research";
const SESSION: &str = "las_01k0z6r1w8f4jy2m7q9v3x5abc";
const CONNECTION: &str = "lcn_01k0z6r1w8f4jy2m7q9v3x5abc";
const ORG_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";

fn prepared(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(responses);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture(&path, &server.api_url, TOKEN, "2999-01-01T00:00:00Z");
    (server, directory, path.to_str().unwrap().to_owned())
}
fn session(status: &str) -> serde_json::Value {
    let mut value = serde_json::json!({"id": SESSION, "organizationId": ORG_ID, "operation":"connect", "status":status,
        "createdAt":"2026-09-05T12:00:00Z", "expiresAt":"2026-09-05T12:15:00Z", "verifier":SECRET});
    if status == "pending" {
        value.as_object_mut().unwrap().insert(
            "authorizationUrl".into(),
            "https://linear.example/oauth/authorize?state=public-fixture-state".into(),
        );
    }
    value
}
fn connection() -> serde_json::Value {
    serde_json::json!({"id":CONNECTION,"organizationId":ORG_ID,"status":"active", "workspaceId":"workspace-1",
        "workspaceName":"Research", "requiredScopes":["read"], "grantedScopes":["read"],
        "lifecycleGeneration":1,"createdAt":"2026-09-05T12:00:00Z","updatedAt":"2026-09-05T12:01:00Z", "token":SECRET})
}
fn assert_no_secret_output(output: &Output, secrets: &[&str]) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for secret in secrets {
        assert!(!stdout.contains(secret) && !stderr.contains(secret));
    }
}
fn env_run(server: &ScriptedServer, credential: &str, args: &[&str]) -> Output {
    run_with_env(args, &deployment_environment(&server.api_url, credential))
}
fn lines(output: &Output) -> Vec<serde_json::Value> {
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

#[test]
fn consent_handoff_and_later_authorization_inspection_are_nonsecret() {
    let (server, _dir, credential) = prepared(vec![
        json_http_response("201 Created", session("pending")),
        json_http_response("200 OK", {
            let mut value = session("completed");
            value
                .as_object_mut()
                .unwrap()
                .insert("resultConnection".into(), connection());
            value
        }),
    ]);
    let started = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--no-wait",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(started.status.success());
    let events = lines(&started);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["outcome"], "pending");
    assert_eq!(events[0]["session"]["id"], SESSION);
    assert!(
        events[0]["session"]["authorizationUrl"]
            .as_str()
            .unwrap()
            .starts_with("https://linear.example/")
    );
    let inspected = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "show",
            ORG,
            SESSION,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(inspected.status.success());
    assert_eq!(
        lines(&inspected)[0]["session"]["resultConnection"]["id"],
        CONNECTION
    );
    assert_eq!(lines(&inspected)[0]["outcome"], "completed");
    for output in [&started, &inspected] {
        assert_no_secret_output(output, &[TOKEN, SECRET]);
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with(&format!(
        "POST /api/v1/organizations/{ORG}/connections/linear/authorization-sessions "
    )));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(requests[0].split_once("\r\n\r\n").unwrap().1)
            .unwrap()["operation"],
        "connect"
    );
    assert!(requests[1].contains(&format!("/authorization-sessions/{SESSION} ")));
}

#[test]
fn polling_completion_uses_the_same_session_and_shows_connection_metadata() {
    let mut done = session("completed");
    done.as_object_mut().unwrap().remove("authorizationUrl");
    done.as_object_mut()
        .unwrap()
        .insert("resultConnection".into(), connection());
    let (server, _dir, credential) = prepared(vec![
        json_http_response("201 Created", session("pending")),
        json_http_response("200 OK", done),
    ]);
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(output.status.success());
    assert_eq!(
        lines(&output)
            .iter()
            .map(|line| line["outcome"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["pending", "completed"]
    );
    assert_eq!(
        lines(&output)[1]["session"]["resultConnection"]["id"],
        CONNECTION
    );
    assert_eq!(
        lines(&output)[1]["session"]["resultConnection"]["workspaceId"],
        "workspace-1"
    );
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains(&format!("/authorization-sessions/{SESSION} ")));
}

#[test]
fn reauthorization_targets_only_the_requested_connection() {
    let mut reauthorized = session("pending");
    reauthorized
        .as_object_mut()
        .unwrap()
        .insert("operation".into(), "reauthorize".into());
    reauthorized
        .as_object_mut()
        .unwrap()
        .insert("connectionId".into(), CONNECTION.into());
    let (server, _dir, credential) =
        prepared(vec![json_http_response("201 Created", reauthorized)]);
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--connection-id",
            CONNECTION,
            "--no-wait",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(output.status.success());
    assert_eq!(lines(&output)[0]["session"]["connectionId"], CONNECTION);
    assert_eq!(lines(&output)[0]["session"]["operation"], "reauthorize");
    let requests = server.finish();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(requests[0].split_once("\r\n\r\n").unwrap().1)
            .unwrap()["operation"],
        "reauthorize"
    );
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
}

#[test]
fn reauthorization_conflict_and_role_loss_have_distinct_outcomes() {
    let problem = |status: &str, code: u16, kind: &str| {
        problem_http_response(
            status,
            serde_json::json!({
                "type": format!("https://api.scherzo.dev/problems/{kind}"), "title": SECRET, "status":code,"detail":SECRET
            }),
        )
    };
    let (server, _dir, credential) = prepared(vec![
        problem("409 Conflict", 409, "source-connection-conflict"),
        problem("403 Forbidden", 403, "forbidden"),
    ]);
    let conflict = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--connection-id",
            CONNECTION,
            "--no-wait",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(conflict.status.code(), Some(1));
    assert_eq!(lines(&conflict)[0]["outcome"], "conflict");
    let lost_role = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "show",
            ORG,
            SESSION,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(lost_role.status.code(), Some(1));
    assert_eq!(lines(&lost_role)[0]["outcome"], "forbidden");
    assert_no_secret_output(&conflict, &[TOKEN, SECRET]);
    assert_no_secret_output(&lost_role, &[TOKEN, SECRET]);
    let requests = server.finish();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(requests[0].split_once("\r\n\r\n").unwrap().1)
            .unwrap()["connectionId"],
        CONNECTION
    );
}

#[test]
fn lost_start_response_reuses_one_key_and_does_not_create_second_session() {
    let (server, _dir, credential) = prepared(vec![
        Vec::new(),
        json_http_response("201 Created", session("pending")),
    ]);
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--no-wait",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(output.status.success());
    assert_eq!(lines(&output)[0]["session"]["id"], SESSION);
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

#[test]
fn unresolved_start_retries_only_within_this_invocation() {
    let (server, _dir, credential) = prepared(vec![Vec::new(), Vec::new()]);
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--no-wait",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(lines(&output)[0]["outcome"], "unreachable");
    assert!(lines(&output)[0]["sessionId"].is_null());
    assert!(lines(&output)[0]["idempotencyKey"].is_null());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
}

#[test]
fn ambiguous_poll_response_keeps_known_session_without_restarting_consent() {
    let (server, _dir, credential) = prepared(vec![
        json_http_response("201 Created", session("pending")),
        Vec::new(),
    ]);
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(
        lines(&output)
            .iter()
            .map(|line| line["outcome"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["pending", "unreachable"]
    );
    assert_eq!(lines(&output)[1]["sessionId"], SESSION);
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains(&format!("/authorization-sessions/{SESSION} ")));
}

#[test]
fn polling_stops_on_owner_role_loss_without_restarting_consent() {
    let (server, _dir, credential) = prepared(vec![
        json_http_response("201 Created", session("pending")),
        problem_http_response(
            "403 Forbidden",
            serde_json::json!({"type":"https://api.scherzo.dev/problems/forbidden","title":SECRET,"status":403}),
        ),
    ]);
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        lines(&output)
            .iter()
            .map(|line| line["outcome"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["pending", "forbidden"]
    );
    assert_eq!(lines(&output)[1]["sessionId"], SESSION);
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn rejected_service_credential_does_not_expose_a_token() {
    let (server, directory, credential) = prepared(vec![problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
        "type":"https://api.scherzo.dev/problems/unauthorized","title":SECRET,"status":401}),
    )]);
    let service_path = directory.path().join("service.key");
    fs::write(
        &service_path,
        "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n",
    )
    .unwrap();
    fs::set_permissions(&service_path, Permissions::from_mode(0o600)).unwrap();
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "show",
            ORG,
            SESSION,
            "--service-api-key-file",
            service_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(lines(&output)[0]["outcome"], "unauthenticated");
    assert_no_secret_output(
        &output,
        &[
            TOKEN,
            SECRET,
            "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ],
    );
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn authorization_help_and_destructive_confirmation_are_available() {
    let help = run(&["connection", "linear", "authorization", "--help"]);
    assert!(help.status.success());
    let text = String::from_utf8(help.stdout).unwrap();
    for leaf in ["create", "show", "wait"] {
        assert!(text.contains(leaf));
    }
    let create_help = run(&["connection", "linear", "authorization", "create", "--help"]);
    assert!(create_help.status.success());
    assert!(!String::from_utf8_lossy(&create_help.stdout).contains("--idempotency-key"));
    for leaf in ["remove", "delete"] {
        let output = run(&["connection", "linear", leaf, ORG, CONNECTION]);
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("--yes"));
    }
}

#[test]
fn polling_respects_rate_limit_and_reports_recovery_required() {
    let mut recovered = session("recovery_required");
    recovered
        .as_object_mut()
        .unwrap()
        .insert("reasonCode".into(), "exchange_ambiguous".into());
    let (server, _dir, credential) = prepared(vec![
        json_http_response("201 Created", session("pending")),
        http_response_with_headers(
            "429 Too Many Requests",
            Some("application/problem+json"),
            &[("Retry-After", "3")],
            &serde_json::to_vec(
                &serde_json::json!({"type":"about:blank","title":SECRET,"status":429}),
            )
            .unwrap(),
        ),
        json_http_response("200 OK", recovered),
    ]);
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "authorization",
            "create",
            ORG,
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        lines(&output)
            .iter()
            .map(|line| line["outcome"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["pending", "recovery_required"]
    );
    assert_eq!(
        lines(&output)[1]["session"]["reasonCode"],
        "exchange_ambiguous"
    );
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
    assert_eq!(server.finish().len(), 3);
}

#[test]
fn ambiguous_disconnect_reuses_exact_key_and_target() {
    let mut disconnected = connection();
    disconnected
        .as_object_mut()
        .unwrap()
        .insert("status".into(), "disconnected".into());
    let (server, _dir, credential) =
        prepared(vec![Vec::new(), json_http_response("200 OK", disconnected)]);
    let output = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "remove",
            ORG,
            CONNECTION,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert!(output.status.success());
    assert_eq!(lines(&output)[0]["outcome"], "disconnected");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    for request in &requests {
        assert!(request.contains(&format!("/connections/linear/{CONNECTION}/disconnect ")));
    }
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
}

#[test]
fn list_show_disconnect_and_delete_use_nonsecret_projections() {
    let (server, _dir, credential) = prepared(vec![
        json_http_response("200 OK", serde_json::json!({"items":[connection()]})),
        json_http_response("200 OK", connection()),
        json_http_response("200 OK", {
            let mut c = connection();
            c.as_object_mut()
                .unwrap()
                .insert("status".into(), "disconnected".into());
            c
        }),
        http_response("204 No Content", None, b""),
    ]);
    let commands: [(&[&str], &str); 4] = [
        (&["list", ORG], "listed"),
        (&["show", ORG, CONNECTION], "shown"),
        (&["remove", ORG, CONNECTION, "--yes"], "disconnected"),
        (&["delete", ORG, CONNECTION, "--yes"], "deleted"),
    ];
    for (args, outcome) in commands {
        let mut command = vec!["connection", "linear"];
        command.extend(args);
        command.extend(["--json", "--allow-insecure-http"]);
        let output = env_run(&server, &credential, &command);
        assert!(
            output.status.success(),
            "{outcome}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(lines(&output)[0]["outcome"], outcome);
        assert_no_secret_output(&output, &[TOKEN, SECRET]);
    }
    assert_eq!(server.finish().len(), 4);
}

#[test]
fn linear_list_rejects_limits_above_server_maximum_before_dispatch() {
    let (server, _dir, credential) = prepared(vec![json_http_response(
        "200 OK",
        serde_json::json!({"items":[]}),
    )]);
    let args = [
        "connection",
        "linear",
        "list",
        ORG,
        "--limit",
        "100",
        "--json",
        "--allow-insecure-http",
    ];
    let accepted = env_run(&server, &credential, &args);
    assert!(accepted.status.success());
    assert_eq!(lines(&accepted)[0]["outcome"], "listed");
    let rejected = env_run(
        &server,
        &credential,
        &[
            "connection",
            "linear",
            "list",
            ORG,
            "--limit",
            "101",
            "--json",
            "--allow-insecure-http",
        ],
    );
    assert_eq!(rejected.status.code(), Some(2));
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("limit=100"));
}

#[cfg(target_os = "linux")]
#[test]
fn interrupted_removal_reports_unknown_commitment_without_retrying() {
    let mut server = ScriptedServer::respond_with_paused_first_response(vec![json_http_response(
        "200 OK",
        connection(),
    )]);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture(&path, &server.api_url, TOKEN, "2999-01-01T00:00:00Z");
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command
        .args([
            "connection",
            "linear",
            "remove",
            ORG,
            CONNECTION,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    command.env_remove(CREDENTIALS_FILE_VARIABLE);
    for (name, value) in deployment_environment(&server.api_url, path.to_str().unwrap()) {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    let request = server.wait_for_request();
    assert!(request.contains(&format!("/connections/linear/{CONNECTION}/disconnect ")));
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();
    assert_eq!(output.status.code(), Some(130));
    assert_eq!(lines(&output)[0]["outcome"], "commitment_unknown");
    assert_eq!(lines(&output)[0]["connectionId"], CONNECTION);
    assert_eq!(
        lines(&output)[0]["idempotencyKey"],
        header_value(&request, "idempotency-key")
    );
    assert_no_secret_output(&output, &[TOKEN, SECRET]);
    assert!(server.finish().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn interruption_after_start_preserves_session_for_later_inspection() {
    for (signal, code) in [
        (rustix::process::Signal::INT, 130),
        (rustix::process::Signal::TERM, 143),
    ] {
        let mut server = ScriptedServer::respond_with_paused_last_response(vec![
            json_http_response("201 Created", session("pending")),
            json_http_response("200 OK", session("completed")),
        ]);
        let directory = private_credential_directory();
        let path = directory.path().join("credentials.json");
        write_credential_fixture(&path, &server.api_url, TOKEN, "2999-01-01T00:00:00Z");
        let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
        command
            .args([
                "connection",
                "linear",
                "authorization",
                "create",
                ORG,
                "--json",
                "--allow-insecure-http",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for variable in DEPLOYMENT_VARIABLES {
            command.env_remove(variable);
        }
        command.env_remove(CREDENTIALS_FILE_VARIABLE);
        for (name, value) in deployment_environment(&server.api_url, path.to_str().unwrap()) {
            command.env(name, value);
        }
        let child = command.spawn().unwrap();
        let start = server.wait_for_request();
        let poll = server.wait_for_request();
        assert!(start.starts_with("POST "));
        assert!(poll.contains(&format!("/authorization-sessions/{SESSION} ")));
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            signal,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();
        assert_eq!(output.status.code(), Some(code));
        assert_eq!(
            lines(&output)
                .iter()
                .map(|line| line["outcome"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["pending", "observation_stopped"]
        );
        assert_eq!(lines(&output)[1]["sessionId"], SESSION);
        assert_no_secret_output(&output, &[TOKEN, SECRET]);
        assert!(server.finish().is_empty());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn interruption_before_start_response_preserves_request_identity() {
    for (signal, code) in [
        (rustix::process::Signal::INT, 130),
        (rustix::process::Signal::TERM, 143),
    ] {
        let mut server =
            ScriptedServer::respond_with_paused_first_response(vec![json_http_response(
                "201 Created",
                session("pending"),
            )]);
        let directory = private_credential_directory();
        let path = directory.path().join("credentials.json");
        write_credential_fixture(&path, &server.api_url, TOKEN, "2999-01-01T00:00:00Z");
        let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
        command
            .args([
                "connection",
                "linear",
                "authorization",
                "create",
                ORG,
                "--json",
                "--allow-insecure-http",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for variable in DEPLOYMENT_VARIABLES {
            command.env_remove(variable);
        }
        command.env_remove(CREDENTIALS_FILE_VARIABLE);
        for (name, value) in deployment_environment(&server.api_url, path.to_str().unwrap()) {
            command.env(name, value);
        }
        let child = command.spawn().unwrap();
        let request = server.wait_for_request();
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            signal,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();
        assert_eq!(output.status.code(), Some(code));
        assert_eq!(lines(&output)[0]["outcome"], "observation_stopped");
        assert!(lines(&output)[0]["sessionId"].is_null());
        assert!(lines(&output)[0].get("idempotencyKey").is_none());
        assert!(!header_value(&request, "idempotency-key").is_empty());
        assert_no_secret_output(&output, &[TOKEN, SECRET]);
        assert!(server.finish().is_empty());
    }
}
