use std::io::Cursor;

use super::HttpTransportPolicy;
use super::artifacts::*;
use super::test_support::ScriptedHttpServer;

fn http_response(status: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n", body.len());
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("Connection: close\r\n\r\n");
    let mut bytes = response.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn inventory_requires_no_store_and_decodes_exact_member_metadata() {
    let body = br#"{
        "artifactSetId":"ats_01k0z6r1w8f4jy2m7q9v3x5abc",
        "sealedAt":"2026-08-17T12:00:00Z",
        "expiresAt":"2026-09-17T12:00:00Z",
        "memberCount":1,
        "totalSizeBytes":2,
        "members":[{
            "path":"result.json","mediaType":"application/json","sizeBytes":2,
            "digest":{"algorithm":"sha256","value":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"}
        }]
    }"#;
    let server = ScriptedHttpServer::respond(http_response(
        "200 OK",
        &[
            ("Content-Type", "application/json"),
            ("Cache-Control", "private, no-store"),
        ],
        body,
    ));
    let mut api = ArtifactApi::new(
        &server.api_url,
        "fixture-token",
        HttpTransportPolicy::AllowInsecureHttp,
    )
    .unwrap();

    let page = api
        .inventory_page("acme", "run_fixture", 200, None)
        .unwrap();

    assert_eq!(page.members[0].path, "result.json");
    assert_eq!(page.members[0].size_bytes, 2);
    let request = server.finish_one();
    assert!(
        request
            .starts_with("GET /api/v1/organizations/acme/runs/run_fixture/artifact-set?limit=200 ")
    );
}

#[test]
fn direct_download_errors_and_debug_never_expose_capability_url() {
    let secret = "url-query-secret-sentinel";
    let server = ScriptedHttpServer::respond(http_response(
        "403 Forbidden",
        &[("Content-Type", "application/xml")],
        b"provider detail",
    ));
    let capability = ArtifactCapabilityMember {
        member: ArtifactMember {
            path: "result.json".to_owned(),
            media_type: "application/json".to_owned(),
            size_bytes: 2,
            sha256: [0; 32],
        },
        url: format!("{}exact?signature={secret}", server.api_url),
    };
    let mut api = ArtifactApi::new(
        &server.api_url,
        "fixture-token",
        HttpTransportPolicy::AllowInsecureHttp,
    )
    .unwrap();
    let mut output = Cursor::new(Vec::new());

    let error = api.download(&capability, &mut output).unwrap_err();

    assert!(!format!("{error:?} {error} {capability:?}").contains(secret));
    let request = server.finish_one();
    assert!(request.starts_with("GET /api/exact?signature="));
}
