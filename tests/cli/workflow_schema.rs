use std::fs;
use std::net::TcpListener;
use std::process::Command;

#[test]
fn embedded_schema_is_emitted_unchanged_without_external_state() {
    let working_directory = tempfile::tempdir().expect("temporary working directory should exist");
    let listener = TcpListener::bind("127.0.0.1:0").expect("network sentinel should bind");
    listener
        .set_nonblocking(true)
        .expect("network sentinel should be nonblocking");
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let entries_before = directory_entries(working_directory.path());

    let output = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .env_clear()
        .current_dir(working_directory.path())
        .args(["workflow", "schema"])
        .env("PATH", working_directory.path())
        .env(
            super::CREDENTIALS_FILE_VARIABLE,
            "/dev/null/workflow-schema-credentials.json",
        )
        .env("SCHERZO_CLOUD_API_URL", &api_url)
        .env(
            "SCHERZO_CLOUD_AUTH_ISSUER",
            "http://auth.workflow-schema.invalid/",
        )
        .env(
            "SCHERZO_CLOUD_AUTH_AUDIENCE",
            "https://api.workflow-schema.invalid",
        )
        .env("SCHERZO_CLOUD_AUTH_CLIENT_ID", "workflow-schema-client")
        .output()
        .expect("scherzo-cloud should run");

    assert!(output.status.success());
    assert_eq!(
        output.stdout,
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-v1.schema.json"
        ))
    );
    assert!(output.stderr.is_empty());
    let schema: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("schema output should be JSON");
    assert_eq!(
        schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    assert_eq!(directory_entries(working_directory.path()), entries_before);
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "schema output must not open a network connection"
    );
}

fn directory_entries(directory: &std::path::Path) -> Vec<std::ffi::OsString> {
    let mut entries = fs::read_dir(directory)
        .expect("working directory should remain readable")
        .map(|entry| {
            entry
                .expect("directory entry should be readable")
                .file_name()
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries
}
