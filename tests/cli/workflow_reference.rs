use std::fs;
use std::net::TcpListener;
use std::process::Command;

#[test]
fn embedded_reference_is_emitted_unchanged_without_external_state() {
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
        .args(["workflow", "reference"])
        .env("PATH", working_directory.path())
        .env(
            super::CREDENTIALS_FILE_VARIABLE,
            "/dev/null/workflow-reference-credentials.json",
        )
        .env("SCHERZO_CLOUD_API_URL", &api_url)
        .env(
            "SCHERZO_CLOUD_AUTH_ISSUER",
            "http://auth.workflow-reference.invalid/",
        )
        .env(
            "SCHERZO_CLOUD_AUTH_AUDIENCE",
            "https://api.workflow-reference.invalid",
        )
        .env("SCHERZO_CLOUD_AUTH_CLIENT_ID", "workflow-reference-client")
        .output()
        .expect("scherzo-cloud should run");

    assert!(output.status.success());
    assert_eq!(
        output.stdout,
        include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/docs/workflow-v1.md"))
    );
    assert!(output.stderr.is_empty());
    assert!(
        output
            .stdout
            .starts_with(b"# Workflow V1 authoring reference\n")
    );
    assert!(
        !output.stdout.contains(&0x1b),
        "reference must not contain ANSI"
    );
    assert_eq!(directory_entries(working_directory.path()), entries_before);
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "reference output must not open a network connection"
    );

    let markdown = std::str::from_utf8(&output.stdout).expect("reference should be UTF-8 Markdown");
    assert!(
        markdown.len() <= 16 * 1024,
        "reference should remain concise"
    );
    assert_sections_are_ordered(
        markdown,
        &[
            "## Authoring loop",
            "## Mental model",
            "## Core document and graph",
            "## Paths and references",
            "## Agent profiles",
            "## Outputs and finalizers",
            "## Complete example",
            "## Safety boundaries",
            "## Schema retrieval and authoritative validation",
        ],
    );
    for required_handoff in [
        "https://docs.scherzo.dev/agent/workflow-authoring.md",
        "https://docs.scherzo.dev/reference/workflow-v1.md",
        "https://docs.scherzo.dev/schemas/workflow-v1.schema.json",
        "scherzo-cloud workflow schema",
        "workflow validate --json",
    ] {
        assert!(
            markdown.contains(required_handoff),
            "reference should contain {required_handoff}"
        );
    }
}

fn assert_sections_are_ordered(markdown: &str, sections: &[&str]) {
    let mut previous_line = 0;
    for section in sections {
        let line = markdown
            .lines()
            .position(|line| line == *section)
            .unwrap_or_else(|| panic!("reference should contain section {section}"));
        assert!(
            line >= previous_line,
            "reference section {section} should be in contract order"
        );
        previous_line = line;
    }
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
