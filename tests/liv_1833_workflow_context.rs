#![allow(
    clippy::disallowed_macros,
    reason = "integration tests use Cargo-provided executable paths"
)]
#![allow(
    clippy::unwrap_used,
    reason = "integration tests use panic shortcuts to express fixture failures"
)]

use std::fs;
use std::process::Command;

#[test]
fn workflow_run_existing_directory_diagnostic_names_run_path() {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    let run_directory = temporary.path().join("existing-run");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::create_dir(&run_directory).unwrap();
    let workflow = source_root.join("workflow.yaml");
    fs::write(
        &workflow,
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args([
            "workflow",
            "run",
            "--source-root",
            source_root.to_str().unwrap(),
            "--execution-root",
            execution_root.to_str().unwrap(),
            "--run-dir",
            run_directory.to_str().unwrap(),
            workflow.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(run_directory.to_str().unwrap()),
        "diagnostic omitted the requested run path: {stderr}"
    );
}
