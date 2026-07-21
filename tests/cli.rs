use std::process::{Command, Output};

const BUILD_VERSION: &str = match option_env!("SCHERZO_CLOUD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};
const BUILD_IDENTITY: &str = match option_env!("SCHERZO_CLOUD_BUILD_IDENTITY") {
    Some(identity) => identity,
    None => "unknown",
};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args(args)
        .output()
        .expect("scherzo-cloud should run")
}

#[test]
fn no_arguments_print_composed_root_help() {
    let output = run(&[]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud [COMMAND]"));
    assert!(stdout.contains("version  Print version information"));
    assert!(stdout.contains("runner   Run and manage the Scherzo Cloud runner"));
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_without_a_subcommand_prints_composed_help() {
    let output = run(&["runner"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud runner [COMMAND]"));
    assert!(stdout.contains("serve  Connect to Scherzo Cloud and serve run assignments"));
    assert!(output.stderr.is_empty());
}

#[test]
fn nested_help_flags_use_the_composed_command_tree() {
    let runner = run(&["runner", "--help"]);
    let serve = run(&["runner", "serve", "--help"]);

    assert!(runner.status.success());
    assert!(
        String::from_utf8_lossy(&runner.stdout)
            .contains("serve  Connect to Scherzo Cloud and serve run assignments")
    );
    assert!(runner.stderr.is_empty());

    assert!(serve.status.success());
    assert!(String::from_utf8_lossy(&serve.stdout).contains("Usage: scherzo-cloud runner serve"));
    assert!(serve.stderr.is_empty());
}

#[test]
fn version_command_and_flag_report_the_build_version() {
    let expected = format!("scherzo-cloud {BUILD_VERSION}\n");

    for args in [["version"].as_slice(), ["--version"].as_slice()] {
        let output = run(args);

        assert!(output.status.success());
        assert_eq!(output.stdout, expected.as_bytes());
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn structured_version_reports_the_version_one_contract() {
    let output = run(&["version", "--json"]);
    let executable_path = std::fs::canonicalize(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .expect("scherzo-cloud executable path should resolve");
    let actual: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("version output should be JSON");
    let expected = serde_json::json!({
        "schemaVersion": 1,
        "command": "scherzo-cloud",
        "version": BUILD_VERSION,
        "executablePath": executable_path.to_string_lossy(),
        "buildIdentity": BUILD_IDENTITY,
    });

    assert!(output.status.success());
    assert_eq!(actual, expected);
    assert!(output.stdout.ends_with(b"\n"));
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_serve_is_an_explicit_unimplemented_stub() {
    let output = run(&["runner", "serve"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"Error: scherzo-cloud runner serve is not implemented yet\n"
    );
}

#[test]
fn unknown_commands_are_usage_errors() {
    let output = run(&["unknown"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unrecognized subcommand 'unknown'"));
}
