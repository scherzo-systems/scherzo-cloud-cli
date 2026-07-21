use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args(args)
        .output()
        .expect("scherzo-cloud should run")
}

#[test]
fn no_arguments_print_help() {
    let output = run(&[]);

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage: scherzo-cloud <COMMAND>"));
    assert!(output.stderr.is_empty());
}

#[test]
fn version_command_is_machine_stable() {
    let output = run(&["version"]);

    assert!(output.status.success());
    assert_eq!(output.stdout, b"scherzo-cloud 0.1.0\n");
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
    assert!(String::from_utf8_lossy(&output.stderr).contains("unrecognized command or option"));
}
