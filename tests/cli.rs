use std::process::{Command, Output};

const BUILD_VERSION: &str = match option_env!("SCHERZO_CLOUD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};
const BUILD_IDENTITY: &str = match option_env!("SCHERZO_CLOUD_BUILD_IDENTITY") {
    Some(identity) => identity,
    None => "unknown",
};

const DEPLOYMENT_VARIABLES: [&str; 4] = [
    "SCHERZO_CLOUD_API_URL",
    "SCHERZO_CLOUD_AUTH_ISSUER",
    "SCHERZO_CLOUD_AUTH_AUDIENCE",
    "SCHERZO_CLOUD_AUTH_CLIENT_ID",
];

fn run(args: &[&str]) -> Output {
    run_with_env(args, &[])
}

fn run_with_env(args: &[&str], environment: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"));
    command.args(args);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }

    command.output().expect("scherzo-cloud should run")
}

#[test]
fn no_arguments_print_composed_root_help() {
    let output = run(&[]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud [OPTIONS] [COMMAND]"));
    assert!(stdout.contains("auth     Authenticate a human with Scherzo Cloud"));
    assert!(stdout.contains("version  Print version information"));
    assert!(stdout.contains("runner   Run and manage the Scherzo Cloud runner"));
    assert!(stdout.contains("--allow-insecure-http"));
    assert!(output.stderr.is_empty());
}

#[test]
fn auth_without_a_subcommand_prints_composed_help_without_loading_deployment() {
    let output = run_with_env(
        &["auth"],
        &[("SCHERZO_CLOUD_API_URL", "partial-override-is-ignored")],
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud auth [OPTIONS] [COMMAND]"));
    assert!(stdout.contains("login   Authenticate through a browser on any machine"));
    assert!(stdout.contains("status  Inspect the server-confirmed authentication state"));
    assert!(stdout.contains("logout  Remove the local human credential"));
    assert!(output.stderr.is_empty());
}

#[test]
fn runner_without_a_subcommand_prints_composed_help() {
    let output = run(&["runner"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage: scherzo-cloud runner [OPTIONS] [COMMAND]"));
    assert!(stdout.contains("serve  Connect to Scherzo Cloud and serve run assignments"));
    assert!(output.stderr.is_empty());
}

#[test]
fn nested_help_flags_use_the_composed_command_tree() {
    let auth = run(&["auth", "--help"]);
    let login = run(&["auth", "login", "--help"]);
    let runner = run(&["runner", "--help"]);
    let serve = run(&["runner", "serve", "--help"]);

    assert!(auth.status.success());
    assert!(
        String::from_utf8_lossy(&auth.stdout)
            .contains("status  Inspect the server-confirmed authentication state")
    );
    assert!(auth.stderr.is_empty());

    assert!(login.status.success());
    let login_stdout = String::from_utf8_lossy(&login.stdout);
    assert!(login_stdout.contains("Usage: scherzo-cloud auth login [OPTIONS]"));
    assert!(login_stdout.contains("--json"));
    assert!(login_stdout.contains("--force"));
    assert!(login_stdout.contains("--allow-insecure-http"));
    assert!(login.stderr.is_empty());

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
fn auth_leaf_commands_are_explicit_unimplemented_stubs() {
    let cases = [
        (
            vec!["auth", "login", "--json", "--force"],
            b"Error: scherzo-cloud auth login is not implemented yet\n".as_slice(),
        ),
        (
            vec!["auth", "status", "--json"],
            b"Error: scherzo-cloud auth status is not implemented yet\n".as_slice(),
        ),
        (
            vec!["auth", "logout", "--json"],
            b"Error: scherzo-cloud auth logout is not implemented yet\n".as_slice(),
        ),
    ];

    for (args, expected_stderr) in cases {
        let output = run(&args);

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(output.stderr, expected_stderr);
    }
}

#[test]
fn insecure_http_flag_is_global() {
    for args in [
        ["--allow-insecure-http", "auth", "status"],
        ["auth", "status", "--allow-insecure-http"],
    ] {
        let output = run(&args);

        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(
            output.stderr,
            b"Error: scherzo-cloud auth status is not implemented yet\n"
        );
    }
}

#[test]
fn partial_deployment_override_fails_before_auth_dispatch() {
    let output = run_with_env(
        &["auth", "status", "--json"],
        &[("SCHERZO_CLOUD_API_URL", "https://api.fixture.example")],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"Error: configure authentication deployment: development deployment overrides must be set together; missing SCHERZO_CLOUD_AUTH_ISSUER, SCHERZO_CLOUD_AUTH_AUDIENCE, SCHERZO_CLOUD_AUTH_CLIENT_ID\n"
    );
}

#[test]
fn complete_https_deployment_override_reaches_auth_dispatch() {
    let output = run_with_env(
        &["auth", "status"],
        &[
            ("SCHERZO_CLOUD_API_URL", "https://api.fixture.example"),
            ("SCHERZO_CLOUD_AUTH_ISSUER", "https://auth.fixture.example/"),
            ("SCHERZO_CLOUD_AUTH_AUDIENCE", "https://api.fixture.example"),
            ("SCHERZO_CLOUD_AUTH_CLIENT_ID", "fixture-public-client"),
        ],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"Error: scherzo-cloud auth status is not implemented yet\n"
    );
}

#[test]
fn networked_auth_requires_http_opt_in_but_local_logout_does_not() {
    let environment = [
        ("SCHERZO_CLOUD_API_URL", "http://api.fixture.example"),
        ("SCHERZO_CLOUD_AUTH_ISSUER", "http://auth.fixture.example/"),
        ("SCHERZO_CLOUD_AUTH_AUDIENCE", "https://api.fixture.example"),
        ("SCHERZO_CLOUD_AUTH_CLIENT_ID", "fixture-public-client"),
    ];
    let rejected = run_with_env(&["auth", "status"], &environment);
    let accepted = run_with_env(&["auth", "status", "--allow-insecure-http"], &environment);
    let local_logout = run_with_env(&["auth", "logout"], &environment);

    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stdout.is_empty());
    assert_eq!(
        rejected.stderr,
        b"Error: configure authentication deployment: SCHERZO_CLOUD_API_URL uses insecure HTTP; rerun with --allow-insecure-http to permit it\n"
    );

    assert_eq!(accepted.status.code(), Some(1));
    assert!(accepted.stdout.is_empty());
    assert_eq!(
        accepted.stderr,
        b"Error: scherzo-cloud auth status is not implemented yet\n"
    );

    assert_eq!(local_logout.status.code(), Some(1));
    assert!(local_logout.stdout.is_empty());
    assert_eq!(
        local_logout.stderr,
        b"Error: scherzo-cloud auth logout is not implemented yet\n"
    );
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
