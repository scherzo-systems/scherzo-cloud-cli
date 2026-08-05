use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

use super::{private_credential_directory, run_with_env, write_runner_credential};

const PI_CHECK_ID: &str = "execution.harness.pi-json-v1";
const CAPABILITY_PROBE: &str = "--no-approve --no-extensions --no-skills --no-prompt-templates --no-themes --no-context-files --help";
const CLOSED_PROBES: &[u8] = b"--version\n--no-approve --no-extensions --no-skills --no-prompt-templates --no-themes --no-context-files --help\n";
pub(super) const COMPLETE_HELP: &str = "pi - fixture\nUsage:\n  pi [options] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --no-session Do not save session\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n  --approve, -a Trust project files for this run\n";
const REQUIRED_CAPABILITIES: &str = "json_event_stream,ephemeral_session,extension_loading,system_prompt_append,invocation_scoped_project_trust";

pub(super) struct PiFixture {
    _directory: tempfile::TempDir,
    executable: PathBuf,
    probe_log: PathBuf,
}

impl PiFixture {
    pub(super) fn new(version: &str, help: &str, executable: bool) -> Self {
        Self::with_execution(version, help, executable, "exit 97")
    }

    pub(super) fn with_execution(
        version: &str,
        help: &str,
        executable: bool,
        execution: &str,
    ) -> Self {
        let directory = tempfile::tempdir().expect("temporary Pi directory should be created");
        let executable_path = directory.path().join("pi");
        let probe_log = directory.path().join("probes.log");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(if executable { 0o755 } else { 0o644 })
            .open(&executable_path)
            .expect("fake Pi should be created");
        writeln!(file, "#!/bin/sh").unwrap();
        writeln!(
            file,
            "printf '%s\\n' \"$*\" >> {}",
            quote(probe_log.to_str().expect("probe log path should be UTF-8"))
        )
        .unwrap();
        writeln!(file, "case \"$*\" in").unwrap();
        writeln!(file, "  --version) printf '%s\\n' {} ;;", quote(version)).unwrap();
        writeln!(
            file,
            "  {}) printf '%s' {} ;;",
            quote(CAPABILITY_PROBE),
            quote(help)
        )
        .unwrap();
        writeln!(file, "  *) {execution} ;;").unwrap();
        writeln!(file, "esac").unwrap();
        drop(file);

        Self {
            _directory: directory,
            executable: executable_path,
            probe_log,
        }
    }

    pub(super) fn executable(&self) -> &str {
        self.executable
            .to_str()
            .expect("fake Pi path should be UTF-8")
    }

    pub(super) fn recorded_probes(&self) -> Vec<u8> {
        fs::read(&self.probe_log).unwrap_or_default()
    }

    pub(super) fn path_directory(&self) -> &Path {
        self.executable.parent().unwrap()
    }
}

pub(super) fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn pi_doctor_json(executable: &str, environment: &[(&str, &str)]) -> std::process::Output {
    run_with_env(
        &[
            "runner",
            "doctor",
            "--check",
            PI_CHECK_ID,
            "--pi-executable",
            executable,
            "--json",
        ],
        environment,
    )
}

fn report_code(output: &std::process::Output) -> String {
    serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["checks"][0]["code"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[test]
fn doctor_retains_an_accepted_patch_release_with_only_the_two_closed_probes() {
    let fixture = PiFixture::new("0.83.7", COMPLETE_HELP, true);
    let first_agent_directory = tempfile::tempdir().expect("first Pi agent directory");
    let second_agent_directory = tempfile::tempdir().expect("second Pi agent directory");
    let first_settings = br#"{"defaultProjectTrust":"never"}"#;
    let second_settings = br#"{"defaultProjectTrust":"always"}"#;
    let first_trust = br#"{"/fixture":true}"#;
    let second_trust = br#"{"/fixture":false}"#;
    for (directory, settings, trust) in [
        (
            &first_agent_directory,
            first_settings.as_slice(),
            first_trust.as_slice(),
        ),
        (
            &second_agent_directory,
            second_settings.as_slice(),
            second_trust.as_slice(),
        ),
    ] {
        fs::write(directory.path().join("settings.json"), settings).unwrap();
        fs::write(directory.path().join("trust.json"), trust).unwrap();
    }
    let first_path = tempfile::tempdir().expect("first ambient PATH");
    let second_path = tempfile::tempdir().expect("second ambient PATH");

    let mut reports = Vec::new();
    for (path, agent_directory) in [
        (first_path.path(), first_agent_directory.path()),
        (second_path.path(), second_agent_directory.path()),
    ] {
        let output = pi_doctor_json(
            fixture.executable(),
            &[
                ("PATH", path.to_str().unwrap()),
                ("PI_CODING_AGENT_DIR", agent_directory.to_str().unwrap()),
            ],
        );
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        reports.push(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap());
    }

    assert_eq!(reports[0], reports[1]);
    assert_eq!(reports[0]["checks"][0]["id"], PI_CHECK_ID);
    assert_eq!(reports[0]["checks"][0]["status"], "pass");
    assert_eq!(reports[0]["checks"][0]["details"]["version"], "0.83.7");
    assert_eq!(reports[0]["checks"][0]["details"]["profile"], "PiJsonV1");
    assert_eq!(
        reports[0]["checks"][0]["details"]["supportedRange"],
        ">=0.83.0 <0.84.0"
    );
    assert_eq!(
        reports[0]["checks"][0]["details"]["capabilities"],
        REQUIRED_CAPABILITIES
    );
    assert_eq!(
        Path::new(
            reports[0]["checks"][0]["details"]["executablePath"]
                .as_str()
                .unwrap()
        ),
        fs::canonicalize(&fixture.executable).unwrap()
    );
    assert_eq!(
        fixture.recorded_probes(),
        [CLOSED_PROBES, CLOSED_PROBES].concat()
    );
    for (directory, settings, trust) in [
        (
            &first_agent_directory,
            first_settings.as_slice(),
            first_trust.as_slice(),
        ),
        (
            &second_agent_directory,
            second_settings.as_slice(),
            second_trust.as_slice(),
        ),
    ] {
        assert_eq!(
            fs::read(directory.path().join("settings.json")).unwrap(),
            settings
        );
        assert_eq!(
            fs::read(directory.path().join("trust.json")).unwrap(),
            trust
        );
    }
}

#[test]
fn doctor_reports_every_closed_installation_failure_with_exact_probe_boundaries() {
    let missing_directory = tempfile::tempdir().expect("missing Pi directory");
    let missing = missing_directory.path().join("missing-pi");
    let missing_output = pi_doctor_json(missing.to_str().unwrap(), &[]);
    assert_eq!(missing_output.status.code(), Some(1));
    assert_eq!(report_code(&missing_output), "missing_pi_installation");

    let cases = [
        (
            "0.83.0",
            COMPLETE_HELP,
            false,
            "unexecutable_pi_installation",
            b"".as_slice(),
        ),
        (
            "not-a-version",
            COMPLETE_HELP,
            true,
            "malformed_pi_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.82.1",
            COMPLETE_HELP,
            true,
            "unsupported_pi_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.84.0",
            COMPLETE_HELP,
            true,
            "unsupported_pi_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.83.0-rc.1",
            COMPLETE_HELP,
            true,
            "malformed_pi_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.83.0",
            "not Pi help\n",
            true,
            "malformed_pi_capabilities",
            CLOSED_PROBES,
        ),
        (
            "0.83.0",
            "pi - fixture\nUsage:\n  pi [options] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --no-session Do not save session\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n",
            true,
            "unsupported_pi_capability",
            CLOSED_PROBES,
        ),
    ];

    for (version, help, executable, expected_code, expected_probes) in cases {
        let fixture = PiFixture::new(version, help, executable);
        let output = pi_doctor_json(fixture.executable(), &[]);

        assert_eq!(output.status.code(), Some(1), "{expected_code}");
        assert!(output.stderr.is_empty(), "{expected_code}");
        assert_eq!(report_code(&output), expected_code);
        assert_eq!(
            fixture.recorded_probes(),
            expected_probes,
            "{expected_code}"
        );
    }

    let unconfigured = run_with_env(&["runner", "doctor", "--check", PI_CHECK_ID, "--json"], &[]);
    assert_eq!(unconfigured.status.code(), Some(1));
    assert_eq!(report_code(&unconfigured), "pi_not_configured");
}

#[test]
fn agent_capable_runner_initialization_uses_the_same_validator_once() {
    let fixture = PiFixture::new("0.83.0", COMPLETE_HELP, true);
    let credential_directory = private_credential_directory();
    let credential_path = write_runner_credential(&credential_directory);
    let output = run_with_env(
        &[
            "runner",
            "serve",
            "--gateway-url",
            "https://not-a-websocket.example.test",
            "--credential-file",
            &credential_path,
            "--workflow-id",
            "wfl_01k0z6r1w8f4jy2m7q9v3x5abr",
            "--workflow-source-root",
            "schemas",
            "--workflow-path",
            "workflow-v1.schema.json",
            "--work-root",
            "tests",
            "--pi-executable",
            fixture.executable(),
        ],
        &[],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        output
            .stderr
            .starts_with(b"Error: invalid runner gateway URL\n")
    );
    assert_eq!(fixture.recorded_probes(), CLOSED_PROBES);

    let incompatible = PiFixture::new("0.84.0", COMPLETE_HELP, true);
    let output = run_with_env(
        &[
            "runner",
            "serve",
            "--gateway-url",
            "wss://gateway.example.test/v1/connect",
            "--credential-file",
            "/credential/must-not-be-read",
            "--workflow-id",
            "wfl_01k0z6r1w8f4jy2m7q9v3x5abr",
            "--workflow-source-root",
            "schemas",
            "--workflow-path",
            "workflow-v1.schema.json",
            "--work-root",
            "tests",
            "--pi-executable",
            incompatible.executable(),
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        output.stderr,
        b"Error: configured Pi version 0.84.0 is unsupported; install a stable Pi release in range >=0.83.0 <0.84.0\n"
    );
    assert_eq!(incompatible.recorded_probes(), b"--version\n");
}

#[test]
fn pinned_conformance_validation_ignores_ambient_force_color() {
    let Some(executable) = option_env!("SCHERZO_PI_CONFORMANCE_EXECUTABLE") else {
        return;
    };

    let baseline = pi_doctor_json(executable, &[]);
    let force_color = pi_doctor_json(executable, &[("FORCE_COLOR", "1")]);

    assert!(baseline.status.success());
    assert_eq!(
        (force_color.status.code(), report_code(&force_color)),
        (Some(0), "ok".to_owned())
    );
    assert_eq!(baseline.stdout, force_color.stdout);
}

#[test]
fn validation_does_not_read_trust_or_execute_project_extensions() {
    let Some(executable) = option_env!("SCHERZO_PI_CONFORMANCE_EXECUTABLE") else {
        return;
    };
    let project_directory = tempfile::tempdir().expect("temporary Pi project directory");
    let extensions_directory = project_directory.path().join(".pi/extensions");
    fs::create_dir_all(&extensions_directory).expect("extensions directory should be created");
    let marker = project_directory.path().join("extension-executed");
    let marker_literal =
        serde_json::to_string(marker.to_str().expect("marker path should be UTF-8"))
            .expect("marker path should encode as JavaScript string");
    fs::write(
        extensions_directory.join("validation-proof.ts"),
        format!(
            "import {{ writeFileSync }} from \"node:fs\";\n\
             writeFileSync({marker_literal}, \"executed\");\n\
             export default function () {{}}\n"
        ),
    )
    .expect("proof extension should be written");
    let agent_directory = tempfile::tempdir().expect("temporary Pi agent directory");
    fs::write(
        agent_directory.path().join("settings.json"),
        br#"{"defaultProjectTrust":"never"}"#,
    )
    .expect("global Pi settings should be written");
    let canonical_project = fs::canonicalize(project_directory.path())
        .expect("temporary Pi project directory should canonicalize");
    fs::write(
        agent_directory.path().join("trust.json"),
        serde_json::to_vec(&serde_json::json!({
            (canonical_project.to_str().expect("project path should be UTF-8")): true
        }))
        .expect("saved trust should encode"),
    )
    .expect("saved trust should be written");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args([
            "runner",
            "doctor",
            "--check",
            PI_CHECK_ID,
            "--pi-executable",
            executable,
            "--json",
        ])
        .current_dir(project_directory.path())
        .env("PI_CODING_AGENT_DIR", agent_directory.path())
        .output()
        .expect("runner doctor should run");

    assert!(output.status.success());
    assert!(
        !marker.exists(),
        "installation validation read saved trust and executed a project extension"
    );
}

#[test]
fn pinned_conformance_executable_is_exact_and_independent_of_path_and_saved_trust() {
    let Some(executable) = option_env!("SCHERZO_PI_CONFORMANCE_EXECUTABLE") else {
        return;
    };
    let first_agent_directory = tempfile::tempdir().expect("first pinned Pi agent directory");
    let second_agent_directory = tempfile::tempdir().expect("second pinned Pi agent directory");
    let first_settings = br#"{"defaultProjectTrust":"ask"}"#;
    let second_settings = br#"{"defaultProjectTrust":"always"}"#;
    let first_trust = br#"{"/saved":false}"#;
    let second_trust = br#"{"/saved":true}"#;
    for (directory, settings, trust) in [
        (
            &first_agent_directory,
            first_settings.as_slice(),
            first_trust.as_slice(),
        ),
        (
            &second_agent_directory,
            second_settings.as_slice(),
            second_trust.as_slice(),
        ),
    ] {
        fs::write(directory.path().join("settings.json"), settings).unwrap();
        fs::write(directory.path().join("trust.json"), trust).unwrap();
    }
    let first_path = tempfile::tempdir().expect("first pinned ambient PATH");
    let second_path = tempfile::tempdir().expect("second pinned ambient PATH");

    let reports = [
        (first_path.path(), first_agent_directory.path()),
        (second_path.path(), second_agent_directory.path()),
    ]
    .map(|(path, agent_directory)| {
        let output = pi_doctor_json(
            executable,
            &[
                ("PATH", path.to_str().unwrap()),
                ("PI_CODING_AGENT_DIR", agent_directory.to_str().unwrap()),
            ],
        );
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
    });

    assert_eq!(reports[0], reports[1]);
    let details = &reports[0]["checks"][0]["details"];
    assert_eq!(details["version"], "0.83.0");
    assert_eq!(details["profile"], "PiJsonV1");
    assert_eq!(details["supportedRange"], ">=0.83.0 <0.84.0");
    assert_eq!(details["capabilities"], REQUIRED_CAPABILITIES);
    assert_eq!(
        Path::new(details["executablePath"].as_str().unwrap()),
        fs::canonicalize(executable).unwrap()
    );
    for (directory, settings, trust) in [
        (
            &first_agent_directory,
            first_settings.as_slice(),
            first_trust.as_slice(),
        ),
        (
            &second_agent_directory,
            second_settings.as_slice(),
            second_trust.as_slice(),
        ),
    ] {
        assert_eq!(
            fs::read(directory.path().join("settings.json")).unwrap(),
            settings
        );
        assert_eq!(
            fs::read(directory.path().join("trust.json")).unwrap(),
            trust
        );
    }
}
