use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};

use super::{private_credential_directory, run_with_env, write_runner_config};

const PI_CHECK_ID: &str = "execution.harness.pi-json-v1";
const CAPABILITY_PROBE: &str = "--no-approve --no-extensions --no-skills --no-prompt-templates --no-themes --no-context-files --help";
const CLOSED_PROBES: &[u8] = b"--version\n--no-approve --no-extensions --no-skills --no-prompt-templates --no-themes --no-context-files --help\n";
pub(super) const COMPLETE_HELP: &str = "pi - fixture\nUsage:\n  pi [options] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --session-dir <dir> Directory for session storage and lookup\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n  --approve, -a Trust project files for this run\n";
const REQUIRED_CAPABILITIES: &str = "json_event_stream,custom_session_directory,extension_loading,system_prompt_append,invocation_scoped_project_trust";

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
        Self::with_execution_and_capability_hook(version, help, executable, execution, ":")
    }

    pub(super) fn with_execution_and_capability_hook(
        version: &str,
        help: &str,
        executable: bool,
        execution: &str,
        capability_hook: &str,
    ) -> Self {
        let directory = tempfile::tempdir().expect("temporary Pi directory should be created");
        let executable_path = directory.path().join("pi");
        let probe_log = directory.path().join("probes.log");
        let mut file = create_executable(&executable_path, executable);
        writeln!(file, "#!/bin/sh").unwrap();
        write_fixture_behavior(
            &mut file,
            &probe_log,
            version,
            help,
            execution,
            capability_hook,
        );

        Self {
            _directory: directory,
            executable: executable_path,
            probe_log,
        }
    }

    fn with_env_node_interpreter(version: &str, help: &str) -> Self {
        let directory = tempfile::tempdir().expect("temporary Pi directory should be created");
        let executable_path = directory.path().join("pi");
        let probe_log = directory.path().join("probes.log");
        let env_executable = std::env::var_os("PATH")
            .and_then(|search_path| {
                std::env::split_paths(&search_path)
                    .map(|directory| directory.join("env"))
                    .find(|candidate| candidate.is_file())
            })
            .expect("env should be available on the inherited PATH");
        let mut executable = create_executable(&executable_path, true);
        writeln!(executable, "#!{} node", env_executable.display()).unwrap();
        drop(executable);

        let mut node = create_executable(&directory.path().join("node"), true);
        writeln!(node, "#!/bin/sh").unwrap();
        writeln!(node, "shift").unwrap();
        write_fixture_behavior(&mut node, &probe_log, version, help, "exit 97", ":");

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

fn create_executable(path: &Path, executable: bool) -> fs::File {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(if executable { 0o755 } else { 0o644 })
        .open(path)
        .expect("fake executable should be created")
}

fn write_fixture_behavior(
    file: &mut fs::File,
    probe_log: &Path,
    version: &str,
    help: &str,
    execution: &str,
    capability_hook: &str,
) {
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
        "  {}) printf '%s' {}; {capability_hook} ;;",
        quote(CAPABILITY_PROBE),
        quote(help)
    )
    .unwrap();
    writeln!(file, "  *) {execution} ;;").unwrap();
    writeln!(file, "esac").unwrap();
}

pub(super) fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn pi_doctor_json(path: &Path, environment: &[(&str, &str)]) -> std::process::Output {
    let path = path.to_str().expect("controlled PATH should be UTF-8");
    let mut environment = Vec::from(environment);
    environment.push(("PATH", path));
    run_with_env(
        &["runner", "doctor", "--check", PI_CHECK_ID, "--json"],
        &environment,
    )
}

fn controlled_path_for(executable: &Path) -> tempfile::TempDir {
    let directory = tempfile::tempdir().expect("controlled Pi PATH should be created");
    symlink(executable, directory.path().join("pi"))
        .expect("controlled Pi PATH should link its selected executable");
    directory
}

fn report_code(output: &std::process::Output) -> String {
    serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["checks"][0]["code"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn assert_invalid_runner_gateway(output: &std::process::Output) {
    assert!(!output.stderr.is_empty());
}

#[test]
fn doctor_selects_path_pi_and_uses_only_the_two_closed_probes() {
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
    let first_path = controlled_path_for(Path::new(fixture.executable()));
    let second_path = controlled_path_for(Path::new(fixture.executable()));

    let mut reports = Vec::new();
    for (path, agent_directory) in [
        (first_path.path(), first_agent_directory.path()),
        (second_path.path(), second_agent_directory.path()),
    ] {
        let output = pi_doctor_json(
            path,
            &[("PI_CODING_AGENT_DIR", agent_directory.to_str().unwrap())],
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
fn doctor_path_lookup_skips_candidates_not_executable_by_current_user() {
    let inaccessible = PiFixture::new("0.83.0", COMPLETE_HELP, true);
    fs::set_permissions(inaccessible.executable(), fs::Permissions::from_mode(0o001)).unwrap();
    let compatible = PiFixture::new("0.83.0", COMPLETE_HELP, true);
    let ordered_path =
        std::env::join_paths([inaccessible.path_directory(), compatible.path_directory()]).unwrap();

    let output = pi_doctor_json(Path::new(&ordered_path), &[]);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(inaccessible.recorded_probes().is_empty());
    assert_eq!(compatible.recorded_probes(), CLOSED_PROBES);
}

#[test]
fn doctor_preserves_selected_path_for_env_interpreter_resolution() {
    let fixture = PiFixture::with_env_node_interpreter("0.83.0", COMPLETE_HELP);

    let output = pi_doctor_json(fixture.path_directory(), &[]);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fixture.recorded_probes(), CLOSED_PROBES);
}

#[test]
fn doctor_reports_every_closed_installation_failure_with_exact_probe_boundaries() {
    let missing_directory = tempfile::tempdir().expect("missing Pi directory");
    let missing_output = pi_doctor_json(missing_directory.path(), &[]);
    assert_eq!(missing_output.status.code(), Some(1));
    assert_eq!(report_code(&missing_output), "missing_pi_installation");

    let unexecutable_directory = tempfile::tempdir().expect("unexecutable Pi directory");
    let unexecutable = unexecutable_directory.path().join("pi");
    fs::write(&unexecutable, "#!/definitely/missing/interpreter\n").unwrap();
    fs::set_permissions(&unexecutable, fs::Permissions::from_mode(0o755)).unwrap();
    let unexecutable_output = pi_doctor_json(unexecutable_directory.path(), &[]);
    assert_eq!(unexecutable_output.status.code(), Some(1));
    assert_eq!(
        report_code(&unexecutable_output),
        "unexecutable_pi_installation"
    );

    let cases = [
        (
            "not-a-version",
            COMPLETE_HELP,
            "malformed_pi_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.82.1",
            COMPLETE_HELP,
            "unsupported_pi_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.84.0",
            COMPLETE_HELP,
            "unsupported_pi_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.83.0-rc.1",
            COMPLETE_HELP,
            "malformed_pi_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.83.0",
            "not Pi help\n",
            "malformed_pi_capabilities",
            CLOSED_PROBES,
        ),
        (
            "0.83.0",
            "pi - fixture\nUsage:\n  pi [options] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --session-dir <dir> Directory for session storage and lookup\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n",
            "unsupported_pi_capability",
            CLOSED_PROBES,
        ),
        (
            "0.83.0",
            "pi - fixture\nUsage:\n  pi [options] [@files...] [messages...]\n  --mode <mode> Output mode: text, json, or rpc\n  --extension, -e <path> Load extension\n  --append-system-prompt <text> Append prompt\n  --approve, -a Trust project files for this run\n",
            "unsupported_pi_capability",
            CLOSED_PROBES,
        ),
    ];

    for (version, help, expected_code, expected_probes) in cases {
        let fixture = PiFixture::new(version, help, true);
        let output = pi_doctor_json(fixture.path_directory(), &[]);

        assert_eq!(output.status.code(), Some(1), "{expected_code}");
        assert!(output.stderr.is_empty(), "{expected_code}");
        assert_eq!(report_code(&output), expected_code);
        assert_eq!(
            fixture.recorded_probes(),
            expected_probes,
            "{expected_code}"
        );
    }
}

#[test]
fn runner_initialization_probes_path_once_and_remains_command_capable_without_compatible_pi() {
    let runner_directory = private_credential_directory();
    let config_path = write_runner_config(
        &runner_directory,
        "https://not-a-websocket.example.test/v1/runner/connect",
    );
    let empty_path = tempfile::tempdir().expect("empty runner PATH");
    let serve_args = ["runner", "serve", "--config", config_path.as_str()];
    let output = run_with_env(
        &serve_args,
        &[("PATH", empty_path.path().to_str().unwrap())],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_invalid_runner_gateway(&output);

    let fixture = PiFixture::new("0.83.0", COMPLETE_HELP, true);
    let output = run_with_env(
        &serve_args,
        &[("PATH", fixture.path_directory().to_str().unwrap())],
    );
    assert_eq!(output.status.code(), Some(1));
    assert_invalid_runner_gateway(&output);
    assert_eq!(fixture.recorded_probes(), CLOSED_PROBES);

    let incompatible = PiFixture::new("0.84.0", COMPLETE_HELP, true);
    let output = run_with_env(
        &serve_args,
        &[("PATH", incompatible.path_directory().to_str().unwrap())],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_invalid_runner_gateway(&output);
    assert_eq!(incompatible.recorded_probes(), b"--version\n");
}

#[test]
fn pinned_conformance_validation_ignores_ambient_force_color() {
    let Some(executable) = option_env!("SCHERZO_PI_CONFORMANCE_EXECUTABLE") else {
        return;
    };

    let path = controlled_path_for(Path::new(executable));
    let baseline = pi_doctor_json(path.path(), &[]);
    let force_color = pi_doctor_json(path.path(), &[("FORCE_COLOR", "1")]);

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

    let path = controlled_path_for(Path::new(executable));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_scherzo-cloud"))
        .args(["runner", "doctor", "--check", PI_CHECK_ID, "--json"])
        .current_dir(project_directory.path())
        .env("PATH", path.path())
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
fn pinned_conformance_executable_is_exact_and_independent_of_saved_trust() {
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
    let first_path = controlled_path_for(Path::new(executable));
    let second_path = controlled_path_for(Path::new(executable));

    let reports = [
        (first_path.path(), first_agent_directory.path()),
        (second_path.path(), second_agent_directory.path()),
    ]
    .map(|(path, agent_directory)| {
        let output = pi_doctor_json(
            path,
            &[("PI_CODING_AGENT_DIR", agent_directory.to_str().unwrap())],
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
