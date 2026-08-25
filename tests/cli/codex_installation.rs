use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, symlink};
use std::path::{Path, PathBuf};

use super::pi_installation::quote;
use super::{assert_human_doctor_detail_matches_json, run_with_env};

const CODEX_CHECK_ID: &str = "execution.harness.codex-app-server-v1";
const CLOSED_PROBES: &[u8] = b"--version\napp-server generate-json-schema --out ../schemas\n";
const REQUIRED_CAPABILITIES: &str = "app_server_schema_v1";
const SCHEMA_FILES: [&str; 9] = [
    "ClientNotification.json",
    "ClientRequest.json",
    "ServerNotification.json",
    "ServerRequest.json",
    "v1/InitializeParams.json",
    "v2/ThreadStartParams.json",
    "v2/ThreadStartResponse.json",
    "v2/TurnInterruptParams.json",
    "v2/TurnStartParams.json",
];

pub(super) struct CodexFixture {
    _directory: tempfile::TempDir,
    executable: PathBuf,
    path_executable: PathBuf,
    probe_log: PathBuf,
}

impl CodexFixture {
    fn new(version: &str, schema_compatible: bool, capability_success: bool) -> Self {
        Self::build(version, schema_compatible, capability_success, false)
    }

    pub(super) fn with_execution(version: &str) -> Self {
        Self::build(version, true, true, true)
    }

    fn build(
        version: &str,
        schema_compatible: bool,
        capability_success: bool,
        execution_enabled: bool,
    ) -> Self {
        let directory = tempfile::tempdir().expect("temporary Codex directory should be created");
        let executable = directory.path().join("codex-real");
        let path_executable = directory.path().join("codex");
        let probe_log = directory.path().join("probes.log");
        let schema_fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex-app-server-v1-schema");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o755)
            .open(&executable)
            .expect("fake Codex should be created");
        writeln!(file, "#!/bin/sh").unwrap();
        writeln!(
            file,
            "printf '%s\\n' \"$*\" >> {}",
            quote(probe_log.to_str().unwrap())
        )
        .unwrap();
        writeln!(file, "case \"$*\" in").unwrap();
        writeln!(
            file,
            "  --version) printf '%s\\n' {} ;;",
            quote(&format!("codex-cli {version}"))
        )
        .unwrap();
        writeln!(
            file,
            "  'app-server generate-json-schema --out ../schemas')"
        )
        .unwrap();
        for relative in SCHEMA_FILES {
            let contents = if !schema_compatible && relative == "ServerNotification.json" {
                "{}\n".to_owned()
            } else {
                fs::read_to_string(schema_fixture.join(relative)).unwrap()
            };
            writeln!(
                file,
                "    printf '%s' {} > \"$4/{}\"",
                quote(&contents),
                relative
            )
            .unwrap();
        }
        writeln!(file, "    test -z \"${{OPENAI_API_KEY+x}}\"").unwrap();
        writeln!(file, "    test -z \"${{CODEX_API_KEY+x}}\"").unwrap();
        writeln!(file, "    test -z \"${{AWS_SECRET_ACCESS_KEY+x}}\"").unwrap();
        writeln!(
            file,
            "    case \"$CODEX_HOME\" in *scherzo-codex-validation-*/codex-home) ;; *) exit 89 ;; esac"
        )
        .unwrap();
        if capability_success {
            writeln!(file, "    ;; ").unwrap();
        } else {
            writeln!(file, "    exit 88 ;; ").unwrap();
        }
        if execution_enabled {
            writeln!(file, "  *'app-server --strict-config --listen stdio://')").unwrap();
            writeln!(file, "    for argument in \"$@\"; do").unwrap();
            writeln!(file, "      case \"$argument\" in").unwrap();
            writeln!(file, "        sqlite_home=\\\"*\\\")").unwrap();
            writeln!(
                file,
                "          CODEX_FIXTURE_SQLITE_HOME=${{argument#sqlite_home=\\\"}}"
            )
            .unwrap();
            writeln!(
                file,
                "          CODEX_FIXTURE_SQLITE_HOME=${{CODEX_FIXTURE_SQLITE_HOME%\\\"}}"
            )
            .unwrap();
            writeln!(file, "          export CODEX_FIXTURE_SQLITE_HOME ;;").unwrap();
            writeln!(file, "      esac").unwrap();
            writeln!(file, "    done").unwrap();
            writeln!(file, "    printf 'codex fixture diagnostic\\n' >&2").unwrap();
            writeln!(
                file,
                "    exec \"$CODEX_FIXTURE_HELPER\" --exact workflow_run::codex_app_server_fixture --ignored --test-threads=1 3>&1 >/dev/null ;;"
            )
            .unwrap();
        }
        writeln!(file, "  *) exit 97 ;; ").unwrap();
        writeln!(file, "esac").unwrap();
        drop(file);
        symlink(&executable, &path_executable).expect("PATH Codex should link to the fixture");

        Self {
            _directory: directory,
            executable,
            path_executable,
            probe_log,
        }
    }

    pub(super) fn path_directory(&self) -> &Path {
        self.path_executable.parent().unwrap()
    }

    pub(super) fn recorded_probes(&self) -> Vec<u8> {
        fs::read(&self.probe_log).unwrap_or_default()
    }
}

fn doctor(path: &Path, environment: &[(&str, &str)], json: bool) -> std::process::Output {
    let mut environment = Vec::from(environment);
    environment.push(("PATH", path.to_str().unwrap()));
    let mut arguments = vec!["runner", "doctor", "--check", CODEX_CHECK_ID];
    if json {
        arguments.push("--json");
    }
    run_with_env(&arguments, &environment)
}

fn doctor_json(path: &Path, environment: &[(&str, &str)]) -> std::process::Output {
    doctor(path, environment, true)
}

fn doctor_human(path: &Path, environment: &[(&str, &str)]) -> std::process::Output {
    doctor(path, environment, false)
}

fn report(output: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn doctor_reports_exact_codex_identity_without_credentials_or_native_configuration() {
    let codex_home = tempfile::tempdir().unwrap();
    let config = codex_home.path().join("config.toml");
    let native_config = b"model_provider = \"native-sentinel\"\n";
    fs::write(&config, native_config).unwrap();
    let fixture = CodexFixture::new("0.149.23", true, true);
    let unselected = CodexFixture::new("0.147.23", true, true);
    let path =
        std::env::join_paths([fixture.path_directory(), unselected.path_directory()]).unwrap();

    let output = doctor_json(
        Path::new(&path),
        &[
            ("CODEX_HOME", codex_home.path().to_str().unwrap()),
            ("OPENAI_API_KEY", "unique-openai-credential-sentinel"),
            ("CODEX_API_KEY", "unique-codex-credential-sentinel"),
            ("AWS_SECRET_ACCESS_KEY", "unique-aws-credential-sentinel"),
        ],
    );

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let report = report(&output);
    let check = &report["checks"][0];
    assert_eq!(check["id"], CODEX_CHECK_ID);
    assert_eq!(check["status"], "pass");
    assert_eq!(check["details"]["version"], "0.149.23");
    assert_eq!(check["details"]["profile"], "CodexAppServerV1");
    assert_eq!(check["details"]["supportedRange"], ">=0.147.0 <0.150.0");
    assert_eq!(check["details"]["qualificationVersion"], "0.149.0");
    assert_eq!(check["details"]["capabilities"], REQUIRED_CAPABILITIES);
    assert_eq!(
        Path::new(check["details"]["executablePath"].as_str().unwrap()),
        fs::canonicalize(&fixture.executable).unwrap()
    );
    for forbidden in [
        "unique-openai-credential-sentinel",
        "unique-codex-credential-sentinel",
        "unique-aws-credential-sentinel",
        "native-sentinel",
        codex_home.path().to_str().unwrap(),
    ] {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(forbidden));
    }
    assert_eq!(fs::read(config).unwrap(), native_config);
    assert_eq!(fixture.recorded_probes(), CLOSED_PROBES);
    assert!(unselected.recorded_probes().is_empty());
}

#[test]
fn doctor_accepts_each_reviewed_codex_minor_line() {
    for version in ["0.147.0", "0.148.999", "0.149.999"] {
        let fixture = CodexFixture::new(version, true, true);
        let output = doctor_json(fixture.path_directory(), &[]);
        assert!(output.status.success(), "{version}");
        let check = &report(&output)["checks"][0];
        assert_eq!(check["status"], "pass", "{version}");
        assert_eq!(check["details"]["version"], version, "{version}");
        assert_eq!(fixture.recorded_probes(), CLOSED_PROBES, "{version}");
    }
}

#[test]
fn doctor_does_not_fall_back_after_the_first_path_candidate_fails() {
    let incompatible = CodexFixture::new("0.146.99", true, true);
    let fallback = CodexFixture::new("0.149.23", true, true);
    let path =
        std::env::join_paths([incompatible.path_directory(), fallback.path_directory()]).unwrap();

    let output = doctor_json(Path::new(&path), &[]);

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        report(&output)["checks"][0]["code"],
        "unsupported_codex_version"
    );
    assert_eq!(incompatible.recorded_probes(), b"--version\n");
    assert!(fallback.recorded_probes().is_empty());
}

#[test]
fn human_doctor_reports_codex_range_and_qualification_when_missing() {
    let missing = tempfile::tempdir().expect("missing Codex directory should be created");
    let human = doctor_human(missing.path(), &[]);
    let json = doctor_json(missing.path(), &[]);
    let report = report(&json);

    assert_eq!(human.status.code(), Some(1));
    assert_eq!(json.status.code(), Some(1));
    for (label, key) in [
        ("profile", "profile"),
        ("supported range", "supportedRange"),
        ("qualification version", "qualificationVersion"),
    ] {
        assert_human_doctor_detail_matches_json(&human, &report, label, key);
    }
    assert_ne!(
        report["checks"][0]["details"]["supportedRange"],
        report["checks"][0]["details"]["qualificationVersion"]
    );
}

#[test]
fn doctor_retains_probed_identity_when_schema_admission_fails() {
    let fixture = CodexFixture::new("0.147.23", false, true);

    let output = doctor_json(fixture.path_directory(), &[]);

    assert_eq!(output.status.code(), Some(1));
    let report = report(&output);
    let check = &report["checks"][0];
    assert_eq!(check["code"], "unsupported_codex_capability");
    let canonical = fs::canonicalize(&fixture.executable).unwrap();
    assert_eq!(
        (
            check["details"]["version"].as_str(),
            check["details"]["profile"].as_str(),
            check["details"]["executablePath"].as_str(),
        ),
        (
            Some("0.147.23"),
            Some("CodexAppServerV1"),
            canonical.to_str(),
        )
    );
}

#[test]
fn doctor_rejects_out_of_range_decorated_and_schema_incompatible_codex() {
    for (version, schema_compatible, capability_success, expected_code, expected_probes) in [
        (
            "0.146.99",
            true,
            true,
            "unsupported_codex_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.150.0",
            true,
            true,
            "unsupported_codex_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.149.0-rc.1",
            true,
            true,
            "malformed_codex_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.149.0+vendor",
            true,
            true,
            "malformed_codex_version",
            b"--version\n".as_slice(),
        ),
        (
            "0.149.0",
            false,
            true,
            "unsupported_codex_capability",
            CLOSED_PROBES,
        ),
        (
            "0.149.0",
            true,
            false,
            "unexecutable_codex_installation",
            CLOSED_PROBES,
        ),
    ] {
        let fixture = CodexFixture::new(version, schema_compatible, capability_success);
        let output = doctor_json(fixture.path_directory(), &[]);

        assert_eq!(output.status.code(), Some(1), "{expected_code}");
        assert!(output.stderr.is_empty(), "{expected_code}");
        assert_eq!(report(&output)["checks"][0]["code"], expected_code);
        assert_eq!(
            fixture.recorded_probes(),
            expected_probes,
            "{expected_code}"
        );
    }
}
