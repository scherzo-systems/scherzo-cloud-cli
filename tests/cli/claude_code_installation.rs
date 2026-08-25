use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use super::pi_installation::{COMPLETE_HELP as PI_COMPLETE_HELP, PiFixture, quote};
use super::{
    assert_human_doctor_detail_matches_json, private_credential_directory, run_with_env,
    write_runner_config,
};

const CLAUDE_CODE_CHECK_ID: &str = "execution.harness.claude-code-stream-json-v1";
const PI_CHECK_ID: &str = "execution.harness.pi-json-v1";
const CLOSED_PROBES: &[u8] = b"--version\n--help\n";
pub(super) const COMPLETE_HELP: &str = "Usage: claude [options] [command] [prompt]\nOptions:\n  -p, --print Print response\n  --input-format <format> Input format: stream-json\n  --output-format <format> Output format: stream-json\n  --verbose Verbose mode\n  --include-partial-messages Include chunks\n  --forward-subagent-text Forward text\n  --session-id <uuid> Use session\n  --permission-mode <mode> Permission mode\n  --setting-sources <sources> Setting sources\n  --model <model> Model\n  --effort <level> Effort\n  --bare Context via --append-system-prompt[-file]\n  --json-schema <schema> Schema\n";
const REQUIRED_CAPABILITIES: &str = "print_mode,stream_json_input,stream_json_output,verbose,partial_messages,forward_subagent_text,session_id,permission_mode,setting_sources,model,effort,append_system_prompt_file,json_schema";

pub(super) struct ClaudeCodeFixture {
    _directory: tempfile::TempDir,
    executable: PathBuf,
    probe_log: PathBuf,
}

impl ClaudeCodeFixture {
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
        let directory =
            tempfile::tempdir().expect("temporary Claude Code directory should be created");
        let executable_path = directory.path().join("claude");
        let probe_log = directory.path().join("probes.log");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(if executable { 0o755 } else { 0o644 })
            .open(&executable_path)
            .expect("fake Claude Code should be created");
        writeln!(file, "#!/bin/sh").unwrap();
        writeln!(
            file,
            "printf '%s\\n' \"$*\" >> {}",
            quote(probe_log.to_str().unwrap())
        )
        .unwrap();
        writeln!(file, "case \"$*\" in").unwrap();
        writeln!(file, "  --version) printf '%s\\n' {} ;;", quote(version)).unwrap();
        writeln!(
            file,
            "  --help) printf '%s' {}; {capability_hook} ;;",
            quote(help)
        )
        .unwrap();
        writeln!(file, "  *) {execution} ;;").unwrap();
        writeln!(file, "esac").unwrap();

        Self {
            _directory: directory,
            executable: executable_path,
            probe_log,
        }
    }

    pub(super) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(super) fn path_directory(&self) -> &Path {
        self.executable.parent().unwrap()
    }

    pub(super) fn recorded_probes(&self) -> Vec<u8> {
        fs::read(&self.probe_log).unwrap_or_default()
    }
}

fn doctor(
    path: &Path,
    checks: &[&str],
    environment: &[(&str, &str)],
    json: bool,
) -> std::process::Output {
    let path = path.to_str().expect("controlled PATH should be UTF-8");
    let mut args = vec!["runner", "doctor"];
    for check in checks {
        args.extend(["--check", check]);
    }
    if json {
        args.push("--json");
    }
    let mut environment = Vec::from(environment);
    environment.push(("PATH", path));
    run_with_env(&args, &environment)
}

fn doctor_json(path: &Path, checks: &[&str], environment: &[(&str, &str)]) -> std::process::Output {
    doctor(path, checks, environment, true)
}

fn doctor_human(
    path: &Path,
    checks: &[&str],
    environment: &[(&str, &str)],
) -> std::process::Output {
    doctor(path, checks, environment, false)
}

fn report(output: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

fn report_code(output: &std::process::Output) -> String {
    report(output)["checks"][0]["code"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[test]
fn runner_startup_discovers_claude_before_rejecting_invalid_operator_configuration() {
    let runner_directory = private_credential_directory();
    let config_path = write_runner_config(
        &runner_directory,
        "https://not-a-websocket.example.test/v1/runner/connect",
    );
    let fixture = ClaudeCodeFixture::new("2.1.241 (Claude Code)", COMPLETE_HELP, true);

    let output = run_with_env(
        &["runner", "serve", "--config", config_path.as_str()],
        &[("PATH", fixture.path_directory().to_str().unwrap())],
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(fixture.recorded_probes(), CLOSED_PROBES);
}

#[test]
fn doctor_reports_the_compatible_claude_code_snapshot_without_ambient_values() {
    let ambient_config = tempfile::tempdir().unwrap();
    let settings = ambient_config.path().join("settings.json");
    fs::write(&settings, br#"{"native":"settings-sentinel"}"#).unwrap();
    let capability_hook = format!(
        "test -z \"${{ANTHROPIC_API_KEY+x}}\"; test -z \"${{AWS_SECRET_ACCESS_KEY+x}}\"; test \"$CLAUDE_CONFIG_DIR\" != {}",
        quote(ambient_config.path().to_str().unwrap())
    );
    let fixture = ClaudeCodeFixture::with_execution_and_capability_hook(
        "2.1.241 (Claude Code)",
        COMPLETE_HELP,
        true,
        "exit 97",
        &capability_hook,
    );

    let output = doctor_json(
        fixture.path_directory(),
        &[CLAUDE_CODE_CHECK_ID],
        &[
            ("CLAUDE_CONFIG_DIR", ambient_config.path().to_str().unwrap()),
            ("ANTHROPIC_API_KEY", "unique-anthropic-sentinel"),
            ("AWS_SECRET_ACCESS_KEY", "unique-aws-sentinel"),
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
    assert_eq!(check["id"], CLAUDE_CODE_CHECK_ID);
    assert_eq!(check["status"], "pass");
    assert_eq!(check["details"]["version"], "2.1.241");
    assert_eq!(check["details"]["supportedRange"], ">=2.1.234 <2.2.0");
    assert_eq!(check["details"]["qualificationVersion"], "2.1.241");
    assert_eq!(check["details"]["profile"], "ClaudeCodeStreamJsonV1");
    assert_eq!(check["details"]["capabilities"], REQUIRED_CAPABILITIES);
    assert_eq!(
        Path::new(check["details"]["executablePath"].as_str().unwrap()),
        fs::canonicalize(fixture.executable()).unwrap()
    );
    for forbidden in [
        "unique-anthropic-sentinel",
        "unique-aws-sentinel",
        "settings-sentinel",
        ambient_config.path().to_str().unwrap(),
    ] {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(forbidden));
    }
    assert_eq!(
        fs::read(&settings).unwrap(),
        br#"{"native":"settings-sentinel"}"#
    );
    assert_eq!(fixture.recorded_probes(), CLOSED_PROBES);
}

#[test]
fn human_doctor_reports_claude_code_policy_when_missing() {
    let missing = tempfile::tempdir().expect("missing Claude Code directory should be created");
    let human = doctor_human(missing.path(), &[CLAUDE_CODE_CHECK_ID], &[]);
    let json = doctor_json(missing.path(), &[CLAUDE_CODE_CHECK_ID], &[]);
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
}

#[test]
fn doctor_rejects_missing_explicit_session_capability() {
    let help = COMPLETE_HELP.replace("  --session-id <uuid> Use session\n", "");
    let fixture = ClaudeCodeFixture::new("2.1.234 (Claude Code)", &help, true);

    let output = doctor_json(fixture.path_directory(), &[CLAUDE_CODE_CHECK_ID], &[]);

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(report_code(&output), "unsupported_claude_code_capability");
}

#[test]
fn doctor_reports_each_claude_code_installation_failure_without_fallback() {
    let missing = tempfile::tempdir().unwrap();
    let output = doctor_json(missing.path(), &[CLAUDE_CODE_CHECK_ID], &[]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(report_code(&output), "missing_claude_code_installation");

    let unexecutable_directory = tempfile::tempdir().unwrap();
    let unexecutable = unexecutable_directory.path().join("claude");
    fs::write(&unexecutable, "#!/definitely/missing/interpreter\n").unwrap();
    fs::set_permissions(&unexecutable, fs::Permissions::from_mode(0o755)).unwrap();
    let fallback = ClaudeCodeFixture::new("2.1.234 (Claude Code)", COMPLETE_HELP, true);
    let path =
        std::env::join_paths([unexecutable_directory.path(), fallback.path_directory()]).unwrap();
    let output = doctor_json(Path::new(&path), &[CLAUDE_CODE_CHECK_ID], &[]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        report_code(&output),
        "unexecutable_claude_code_installation"
    );
    assert!(fallback.recorded_probes().is_empty());

    let missing_schema_help = COMPLETE_HELP.replace("--json-schema", "--schema");
    for (version, help, expected_code, expected_probes) in [
        (
            "not-a-version",
            COMPLETE_HELP,
            "malformed_claude_code_version",
            b"--version\n".as_slice(),
        ),
        (
            "2.1.222 (Claude Code)",
            COMPLETE_HELP,
            "unsupported_claude_code_version",
            b"--version\n".as_slice(),
        ),
        (
            "2.1.233 (Claude Code)",
            COMPLETE_HELP,
            "unsupported_claude_code_version",
            b"--version\n".as_slice(),
        ),
        (
            "2.2.0 (Claude Code)",
            COMPLETE_HELP,
            "unsupported_claude_code_version",
            b"--version\n".as_slice(),
        ),
        (
            "2.1.234-rc.1 (Claude Code)",
            COMPLETE_HELP,
            "malformed_claude_code_version",
            b"--version\n".as_slice(),
        ),
        (
            "2.1.234 (Claude Code)",
            "not Claude help\n",
            "malformed_claude_code_capabilities",
            CLOSED_PROBES,
        ),
        (
            "2.1.234 (Claude Code)",
            missing_schema_help.as_str(),
            "unsupported_claude_code_capability",
            CLOSED_PROBES,
        ),
    ] {
        let fixture = ClaudeCodeFixture::new(version, help, true);
        let output = doctor_json(fixture.path_directory(), &[CLAUDE_CODE_CHECK_ID], &[]);
        assert_eq!(output.status.code(), Some(1), "{expected_code}");
        assert_eq!(report_code(&output), expected_code);
        let details = &report(&output)["checks"][0]["details"];
        assert_eq!(details["supportedRange"], ">=2.1.234 <2.2.0");
        assert_eq!(details["qualificationVersion"], "2.1.241");
        if expected_code == "unsupported_claude_code_version" {
            assert_eq!(
                details["version"],
                version.trim_end_matches(" (Claude Code)")
            );
        }
        assert_eq!(fixture.recorded_probes(), expected_probes);
    }

    let incompatible = ClaudeCodeFixture::new("2.2.0 (Claude Code)", COMPLETE_HELP, true);
    let compatible = ClaudeCodeFixture::new("2.1.234 (Claude Code)", COMPLETE_HELP, true);
    let ordered_path =
        std::env::join_paths([incompatible.path_directory(), compatible.path_directory()]).unwrap();
    let output = doctor_json(Path::new(&ordered_path), &[CLAUDE_CODE_CHECK_ID], &[]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(report_code(&output), "unsupported_claude_code_version");
    assert_eq!(incompatible.recorded_probes(), b"--version\n");
    assert!(compatible.recorded_probes().is_empty());
}

#[test]
fn doctor_reports_pi_and_claude_code_independently_for_every_installation_combination() {
    let pi = PiFixture::new("0.84.2", PI_COMPLETE_HELP, true);
    let claude = ClaudeCodeFixture::new("2.1.241 (Claude Code)", COMPLETE_HELP, true);
    let empty = tempfile::tempdir().unwrap();
    let pi_only = pi.path_directory();
    let claude_only = claude.path_directory();
    let both = std::env::join_paths([pi_only, claude_only]).unwrap();

    for (path, expected_statuses) in [
        (empty.path(), ["fail", "fail"]),
        (pi_only, ["pass", "fail"]),
        (claude_only, ["fail", "pass"]),
        (Path::new(&both), ["pass", "pass"]),
    ] {
        let output = doctor_json(path, &[PI_CHECK_ID, CLAUDE_CODE_CHECK_ID], &[]);
        let report = report(&output);
        let checks = report["checks"].as_array().unwrap();
        assert_eq!(checks[0]["id"], PI_CHECK_ID);
        assert_eq!(checks[1]["id"], CLAUDE_CODE_CHECK_ID);
        assert_eq!(checks[0]["status"], expected_statuses[0]);
        assert_eq!(checks[1]["status"], expected_statuses[1]);
        assert!(
            checks
                .iter()
                .all(|check| check.get("environment").is_none())
        );
    }
}
