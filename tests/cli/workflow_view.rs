#[cfg(target_os = "linux")]
use std::collections::BTreeMap;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::io::{Read as _, Write as _};
#[cfg(target_os = "linux")]
use std::net::TcpListener;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::process::{Output, Stdio};

#[cfg(target_os = "linux")]
use nix::pty::{Winsize, openpty};
#[cfg(target_os = "linux")]
use nix::sys::stat::Mode;
#[cfg(target_os = "linux")]
use nix::unistd::mkfifo;
#[cfg(target_os = "linux")]
use rustix::fd::OwnedFd;
#[cfg(target_os = "linux")]
use rustix::process::{Pid, Signal, kill_process};
#[cfg(target_os = "linux")]
use rustix::termios::Termios;

use super::workflow_run::isolated_command;
#[cfg(target_os = "linux")]
use super::workflow_run::{
    RunBundle, open_tui_pty, run, signal_bundle, spawn_tui_run, wait_for_process_poll,
};

fn view_args(run_directory: &Path, options: &[&str]) -> Vec<String> {
    let mut args = vec![
        "workflow".to_owned(),
        "view".to_owned(),
        "--run-dir".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ];
    args.extend(options.iter().map(|option| (*option).to_owned()));
    args
}

#[cfg(target_os = "linux")]
fn retry_args(run_directory: &Path, execution_root: &Path) -> Vec<String> {
    vec![
        "workflow".to_owned(),
        "retry".to_owned(),
        "--run-dir".to_owned(),
        run_directory.to_string_lossy().into_owned(),
        "--execution-root".to_owned(),
        execution_root.to_string_lossy().into_owned(),
        "--plain".to_owned(),
        "--color".to_owned(),
        "never".to_owned(),
    ]
}

#[cfg(target_os = "linux")]
fn status_args(run_directory: &Path) -> Vec<String> {
    vec![
        "workflow".to_owned(),
        "status".to_owned(),
        "--run-dir".to_owned(),
        run_directory.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ]
}

#[cfg(target_os = "linux")]
fn durable_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let mut children = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        children.sort();
        for child in children {
            if child.is_dir() {
                visit(root, &child, files);
            } else {
                files.insert(
                    child.strip_prefix(root).unwrap().to_owned(),
                    fs::read(child).unwrap(),
                );
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

#[cfg(target_os = "linux")]
fn successful_run(name: &str) -> (RunBundle, PathBuf) {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let run_directory = bundle.result(name);
    let produced = run(&bundle.args(&run_directory));
    assert!(
        produced.status.success(),
        "{}",
        String::from_utf8_lossy(&produced.stderr)
    );
    (bundle, run_directory)
}

#[test]
fn view_rejects_noninteractive_presentation_options() {
    let missing = tempfile::tempdir().unwrap().path().join("missing-run");
    for option in ["--plain", "--json"] {
        let output = isolated_command(&view_args(&missing, &[option]))
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument"));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn view_capability_gate_checks_each_required_terminal_capability_before_run_reads() {
    let missing = tempfile::tempdir().unwrap().path().join("missing-run");
    let args = view_args(&missing, &[]);
    for (stdin_terminal, stdout_terminal, term) in [
        (false, true, Some("xterm")),
        (true, false, Some("xterm")),
        (true, true, None),
        (true, true, Some("")),
        (true, true, Some("dumb")),
    ] {
        let output = run_with_terminal_arrangement(&args, stdin_terminal, stdout_terminal, term);

        assert_eq!(output.status.code(), Some(1));
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert!(diagnostic.contains("requires terminal stdin, terminal stdout, and a usable TERM"));
        assert!(!diagnostic.contains("run_directory_unavailable"));
    }
}

#[cfg(not(target_os = "linux"))]
#[test]
fn view_capability_gate_precedes_durable_run_reads() {
    let missing = tempfile::tempdir().unwrap().path().join("missing-run");
    let output = isolated_command(&view_args(&missing, &[]))
        .env("TERM", "xterm")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(diagnostic.contains("requires terminal stdin, terminal stdout, and a usable TERM"));
    assert!(!diagnostic.contains("run_directory_unavailable"));
}

#[cfg(target_os = "linux")]
#[test]
fn view_selects_current_and_historical_attempts_without_blocking_status_or_retry() {
    let argv = serde_json::to_string(&[
        "sh",
        "-c",
        "if test -f first-attempt-complete; then printf current-success; else printf historical-failure >&2; : > first-attempt-complete; exit 17; fi",
    ])
    .unwrap();
    let bundle = RunBundle::new(&format!(
        "schemaVersion: 1\nsteps:\n  execute:\n    kind: cmd\n    command:\n      argv: {argv}\n"
    ));
    let run_directory = bundle.result("history");
    let initial = run(&bundle.args(&run_directory));
    assert_eq!(initial.status.code(), Some(1));

    let failed_before = durable_files(&run_directory);
    let failed_view = TuiSession::start(&view_args(&run_directory, &[]));
    let (failed_status, failed_transcript) = failed_view.finish(b"q");
    assert!(failed_status.success());
    let failed_writes = terminal_writes(&failed_transcript);
    assert!(
        failed_writes.contains("attempt1of1·currentatsnapshot·initial"),
        "{failed_writes:?}"
    );
    assert!(
        failed_writes.contains("attemptstateworkflow_failed·outcomefailed"),
        "{failed_writes:?}"
    );
    assert_eq!(durable_files(&run_directory), failed_before);

    let retried = run(&retry_args(&run_directory, bundle.execution_root()));
    assert!(
        retried.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&retried.stdout),
        String::from_utf8_lossy(&retried.stderr)
    );

    let settled_before = durable_files(&run_directory);
    let current_view = TuiSession::start(&view_args(&run_directory, &[]));
    let status = isolated_command(&status_args(&run_directory))
        .output()
        .unwrap();
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["state"]["currentAttemptNumber"], 2);
    assert_eq!(status["state"]["attempts"][0]["state"], "workflow_failed");
    assert_eq!(status["state"]["attempts"][1]["state"], "succeeded");
    let (current_status, current_transcript) = current_view.finish(b"q");
    assert!(current_status.success());
    let current_writes = terminal_writes(&current_transcript);
    assert!(current_writes.contains("attempt2of2·currentatsnapshot"));
    assert!(current_writes.contains("attemptstatesucceeded·outcomesucceeded"));

    let historical_view = TuiSession::start(&view_args(
        &run_directory,
        &["--attempt", "1", "--color", "never"],
    ));
    let (historical_status, historical_transcript) = historical_view.finish(b"q");
    assert!(historical_status.success());
    let historical_writes = terminal_writes(&historical_transcript);
    assert!(historical_writes.contains("attempt1of2·historical·initial"));
    assert!(historical_writes.contains("attemptstateworkflow_failed·outcomefailed"));
    assert_eq!(durable_files(&run_directory), settled_before);
}

#[cfg(target_os = "linux")]
#[test]
fn cancelled_attempt_is_inspectable_and_ctrl_c_restores_the_terminal() {
    let bundle = signal_bundle();
    let run_directory = bundle.result("cancelled");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let child = isolated_command(&bundle.args(&run_directory))
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "signal-exit")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut event = [0_u8; 1];
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [1]);
    let owner = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(owner, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);
    let cancelled = child.wait_with_output().unwrap();
    assert_eq!(cancelled.status.code(), Some(130));

    let before = durable_files(&run_directory);
    let quit_view = TuiSession::start(&view_args(&run_directory, &[]));
    let (quit_status, quit_transcript) = quit_view.finish(b"q");
    assert!(quit_status.success());
    let quit_writes = terminal_writes(&quit_transcript);
    assert!(
        quit_writes.contains("attemptstatecancelled·outcomecancelled"),
        "{quit_writes:?}"
    );

    let interrupted_view = TuiSession::start(&view_args(&run_directory, &[]));
    let (interrupted_status, interrupted_transcript) = interrupted_view.finish(b"\x03");
    assert_eq!(interrupted_status.code(), Some(130));
    assert!(
        terminal_writes(&interrupted_transcript).contains("attemptstatecancelled·outcomecancelled")
    );
    assert_eq!(durable_files(&run_directory), before);
}

#[cfg(target_os = "linux")]
#[test]
fn view_reports_unavailable_attempts_and_results_before_terminal_ownership() {
    let (_bundle, run_directory) = successful_run("unavailable");

    let (unknown_status, unknown_transcript) =
        run_view_to_early_exit(&view_args(&run_directory, &["--attempt", "2"]));
    assert_eq!(unknown_status.code(), Some(1));
    assert!(unknown_transcript.contains("attempt 2 (attempt_unknown)"));
    assert!(!unknown_transcript.contains("\u{1b}[?1049h"));

    fs::remove_file(run_directory.join("attempts/000001/result/result.json")).unwrap();
    let (missing_status, missing_transcript) =
        run_view_to_early_exit(&view_args(&run_directory, &[]));
    assert_eq!(missing_status.code(), Some(1));
    assert!(missing_transcript.contains("published_result_unavailable"));
    assert!(!missing_transcript.contains("\u{1b}[?1049h"));
}

#[cfg(target_os = "linux")]
#[test]
fn signal_interrupts_a_blocked_archive_read_without_waiting_for_filesystem_completion() {
    let (_bundle, run_directory) = successful_run("blocked-read");
    let run_file = run_directory.join("run.json");
    let original_run = fs::read(&run_file).unwrap();
    fs::remove_file(&run_file).unwrap();
    mkfifo(&run_file, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();

    let (master, slave) = open_tui_pty();
    let original_mode = rustix::termios::tcgetattr(&slave).unwrap();
    let (mut child, master_writer, reader) =
        spawn_tui_run(&view_args(&run_directory, &[]), master, &slave);
    let process = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    let tasks = Path::new("/proc")
        .join(process.as_raw_pid().to_string())
        .join("task");
    let loader_started = (0..500).any(|_| {
        let started = fs::read_dir(&tasks)
            .ok()
            .is_some_and(|threads| threads.count() > 1);
        if !started {
            wait_for_process_poll();
        }
        started
    });
    assert!(
        loader_started,
        "archive loader did not start its filesystem worker"
    );

    kill_process(process, Signal::INT).unwrap();
    let status = wait_for_exit(&mut child, "signal during blocked archive read");
    assert_eq!(status.code(), Some(130));
    assert_terminal_mode(&slave, &original_mode);
    drop(slave);
    drop(master_writer);
    let transcript = String::from_utf8_lossy(&reader.join().unwrap()).into_owned();
    assert!(!transcript.contains("run_directory_invalid"));

    fs::remove_file(&run_file).unwrap();
    fs::write(&run_file, &original_run).unwrap();
    assert_eq!(fs::read(run_file).unwrap(), original_run);
}

#[cfg(target_os = "linux")]
#[test]
fn terminal_setup_failure_after_raw_mode_restores_input_and_preserves_the_archive() {
    let (_bundle, run_directory) = successful_run("terminal-failure");
    let before = durable_files(&run_directory);
    let size = Winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = openpty(Some(&size), None::<&nix::sys::termios::Termios>).unwrap();
    let original_mode = rustix::termios::tcgetattr(&pty.slave).unwrap();
    let child_input = rustix::io::dup(&pty.slave).unwrap();
    let child_output = rustix::io::dup(&pty.slave).unwrap();
    let output = isolated_command(&view_args(&run_directory, &[]))
        .env("TERM", "xterm")
        .stdin(Stdio::from(child_input))
        .stdout(Stdio::from(child_output))
        .stderr(Stdio::piped())
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("TerminalSetup (InvalidData)"));
    assert_terminal_mode(&pty.slave, &original_mode);
    assert_eq!(durable_files(&run_directory), before);
}

#[cfg(target_os = "linux")]
struct TuiSession {
    child: std::process::Child,
    master_writer: fs::File,
    reader: std::thread::JoinHandle<Vec<u8>>,
    slave: OwnedFd,
    original_mode: Termios,
}

#[cfg(target_os = "linux")]
impl TuiSession {
    fn start(args: &[String]) -> Self {
        let (master, slave) = open_tui_pty();
        let original_mode = rustix::termios::tcgetattr(&slave).unwrap();
        let (mut child, master_writer, reader) = spawn_tui_run(args, master, &slave);
        let setup_completed = (0..500)
            .find_map(|_| {
                let current = rustix::termios::tcgetattr(&slave).unwrap();
                if current.local_modes != original_mode.local_modes {
                    Some(true)
                } else if child.try_wait().unwrap().is_some() {
                    Some(false)
                } else {
                    wait_for_process_poll();
                    None
                }
            })
            .unwrap_or(false);
        if !setup_completed {
            let _ = child.kill();
            let _ = child.wait();
            drop(slave);
            drop(master_writer);
            let transcript = reader.join().unwrap();
            panic!(
                "workflow view did not take terminal ownership: {:?}",
                String::from_utf8_lossy(&transcript)
            );
        }
        Self {
            child,
            master_writer,
            reader,
            slave,
            original_mode,
        }
    }

    fn finish(mut self, input: &[u8]) -> (std::process::ExitStatus, String) {
        self.master_writer.write_all(input).unwrap();
        self.master_writer.flush().unwrap();
        let status = wait_for_exit(&mut self.child, "terminal input");
        assert_terminal_mode(&self.slave, &self.original_mode);
        drop(self.slave);
        drop(self.master_writer);
        let transcript = String::from_utf8_lossy(&self.reader.join().unwrap()).into_owned();
        assert!(transcript.contains("\u{1b}[?1049h"));
        let restored = transcript
            .rfind("\u{1b}[?1049l")
            .expect("workflow view must leave the alternate screen");
        assert!(!transcript[restored..].contains("── summary"));
        assert!(!transcript[restored..].contains("result succeeded · exit 0"));
        (status, transcript)
    }
}

#[cfg(target_os = "linux")]
fn wait_for_exit(child: &mut std::process::Child, action: &str) -> std::process::ExitStatus {
    (0..200)
        .find_map(|_| {
            let status = child.try_wait().unwrap();
            if status.is_none() {
                wait_for_process_poll();
            }
            status
        })
        .unwrap_or_else(|| {
            let _ = child.kill();
            let _ = child.wait();
            panic!("workflow view did not exit after {action}")
        })
}

#[cfg(target_os = "linux")]
fn assert_terminal_mode(slave: &OwnedFd, expected: &Termios) {
    let actual = rustix::termios::tcgetattr(slave).unwrap();
    assert_eq!(actual.input_modes, expected.input_modes);
    assert_eq!(actual.output_modes, expected.output_modes);
    assert_eq!(actual.control_modes, expected.control_modes);
    assert_eq!(actual.local_modes, expected.local_modes);
    assert_eq!(
        actual.special_codes[rustix::termios::SpecialCodeIndex::VMIN],
        expected.special_codes[rustix::termios::SpecialCodeIndex::VMIN]
    );
    assert_eq!(
        actual.special_codes[rustix::termios::SpecialCodeIndex::VTIME],
        expected.special_codes[rustix::termios::SpecialCodeIndex::VTIME]
    );
}

#[cfg(target_os = "linux")]
fn run_view_to_early_exit(args: &[String]) -> (std::process::ExitStatus, String) {
    let (master, slave) = open_tui_pty();
    let original_mode = rustix::termios::tcgetattr(&slave).unwrap();
    let (mut child, master_writer, reader) = spawn_tui_run(args, master, &slave);
    let status = wait_for_exit(&mut child, "archive load failure");
    assert_terminal_mode(&slave, &original_mode);
    drop(slave);
    drop(master_writer);
    let transcript = String::from_utf8_lossy(&reader.join().unwrap()).into_owned();
    (status, transcript)
}

#[cfg(target_os = "linux")]
fn terminal_writes(transcript: &str) -> String {
    let mut visible = String::new();
    let mut characters = transcript.chars();
    while let Some(character) = characters.next() {
        if character != '\u{1b}' {
            visible.push(character);
            continue;
        }
        if characters.next() != Some('[') {
            continue;
        }
        for parameter in characters.by_ref() {
            if ('@'..='~').contains(&parameter) {
                break;
            }
        }
    }
    visible
}

#[cfg(target_os = "linux")]
fn run_with_terminal_arrangement(
    args: &[String],
    stdin_terminal: bool,
    stdout_terminal: bool,
    term: Option<&str>,
) -> Output {
    let (_master, slave) = open_tui_pty();
    let mut command = isolated_command(args);
    command
        .stdin(if stdin_terminal {
            Stdio::from(rustix::io::dup(&slave).unwrap())
        } else {
            Stdio::null()
        })
        .stdout(if stdout_terminal {
            Stdio::from(rustix::io::dup(&slave).unwrap())
        } else {
            Stdio::piped()
        })
        .stderr(Stdio::piped());
    if let Some(term) = term {
        command.env("TERM", term);
    } else {
        command.env_remove("TERM");
    }
    command.output().unwrap()
}
