use std::ffi::OsStr;
use std::process::{Command, Stdio};

pub(crate) fn fixture_git_command(program: impl AsRef<OsStr>) -> Command {
    let path = std::env::var_os("PATH");
    let mut command = Command::new(program);
    command
        .args([
            "--no-pager",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgSign=false",
            "-c",
            "tag.gpgSign=false",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
        ])
        .env_clear()
        .env("HOME", "/nonexistent")
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_PAGER", "cat")
        .env("GIT_EDITOR", "true")
        .env("GIT_SEQUENCE_EDITOR", "true")
        .env("PAGER", "cat")
        .env("EDITOR", "true")
        .stdin(Stdio::null());
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command
}
