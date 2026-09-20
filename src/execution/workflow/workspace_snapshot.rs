use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::os::fd::OwnedFd;
use std::path::{Component, Path};
use std::time::Duration;

use ring::digest::{Context, SHA256};
use rustix::fs::{AtFlags, FileType, Mode, OFlags, fstat, openat, readlinkat, statat};
use rustix::io::dup;
use serde::{Deserialize, Serialize};

use super::private_staging::open_directory_path;
use super::schema_common::{is_lowercase_hex, lowercase_hex, utc_timestamp};
use crate::process::{CommandProbeError, CommandRequest, CommandRunner, SystemCommandRunner};

pub(super) const WORKSPACE_SNAPSHOT_ALGORITHM: &str = "git_worktree_sha256_v1";
const SNAPSHOT_DOMAIN: &[u8] = b"scherzo-workspace-snapshot\0git_worktree_sha256_v1\0";
const GIT_TIMEOUT: Duration = Duration::from_secs(30);
const MAXIMUM_GIT_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_ENTRIES: usize = 1_000_000;
const MAXIMUM_CONTENT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const COPY_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WorkspaceSnapshotSettlementV1 {
    Engine,
    AbandonmentRecovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WorkspaceSnapshotUnavailableReasonV1 {
    GitUnavailable,
    GitOutputLimitExceeded,
    NotWorkTree,
    ExecutionRootNotWorkTreeRoot,
    EntryLimitExceeded,
    ContentLimitExceeded,
    Gitlink,
    UnsupportedEntry,
    IoUnavailable,
    MutationAroundRead,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WorkspaceSnapshotV1 {
    pub(super) algorithm: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) taken_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) unavailable: Option<WorkspaceSnapshotUnavailableReasonV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) settled_by: Option<WorkspaceSnapshotSettlementV1>,
}

impl WorkspaceSnapshotV1 {
    pub(super) fn validate(&self, settlement: bool) -> bool {
        let available = self
            .value
            .as_deref()
            .is_some_and(|value| is_lowercase_hex(value, 64))
            && self.taken_at.as_deref().is_some_and(|value| {
                super::schema_common::parse_canonical_utc_timestamp(value).is_some()
            })
            && self.unavailable.is_none();
        let unavailable =
            self.value.is_none() && self.taken_at.is_none() && self.unavailable.is_some();
        self.algorithm == WORKSPACE_SNAPSHOT_ALGORITHM
            && (available || unavailable)
            && (self.settled_by.is_some() == settlement)
    }

    fn unavailable(
        reason: WorkspaceSnapshotUnavailableReasonV1,
        settled_by: Option<WorkspaceSnapshotSettlementV1>,
    ) -> Self {
        Self {
            algorithm: WORKSPACE_SNAPSHOT_ALGORITHM.to_owned(),
            value: None,
            taken_at: None,
            unavailable: Some(reason),
            settled_by,
        }
    }
}

pub(super) fn capture_settlement_snapshot(
    execution_root: &Path,
    settled_by: WorkspaceSnapshotSettlementV1,
) -> WorkspaceSnapshotV1 {
    capture_with(
        execution_root,
        Some(settled_by),
        &SystemCommandRunner,
        &mut NoopSnapshotObserver,
    )
}

trait SnapshotObserver {
    fn enumerated(&mut self, _paths: &[Vec<u8>]) {}
    fn content_read(&mut self, _path: &[u8]) {}
}

struct NoopSnapshotObserver;

impl SnapshotObserver for NoopSnapshotObserver {}

fn capture_with(
    execution_root: &Path,
    settled_by: Option<WorkspaceSnapshotSettlementV1>,
    runner: &impl CommandRunner,
    observer: &mut impl SnapshotObserver,
) -> WorkspaceSnapshotV1 {
    let result = capture_digest(execution_root, runner, observer);
    match result {
        Ok(value) => {
            let taken_at = utc_timestamp(scherzo_cloud_support::utc_now()).ok();
            match taken_at {
                Some(taken_at) => WorkspaceSnapshotV1 {
                    algorithm: WORKSPACE_SNAPSHOT_ALGORITHM.to_owned(),
                    value: Some(value),
                    taken_at: Some(taken_at),
                    unavailable: None,
                    settled_by,
                },
                None => WorkspaceSnapshotV1::unavailable(
                    WorkspaceSnapshotUnavailableReasonV1::IoUnavailable,
                    settled_by,
                ),
            }
        }
        Err(reason) => WorkspaceSnapshotV1::unavailable(reason, settled_by),
    }
}

fn capture_digest(
    execution_root: &Path,
    runner: &impl CommandRunner,
    observer: &mut impl SnapshotObserver,
) -> Result<String, WorkspaceSnapshotUnavailableReasonV1> {
    let canonical_root = std::fs::canonicalize(execution_root)
        .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?;
    let repository = run_git(runner, &canonical_root, &["rev-parse", "--show-toplevel"])?;
    if !repository.success {
        return Err(WorkspaceSnapshotUnavailableReasonV1::NotWorkTree);
    }
    let repository_root = repository
        .stdout
        .strip_suffix(b"\n")
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .ok_or(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)?;
    let repository_root = std::fs::canonicalize(repository_root)
        .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)?;
    if repository_root != canonical_root {
        return Err(WorkspaceSnapshotUnavailableReasonV1::ExecutionRootNotWorkTreeRoot);
    }

    let first = observe_entries(runner, &canonical_root)?;
    observer.enumerated(&first.paths);

    let root = open_directory_path(&canonical_root)
        .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?;
    let first_digest = hash_entries(&root, &first.paths, &first.tracked, observer)?;
    let second = observe_entries(runner, &canonical_root)?;
    if first != second {
        return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead);
    }
    let second_digest = hash_entries(&root, &second.paths, &second.tracked, observer)?;
    let third = observe_entries(runner, &canonical_root)?;
    if second != third || first_digest != second_digest {
        return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead);
    }
    Ok(second_digest)
}

#[derive(Eq, PartialEq)]
struct EntryObservation {
    tracked: BTreeMap<Vec<u8>, Vec<u8>>,
    paths: Vec<Vec<u8>>,
}

fn observe_entries(
    runner: &impl CommandRunner,
    root: &Path,
) -> Result<EntryObservation, WorkspaceSnapshotUnavailableReasonV1> {
    let paths = enumerate_paths(runner, root)?;
    let staged = run_git(runner, root, &["ls-files", "--cached", "--stage", "-z"])?;
    if !staged.success {
        return Err(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable);
    }
    let tracked = parse_staged_entries(&staged.stdout)?;
    Ok(EntryObservation { tracked, paths })
}

fn hash_entries(
    root: &OwnedFd,
    paths: &[Vec<u8>],
    tracked: &BTreeMap<Vec<u8>, Vec<u8>>,
    observer: &mut impl SnapshotObserver,
) -> Result<String, WorkspaceSnapshotUnavailableReasonV1> {
    let mut digest = Context::new(&SHA256);
    digest.update(SNAPSHOT_DOMAIN);
    let mut content_bytes = 0_u64;
    for path in paths {
        hash_entry(
            &mut digest,
            root,
            path,
            tracked.contains_key(path),
            &mut content_bytes,
            observer,
        )?;
    }
    Ok(lowercase_hex(digest.finish().as_ref()))
}

fn run_git(
    runner: &impl CommandRunner,
    root: &Path,
    args: &[&str],
) -> Result<crate::process::CommandOutput, WorkspaceSnapshotUnavailableReasonV1> {
    let output = runner
        .run(CommandRequest {
            program: Path::new("git"),
            args,
            timeout: GIT_TIMEOUT,
            maximum_stdout_bytes: MAXIMUM_GIT_OUTPUT_BYTES,
            clear_environment: false,
            environment: &[],
            current_directory: Some(root),
        })
        .map_err(|failure| match failure {
            CommandProbeError::CommandNotFound
            | CommandProbeError::Spawn
            | CommandProbeError::Timeout
            | CommandProbeError::Wait
            | CommandProbeError::PipeRead => WorkspaceSnapshotUnavailableReasonV1::GitUnavailable,
        })?;
    if output.truncated {
        Err(WorkspaceSnapshotUnavailableReasonV1::GitOutputLimitExceeded)
    } else {
        Ok(output)
    }
}

fn parse_staged_entries(
    bytes: &[u8],
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, WorkspaceSnapshotUnavailableReasonV1> {
    let mut entries = BTreeMap::new();
    for entry in nul_entries(bytes)? {
        let separator = entry
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)?;
        let (facts, path_with_separator) = entry.split_at(separator);
        let path = path_with_separator
            .get(1..)
            .ok_or(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)?;
        let mut fields = facts.split(|byte| *byte == b' ');
        let mode = fields
            .next()
            .ok_or(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)?;
        let oid = fields
            .next()
            .ok_or(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)?;
        let stage = fields
            .next()
            .ok_or(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)?;
        if fields.next().is_some() || oid.is_empty() || stage != b"0" || mode == b"160000" {
            return Err(if mode == b"160000" {
                WorkspaceSnapshotUnavailableReasonV1::Gitlink
            } else {
                WorkspaceSnapshotUnavailableReasonV1::GitUnavailable
            });
        }
        validate_relative_path(path)?;
        if entries.insert(path.to_vec(), facts.to_vec()).is_some() {
            return Err(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable);
        }
    }
    Ok(entries)
}

fn enumerate_paths(
    runner: &impl CommandRunner,
    root: &Path,
) -> Result<Vec<Vec<u8>>, WorkspaceSnapshotUnavailableReasonV1> {
    let output = run_git(
        runner,
        root,
        &[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
    )?;
    if !output.success {
        return Err(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable);
    }
    let mut paths = BTreeSet::new();
    for path in nul_entries(&output.stdout)? {
        validate_relative_path(path)?;
        if !paths.insert(path.to_vec()) {
            return Err(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable);
        }
        if paths.len() > MAXIMUM_ENTRIES {
            return Err(WorkspaceSnapshotUnavailableReasonV1::EntryLimitExceeded);
        }
    }
    Ok(paths.into_iter().collect())
}

fn nul_entries(bytes: &[u8]) -> Result<Vec<&[u8]>, WorkspaceSnapshotUnavailableReasonV1> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if !bytes.ends_with(&[0]) {
        return Err(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable);
    }
    Ok(bytes[..bytes.len() - 1].split(|byte| *byte == 0).collect())
}

fn validate_relative_path(path: &[u8]) -> Result<(), WorkspaceSnapshotUnavailableReasonV1> {
    if path.is_empty()
        || path.starts_with(b"/")
        || path
            .split(|byte| *byte == b'/')
            .any(|component| component.is_empty() || component == b"." || component == b"..")
    {
        Err(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)
    } else {
        Ok(())
    }
}

fn hash_entry(
    digest: &mut Context,
    root: &OwnedFd,
    path: &[u8],
    tracked: bool,
    content_bytes: &mut u64,
    observer: &mut impl SnapshotObserver,
) -> Result<(), WorkspaceSnapshotUnavailableReasonV1> {
    let Some((parent, name)) = open_parent(root, path)? else {
        return hash_missing_entry(digest, path, tracked);
    };
    let before = match statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(metadata) => metadata,
        Err(rustix::io::Errno::NOENT) => return hash_missing_entry(digest, path, tracked),
        Err(_) => return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead),
    };
    let kind = FileType::from_raw_mode(before.st_mode);
    let mode = if kind == FileType::RegularFile {
        if before.st_mode & 0o111 == 0 {
            b"100644".as_slice()
        } else {
            b"100755".as_slice()
        }
    } else if kind == FileType::Symlink {
        b"120000".as_slice()
    } else {
        return Err(WorkspaceSnapshotUnavailableReasonV1::UnsupportedEntry);
    };
    hash_field(digest, path)?;
    hash_field(digest, mode)?;

    if kind == FileType::RegularFile {
        let descriptor = openat(
            &parent,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead)?;
        let opened =
            fstat(&descriptor).map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?;
        if !same_identity(&before, &opened) || opened.st_size < 0 {
            return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead);
        }
        let size = u64::try_from(opened.st_size)
            .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::ContentLimitExceeded)?;
        *content_bytes = content_bytes
            .checked_add(size)
            .filter(|total| *total <= MAXIMUM_CONTENT_BYTES)
            .ok_or(WorkspaceSnapshotUnavailableReasonV1::ContentLimitExceeded)?;
        digest.update(&size.to_be_bytes());
        let mut file = File::from(descriptor);
        let mut remaining = size;
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        while remaining != 0 {
            let maximum = usize::try_from(remaining.min(COPY_BUFFER_BYTES as u64))
                .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?;
            let read = file
                .read(&mut buffer[..maximum])
                .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?;
            if read == 0 {
                return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead);
            }
            digest.update(&buffer[..read]);
            remaining -= u64::try_from(read)
                .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?;
        }
        let mut extra = [0_u8; 1];
        if file
            .read(&mut extra)
            .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?
            != 0
        {
            return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead);
        }
        observer.content_read(path);
        let after =
            fstat(&file).map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?;
        let named = statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead)?;
        if !stable_metadata(&before, &after) || !same_identity(&before, &named) {
            return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead);
        }
    } else {
        let target = readlinkat(&parent, name, Vec::new())
            .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead)?;
        let target = target.as_bytes();
        let size = u64::try_from(target.len())
            .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::ContentLimitExceeded)?;
        *content_bytes = content_bytes
            .checked_add(size)
            .filter(|total| *total <= MAXIMUM_CONTENT_BYTES)
            .ok_or(WorkspaceSnapshotUnavailableReasonV1::ContentLimitExceeded)?;
        hash_field(digest, target)?;
        observer.content_read(path);
        let after = statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead)?;
        if !stable_metadata(&before, &after) {
            return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead);
        }
    }
    Ok(())
}

fn hash_missing_entry(
    digest: &mut Context,
    path: &[u8],
    tracked: bool,
) -> Result<(), WorkspaceSnapshotUnavailableReasonV1> {
    if !tracked {
        return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead);
    }
    hash_field(digest, path)?;
    hash_field(digest, b"000000")?;
    hash_field(digest, b"")
}

fn open_parent<'a>(
    root: &OwnedFd,
    path: &'a [u8],
) -> Result<Option<(OwnedFd, &'a OsStr)>, WorkspaceSnapshotUnavailableReasonV1> {
    use std::os::unix::ffi::OsStrExt as _;

    let path = Path::new(OsStr::from_bytes(path));
    let mut components = path.components().peekable();
    let mut parent = dup(root).map_err(|_| WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)?;
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable);
        };
        if components.peek().is_none() {
            return Ok(Some((parent, name)));
        }
        parent = match openat(
            &parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(parent) => parent,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(_) => return Err(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead),
        };
    }
    Err(WorkspaceSnapshotUnavailableReasonV1::GitUnavailable)
}

fn hash_field(
    digest: &mut Context,
    bytes: &[u8],
) -> Result<(), WorkspaceSnapshotUnavailableReasonV1> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| WorkspaceSnapshotUnavailableReasonV1::ContentLimitExceeded)?;
    digest.update(&length.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

fn same_identity(left: &rustix::fs::Stat, right: &rustix::fs::Stat) -> bool {
    left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && left.st_mode == right.st_mode
        && left.st_size == right.st_size
}

fn stable_metadata(left: &rustix::fs::Stat, right: &rustix::fs::Stat) -> bool {
    same_identity(left, right)
        && left.st_mtime == right.st_mtime
        && left.st_mtime_nsec == right.st_mtime_nsec
        && left.st_ctime == right.st_ctime
        && left.st_ctime_nsec == right.st_ctime_nsec
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::ffi::OsStringExt as _;
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    use std::process::Command;

    use super::*;

    fn git(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(root)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn repository() -> tempfile::TempDir {
        let temporary = tempfile::tempdir().unwrap();
        git(temporary.path(), &["init", "--quiet"]);
        git(
            temporary.path(),
            &["config", "user.name", "Snapshot Fixture"],
        );
        git(
            temporary.path(),
            &["config", "user.email", "snapshot@example.invalid"],
        );
        fs::write(temporary.path().join("tracked"), b"initial\n").unwrap();
        fs::write(temporary.path().join(".gitignore"), b"*.ignored\n").unwrap();
        git(temporary.path(), &["add", "tracked", ".gitignore"]);
        git(temporary.path(), &["commit", "--quiet", "-m", "initial"]);
        temporary
    }

    fn head_oid(root: &Path) -> String {
        let oid = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(root)
            .output()
            .unwrap()
            .stdout;
        std::str::from_utf8(&oid).unwrap().trim().to_owned()
    }

    fn available(snapshot: &WorkspaceSnapshotV1) -> &str {
        snapshot.value.as_deref().unwrap()
    }

    #[test]
    fn snapshot_distinguishes_content_modes_untracked_and_symlinks() {
        let repository = repository();
        let root = repository.path();
        let initial = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        assert!(initial.validate(false));

        fs::write(root.join("transient.ignored"), b"excluded\n").unwrap();
        let ignored = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        assert_eq!(available(&initial), available(&ignored));

        fs::create_dir(root.join("nested")).unwrap();
        fs::write(root.join("nested/untracked"), b"nested\n").unwrap();
        let nested = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        assert_ne!(available(&ignored), available(&nested));
        fs::remove_dir_all(root.join("nested")).unwrap();

        fs::write(root.join("tracked"), b"changed\n").unwrap();
        let changed = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        assert_ne!(available(&initial), available(&changed));

        fs::write(root.join("tracked"), b"initial\n").unwrap();
        let mut permissions = fs::metadata(root.join("tracked")).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(root.join("tracked"), permissions).unwrap();
        let executable = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        assert_ne!(available(&initial), available(&executable));

        fs::remove_file(root.join("tracked")).unwrap();
        symlink("target-a", root.join("tracked")).unwrap();
        let symlink_a = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        fs::remove_file(root.join("tracked")).unwrap();
        symlink("target-b", root.join("tracked")).unwrap();
        let symlink_b = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        assert_ne!(available(&symlink_a), available(&symlink_b));
        fs::remove_file(root.join("tracked")).unwrap();
        symlink(
            std::ffi::OsString::from_vec(vec![b't', 0xff]),
            root.join("tracked"),
        )
        .unwrap();
        let raw_symlink = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        assert_ne!(available(&symlink_b), available(&raw_symlink));

        fs::write(root.join("untracked"), b"extra\n").unwrap();
        let untracked = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);
        assert_ne!(available(&raw_symlink), available(&untracked));
    }

    struct MutateAfterRead {
        root: std::path::PathBuf,
        mutated: bool,
    }

    impl SnapshotObserver for MutateAfterRead {
        fn content_read(&mut self, path: &[u8]) {
            if !self.mutated && path == b"tracked" {
                fs::write(self.root.join("tracked"), b"mutated\n").unwrap();
                self.mutated = true;
            }
        }
    }

    #[test]
    fn snapshot_reports_mutation_around_read_without_using_time_for_synchronization() {
        let repository = repository();
        let mut observer = MutateAfterRead {
            root: repository.path().to_owned(),
            mutated: false,
        };
        let snapshot = capture_with(repository.path(), None, &SystemCommandRunner, &mut observer);
        assert_eq!(
            snapshot.unavailable,
            Some(WorkspaceSnapshotUnavailableReasonV1::MutationAroundRead)
        );
    }

    #[test]
    fn snapshot_records_stable_deletion_when_a_tracked_parent_is_absent() {
        let repository = repository();
        let root = repository.path();
        fs::create_dir(root.join("nested")).unwrap();
        fs::write(root.join("nested/tracked"), b"nested\n").unwrap();
        git(root, &["add", "nested/tracked"]);
        let present = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);

        fs::remove_dir_all(root.join("nested")).unwrap();
        let deleted = capture_with(root, None, &SystemCommandRunner, &mut NoopSnapshotObserver);

        assert!(deleted.validate(false));
        assert_ne!(available(&present), available(&deleted));
    }

    struct ReplaceIndexEntryWithGitlink {
        root: std::path::PathBuf,
        oid: String,
        mutated: bool,
    }

    impl SnapshotObserver for ReplaceIndexEntryWithGitlink {
        fn enumerated(&mut self, _paths: &[Vec<u8>]) {
            if !self.mutated {
                git(
                    &self.root,
                    &[
                        "update-index",
                        "--add",
                        "--cacheinfo",
                        "160000",
                        &self.oid,
                        "tracked",
                    ],
                );
                self.mutated = true;
            }
        }
    }

    #[test]
    fn snapshot_rechecks_staged_facts_after_enumeration() {
        let repository = repository();
        let mut observer = ReplaceIndexEntryWithGitlink {
            root: repository.path().to_owned(),
            oid: head_oid(repository.path()),
            mutated: false,
        };

        let snapshot = capture_with(repository.path(), None, &SystemCommandRunner, &mut observer);

        assert_eq!(
            snapshot.unavailable,
            Some(WorkspaceSnapshotUnavailableReasonV1::Gitlink)
        );
    }

    #[test]
    fn unavailable_snapshot_retains_typed_reason_and_settlement_provenance() {
        let snapshot = capture_settlement_snapshot(
            Path::new("/snapshot-fixture-does-not-exist"),
            WorkspaceSnapshotSettlementV1::AbandonmentRecovery,
        );
        assert_eq!(
            snapshot.unavailable,
            Some(WorkspaceSnapshotUnavailableReasonV1::IoUnavailable)
        );
        assert_eq!(
            snapshot.settled_by,
            Some(WorkspaceSnapshotSettlementV1::AbandonmentRecovery)
        );
        assert!(snapshot.validate(true));
    }

    #[test]
    fn snapshot_rejects_gitlinks() {
        let repository = repository();
        let oid = head_oid(repository.path());
        git(
            repository.path(),
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                "160000",
                &oid,
                "vendor",
            ],
        );
        let snapshot = capture_with(
            repository.path(),
            Some(WorkspaceSnapshotSettlementV1::Engine),
            &SystemCommandRunner,
            &mut NoopSnapshotObserver,
        );
        assert_eq!(
            snapshot.unavailable,
            Some(WorkspaceSnapshotUnavailableReasonV1::Gitlink)
        );
        assert!(snapshot.validate(true));
    }
}
