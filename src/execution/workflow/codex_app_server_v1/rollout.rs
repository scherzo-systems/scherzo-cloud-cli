use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufRead as _, BufReader, Read as _, Seek as _, SeekFrom};
use std::os::fd::OwnedFd;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, fchmod, fstat, mkdirat, openat, statat, unlinkat,
};
use rustix::io::{Errno, dup};
use serde_json::Value;

use crate::execution::workflow::agent::{AgentFailureCause, AgentHarnessSetupStage};
use crate::execution::workflow::agent_diagnostics::{
    AgentDiagnosticSession, CodexRolloutRejectionReason,
};
use crate::execution::workflow::private_staging::{open_directory_path, same_file};

const MAXIMUM_SESSION_META_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_RETAINED_ROLLOUT_BYTES: u64 = 64 * 1024 * 1024;
const SESSIONS_DIRECTORY: &str = "sessions";

#[derive(Clone, Debug)]
pub(super) struct CodexRolloutCorrelation {
    pub(super) thread_id: Arc<str>,
    pub(super) reported_path: PathBuf,
    pub(super) materialized: bool,
}

#[derive(Debug)]
pub(super) struct CodexRolloutBridge {
    codex_home_path: PathBuf,
    codex_home: OwnedFd,
    sqlite_home_path: PathBuf,
    sqlite_home: OwnedFd,
}

impl CodexRolloutBridge {
    pub(super) fn prepare(
        environment: &std::collections::BTreeMap<OsString, OsString>,
        sqlite_home_path: &Path,
    ) -> Result<Self, AgentFailureCause> {
        let codex_home_path = environment
            .get(OsStr::new("CODEX_HOME"))
            .map(PathBuf::from)
            .or_else(|| {
                environment
                    .get(OsStr::new("HOME"))
                    .map(PathBuf::from)
                    .map(|home| home.join(".codex"))
            })
            .ok_or_else(setup_failed)?;
        let codex_home =
            validated_or_created_codex_home(&codex_home_path).map_err(|_| setup_failed())?;
        let sqlite_home_path =
            std::fs::canonicalize(sqlite_home_path).map_err(|_| setup_failed())?;
        let sqlite_home = validated_directory(&sqlite_home_path).map_err(|_| setup_failed())?;
        Ok(Self {
            codex_home_path,
            codex_home,
            sqlite_home_path,
            sqlite_home,
        })
    }

    pub(super) fn codex_home_path(&self) -> &Path {
        &self.codex_home_path
    }

    pub(super) fn sqlite_home_path(&self) -> &Path {
        &self.sqlite_home_path
    }

    pub(super) fn sqlite_home(&self) -> &OwnedFd {
        &self.sqlite_home
    }

    pub(super) fn verify_bindings(&self) -> Result<(), AgentFailureCause> {
        verify_binding(&self.codex_home_path, &self.codex_home).map_err(|_| setup_failed())?;
        verify_binding(&self.sqlite_home_path, &self.sqlite_home).map_err(|_| setup_failed())
    }

    pub(super) fn retain(
        &self,
        diagnostic_session: &AgentDiagnosticSession,
        correlation: &CodexRolloutCorrelation,
    ) -> Result<(), CodexRolloutRejectionReason> {
        self.retain_with_operations(
            diagnostic_session,
            correlation,
            MAXIMUM_RETAINED_ROLLOUT_BYTES,
            || {},
            sync_directory,
        )
    }

    fn retain_with_hook(
        &self,
        diagnostic_session: &AgentDiagnosticSession,
        correlation: &CodexRolloutCorrelation,
        after_open: impl FnOnce(),
    ) -> Result<(), CodexRolloutRejectionReason> {
        self.retain_with_operations(
            diagnostic_session,
            correlation,
            MAXIMUM_RETAINED_ROLLOUT_BYTES,
            after_open,
            sync_directory,
        )
    }

    fn retain_with_operations(
        &self,
        diagnostic_session: &AgentDiagnosticSession,
        correlation: &CodexRolloutCorrelation,
        maximum_bytes: u64,
        after_open: impl FnOnce(),
        sync_source_parent: impl FnOnce(&OwnedFd) -> Result<(), ()>,
    ) -> Result<(), CodexRolloutRejectionReason> {
        verify_binding(&self.codex_home_path, &self.codex_home)
            .map_err(|_| CodexRolloutRejectionReason::StateBoundaryUnavailable)?;
        let source = match self.open_rollout(correlation) {
            Ok(source) => source,
            Err(CodexRolloutRejectionReason::RolloutMissing) if !correlation.materialized => {
                return Ok(());
            }
            Err(reason) => return Err(reason),
        };
        validate_rollout_identity(&source.file, &correlation.thread_id)?;
        after_open();

        let mut reader = source
            .file
            .try_clone()
            .map_err(|_| CodexRolloutRejectionReason::RetainedStorageUnavailable)?;
        reader
            .seek(SeekFrom::Start(0))
            .map_err(|_| CodexRolloutRejectionReason::RetainedStorageUnavailable)?;
        let (retained, complete) = diagnostic_session
            .write_codex_rollout_from(&mut reader, maximum_bytes)
            .map_err(|_| CodexRolloutRejectionReason::RetainedStorageUnavailable)?;

        self.verify_opened_rollout_path(&source)?;
        unlinkat(&source.parent, &source.name, AtFlags::empty())
            .map_err(|_| CodexRolloutRejectionReason::AmbientRemovalFailed)?;
        sync_source_parent(&source.parent)
            .map_err(|_| CodexRolloutRejectionReason::AmbientRemovalFailed)?;
        retained.keep();
        if complete {
            Ok(())
        } else {
            Err(CodexRolloutRejectionReason::RolloutTooLarge)
        }
    }

    fn open_rollout(
        &self,
        correlation: &CodexRolloutCorrelation,
    ) -> Result<OpenedRollout, CodexRolloutRejectionReason> {
        let relative = correlation
            .reported_path
            .strip_prefix(&self.codex_home_path)
            .map_err(|_| CodexRolloutRejectionReason::PathOutsideStateBoundary)?;
        let components = relative
            .components()
            .map(|component| match component {
                Component::Normal(component) => Ok(component.to_owned()),
                _ => Err(CodexRolloutRejectionReason::PathShapeInvalid),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let [sessions, year, month, day, name] = components.as_slice() else {
            return Err(CodexRolloutRejectionReason::PathShapeInvalid);
        };
        if sessions != OsStr::new(SESSIONS_DIRECTORY)
            || !is_ascii_digits(year, 4)
            || !is_ascii_digits(month, 2)
            || !is_ascii_digits(day, 2)
            || !rollout_name_matches(name, &correlation.thread_id)
        {
            return Err(CodexRolloutRejectionReason::PathShapeInvalid);
        }

        let parent_components = [
            sessions.to_owned(),
            year.to_owned(),
            month.to_owned(),
            day.to_owned(),
        ];
        let parent = self.open_rollout_parent(&parent_components)?;
        let named = match statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(metadata) => metadata,
            Err(Errno::NOENT) => return Err(CodexRolloutRejectionReason::RolloutMissing),
            Err(_) => return Err(CodexRolloutRejectionReason::PathComponentInvalid),
        };
        if FileType::from_raw_mode(named.st_mode) != FileType::RegularFile {
            return Err(CodexRolloutRejectionReason::UnexpectedFileKind);
        }
        let descriptor = openat(
            &parent,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| CodexRolloutRejectionReason::PathReplaced)?;
        let opened = fstat(&descriptor).map_err(|_| CodexRolloutRejectionReason::PathReplaced)?;
        if opened.st_dev != named.st_dev || opened.st_ino != named.st_ino {
            return Err(CodexRolloutRejectionReason::PathReplaced);
        }
        Ok(OpenedRollout {
            file: File::from(descriptor),
            parent,
            parent_components,
            name: name.to_owned(),
            device: opened.st_dev,
            inode: opened.st_ino,
        })
    }

    fn open_rollout_parent(
        &self,
        components: &[OsString; 4],
    ) -> Result<OwnedFd, CodexRolloutRejectionReason> {
        let mut parent = dup(&self.codex_home)
            .map_err(|_| CodexRolloutRejectionReason::StateBoundaryUnavailable)?;
        for component in components {
            parent = openat(
                &parent,
                component,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| CodexRolloutRejectionReason::PathComponentInvalid)?;
        }
        Ok(parent)
    }

    fn verify_opened_rollout_path(
        &self,
        source: &OpenedRollout,
    ) -> Result<(), CodexRolloutRejectionReason> {
        verify_binding(&self.codex_home_path, &self.codex_home)
            .map_err(|_| CodexRolloutRejectionReason::PathReplaced)?;
        let reopened = self
            .open_rollout_parent(&source.parent_components)
            .map_err(|_| CodexRolloutRejectionReason::PathReplaced)?;
        if !same_file(&source.parent, &reopened)
            .map_err(|_| CodexRolloutRejectionReason::PathReplaced)?
        {
            return Err(CodexRolloutRejectionReason::PathReplaced);
        }
        let named = statat(&reopened, &source.name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| CodexRolloutRejectionReason::PathReplaced)?;
        if FileType::from_raw_mode(named.st_mode) != FileType::RegularFile
            || named.st_dev != source.device
            || named.st_ino != source.inode
        {
            return Err(CodexRolloutRejectionReason::PathReplaced);
        }
        Ok(())
    }
}

struct OpenedRollout {
    file: File,
    parent: OwnedFd,
    parent_components: [OsString; 4],
    name: OsString,
    device: libc::dev_t,
    inode: libc::ino_t,
}

fn validated_or_created_codex_home(path: &Path) -> Result<OwnedFd, ()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => validated_directory(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent_path = path.parent().ok_or(())?;
            let name = path.file_name().ok_or(())?;
            let parent = validated_directory(parent_path)?;
            match mkdirat(&parent, name, Mode::RWXU) {
                Ok(()) | Err(Errno::EXIST) => {}
                Err(_) => return Err(()),
            }
            let directory = openat(
                &parent,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| ())?;
            fchmod(&directory, Mode::RWXU).map_err(|_| ())?;
            sync_directory(&directory)?;
            sync_directory(&parent)?;
            verify_binding(path, &directory)?;
            Ok(directory)
        }
        Err(_) => Err(()),
    }
}

fn validated_directory(path: &Path) -> Result<OwnedFd, ()> {
    if !path.is_absolute() || std::fs::canonicalize(path).ok().as_deref() != Some(path) {
        return Err(());
    }
    open_directory_path(path).map_err(|_| ())
}

fn verify_binding(path: &Path, retained: &OwnedFd) -> Result<(), ()> {
    let reopened = validated_directory(path)?;
    same_file(retained, &reopened)
        .map_err(|_| ())?
        .then_some(())
        .ok_or(())
}

fn validate_rollout_identity(
    source: &File,
    thread_id: &str,
) -> Result<(), CodexRolloutRejectionReason> {
    let reader = source
        .try_clone()
        .map_err(|_| CodexRolloutRejectionReason::ThreadIdentityMismatch)?;
    let mut reader = BufReader::new(reader).take(MAXIMUM_SESSION_META_BYTES + 1);
    let mut first_record = Vec::new();
    reader
        .read_until(b'\n', &mut first_record)
        .map_err(|_| CodexRolloutRejectionReason::ThreadIdentityMismatch)?;
    if first_record.is_empty()
        || u64::try_from(first_record.len()).unwrap_or(u64::MAX) > MAXIMUM_SESSION_META_BYTES
    {
        return Err(CodexRolloutRejectionReason::ThreadIdentityMismatch);
    }
    let record = serde_json::from_slice::<Value>(&first_record)
        .map_err(|_| CodexRolloutRejectionReason::ThreadIdentityMismatch)?;
    if record.get("type").and_then(Value::as_str) != Some("session_meta")
        || record.pointer("/payload/id").and_then(Value::as_str) != Some(thread_id)
        || record
            .pointer("/payload/session_id")
            .and_then(Value::as_str)
            != Some(thread_id)
    {
        return Err(CodexRolloutRejectionReason::ThreadIdentityMismatch);
    }
    Ok(())
}

fn is_ascii_digits(component: &OsStr, length: usize) -> bool {
    component.to_str().is_some_and(|value| {
        value.len() == length && value.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn rollout_name_matches(name: &OsStr, thread_id: &str) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    name.starts_with("rollout-") && name.ends_with(&format!("-{thread_id}.jsonl"))
}

pub(super) fn is_codex_thread_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase(),
        })
        && bytes[14] == b'7'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
}

fn sync_directory(directory: &OwnedFd) -> Result<(), ()> {
    let duplicate = dup(directory).map_err(|_| ())?;
    File::from(duplicate).sync_all().map_err(|_| ())
}

fn setup_failed() -> AgentFailureCause {
    AgentFailureCause::HarnessSetupFailed {
        stage: AgentHarnessSetupStage::ExecutableLaunch,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::os::unix::fs::symlink;

    use serde_json::json;

    use super::*;

    const THREAD_ID: &str = "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e12";

    struct Fixture {
        _temporary: tempfile::TempDir,
        root: PathBuf,
        bridge: CodexRolloutBridge,
        diagnostics: AgentDiagnosticSession,
        source: PathBuf,
        retained: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let root = std::fs::canonicalize(temporary.path()).unwrap();
            let codex_home = root.join("codex-home");
            let sqlite_home = root.join("sqlite-home");
            let diagnostic_session = root.join("diagnostics/session");
            std::fs::create_dir_all(&codex_home).unwrap();
            std::fs::create_dir_all(&sqlite_home).unwrap();
            let source = codex_home.join(format!(
                "sessions/2026/08/18/rollout-2026-08-18T00-00-00-{THREAD_ID}.jsonl"
            ));
            std::fs::create_dir_all(source.parent().unwrap()).unwrap();
            let diagnostics = AgentDiagnosticSession::codex_fixture(diagnostic_session.clone());
            let bridge = CodexRolloutBridge::prepare(
                &std::collections::BTreeMap::from([
                    (OsString::from("CODEX_HOME"), codex_home.into_os_string()),
                    (OsString::from("HOME"), root.join("home").into_os_string()),
                ]),
                &sqlite_home,
            )
            .unwrap();
            Self {
                _temporary: temporary,
                root,
                bridge,
                diagnostics,
                source,
                retained: diagnostic_session.join("rollout.jsonl"),
            }
        }

        fn correlation(&self) -> CodexRolloutCorrelation {
            CodexRolloutCorrelation {
                thread_id: Arc::from(THREAD_ID),
                reported_path: self.source.clone(),
                materialized: true,
            }
        }

        fn write_rollout(&self, thread_id: &str) {
            let first = json!({
                "timestamp": "2026-08-18T00:00:00Z",
                "type": "session_meta",
                "payload": {"id": thread_id, "session_id": thread_id},
            });
            std::fs::write(
                &self.source,
                format!("{}\n{{partial", serde_json::to_string(&first).unwrap()),
            )
            .unwrap();
        }
    }

    #[test]
    fn creates_only_a_missing_default_codex_home_under_a_validated_home() {
        let temporary = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temporary.path()).unwrap();
        let home = root.join("home");
        let sqlite_home = root.join("sqlite-home");
        std::fs::create_dir(&home).unwrap();
        std::fs::create_dir(&sqlite_home).unwrap();

        let bridge = CodexRolloutBridge::prepare(
            &std::collections::BTreeMap::from([(OsString::from("HOME"), home.into_os_string())]),
            &sqlite_home,
        )
        .unwrap();

        assert_eq!(bridge.codex_home_path(), root.join("home/.codex"));
        bridge.verify_bindings().unwrap();
    }

    #[test]
    fn securely_retains_the_correlated_rollout_and_removes_ambient_history() {
        let fixture = Fixture::new();
        fixture.write_rollout(THREAD_ID);

        fixture
            .bridge
            .retain(&fixture.diagnostics, &fixture.correlation())
            .unwrap();

        assert!(!fixture.source.exists());
        assert!(fixture.retained.is_file());
        assert!(
            std::fs::read(&fixture.retained)
                .unwrap()
                .ends_with(b"{partial")
        );
    }

    #[test]
    fn rejects_outside_symlink_and_cross_thread_sources_without_importing_them() {
        for scenario in ["outside", "symlink", "identity"] {
            let fixture = Fixture::new();
            fixture.write_rollout(if scenario == "identity" {
                "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e13"
            } else {
                THREAD_ID
            });
            let mut correlation = fixture.correlation();
            let expected = match scenario {
                "outside" => {
                    correlation.reported_path = fixture.root.join("outside.jsonl");
                    CodexRolloutRejectionReason::PathOutsideStateBoundary
                }
                "symlink" => {
                    let target = fixture.root.join("attacker.jsonl");
                    std::fs::write(&target, std::fs::read(&fixture.source).unwrap()).unwrap();
                    std::fs::remove_file(&fixture.source).unwrap();
                    symlink(target, &fixture.source).unwrap();
                    CodexRolloutRejectionReason::UnexpectedFileKind
                }
                "identity" => CodexRolloutRejectionReason::ThreadIdentityMismatch,
                _ => unreachable!(),
            };

            assert!(matches!(
                fixture.bridge.retain(&fixture.diagnostics, &correlation),
                Err(reason) if std::mem::discriminant(&reason) == std::mem::discriminant(&expected)
            ));
            assert!(!fixture.retained.exists());
        }
    }

    fn assert_replacement_rejected(
        fixture: &Fixture,
        replacement: &Path,
        result: Result<(), CodexRolloutRejectionReason>,
    ) {
        assert!(matches!(
            result,
            Err(CodexRolloutRejectionReason::PathReplaced)
        ));
        assert!(!fixture.retained.exists());
        assert_eq!(std::fs::read(replacement).unwrap(), b"attacker bytes\n");
    }

    #[test]
    fn detects_source_path_replacement_before_ambient_cleanup() {
        let fixture = Fixture::new();
        fixture.write_rollout(THREAD_ID);
        let replacement = fixture.source.clone();

        let result =
            fixture
                .bridge
                .retain_with_hook(&fixture.diagnostics, &fixture.correlation(), || {
                    std::fs::remove_file(&replacement).unwrap();
                    std::fs::write(&replacement, b"attacker bytes\n").unwrap();
                });

        assert_replacement_rejected(&fixture, &fixture.source, result);
    }

    #[test]
    fn detects_source_parent_replacement_before_ambient_cleanup() {
        let fixture = Fixture::new();
        fixture.write_rollout(THREAD_ID);
        let source_parent = fixture.source.parent().unwrap().to_owned();
        let displaced_parent = source_parent.with_file_name("displaced-day");
        let replacement_source = fixture.source.clone();

        let result =
            fixture
                .bridge
                .retain_with_hook(&fixture.diagnostics, &fixture.correlation(), || {
                    std::fs::rename(&source_parent, &displaced_parent).unwrap();
                    std::fs::create_dir(&source_parent).unwrap();
                    std::fs::write(&replacement_source, b"attacker bytes\n").unwrap();
                });

        assert_replacement_rejected(&fixture, &replacement_source, result);
        assert!(
            displaced_parent
                .join(fixture.source.file_name().unwrap())
                .is_file()
        );
    }

    #[test]
    fn oversized_rollout_retains_only_a_bounded_diagnostic_prefix() {
        const LIMIT: u64 = 512;
        let fixture = Fixture::new();
        fixture.write_rollout(THREAD_ID);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&fixture.source)
            .unwrap()
            .write_all(&vec![b'x'; usize::try_from(LIMIT).unwrap()])
            .unwrap();

        let result = fixture.bridge.retain_with_operations(
            &fixture.diagnostics,
            &fixture.correlation(),
            LIMIT,
            || {},
            sync_directory,
        );

        assert!(matches!(
            result,
            Err(CodexRolloutRejectionReason::RolloutTooLarge)
        ));
        assert!(!fixture.source.exists());
        assert_eq!(std::fs::metadata(&fixture.retained).unwrap().len(), LIMIT);
    }

    #[test]
    fn ambient_sync_failure_does_not_commit_the_pending_copy() {
        let fixture = Fixture::new();
        fixture.write_rollout(THREAD_ID);

        let result = fixture.bridge.retain_with_operations(
            &fixture.diagnostics,
            &fixture.correlation(),
            MAXIMUM_RETAINED_ROLLOUT_BYTES,
            || {},
            |_| Err(()),
        );

        assert!(matches!(
            result,
            Err(CodexRolloutRejectionReason::AmbientRemovalFailed)
        ));
        assert!(!fixture.source.exists());
        assert!(!fixture.retained.exists());
    }
}
