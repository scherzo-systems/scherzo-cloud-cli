use std::fs::File;
use std::io::Write as _;
use std::os::fd::{AsFd as _, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustix::fs::{AtFlags, Mode, OFlags, fchmod, fstat, mkdirat, openat, unlinkat};
use rustix::io::{Errno, dup};
use serde::Serialize;

use super::agent::{AgentCompatibilityProfile, AgentInvocationIdentity};
use super::private_staging::{open_directory_path, remove_open_tree_at};

const DIAGNOSTICS_DIRECTORY: &str = "diagnostics";
const PI_JSON_V1_DIRECTORY: &str = "pi-json-v1";
const CLAUDE_CODE_STREAM_JSON_V1_DIRECTORY: &str = "claude-code-stream-json-v1";
const NATIVE_SESSION_DIRECTORY: &str = "session";
const METADATA_FILE: &str = "metadata.json";
const IDENTITY_ATTEMPTS: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AgentDiagnosticSessionError;

#[derive(Clone)]
pub(crate) struct AgentDiagnosticSessionStore {
    root: Arc<OwnedFd>,
    path: Arc<PathBuf>,
    local_owner: Option<LocalDiagnosticOwner>,
}

#[derive(Clone)]
struct LocalDiagnosticOwner {
    local_run_id: Arc<str>,
    attempt_number: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct AgentDiagnosticSession {
    directory: PathBuf,
    directory_handle: Arc<OwnedFd>,
    pi_native_session: Option<PiNativeDiagnosticSession>,
}

#[derive(Clone, Debug)]
struct PiNativeDiagnosticSession {
    directory: PathBuf,
    directory_handle: Arc<OwnedFd>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PiDiagnosticSessionMetadata<'a> {
    schema_version: u8,
    local_run_id: &'a str,
    attempt_number: u64,
    step_id: &'a str,
    invocation_id: u64,
    profile: &'static str,
    pi_version: &'a str,
    native_session: NativeSessionMetadata,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NativeSessionMetadata {
    relative_directory: &'static str,
    format_version: u8,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClaudeCodeDiagnosticSessionMetadata<'a> {
    schema_version: u8,
    local_run_id: &'a str,
    attempt_number: u64,
    step_id: &'a str,
    invocation_id: u64,
    profile: &'static str,
    claude_code_version: &'a str,
    native_session_persistence: bool,
}

impl AgentCompatibilityProfile {
    const fn diagnostic_directory(self) -> &'static str {
        match self {
            Self::PiJsonV1 => PI_JSON_V1_DIRECTORY,
            Self::ClaudeCodeStreamJsonV1 => CLAUDE_CODE_STREAM_JSON_V1_DIRECTORY,
        }
    }
}

impl AgentDiagnosticSessionStore {
    pub(crate) fn create(
        attempt_directory: &OwnedFd,
        attempt_path: &Path,
        local_run_id: Arc<str>,
        attempt_number: u64,
    ) -> Result<Self, AgentDiagnosticSessionError> {
        if attempt_number == 0 {
            return Err(AgentDiagnosticSessionError);
        }
        Self::create_with_owner(
            attempt_directory,
            attempt_path,
            Some(LocalDiagnosticOwner {
                local_run_id,
                attempt_number,
            }),
        )
    }

    pub(crate) fn create_transient(
        parent: &OwnedFd,
        parent_path: &Path,
    ) -> Result<Self, AgentDiagnosticSessionError> {
        Self::create_with_owner(parent, parent_path, None)
    }

    fn create_with_owner(
        attempt_directory: &OwnedFd,
        attempt_path: &Path,
        local_owner: Option<LocalDiagnosticOwner>,
    ) -> Result<Self, AgentDiagnosticSessionError> {
        if !attempt_path.is_absolute() {
            return Err(AgentDiagnosticSessionError);
        }
        mkdirat(attempt_directory, DIAGNOSTICS_DIRECTORY, Mode::RWXU)
            .map_err(|_| AgentDiagnosticSessionError)?;
        let root = match open_directory(attempt_directory, DIAGNOSTICS_DIRECTORY) {
            Ok(directory) => directory,
            Err(_) => {
                let _ = unlinkat(attempt_directory, DIAGNOSTICS_DIRECTORY, AtFlags::REMOVEDIR);
                return Err(AgentDiagnosticSessionError);
            }
        };
        if fchmod(&root, Mode::RWXU).is_err()
            || sync_directory(&root).is_err()
            || sync_directory(attempt_directory).is_err()
        {
            let _ = unlinkat(attempt_directory, DIAGNOSTICS_DIRECTORY, AtFlags::REMOVEDIR);
            return Err(AgentDiagnosticSessionError);
        }
        Ok(Self {
            root: Arc::new(root),
            path: Arc::new(attempt_path.join(DIAGNOSTICS_DIRECTORY)),
            local_owner,
        })
    }

    pub(crate) fn allocate(
        &self,
        identity: &AgentInvocationIdentity,
        profile: AgentCompatibilityProfile,
        harness_version: &str,
    ) -> Result<AgentDiagnosticSession, AgentDiagnosticSessionError> {
        let profile_directory_name = profile.diagnostic_directory();
        let profile_directory =
            create_or_open_directory(self.root.as_ref(), profile_directory_name)?;
        let profile_path = self.path.join(profile_directory_name);
        for _ in 0..IDENTITY_ATTEMPTS {
            let directory_name = format!(
                "invocation-{}",
                ulid::Ulid::generate().to_string().to_ascii_lowercase()
            );
            match mkdirat(&profile_directory, &directory_name, Mode::RWXU) {
                Ok(()) => {
                    let invocation = match open_directory(&profile_directory, &directory_name) {
                        Ok(directory) => directory,
                        Err(_) => {
                            let _ =
                                unlinkat(&profile_directory, &directory_name, AtFlags::REMOVEDIR);
                            return Err(AgentDiagnosticSessionError);
                        }
                    };
                    let result = self.prepare_invocation(
                        &profile_directory,
                        &profile_path,
                        &invocation,
                        &directory_name,
                        identity,
                        profile,
                        harness_version,
                    );
                    if result.is_err() {
                        let _ =
                            remove_open_tree_at(&profile_directory, &directory_name, &invocation);
                    }
                    return result;
                }
                Err(Errno::EXIST) => {}
                Err(_) => return Err(AgentDiagnosticSessionError),
            }
        }
        Err(AgentDiagnosticSessionError)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "diagnostic allocation keeps owner, profile, and invocation identity explicit"
    )]
    fn prepare_invocation(
        &self,
        profile_directory: &OwnedFd,
        profile_path: &Path,
        invocation: &OwnedFd,
        directory_name: &str,
        identity: &AgentInvocationIdentity,
        profile: AgentCompatibilityProfile,
        harness_version: &str,
    ) -> Result<AgentDiagnosticSession, AgentDiagnosticSessionError> {
        fchmod(invocation, Mode::RWXU).map_err(|_| AgentDiagnosticSessionError)?;
        let invocation_path = profile_path.join(directory_name);
        let pi_native_session = match profile {
            AgentCompatibilityProfile::PiJsonV1 => {
                mkdirat(invocation, NATIVE_SESSION_DIRECTORY, Mode::RWXU)
                    .map_err(|_| AgentDiagnosticSessionError)?;
                let directory_handle = open_directory(invocation, NATIVE_SESSION_DIRECTORY)
                    .map_err(|_| AgentDiagnosticSessionError)?;
                fchmod(&directory_handle, Mode::RWXU).map_err(|_| AgentDiagnosticSessionError)?;
                Some(PiNativeDiagnosticSession {
                    directory: invocation_path.join(NATIVE_SESSION_DIRECTORY),
                    directory_handle: Arc::new(directory_handle),
                })
            }
            AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => None,
        };

        if let Some(owner) = &self.local_owner {
            let bytes = metadata_bytes(owner, identity, profile, harness_version)?;
            write_immutable_metadata(invocation, &bytes)?;
        }
        if let Some(native_session) = &pi_native_session {
            sync_directory(native_session.directory_handle.as_ref())?;
        }
        sync_directory(invocation)?;
        sync_directory(profile_directory)?;
        sync_directory(self.root.as_ref())?;

        let session = AgentDiagnosticSession {
            directory: invocation_path,
            directory_handle: Arc::new(dup(invocation).map_err(|_| AgentDiagnosticSessionError)?),
            pi_native_session,
        };
        session.verify_path_binding()?;
        if profile == AgentCompatibilityProfile::PiJsonV1 {
            session.verify_pi_native_session_path_binding()?;
        }
        Ok(session)
    }
}

fn metadata_bytes(
    owner: &LocalDiagnosticOwner,
    identity: &AgentInvocationIdentity,
    profile: AgentCompatibilityProfile,
    harness_version: &str,
) -> Result<Vec<u8>, AgentDiagnosticSessionError> {
    let mut bytes = match profile {
        AgentCompatibilityProfile::PiJsonV1 => {
            serde_json::to_vec_pretty(&PiDiagnosticSessionMetadata {
                schema_version: 1,
                local_run_id: &owner.local_run_id,
                attempt_number: owner.attempt_number,
                step_id: identity.step(),
                invocation_id: identity.invocation().transition_sequence.get(),
                profile: "PiJsonV1",
                pi_version: harness_version,
                native_session: NativeSessionMetadata {
                    relative_directory: NATIVE_SESSION_DIRECTORY,
                    format_version: 3,
                },
            })
        }
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
            serde_json::to_vec_pretty(&ClaudeCodeDiagnosticSessionMetadata {
                schema_version: 1,
                local_run_id: &owner.local_run_id,
                attempt_number: owner.attempt_number,
                step_id: identity.step(),
                invocation_id: identity.invocation().transition_sequence.get(),
                profile: "ClaudeCodeStreamJsonV1",
                claude_code_version: harness_version,
                native_session_persistence: false,
            })
        }
    }
    .map_err(|_| AgentDiagnosticSessionError)?;
    bytes.push(b'\n');
    Ok(bytes)
}

impl AgentDiagnosticSession {
    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    pub(crate) fn pi_native_session_directory(&self) -> Option<&Path> {
        self.pi_native_session
            .as_ref()
            .map(|session| session.directory.as_path())
    }

    pub(crate) fn verify_path_binding(&self) -> Result<(), AgentDiagnosticSessionError> {
        verify_directory_binding(&self.directory, self.directory_handle.as_ref())
    }

    pub(crate) fn verify_pi_native_session_path_binding(
        &self,
    ) -> Result<(), AgentDiagnosticSessionError> {
        let native_session = self
            .pi_native_session
            .as_ref()
            .ok_or(AgentDiagnosticSessionError)?;
        verify_directory_binding(
            &native_session.directory,
            native_session.directory_handle.as_ref(),
        )
    }

    #[cfg(test)]
    pub(crate) fn fixture(native_session_directory: PathBuf) -> Self {
        std::fs::create_dir_all(&native_session_directory).unwrap();
        let directory = native_session_directory
            .parent()
            .expect("the native diagnostic-session fixture must have a parent")
            .to_owned();
        Self {
            directory_handle: Arc::new(
                open_directory_path(&directory)
                    .expect("the diagnostic-session fixture must be openable"),
            ),
            directory,
            pi_native_session: Some(PiNativeDiagnosticSession {
                directory_handle: Arc::new(
                    open_directory_path(&native_session_directory)
                        .expect("the native diagnostic-session fixture must be openable"),
                ),
                directory: native_session_directory,
            }),
        }
    }
}

fn verify_directory_binding(
    path: &Path,
    retained: &OwnedFd,
) -> Result<(), AgentDiagnosticSessionError> {
    if !path.is_absolute() {
        return Err(AgentDiagnosticSessionError);
    }
    let reopened = open_directory_path(path).map_err(|_| AgentDiagnosticSessionError)?;
    let retained = fstat(retained).map_err(|_| AgentDiagnosticSessionError)?;
    let named = fstat(&reopened).map_err(|_| AgentDiagnosticSessionError)?;
    (retained.st_dev == named.st_dev && retained.st_ino == named.st_ino)
        .then_some(())
        .ok_or(AgentDiagnosticSessionError)
}

fn create_or_open_directory(
    parent: &OwnedFd,
    name: &str,
) -> Result<OwnedFd, AgentDiagnosticSessionError> {
    match mkdirat(parent, name, Mode::RWXU) {
        Ok(()) | Err(Errno::EXIST) => {}
        Err(_) => return Err(AgentDiagnosticSessionError),
    }
    let directory = open_directory(parent, name).map_err(|_| AgentDiagnosticSessionError)?;
    fchmod(&directory, Mode::RWXU).map_err(|_| AgentDiagnosticSessionError)?;
    Ok(directory)
}

fn write_immutable_metadata(
    invocation: &OwnedFd,
    bytes: &[u8],
) -> Result<(), AgentDiagnosticSessionError> {
    let descriptor = openat(
        invocation,
        METADATA_FILE,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| AgentDiagnosticSessionError)?;
    let mut file = File::from(descriptor);
    file.write_all(bytes)
        .and_then(|()| file.flush())
        .and_then(|()| file.sync_all())
        .map_err(|_| AgentDiagnosticSessionError)?;
    fchmod(file.as_fd(), Mode::RUSR).map_err(|_| AgentDiagnosticSessionError)
}

fn open_directory(parent: &OwnedFd, name: &str) -> Result<OwnedFd, Errno> {
    openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
}

fn sync_directory(directory: &OwnedFd) -> Result<(), AgentDiagnosticSessionError> {
    let duplicate = dup(directory).map_err(|_| AgentDiagnosticSessionError)?;
    File::from(duplicate)
        .sync_all()
        .map_err(|_| AgentDiagnosticSessionError)
}

#[cfg(test)]
mod tests {
    use std::os::fd::OwnedFd;
    use std::os::unix::fs::PermissionsExt as _;

    use serde_json::json;

    use super::*;
    use crate::execution::workflow::agent::WorkflowRunId;
    use crate::execution::workflow::runtime::{ActionId, TransitionSequence};

    fn diagnostic_store(temporary: &tempfile::TempDir) -> (PathBuf, AgentDiagnosticSessionStore) {
        let attempt_path = temporary.path().join("attempts/000001");
        std::fs::create_dir_all(&attempt_path).unwrap();
        let attempt: OwnedFd = std::fs::File::open(&attempt_path).unwrap().into();
        let store = AgentDiagnosticSessionStore::create(
            &attempt,
            &attempt_path,
            Arc::from("00000000-0000-4000-8000-000000000001"),
            1,
        )
        .unwrap();
        (attempt_path, store)
    }

    fn invocation_identity() -> AgentInvocationIdentity {
        AgentInvocationIdentity::new(
            WorkflowRunId::from(Arc::from("run")),
            Arc::from("agent-step"),
            ActionId {
                transition_sequence: TransitionSequence::default(),
            },
        )
    }

    #[test]
    fn successful_allocation_returns_the_descriptor_bound_profile_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let (attempt_path, store) = diagnostic_store(&temporary);
        let identity = invocation_identity();
        let allocated = store
            .allocate(&identity, AgentCompatibilityProfile::PiJsonV1, "0.83.0")
            .unwrap();
        allocated.verify_path_binding().unwrap();
        allocated.verify_pi_native_session_path_binding().unwrap();

        let moved_attempt_path = temporary.path().join("attempts/moved-000001");
        std::fs::rename(&attempt_path, &moved_attempt_path).unwrap();
        std::fs::create_dir_all(&attempt_path).unwrap();

        assert!(matches!(
            store.allocate(&identity, AgentCompatibilityProfile::PiJsonV1, "0.83.0"),
            Err(AgentDiagnosticSessionError)
        ));
    }

    #[test]
    fn allocations_are_owner_private_and_never_reuse_pi_native_session_storage() {
        let temporary = tempfile::tempdir().unwrap();
        let (_, store) = diagnostic_store(&temporary);
        let identity = invocation_identity();

        let first = store
            .allocate(&identity, AgentCompatibilityProfile::PiJsonV1, "0.83.0")
            .unwrap();
        std::fs::write(
            first
                .pi_native_session_directory()
                .unwrap()
                .join("partial.jsonl"),
            b"{partial",
        )
        .unwrap();
        let second = store
            .allocate(&identity, AgentCompatibilityProfile::PiJsonV1, "0.83.0")
            .unwrap();

        let first_directory = first.pi_native_session_directory().unwrap();
        let second_directory = second.pi_native_session_directory().unwrap();
        assert_ne!(first_directory, second_directory);
        assert_eq!(std::fs::read_dir(second_directory).unwrap().count(), 0);
        for directory in [first_directory, second_directory] {
            assert_eq!(
                std::fs::metadata(directory).unwrap().permissions().mode() & 0o7777,
                0o700
            );
            let metadata: serde_json::Value = serde_json::from_slice(
                &std::fs::read(directory.parent().unwrap().join(METADATA_FILE)).unwrap(),
            )
            .unwrap();
            assert_eq!(
                metadata,
                json!({
                    "schemaVersion": 1,
                    "localRunId": "00000000-0000-4000-8000-000000000001",
                    "attemptNumber": 1,
                    "stepId": "agent-step",
                    "invocationId": 0,
                    "profile": "PiJsonV1",
                    "piVersion": "0.83.0",
                    "nativeSession": {
                        "relativeDirectory": "session",
                        "formatVersion": 3
                    }
                })
            );
        }
    }

    #[test]
    fn claude_diagnostics_use_their_own_root_without_native_session_persistence() {
        let temporary = tempfile::tempdir().unwrap();
        let (attempt_path, store) = diagnostic_store(&temporary);
        let identity = invocation_identity();

        let claude = store
            .allocate(
                &identity,
                AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
                "2.1.222",
            )
            .unwrap();
        assert!(claude.pi_native_session_directory().is_none());
        assert_eq!(
            claude.directory().parent(),
            Some(
                attempt_path
                    .join(DIAGNOSTICS_DIRECTORY)
                    .join(CLAUDE_CODE_STREAM_JSON_V1_DIRECTORY)
                    .as_path()
            )
        );
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(claude.directory().join(METADATA_FILE)).unwrap())
                .unwrap();
        // Keep this profile-local expected document explicit; sharing it with Pi's
        // exact-binary fixture would obscure the intentionally different metadata fields.
        // jscpd:ignore-start
        assert_eq!(
            metadata,
            json!({
                "schemaVersion": 1,
                "localRunId": "00000000-0000-4000-8000-000000000001",
                "attemptNumber": 1,
                "stepId": "agent-step",
                "invocationId": 0,
                "profile": "ClaudeCodeStreamJsonV1",
                "claudeCodeVersion": "2.1.222",
                "nativeSessionPersistence": false
            })
        );
        // jscpd:ignore-end
    }
}
