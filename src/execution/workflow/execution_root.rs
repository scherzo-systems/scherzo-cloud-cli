use std::fmt;
use std::fs;
use std::os::fd::OwnedFd;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use rustix::fs::{Access, AtFlags, FileType, Mode, OFlags, accessat, fstat, open, openat, statat};
use rustix::io::{Errno, dup};

#[cfg(test)]
use super::test_support::SynchronousGate;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutionRootAdmissionFailure {
    Unavailable,
    NotDirectory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkingDirectorySelectionFailure {
    ExecutionRootRebound,
    Unavailable,
    EscapesExecutionRoot,
    NotDirectory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutableCandidate {
    Executable,
    Missing,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DirectoryIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

#[derive(Clone)]
pub(crate) struct AdmittedExecutionRoot {
    inner: Arc<AdmittedExecutionRootInner>,
}

struct AdmittedExecutionRootInner {
    provenance_path: PathBuf,
    directory: OwnedFd,
    identity: DirectoryIdentity,
    #[cfg(test)]
    prelaunch_boundary: Mutex<Option<ExecutionRootPrelaunchBoundary>>,
}

impl fmt::Debug for AdmittedExecutionRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmittedExecutionRoot")
            .field("provenance_path", &self.inner.provenance_path)
            .field("identity", &self.inner.identity)
            .finish_non_exhaustive()
    }
}

impl AdmittedExecutionRoot {
    pub(super) fn admit(root: &Path) -> Result<Self, ExecutionRootAdmissionFailure> {
        let provenance_path =
            fs::canonicalize(root).map_err(|_| ExecutionRootAdmissionFailure::Unavailable)?;
        let directory = open_directory(&provenance_path).map_err(|failure| {
            if failure == Errno::NOTDIR {
                ExecutionRootAdmissionFailure::NotDirectory
            } else {
                ExecutionRootAdmissionFailure::Unavailable
            }
        })?;
        let identity = directory_identity(&directory)
            .map_err(|_| ExecutionRootAdmissionFailure::Unavailable)?;
        Ok(Self {
            inner: Arc::new(AdmittedExecutionRootInner {
                provenance_path,
                directory,
                identity,
                #[cfg(test)]
                prelaunch_boundary: Mutex::new(None),
            }),
        })
    }

    pub(crate) fn provenance_path(&self) -> &Path {
        &self.inner.provenance_path
    }

    pub(super) fn directory(&self) -> &OwnedFd {
        &self.inner.directory
    }

    pub(super) fn is_same_directory(&self, other: &Self) -> bool {
        self.inner.identity == other.inner.identity
    }

    pub(super) fn pathname_is_bound(&self) -> bool {
        open_directory(&self.inner.provenance_path)
            .and_then(|candidate| directory_identity(&candidate))
            .is_ok_and(|identity| identity == self.inner.identity)
    }

    pub(crate) fn bind_command_ref(
        &self,
        command: &mut Command,
    ) -> Result<(), WorkingDirectorySelectionFailure> {
        if !self.pathname_is_bound() {
            return Err(WorkingDirectorySelectionFailure::ExecutionRootRebound);
        }
        let directory = dup(&self.inner.directory)
            .map_err(|_| WorkingDirectorySelectionFailure::Unavailable)?;
        bind_directory(command, directory);
        Ok(())
    }

    pub(super) fn select_working_directory(
        &self,
        declared_cwd: Option<&str>,
    ) -> Result<AdmittedWorkingDirectory, WorkingDirectorySelectionFailure> {
        if !self.pathname_is_bound() {
            return Err(WorkingDirectorySelectionFailure::ExecutionRootRebound);
        }
        let working_directory_path = declared_cwd.map_or_else(
            || self.inner.provenance_path.clone(),
            |cwd| self.inner.provenance_path.join(cwd),
        );
        let directory = open_working_directory(&working_directory_path).map_err(|failure| {
            if failure == Errno::NOTDIR {
                WorkingDirectorySelectionFailure::NotDirectory
            } else {
                WorkingDirectorySelectionFailure::Unavailable
            }
        })?;
        if !self
            .contains_directory(&directory)
            .map_err(|_| WorkingDirectorySelectionFailure::Unavailable)?
        {
            return Err(WorkingDirectorySelectionFailure::EscapesExecutionRoot);
        }

        #[cfg(test)]
        self.wait_at_prelaunch_boundary();

        Ok(AdmittedWorkingDirectory {
            provenance_path: working_directory_path,
            directory,
            execution_root: self.clone(),
        })
    }

    pub(super) fn contains_directory(&self, directory: &OwnedFd) -> Result<bool, Errno> {
        let mut current = dup(directory)?;
        loop {
            let current_identity = directory_identity(&current)?;
            if current_identity == self.inner.identity {
                return Ok(true);
            }
            let parent = openat(&current, "..", directory_open_flags(), Mode::empty())?;
            let parent_identity = directory_identity(&parent)?;
            if parent_identity == current_identity {
                return Ok(false);
            }
            current = parent;
        }
    }

    #[cfg(test)]
    pub(super) fn set_prelaunch_boundary(&self, boundary: ExecutionRootPrelaunchBoundary) {
        *self
            .inner
            .prelaunch_boundary
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(boundary);
    }

    #[cfg(test)]
    fn wait_at_prelaunch_boundary(&self) {
        let boundary = self
            .inner
            .prelaunch_boundary
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(boundary) = boundary {
            boundary.block_until_resumed();
        }
    }
}

pub(super) struct AdmittedWorkingDirectory {
    provenance_path: PathBuf,
    directory: OwnedFd,
    execution_root: AdmittedExecutionRoot,
}

impl fmt::Debug for AdmittedWorkingDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmittedWorkingDirectory")
            .field("provenance_path", &self.provenance_path)
            .finish_non_exhaustive()
    }
}

impl AdmittedWorkingDirectory {
    pub(super) fn provenance_path(&self) -> &Path {
        &self.provenance_path
    }

    pub(super) fn validate_execution_root(&self) -> bool {
        self.execution_root.pathname_is_bound()
    }

    pub(super) fn protocol_path(&self) -> Result<PathBuf, WorkingDirectorySelectionFailure> {
        if !self.validate_execution_root() {
            return Err(WorkingDirectorySelectionFailure::ExecutionRootRebound);
        }
        let path = fs::canonicalize(&self.provenance_path)
            .map_err(|_| WorkingDirectorySelectionFailure::Unavailable)?;
        let candidate = open_working_directory(&path)
            .map_err(|_| WorkingDirectorySelectionFailure::Unavailable)?;
        let selected_identity = directory_identity(&self.directory)
            .map_err(|_| WorkingDirectorySelectionFailure::Unavailable)?;
        let candidate_identity = directory_identity(&candidate)
            .map_err(|_| WorkingDirectorySelectionFailure::Unavailable)?;
        if candidate_identity != selected_identity {
            return Err(WorkingDirectorySelectionFailure::Unavailable);
        }
        Ok(path)
    }

    pub(super) fn executable_candidate(&self, candidate: &Path) -> ExecutableCandidate {
        let metadata = match statat(&self.directory, candidate, AtFlags::empty()) {
            Ok(metadata) => metadata,
            Err(Errno::NOENT | Errno::NOTDIR) => return ExecutableCandidate::Missing,
            Err(_) => return ExecutableCandidate::Unavailable,
        };
        if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
            return ExecutableCandidate::Unavailable;
        }
        if accessat(
            &self.directory,
            candidate,
            Access::EXEC_OK,
            AtFlags::EACCESS,
        )
        .is_err()
        {
            return ExecutableCandidate::Unavailable;
        }
        ExecutableCandidate::Executable
    }

    pub(super) fn bind_command_ref(
        &self,
        command: &mut Command,
    ) -> Result<(), WorkingDirectorySelectionFailure> {
        if !self.validate_execution_root() {
            return Err(WorkingDirectorySelectionFailure::ExecutionRootRebound);
        }
        let directory =
            dup(&self.directory).map_err(|_| WorkingDirectorySelectionFailure::Unavailable)?;
        bind_directory(command, directory);
        Ok(())
    }

    pub(super) fn bind_command(self, command: &mut Command) {
        bind_directory(command, self.directory);
    }
}

#[allow(
    unsafe_code,
    reason = "the pre-exec hook performs only the async-signal-safe fchdir operation"
)]
fn bind_directory(command: &mut Command, directory: OwnedFd) {
    // SAFETY: `fchdir` is async-signal-safe, the closure owns its descriptor,
    // and error conversion performs no process-global or heap-backed work.
    unsafe {
        command.pre_exec(move || {
            rustix::process::fchdir(&directory)
                .map_err(|failure| std::io::Error::from_raw_os_error(failure.raw_os_error()))
        });
    }
}

pub(super) fn open_directory(path: &Path) -> Result<OwnedFd, Errno> {
    open(path, directory_open_flags(), Mode::empty())
}

fn open_working_directory(path: &Path) -> Result<OwnedFd, Errno> {
    open(path, working_directory_open_flags(), Mode::empty())
}

pub(super) fn directory_open_flags() -> OFlags {
    platform_directory_open_flags() | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

fn working_directory_open_flags() -> OFlags {
    platform_directory_open_flags() | OFlags::DIRECTORY | OFlags::CLOEXEC
}

fn platform_directory_open_flags() -> OFlags {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        OFlags::PATH
    }
    #[cfg(target_vendor = "apple")]
    {
        OFlags::from_bits_retain(libc::O_SEARCH.unsigned_abs())
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    {
        OFlags::RDONLY
    }
}

fn directory_identity(directory: &OwnedFd) -> Result<DirectoryIdentity, Errno> {
    let metadata = fstat(directory)?;
    Ok(DirectoryIdentity {
        device: metadata.st_dev,
        inode: metadata.st_ino,
    })
}

#[cfg(test)]
pub(super) type ExecutionRootPrelaunchBoundary = SynchronousGate;
