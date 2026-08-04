use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;

use rustix::fs::{
    AtFlags, Dir, FileType, Mode, OFlags, fchmod, fstat, mkdirat, openat, statat, unlinkat,
};
use rustix::io::Errno;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum StagingLifecycle {
    Active,
    CleanupFailed,
    Released,
}

pub(super) fn mark_cleanup_failed(lifecycle: &RwLock<StagingLifecycle>) {
    let Ok(mut lifecycle) = lifecycle.write() else {
        return;
    };
    if *lifecycle == StagingLifecycle::Active {
        *lifecycle = StagingLifecycle::CleanupFailed;
    }
}

pub(super) fn cleanup_staging<Error>(
    lifecycle: &RwLock<StagingLifecycle>,
    unavailable: Error,
    cleanup_active: impl FnOnce() -> Result<(), Error>,
) -> Result<(), Error> {
    let mut lifecycle = lifecycle.write().map_err(|_| unavailable)?;
    if *lifecycle == StagingLifecycle::Released {
        return Ok(());
    }
    let result = cleanup_active();
    *lifecycle = if result.is_ok() {
        StagingLifecycle::Released
    } else {
        StagingLifecycle::CleanupFailed
    };
    result
}

pub(super) fn create_staging_root(
    parent: &OwnedFd,
    prefix: &str,
    attempts: usize,
) -> Result<(Arc<str>, OwnedFd), ()> {
    for _ in 0..attempts {
        let identity = Arc::<str>::from(format!(
            "{prefix}-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        ));
        match mkdirat(parent, identity.as_ref(), Mode::RWXU) {
            Ok(()) => match openat(
                parent,
                identity.as_ref(),
                directory_open_flags(),
                Mode::empty(),
            ) {
                Ok(directory) if fchmod(&directory, Mode::RWXU).is_ok() => {
                    return Ok((identity, directory));
                }
                Ok(_) | Err(_) => {
                    let _ = unlinkat(parent, identity.as_ref(), AtFlags::REMOVEDIR);
                    return Err(());
                }
            },
            Err(Errno::EXIST) => {}
            Err(_) => return Err(()),
        }
    }
    Err(())
}

pub(super) fn remove_tree_at(parent: &OwnedFd, identity: &str) -> Result<(), Errno> {
    let directory = match openat(parent, identity, directory_open_flags(), Mode::empty()) {
        Ok(directory) => directory,
        Err(Errno::NOENT) => return Ok(()),
        Err(failure) => return Err(failure),
    };
    remove_open_tree_at(parent, identity, &directory)
}

pub(super) fn remove_open_tree_at(
    parent: &OwnedFd,
    identity: &str,
    directory: &OwnedFd,
) -> Result<(), Errno> {
    remove_directory_contents(directory)?;
    let opened = fstat(directory)?;
    let named = statat(parent, identity, AtFlags::SYMLINK_NOFOLLOW)?;
    if opened.st_dev != named.st_dev
        || opened.st_ino != named.st_ino
        || FileType::from_raw_mode(named.st_mode) != FileType::Directory
    {
        return Err(Errno::IO);
    }
    unlinkat(parent, identity, AtFlags::REMOVEDIR)
}

pub(super) fn remove_staging_root(
    parent: &OwnedFd,
    identity: &str,
    root: &OwnedFd,
) -> Result<(), Errno> {
    remove_open_tree_at(parent, identity, root)
}

fn remove_directory_contents(directory: &OwnedFd) -> Result<(), Errno> {
    fchmod(directory, Mode::RWXU)?;
    let entries = Dir::read_from(directory)?
        .filter_map(|entry| match entry {
            Ok(entry) if !matches!(entry.file_name().to_bytes(), b"." | b"..") => {
                Some(Ok(entry.file_name().to_owned()))
            }
            Ok(_) => None,
            Err(failure) => Some(Err(failure)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    for name in entries {
        let metadata = statat(directory, &name, AtFlags::SYMLINK_NOFOLLOW)?;
        if FileType::from_raw_mode(metadata.st_mode) == FileType::Directory {
            let child = openat(directory, &name, directory_open_flags(), Mode::empty())?;
            remove_directory_contents(&child)?;
            unlinkat(directory, &name, AtFlags::REMOVEDIR)?;
        } else {
            unlinkat(directory, &name, AtFlags::empty())?;
        }
    }
    Ok(())
}

pub(super) fn create_payload_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

pub(super) fn finish_payload_file(mut file: File) -> io::Result<()> {
    file.flush()?;
    file.set_permissions(std::fs::Permissions::from_mode(0o400))
}

fn directory_open_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(test)]
#[derive(Default)]
pub(super) struct CleanupBlocker(AtomicBool);

#[cfg(test)]
impl CleanupBlocker {
    pub(super) fn block(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub(super) fn unblock(&self) {
        self.0.store(false, Ordering::Release);
    }

    pub(super) fn is_blocked(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
