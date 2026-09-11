use std::os::fd::OwnedFd;

use rustix::ffi::{CStr, CString};
use rustix::fs::{
    AtFlags, Dir, FileType, Mode, OFlags, Stat, fchmod, fstat, openat, statat, unlinkat,
};
use rustix::io::Errno;
use rustix::path::Arg;

const DIRECTORY_ENTRY_BATCH_SIZE: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemovalError {
    Filesystem(Errno),
    Replaced,
}

impl From<Errno> for RemovalError {
    fn from(error: Errno) -> Self {
        Self::Filesystem(error)
    }
}

pub(crate) fn open_directory_at(parent: &OwnedFd, identity: &str) -> Result<OwnedFd, Errno> {
    openat(parent, identity, directory_open_flags(), Mode::empty())
}

pub(crate) fn remove_tree_at(parent: &OwnedFd, identity: &str) -> Result<(), RemovalError> {
    let directory = match openat(parent, identity, directory_open_flags(), Mode::empty()) {
        Ok(directory) => directory,
        Err(Errno::NOENT) => return Ok(()),
        Err(failure) => return Err(failure.into()),
    };
    remove_open_tree_at(parent, identity, &directory)
}

pub(crate) fn remove_open_tree_at(
    parent: &OwnedFd,
    identity: &str,
    directory: &OwnedFd,
) -> Result<(), RemovalError> {
    let identity = identity.into_c_str()?;
    remove_open_tree_named(parent, &identity, directory, &mut |_, _| {})
}

fn remove_open_tree_named<H>(
    parent: &OwnedFd,
    identity: &CStr,
    directory: &OwnedFd,
    before_permission_restore: &mut H,
) -> Result<(), RemovalError>
where
    H: FnMut(&OwnedFd, &CStr),
{
    validate_named_directory(parent, identity, directory)?;
    before_permission_restore(parent, identity);
    validate_named_directory(parent, identity, directory)?;
    fchmod(directory, Mode::RWXU)?;
    validate_named_directory(parent, identity, directory)?;
    remove_directory_contents(directory, before_permission_restore)?;
    validate_named_directory(parent, identity, directory)?;
    unlinkat(parent, identity, AtFlags::REMOVEDIR).map_err(classify_unlink_error)
}

fn remove_directory_contents<H>(
    directory: &OwnedFd,
    before_permission_restore: &mut H,
) -> Result<(), RemovalError>
where
    H: FnMut(&OwnedFd, &CStr),
{
    loop {
        let entries = directory_entry_batch(directory)?;
        if entries.is_empty() {
            return Ok(());
        }
        for name in entries {
            remove_entry(directory, &name, before_permission_restore)?;
        }
    }
}

fn directory_entry_batch(directory: &OwnedFd) -> Result<Vec<CString>, RemovalError> {
    let mut entries = Vec::with_capacity(DIRECTORY_ENTRY_BATCH_SIZE);
    for entry in Dir::read_from(directory)? {
        let entry = entry?;
        if matches!(entry.file_name().to_bytes(), b"." | b"..") {
            continue;
        }
        entries.push(entry.file_name().to_owned());
        if entries.len() == DIRECTORY_ENTRY_BATCH_SIZE {
            break;
        }
    }
    Ok(entries)
}

fn remove_entry<H>(
    parent: &OwnedFd,
    name: &CStr,
    before_permission_restore: &mut H,
) -> Result<(), RemovalError>
where
    H: FnMut(&OwnedFd, &CStr),
{
    let observed =
        statat(parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(classify_restat_error)?;
    if FileType::from_raw_mode(observed.st_mode) == FileType::Directory {
        let child = openat(parent, name, directory_open_flags(), Mode::empty())
            .map_err(classify_open_after_stat_error)?;
        let opened = fstat(&child)?;
        if !same_identity(&observed, &opened) {
            return Err(RemovalError::Replaced);
        }
        remove_open_tree_named(parent, name, &child, before_permission_restore)
    } else {
        let current =
            statat(parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(classify_restat_error)?;
        if !same_identity(&observed, &current) {
            return Err(RemovalError::Replaced);
        }
        unlinkat(parent, name, AtFlags::empty()).map_err(classify_unlink_error)
    }
}

fn validate_named_directory(
    parent: &OwnedFd,
    identity: &CStr,
    directory: &OwnedFd,
) -> Result<(), RemovalError> {
    let parent_metadata = fstat(parent)?;
    let opened = fstat(directory)?;
    let named =
        statat(parent, identity, AtFlags::SYMLINK_NOFOLLOW).map_err(classify_restat_error)?;
    if FileType::from_raw_mode(named.st_mode) != FileType::Directory
        || opened.st_uid != rustix::process::geteuid().as_raw()
        || opened.st_dev != parent_metadata.st_dev
        || !same_identity(&opened, &named)
    {
        return Err(RemovalError::Replaced);
    }
    Ok(())
}

fn same_identity(left: &Stat, right: &Stat) -> bool {
    left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && FileType::from_raw_mode(left.st_mode) == FileType::from_raw_mode(right.st_mode)
}

fn classify_open_after_stat_error(error: Errno) -> RemovalError {
    match error {
        Errno::NOENT | Errno::NOTDIR | Errno::LOOP => RemovalError::Replaced,
        _ => RemovalError::Filesystem(error),
    }
}

fn classify_restat_error(error: Errno) -> RemovalError {
    match error {
        Errno::NOENT | Errno::NOTDIR | Errno::LOOP => RemovalError::Replaced,
        _ => RemovalError::Filesystem(error),
    }
}

fn classify_unlink_error(error: Errno) -> RemovalError {
    match error {
        Errno::NOENT | Errno::NOTDIR | Errno::ISDIR | Errno::NOTEMPTY | Errno::EXIST => {
            RemovalError::Replaced
        }
        _ => RemovalError::Filesystem(error),
    }
}

fn directory_open_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

#[cfg(test)]
mod tests {
    use std::fs::{self, Permissions};
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::Path;

    use rustix::fs::open;

    use super::*;

    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
    }

    #[test]
    fn removal_reenumerates_directories_larger_than_one_batch() {
        let temporary = tempfile::tempdir().unwrap();
        let owned = temporary.path().join("owned");
        fs::create_dir(&owned).unwrap();
        for index in 0..DIRECTORY_ENTRY_BATCH_SIZE * 2 + 1 {
            let path = owned.join(format!("entry-{index:03}"));
            fs::write(&path, b"value").unwrap();
            fs::set_permissions(path, Permissions::from_mode(0o400)).unwrap();
        }
        fs::set_permissions(&owned, Permissions::from_mode(0o500)).unwrap();
        let parent = open(temporary.path(), directory_open_flags(), Mode::empty()).unwrap();

        remove_tree_at(&parent, "owned").unwrap();

        assert!(!owned.exists());
    }

    #[test]
    fn relocated_directory_is_rejected_before_permission_restoration() {
        let temporary = tempfile::tempdir().unwrap();
        let owned = temporary.path().join("owned");
        let nested = owned.join("nested");
        let outside = temporary.path().join("outside");
        let relocated = outside.join("relocated");
        fs::create_dir(&owned).unwrap();
        fs::create_dir(&nested).unwrap();
        fs::create_dir(&outside).unwrap();
        let sentinel = nested.join("sentinel");
        fs::write(&sentinel, b"outside authority").unwrap();
        fs::set_permissions(&sentinel, Permissions::from_mode(0o400)).unwrap();
        fs::set_permissions(&nested, Permissions::from_mode(0o500)).unwrap();
        let parent = open(temporary.path(), directory_open_flags(), Mode::empty()).unwrap();
        let directory = openat(&parent, "owned", directory_open_flags(), Mode::empty()).unwrap();

        let result = remove_open_tree_named(&parent, c"owned", &directory, &mut |_, identity| {
            if identity.to_bytes() == b"nested" {
                assert_eq!(mode(&owned), 0o700);
                assert_ne!(mode(&outside) & 0o200, 0);
                fs::set_permissions(&nested, Permissions::from_mode(0o700)).unwrap();
                fs::rename(&nested, &relocated).unwrap();
                fs::set_permissions(&relocated, Permissions::from_mode(0o500)).unwrap();
            }
        });

        assert_eq!(result, Err(RemovalError::Replaced));
        assert_eq!(
            fs::read(relocated.join("sentinel")).unwrap(),
            b"outside authority"
        );
        assert_eq!(mode(&relocated), 0o500);
        assert_eq!(mode(&relocated.join("sentinel")), 0o400);

        fs::set_permissions(&relocated, Permissions::from_mode(0o700)).unwrap();
    }
}
