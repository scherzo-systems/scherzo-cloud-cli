use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;

use rustix::fs::{RenameFlags, renameat_with};
use rustix::io::Errno;
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CommitError {
    DestinationExists,
    Unavailable,
    DurabilityUnconfirmed,
}

pub(super) fn commit_noreplace(
    staging: TempDir,
    parent: &Path,
    destination_name: &OsStr,
) -> Result<(), CommitError> {
    let staging_name = staging.path().file_name().ok_or(CommitError::Unavailable)?;
    let parent_file = File::open(parent).map_err(|_| CommitError::Unavailable)?;
    match renameat_with(
        &parent_file,
        staging_name,
        &parent_file,
        destination_name,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {}
        Err(Errno::EXIST | Errno::NOTEMPTY) => {
            return Err(CommitError::DestinationExists);
        }
        Err(_) => return Err(CommitError::Unavailable),
    }

    // The rename transferred the directory to its final name. Relinquish temporary-directory
    // cleanup before syncing the parent so a durability error cannot remove committed output.
    let _committed_path = staging.keep();
    parent_file
        .sync_all()
        .map_err(|_| CommitError::DurabilityUnconfirmed)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs;

    use super::{CommitError, commit_noreplace};

    #[test]
    fn parent_open_failure_cleans_owned_staging() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir_in(root.path()).unwrap();
        let staging_path = staging.path().to_owned();
        fs::write(staging.path().join("private-input"), b"private bytes").unwrap();

        let result = commit_noreplace(
            staging,
            &root.path().join("missing-parent"),
            OsStr::new("download"),
        );

        assert_eq!(result, Err(CommitError::Unavailable));
        assert!(!staging_path.exists());
    }

    #[test]
    fn rename_failure_cleans_owned_staging() {
        let root = tempfile::tempdir().unwrap();
        let actual_parent = root.path().join("actual");
        let wrong_parent = root.path().join("wrong");
        fs::create_dir_all(&actual_parent).unwrap();
        fs::create_dir_all(&wrong_parent).unwrap();
        let staging = tempfile::tempdir_in(&actual_parent).unwrap();
        let staging_path = staging.path().to_owned();
        fs::write(staging.path().join("private-input"), b"private bytes").unwrap();

        let result = commit_noreplace(staging, &wrong_parent, OsStr::new("download"));

        assert_eq!(result, Err(CommitError::Unavailable));
        assert!(!staging_path.exists());
        assert!(!wrong_parent.join("download").exists());
    }

    #[test]
    fn successful_rename_relinquishes_cleanup_of_the_destination() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir_in(root.path()).unwrap();
        let staging_path = staging.path().to_owned();
        fs::write(staging.path().join("input"), b"committed bytes").unwrap();

        let result = commit_noreplace(staging, root.path(), OsStr::new("download"));

        assert_eq!(result, Ok(()));
        assert!(!staging_path.exists());
        assert_eq!(
            fs::read(root.path().join("download/input")).unwrap(),
            b"committed bytes"
        );
    }
}
