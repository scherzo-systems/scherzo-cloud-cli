use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Seek as _, SeekFrom};
use std::os::fd::OwnedFd;
use std::sync::atomic::AtomicBool;

use super::artifact_limits::{MAXIMUM_CARRIERS, MAXIMUM_EXPORT_ENTRIES, MAXIMUM_ROOT_ENTRIES};
use super::artifact_primitives;
use super::git_artifact::{
    GitArtifactDescriptor, GitArtifactValidationBudget, validate_git_bundle,
};
use super::publication::{ExportV1, WorkflowResultV1};
use super::result_metadata;
use rustix::fs::{AtFlags, FileType, Mode, OFlags, fstat, openat, statat};

const RESULT_FILE: &str = "result.json";
const EXPORT_DIRECTORY: &str = "exports";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactSetError {
    Invalid,
    ResultFileUnavailable,
    CarrierLimitExceeded,
}

// Most invalid sets have no portable diagnostic equivalent without inspecting
// every boundary independently. The carrier count is unambiguous in metadata.
impl ArtifactSetError {
    pub(crate) fn code(self) -> Option<&'static str> {
        match self {
            Self::Invalid | Self::ResultFileUnavailable => None,
            Self::CarrierLimitExceeded => {
                Some(super::artifact_limits::CARRIER_LIMIT_DIAGNOSTIC_CODE)
            }
        }
    }
}

pub(crate) fn read_and_validate(
    root: &OwnedFd,
    maximum_result_bytes: u64,
) -> Result<WorkflowResultV1, ArtifactSetError> {
    read_and_validate_observing(root, maximum_result_bytes, || {}, |_| Ok(()))
}

pub(crate) fn read_and_validate_observing(
    root: &OwnedFd,
    maximum_result_bytes: u64,
    on_open: impl FnOnce(),
    on_bytes: impl FnOnce(&[u8]) -> Result<(), ArtifactSetError>,
) -> Result<WorkflowResultV1, ArtifactSetError> {
    let descriptor = openat(
        root,
        RESULT_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ArtifactSetError::ResultFileUnavailable)?;
    let before = fstat(&descriptor).map_err(|_| ArtifactSetError::ResultFileUnavailable)?;
    let before_size =
        u64::try_from(before.st_size).map_err(|_| ArtifactSetError::ResultFileUnavailable)?;
    if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile
        || before_size > maximum_result_bytes
    {
        return Err(ArtifactSetError::ResultFileUnavailable);
    }
    on_open();
    let mut file = File::from(descriptor);
    let bytes =
        artifact_primitives::read_bounded(&mut file, maximum_result_bytes, &AtomicBool::new(false))
            .map_err(|_| ArtifactSetError::ResultFileUnavailable)?;
    if u64::try_from(bytes.len()) != Ok(before_size)
        || artifact_primitives::retained_file_changed(root, RESULT_FILE, &file, &before)
    {
        return Err(ArtifactSetError::ResultFileUnavailable);
    }
    on_bytes(&bytes)?;
    let result = match result_metadata::decode(&bytes) {
        Ok(result) => result,
        Err(_) => {
            // Preserve the specific carrier diagnostic even when the general
            // metadata validator rejects an over-limit export map first.
            let document =
                result_metadata::decode_document(&bytes).map_err(|_| ArtifactSetError::Invalid)?;
            let result: WorkflowResultV1 =
                serde_json::from_value(document).map_err(|_| ArtifactSetError::Invalid)?;
            if carrier_metadata(&result).len() > MAXIMUM_CARRIERS {
                return Err(ArtifactSetError::CarrierLimitExceeded);
            }
            return Err(ArtifactSetError::Invalid);
        }
    };
    validate(root, &result)?;
    Ok(result)
}

pub(crate) fn validate(root: &OwnedFd, result: &WorkflowResultV1) -> Result<(), ArtifactSetError> {
    if carrier_metadata(result).len() > MAXIMUM_CARRIERS {
        return Err(ArtifactSetError::CarrierLimitExceeded);
    }
    result_metadata::validate(result).map_err(|_| ArtifactSetError::Invalid)?;
    if bounded_entry_names(root, MAXIMUM_ROOT_ENTRIES)?
        != BTreeSet::from([
            RESULT_FILE.as_bytes().to_vec(),
            EXPORT_DIRECTORY.as_bytes().to_vec(),
        ])
        || FileType::from_raw_mode(
            statat(root, RESULT_FILE, AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|_| ArtifactSetError::Invalid)?
                .st_mode,
        ) != FileType::RegularFile
    {
        return Err(ArtifactSetError::Invalid);
    }

    let exports = openat(
        root,
        EXPORT_DIRECTORY,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ArtifactSetError::Invalid)?;
    let named_exports = statat(root, EXPORT_DIRECTORY, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| ArtifactSetError::Invalid)?;
    if FileType::from_raw_mode(named_exports.st_mode) != FileType::Directory {
        return Err(ArtifactSetError::Invalid);
    }
    let opened_exports = fstat(&exports).map_err(|_| ArtifactSetError::Invalid)?;
    if !artifact_primitives::same_identity(&named_exports, &opened_exports) {
        return Err(ArtifactSetError::Invalid);
    }

    let carriers = carrier_metadata(result);
    let expected_names = carriers
        .keys()
        .map(|path| {
            path.strip_prefix("exports/")
                .map(|name| name.as_bytes().to_vec())
                .ok_or(ArtifactSetError::Invalid)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if bounded_entry_names(&exports, MAXIMUM_EXPORT_ENTRIES)? != expected_names {
        return Err(ArtifactSetError::Invalid);
    }

    let mut total_bytes = 0_u64;
    let mut git_budget = GitArtifactValidationBudget::default();
    for (path, metadata) in carriers {
        let name = path
            .strip_prefix("exports/")
            .ok_or(ArtifactSetError::Invalid)?;
        validate_carrier(&exports, name, &metadata, &mut total_bytes, &mut git_budget)?;
    }
    Ok(())
}

struct CarrierMetadata<'a> {
    kind: &'a str,
    size_bytes: u64,
    digest: &'a super::publication::DigestV1,
    git: Option<GitArtifactDescriptor<'a>>,
}

fn carrier_metadata(result: &WorkflowResultV1) -> BTreeMap<&str, CarrierMetadata<'_>> {
    result
        .exports
        .values()
        .filter_map(|export| match export {
            ExportV1::Available {
                kind,
                path,
                size_bytes,
                digest,
                ..
            } => Some((
                path.as_str(),
                CarrierMetadata {
                    kind,
                    size_bytes: *size_bytes,
                    digest,
                    git: None,
                },
            )),
            ExportV1::GitBranch {
                base_oid,
                head_oid,
                tree_oid,
                carrier: Some(carrier),
                ..
            } => Some((
                carrier.path.as_str(),
                CarrierMetadata {
                    kind: "git_branch",
                    size_bytes: carrier.size_bytes,
                    digest: &carrier.digest,
                    git: Some(GitArtifactDescriptor {
                        base_oid,
                        head_oid,
                        tree_oid,
                    }),
                },
            )),
            ExportV1::GitBranch { carrier: None, .. } | ExportV1::Unavailable { .. } => None,
        })
        .collect()
}

fn validate_carrier(
    exports: &OwnedFd,
    name: &str,
    metadata: &CarrierMetadata<'_>,
    total_bytes: &mut u64,
    git_budget: &mut GitArtifactValidationBudget,
) -> Result<(), ArtifactSetError> {
    let CarrierMetadata {
        kind,
        size_bytes,
        digest,
        git,
    } = metadata;
    let descriptor = openat(
        exports,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ArtifactSetError::Invalid)?;
    let before = fstat(&descriptor).map_err(|_| ArtifactSetError::Invalid)?;
    if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile {
        return Err(ArtifactSetError::Invalid);
    }

    let mut file = File::from(descriptor);
    let (observed, observed_digest) =
        artifact_primitives::hash_bounded(&mut file, total_bytes, &AtomicBool::new(false))
            .map_err(|_| ArtifactSetError::Invalid)?;
    if observed != *size_bytes || observed_digest != digest.value {
        return Err(ArtifactSetError::Invalid);
    }

    match *kind {
        "file" => {}
        "text" => {
            file.seek(SeekFrom::Start(0))
                .map_err(|_| ArtifactSetError::Invalid)?;
            artifact_primitives::validate_utf8(&mut file, &AtomicBool::new(false))
                .map_err(|_| ArtifactSetError::Invalid)?;
        }
        "json" => {
            file.seek(SeekFrom::Start(0))
                .map_err(|_| ArtifactSetError::Invalid)?;
            validate_canonical_json(&mut file)?;
        }
        "git_branch" => {
            let descriptor = git.ok_or(ArtifactSetError::Invalid)?;
            validate_git_bundle(&mut file, descriptor, git_budget, &AtomicBool::new(false))
                .map_err(|_| ArtifactSetError::Invalid)?;
        }
        _ => return Err(ArtifactSetError::Invalid),
    }

    if artifact_primitives::retained_file_changed(exports, name, &file, &before) {
        return Err(ArtifactSetError::Invalid);
    }
    Ok(())
}

fn validate_canonical_json(
    reader: &mut (impl Read + std::io::Seek),
) -> Result<(), ArtifactSetError> {
    super::artifact_json::validate(reader).map_err(|_| ArtifactSetError::Invalid)
}

fn bounded_entry_names(
    directory: &OwnedFd,
    maximum: usize,
) -> Result<BTreeSet<Vec<u8>>, ArtifactSetError> {
    artifact_primitives::enumerate_names(directory, maximum, &AtomicBool::new(false))
        .map_err(|_| ArtifactSetError::Invalid)?
        .ok_or(ArtifactSetError::Invalid)
}
