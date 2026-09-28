use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};

use ring::digest::{Context, SHA256};
use rustix::fs::{AtFlags, Dir, FileType, Stat, fstat, statat};

use super::artifact::{COPY_BUFFER_BYTES, COPY_BUFFER_BYTES_U64};
use super::artifact_limits::{MAXIMUM_CARRIER_BYTES, MAXIMUM_TOTAL_CARRIER_BYTES};
use super::schema_common::lowercase_hex;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PrimitiveFailure {
    Unavailable,
    Interrupted,
    LimitExceeded,
    TotalLimitExceeded,
    InvalidText,
}

fn check(cancelled: &AtomicBool) -> Result<(), PrimitiveFailure> {
    if cancelled.load(Ordering::Acquire) {
        Err(PrimitiveFailure::Interrupted)
    } else {
        Ok(())
    }
}

fn read_checked(
    reader: &mut impl Read,
    buffer: &mut [u8],
    cancelled: &AtomicBool,
) -> Result<usize, PrimitiveFailure> {
    check(cancelled)?;
    reader
        .read(buffer)
        .map_err(|_| PrimitiveFailure::Unavailable)
}

pub(super) fn read_bounded(
    file: &mut impl Read,
    maximum: u64,
    cancelled: &AtomicBool,
) -> Result<Vec<u8>, PrimitiveFailure> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let observed = u64::try_from(bytes.len()).map_err(|_| PrimitiveFailure::Unavailable)?;
        let permitted = maximum
            .saturating_sub(observed)
            .saturating_add(1)
            .min(COPY_BUFFER_BYTES_U64);
        let permitted = usize::try_from(permitted).map_err(|_| PrimitiveFailure::Unavailable)?;
        let read = read_checked(file, &mut buffer[..permitted], cancelled)?;
        if read == 0 {
            return Ok(bytes);
        }
        bytes.extend_from_slice(&buffer[..read]);
        if u64::try_from(bytes.len()).map_err(|_| PrimitiveFailure::Unavailable)? > maximum {
            return Err(PrimitiveFailure::LimitExceeded);
        }
    }
}

pub(super) fn hash_bounded(
    file: &mut impl Read,
    total_bytes: &mut u64,
    cancelled: &AtomicBool,
) -> Result<(u64, String), PrimitiveFailure> {
    let mut context = Context::new(&SHA256);
    let mut observed = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let per_remaining = MAXIMUM_CARRIER_BYTES.saturating_sub(observed);
        let total_remaining = MAXIMUM_TOTAL_CARRIER_BYTES.saturating_sub(*total_bytes);
        let permitted = per_remaining
            .min(total_remaining)
            .saturating_add(1)
            .min(COPY_BUFFER_BYTES_U64);
        let permitted = usize::try_from(permitted).map_err(|_| PrimitiveFailure::Unavailable)?;
        let read = read_checked(file, &mut buffer[..permitted], cancelled)?;
        if read == 0 {
            return Ok((observed, lowercase_hex(context.finish().as_ref())));
        }
        let read_bytes = u64::try_from(read).map_err(|_| PrimitiveFailure::Unavailable)?;
        observed = observed
            .checked_add(read_bytes)
            .ok_or(PrimitiveFailure::LimitExceeded)?;
        *total_bytes = total_bytes
            .checked_add(read_bytes)
            .ok_or(PrimitiveFailure::TotalLimitExceeded)?;
        if observed > MAXIMUM_CARRIER_BYTES {
            return Err(PrimitiveFailure::LimitExceeded);
        }
        if *total_bytes > MAXIMUM_TOTAL_CARRIER_BYTES {
            return Err(PrimitiveFailure::TotalLimitExceeded);
        }
        context.update(&buffer[..read]);
    }
}

pub(super) fn validate_utf8(
    reader: &mut impl Read,
    cancelled: &AtomicBool,
) -> Result<(), PrimitiveFailure> {
    let mut pending = Vec::with_capacity(4);
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = read_checked(reader, &mut buffer, cancelled)?;
        if read == 0 {
            return if pending.is_empty() {
                Ok(())
            } else {
                Err(PrimitiveFailure::InvalidText)
            };
        }
        pending.extend_from_slice(&buffer[..read]);
        match std::str::from_utf8(&pending) {
            Ok(_) => pending.clear(),
            Err(error) if error.error_len().is_some() => return Err(PrimitiveFailure::InvalidText),
            Err(error) => {
                let suffix = pending.split_off(error.valid_up_to());
                if suffix.len() > 3 {
                    return Err(PrimitiveFailure::InvalidText);
                }
                pending = suffix;
            }
        }
    }
}

pub(super) fn same_identity(left: &Stat, right: &Stat) -> bool {
    left.st_dev == right.st_dev && left.st_ino == right.st_ino
}

pub(super) fn retained_file_changed(
    directory: &OwnedFd,
    name: &str,
    file: &File,
    before: &Stat,
) -> bool {
    let Ok(after) = fstat(file) else {
        return true;
    };
    let Ok(named) = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) else {
        return true;
    };
    FileType::from_raw_mode(named.st_mode) != FileType::RegularFile
        || !same_identity(before, &after)
        || before.st_size != after.st_size
        || !same_identity(before, &named)
}

pub(super) fn enumerate_names(
    directory: &OwnedFd,
    maximum: usize,
    cancelled: &AtomicBool,
) -> Result<Option<BTreeSet<Vec<u8>>>, PrimitiveFailure> {
    let mut names = BTreeSet::new();
    for entry in Dir::read_from(directory).map_err(|_| PrimitiveFailure::Unavailable)? {
        check(cancelled)?;
        let entry = entry.map_err(|_| PrimitiveFailure::Unavailable)?;
        let name = entry.file_name().to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        names.insert(name.to_vec());
        if names.len() > maximum {
            return Ok(None);
        }
    }
    Ok(Some(names))
}
