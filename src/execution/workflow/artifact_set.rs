use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read, Seek as _, SeekFrom};
use std::os::fd::OwnedFd;

use ring::digest::{Context as DigestContext, SHA256};
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, Stat, fstat, openat, statat};
use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::publication::{ExportV1, WorkflowResultV1};
use super::result_metadata;
use super::schema_common::lowercase_hex;

const RESULT_FILE: &str = "result.json";
const EXPORT_DIRECTORY: &str = "exports";
const MAXIMUM_ROOT_ENTRIES: usize = 4_097;
const MAXIMUM_EXPORT_ENTRIES: usize = 2_049;
const MAXIMUM_CARRIER_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_TOTAL_CARRIER_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const COPY_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactSetError;

pub(crate) fn read_and_validate(
    root: &OwnedFd,
    maximum_result_bytes: u64,
) -> Result<WorkflowResultV1, ArtifactSetError> {
    let descriptor = openat(
        root,
        RESULT_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ArtifactSetError)?;
    let before = fstat(&descriptor).map_err(|_| ArtifactSetError)?;
    let before_size = u64::try_from(before.st_size).map_err(|_| ArtifactSetError)?;
    if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile
        || before_size > maximum_result_bytes
    {
        return Err(ArtifactSetError);
    }
    let mut file = File::from(descriptor);
    let mut bytes = Vec::new();
    file.by_ref()
        .take(
            maximum_result_bytes
                .checked_add(1)
                .ok_or(ArtifactSetError)?,
        )
        .read_to_end(&mut bytes)
        .map_err(|_| ArtifactSetError)?;
    if u64::try_from(bytes.len()) != Ok(before_size) {
        return Err(ArtifactSetError);
    }
    validate_retained_regular_file(root, RESULT_FILE, &file, &before)?;
    let result = result_metadata::decode(&bytes).map_err(|_| ArtifactSetError)?;
    validate(root, &result)?;
    Ok(result)
}

pub(crate) fn validate(root: &OwnedFd, result: &WorkflowResultV1) -> Result<(), ArtifactSetError> {
    result_metadata::validate(result).map_err(|_| ArtifactSetError)?;
    if bounded_entry_names(root, MAXIMUM_ROOT_ENTRIES)?
        != BTreeSet::from([
            RESULT_FILE.as_bytes().to_vec(),
            EXPORT_DIRECTORY.as_bytes().to_vec(),
        ])
        || FileType::from_raw_mode(
            statat(root, RESULT_FILE, AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|_| ArtifactSetError)?
                .st_mode,
        ) != FileType::RegularFile
    {
        return Err(ArtifactSetError);
    }

    let exports = openat(
        root,
        EXPORT_DIRECTORY,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ArtifactSetError)?;
    let named_exports =
        statat(root, EXPORT_DIRECTORY, AtFlags::SYMLINK_NOFOLLOW).map_err(|_| ArtifactSetError)?;
    if FileType::from_raw_mode(named_exports.st_mode) != FileType::Directory {
        return Err(ArtifactSetError);
    }
    let opened_exports = fstat(&exports).map_err(|_| ArtifactSetError)?;
    if named_exports.st_dev != opened_exports.st_dev
        || named_exports.st_ino != opened_exports.st_ino
    {
        return Err(ArtifactSetError);
    }

    let carriers = carrier_metadata(result);
    let expected_names = carriers
        .keys()
        .map(|path| {
            path.strip_prefix("exports/")
                .map(|name| name.as_bytes().to_vec())
                .ok_or(ArtifactSetError)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if bounded_entry_names(&exports, MAXIMUM_EXPORT_ENTRIES)? != expected_names {
        return Err(ArtifactSetError);
    }

    let mut total_bytes = 0_u64;
    for (path, metadata) in carriers {
        let name = path.strip_prefix("exports/").ok_or(ArtifactSetError)?;
        validate_carrier(&exports, name, metadata, &mut total_bytes)?;
    }
    Ok(())
}

fn carrier_metadata(result: &WorkflowResultV1) -> BTreeMap<&str, &ExportV1> {
    result
        .exports
        .values()
        .filter_map(|export| match export {
            ExportV1::Available { path, .. } => Some((path.as_str(), export)),
            ExportV1::Unavailable { .. } => None,
        })
        .collect()
}

fn validate_carrier(
    exports: &OwnedFd,
    name: &str,
    metadata: &ExportV1,
    total_bytes: &mut u64,
) -> Result<(), ArtifactSetError> {
    let ExportV1::Available {
        kind,
        size_bytes,
        digest,
        ..
    } = metadata
    else {
        return Err(ArtifactSetError);
    };
    let descriptor = openat(
        exports,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ArtifactSetError)?;
    let before = fstat(&descriptor).map_err(|_| ArtifactSetError)?;
    if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile {
        return Err(ArtifactSetError);
    }

    let mut file = File::from(descriptor);
    let mut context = DigestContext::new(&SHA256);
    let mut observed = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let remaining = MAXIMUM_CARRIER_BYTES.saturating_sub(observed);
        let permitted = usize::try_from(
            remaining
                .saturating_add(1)
                .min(u64::try_from(buffer.len()).map_err(|_| ArtifactSetError)?),
        )
        .map_err(|_| ArtifactSetError)?;
        let read = file
            .read(&mut buffer[..permitted])
            .map_err(|_| ArtifactSetError)?;
        if read == 0 {
            break;
        }
        let read = u64::try_from(read).map_err(|_| ArtifactSetError)?;
        observed = observed.checked_add(read).ok_or(ArtifactSetError)?;
        if observed > MAXIMUM_CARRIER_BYTES {
            return Err(ArtifactSetError);
        }
        context.update(&buffer[..usize::try_from(read).map_err(|_| ArtifactSetError)?]);
    }
    *total_bytes = total_bytes.checked_add(observed).ok_or(ArtifactSetError)?;
    if *total_bytes > MAXIMUM_TOTAL_CARRIER_BYTES
        || observed != *size_bytes
        || lowercase_hex(context.finish().as_ref()) != digest.value
    {
        return Err(ArtifactSetError);
    }

    match kind.as_str() {
        "file" => {}
        "text" => {
            file.seek(SeekFrom::Start(0))
                .map_err(|_| ArtifactSetError)?;
            validate_utf8(&mut file)?;
        }
        "json" => {
            file.seek(SeekFrom::Start(0))
                .map_err(|_| ArtifactSetError)?;
            validate_canonical_json(&mut file)?;
        }
        _ => return Err(ArtifactSetError),
    }

    validate_retained_regular_file(exports, name, &file, &before)
}

fn validate_retained_regular_file(
    directory: &OwnedFd,
    name: &str,
    file: &File,
    before: &Stat,
) -> Result<(), ArtifactSetError> {
    let after = fstat(file).map_err(|_| ArtifactSetError)?;
    let named = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW).map_err(|_| ArtifactSetError)?;
    if FileType::from_raw_mode(named.st_mode) != FileType::RegularFile
        || before.st_dev != after.st_dev
        || before.st_ino != after.st_ino
        || before.st_size != after.st_size
        || before.st_dev != named.st_dev
        || before.st_ino != named.st_ino
    {
        return Err(ArtifactSetError);
    }
    Ok(())
}

fn validate_utf8(reader: &mut impl Read) -> Result<(), ArtifactSetError> {
    let mut pending = Vec::with_capacity(4);
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer).map_err(|_| ArtifactSetError)?;
        if read == 0 {
            return if pending.is_empty() {
                Ok(())
            } else {
                Err(ArtifactSetError)
            };
        }
        pending.extend_from_slice(&buffer[..read]);
        match std::str::from_utf8(&pending) {
            Ok(_) => pending.clear(),
            Err(error) if error.error_len().is_some() => return Err(ArtifactSetError),
            Err(error) => {
                let suffix = pending.split_off(error.valid_up_to());
                if suffix.len() > 3 {
                    return Err(ArtifactSetError);
                }
                pending = suffix;
            }
        }
    }
}

fn validate_canonical_json(reader: &mut impl Read) -> Result<(), ArtifactSetError> {
    let mut compact = CompactJsonReader::new(reader);
    let mut deserializer = serde_json::Deserializer::from_reader(&mut compact);
    OrderedUniqueValue::deserialize(&mut deserializer).map_err(|_| ArtifactSetError)?;
    deserializer.end().map_err(|_| ArtifactSetError)
}

fn bounded_entry_names(
    directory: &OwnedFd,
    maximum: usize,
) -> Result<BTreeSet<Vec<u8>>, ArtifactSetError> {
    let mut entries = BTreeSet::new();
    for entry in Dir::read_from(directory).map_err(|_| ArtifactSetError)? {
        let entry = entry.map_err(|_| ArtifactSetError)?;
        let name = entry.file_name().to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        if entries.len() == maximum {
            return Err(ArtifactSetError);
        }
        entries.insert(name.to_vec());
    }
    Ok(entries)
}

struct CompactJsonReader<Reader> {
    inner: Reader,
    in_string: bool,
    escaped: bool,
}

impl<Reader> CompactJsonReader<Reader> {
    fn new(inner: Reader) -> Self {
        Self {
            inner,
            in_string: false,
            escaped: false,
        }
    }
}

impl<Reader: Read> Read for CompactJsonReader<Reader> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(bytes)?;
        for byte in &bytes[..read] {
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if *byte == b'\\' {
                    self.escaped = true;
                } else if *byte == b'"' {
                    self.in_string = false;
                }
            } else if *byte == b'"' {
                self.in_string = true;
            } else if byte.is_ascii_whitespace() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "noncanonical JSON whitespace",
                ));
            }
        }
        Ok(read)
    }
}

struct OrderedUniqueValue;

impl<'de> Deserialize<'de> for OrderedUniqueValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(OrderedUniqueValueVisitor)
    }
}

struct OrderedUniqueValueVisitor;

impl<'de> Visitor<'de> for OrderedUniqueValueVisitor {
    type Value = OrderedUniqueValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("compact JSON with ordered unique object members")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(OrderedUniqueValue)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<OrderedUniqueValue>()?.is_some() {}
        Ok(OrderedUniqueValue)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut previous: Option<String> = None;
        while let Some(name) = map.next_key::<String>()? {
            if previous
                .as_ref()
                .is_some_and(|previous| previous.as_bytes() >= name.as_bytes())
            {
                return Err(A::Error::custom(
                    "unordered or duplicate JSON object member",
                ));
            }
            previous = Some(name);
            map.next_value::<OrderedUniqueValue>()?;
        }
        Ok(OrderedUniqueValue)
    }
}
