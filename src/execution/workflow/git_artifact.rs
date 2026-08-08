use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use flate2::{Decompress, FlushDecompress, Status};
use ring::digest::{Context as DigestContext, SHA1_FOR_LEGACY_USE_ONLY};

use super::schema_common::is_lowercase_hex;

const MAXIMUM_BUNDLE_HEADER_BYTES: usize = 1024 * 1024;
const MAXIMUM_PACK_ENTRIES: usize = 1_000_000;
const MAXIMUM_INFLATED_GIT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_DELTA_DEPTH: u8 = 64;
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const PACK_CHECKSUM_BYTES: u64 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GitArtifactFailure {
    Header,
    Profile,
    Pack,
    Checksum,
    Content,
    StructureLimit,
    Unavailable,
    Scratch,
    Interrupted,
}

#[derive(Clone, Copy)]
pub(super) struct GitArtifactDescriptor<'a> {
    pub(super) base_oid: &'a str,
    pub(super) head_oid: &'a str,
    pub(super) tree_oid: &'a str,
}

#[derive(Default)]
pub(super) struct GitArtifactValidationBudget {
    inflated_bytes: u64,
    scratch_bytes: u64,
}

pub(super) fn validate_git_bundle(
    file: &mut File,
    descriptor: GitArtifactDescriptor<'_>,
    budget: &mut GitArtifactValidationBudget,
    cancelled: &AtomicBool,
) -> Result<(), GitArtifactFailure> {
    check_cancelled(cancelled)?;
    let body_offset = validate_bundle_header(file, descriptor)?;
    let scratch = tempfile::tempdir().map_err(|_| GitArtifactFailure::Scratch)?;
    let validation = (|| {
        let mut entries = parse_pack(file, body_offset, scratch.path(), budget, cancelled)?;
        resolve_and_validate_objects(&mut entries, descriptor, scratch.path(), budget, cancelled)
    })();
    let cleanup = scratch.close();
    match (validation, cleanup) {
        (Err(GitArtifactFailure::Interrupted), _) => Err(GitArtifactFailure::Interrupted),
        (_, Err(_)) => Err(GitArtifactFailure::Scratch),
        (validation, Ok(())) => validation,
    }
}

fn validate_bundle_header(
    file: &mut File,
    descriptor: GitArtifactDescriptor<'_>,
) -> Result<u64, GitArtifactFailure> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| GitArtifactFailure::Unavailable)?;
    let mut reader = BufReader::new(file);
    let mut total = 0_usize;
    let mut line = Vec::new();
    read_header_line(&mut reader, &mut line, &mut total)?;
    if line != b"# v2 git bundle\n" {
        return Err(GitArtifactFailure::Header);
    }

    let mut prerequisites = Vec::new();
    let mut references = Vec::new();
    let mut capabilities = 0_usize;
    loop {
        read_header_line(&mut reader, &mut line, &mut total)?;
        if line == b"\n" {
            break;
        }
        let content = line.strip_suffix(b"\n").ok_or(GitArtifactFailure::Header)?;
        if let Some(prerequisite) = content.strip_prefix(b"-") {
            if prerequisite.len() < 41
                || prerequisite.get(40) != Some(&b' ')
                || !valid_oid_bytes(&prerequisite[..40])
            {
                return Err(GitArtifactFailure::Header);
            }
            prerequisites.push(prerequisite[..40].to_vec());
        } else if content.starts_with(b"@") {
            if content.len() == 1 || content.iter().any(|byte| byte.is_ascii_control()) {
                return Err(GitArtifactFailure::Header);
            }
            capabilities = capabilities.saturating_add(1);
        } else {
            if content.len() < 42
                || content.get(40) != Some(&b' ')
                || !valid_oid_bytes(&content[..40])
                || !valid_refname(&content[41..])
            {
                return Err(GitArtifactFailure::Header);
            }
            references.push((content[..40].to_vec(), content[41..].to_vec()));
        }
    }
    let body_offset = reader
        .stream_position()
        .map_err(|_| GitArtifactFailure::Unavailable)?;
    reader
        .seek(SeekFrom::Start(body_offset))
        .map_err(|_| GitArtifactFailure::Unavailable)?;

    if capabilities != 0
        || prerequisites.as_slice() != [descriptor.base_oid.as_bytes()]
        || references.as_slice()
            != [(
                descriptor.head_oid.as_bytes().to_vec(),
                b"refs/scherzo/head".to_vec(),
            )]
    {
        return Err(GitArtifactFailure::Profile);
    }
    Ok(body_offset)
}

fn read_header_line(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
    total: &mut usize,
) -> Result<(), GitArtifactFailure> {
    line.clear();
    let remaining = MAXIMUM_BUNDLE_HEADER_BYTES
        .checked_sub(*total)
        .ok_or(GitArtifactFailure::StructureLimit)?;
    if remaining == 0 {
        return Err(GitArtifactFailure::StructureLimit);
    }
    let read = reader
        .take(u64::try_from(remaining).map_err(|_| GitArtifactFailure::StructureLimit)?)
        .read_until(b'\n', line)
        .map_err(|_| GitArtifactFailure::Unavailable)?;
    if read == 0 || line.last() != Some(&b'\n') {
        return Err(if *total + read >= MAXIMUM_BUNDLE_HEADER_BYTES {
            GitArtifactFailure::StructureLimit
        } else {
            GitArtifactFailure::Header
        });
    }
    *total = total
        .checked_add(read)
        .ok_or(GitArtifactFailure::StructureLimit)?;
    Ok(())
}

fn valid_oid_bytes(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok_and(|oid| is_lowercase_hex(oid, 40))
}

fn valid_refname(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && bytes != b"@"
        && !bytes.starts_with(b"/")
        && !bytes.ends_with(b"/")
        && !bytes.ends_with(b".")
        && !bytes
            .windows(2)
            .any(|pair| pair == b".." || pair == b"//" || pair == b"@{")
        && bytes.split(|byte| *byte == b'/').all(|component| {
            !component.is_empty() && !component.starts_with(b".") && !component.ends_with(b".lock")
        })
        && !bytes.iter().any(|byte| {
            byte.is_ascii_control()
                || byte.is_ascii_whitespace()
                || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        })
}

impl GitArtifactValidationBudget {
    fn add_inflated(&mut self, bytes: u64) -> Result<(), GitArtifactFailure> {
        self.inflated_bytes = bounded_add(self.inflated_bytes, bytes)?;
        self.scratch_bytes = bounded_add(self.scratch_bytes, bytes)?;
        Ok(())
    }

    fn add_reconstructed(&mut self, bytes: u64) -> Result<(), GitArtifactFailure> {
        self.inflated_bytes = bounded_add(self.inflated_bytes, bytes)?;
        self.scratch_bytes = bounded_add(self.scratch_bytes, bytes)?;
        Ok(())
    }
}

fn bounded_add(current: u64, bytes: u64) -> Result<u64, GitArtifactFailure> {
    current
        .checked_add(bytes)
        .filter(|total| *total <= MAXIMUM_INFLATED_GIT_BYTES)
        .ok_or(GitArtifactFailure::StructureLimit)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectKind {
    Commit,
    Tree,
    Blob,
    Tag,
}

impl ObjectKind {
    fn from_pack_type(kind: u8) -> Option<Self> {
        match kind {
            1 => Some(Self::Commit),
            2 => Some(Self::Tree),
            3 => Some(Self::Blob),
            4 => Some(Self::Tag),
            _ => None,
        }
    }

    fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Commit => b"commit",
            Self::Tree => b"tree",
            Self::Blob => b"blob",
            Self::Tag => b"tag",
        }
    }
}

struct ObjectRecord {
    kind: ObjectKind,
    path: PathBuf,
    size: u64,
    oid: [u8; 20],
    depth: u8,
}

#[derive(Clone, Copy)]
enum DeltaBase {
    Entry(usize),
    Oid([u8; 20]),
}

struct DeltaRecord {
    base: DeltaBase,
    path: PathBuf,
    minimum_depth: u8,
}

enum PackEntry {
    Object(ObjectRecord),
    Delta(DeltaRecord),
}

impl PackEntry {
    fn minimum_depth(&self) -> u8 {
        match self {
            Self::Object(object) => object.depth,
            Self::Delta(delta) => delta.minimum_depth,
        }
    }
}

fn parse_pack(
    file: &mut File,
    body_offset: u64,
    scratch: &Path,
    budget: &mut GitArtifactValidationBudget,
    cancelled: &AtomicBool,
) -> Result<Vec<PackEntry>, GitArtifactFailure> {
    let length = file
        .metadata()
        .map_err(|_| GitArtifactFailure::Unavailable)?
        .len();
    let checksum_offset = length
        .checked_sub(PACK_CHECKSUM_BYTES)
        .filter(|offset| *offset >= body_offset.saturating_add(12))
        .ok_or(GitArtifactFailure::Pack)?;
    file.seek(SeekFrom::Start(body_offset))
        .map_err(|_| GitArtifactFailure::Unavailable)?;
    let mut reader = PackReader {
        file,
        position: body_offset,
        checksum_offset,
        digest: DigestContext::new(&SHA1_FOR_LEGACY_USE_ONLY),
    };
    let mut header = [0_u8; 12];
    reader.read_exact_hashed(&mut header)?;
    let version = u32::from_be_bytes(
        header[4..8]
            .try_into()
            .map_err(|_| GitArtifactFailure::Pack)?,
    );
    let count = usize::try_from(u32::from_be_bytes(
        header[8..12]
            .try_into()
            .map_err(|_| GitArtifactFailure::Pack)?,
    ))
    .map_err(|_| GitArtifactFailure::StructureLimit)?;
    if &header[..4] != b"PACK"
        || !matches!(version, 2 | 3)
        || !(1..=MAXIMUM_PACK_ENTRIES).contains(&count)
    {
        return Err(if count > MAXIMUM_PACK_ENTRIES {
            GitArtifactFailure::StructureLimit
        } else {
            GitArtifactFailure::Pack
        });
    }

    let mut entries: Vec<PackEntry> = Vec::with_capacity(count);
    let mut offsets = BTreeMap::new();
    for index in 0..count {
        check_cancelled(cancelled)?;
        let entry_offset = reader.position;
        let (kind, declared_size) = read_entry_header(&mut reader)?;
        let base = match kind {
            6 => {
                let distance = read_offset_distance(&mut reader)?;
                let base_offset = entry_offset
                    .checked_sub(distance)
                    .ok_or(GitArtifactFailure::Pack)?;
                let base_index = offsets
                    .get(&base_offset)
                    .copied()
                    .ok_or(GitArtifactFailure::Pack)?;
                Some(DeltaBase::Entry(base_index))
            }
            7 => {
                let mut oid = [0_u8; 20];
                reader.read_exact_hashed(&mut oid)?;
                Some(DeltaBase::Oid(oid))
            }
            _ => None,
        };
        let inflated_path = scratch.join(format!("inflated-{index:07}"));
        let inflated = create_scratch_file(&inflated_path)?;
        let observed = reader.inflate_to(inflated, declared_size, budget, cancelled)?;
        if observed != declared_size {
            return Err(GitArtifactFailure::Pack);
        }
        offsets.insert(entry_offset, index);
        let entry = match (ObjectKind::from_pack_type(kind), base) {
            (Some(kind), None) => PackEntry::Object(object_record(
                kind,
                inflated_path,
                declared_size,
                0,
                cancelled,
            )?),
            (None, Some(base)) => {
                validate_delta_program(&inflated_path, cancelled)?;
                let minimum_depth = match base {
                    DeltaBase::Entry(base) => entries[base].minimum_depth(),
                    DeltaBase::Oid(_) => 0,
                }
                .checked_add(1)
                .filter(|depth| *depth <= MAXIMUM_DELTA_DEPTH)
                .ok_or(GitArtifactFailure::StructureLimit)?;
                PackEntry::Delta(DeltaRecord {
                    base,
                    path: inflated_path,
                    minimum_depth,
                })
            }
            _ => return Err(GitArtifactFailure::Pack),
        };
        entries.push(entry);
    }
    reader.finish()?;
    Ok(entries)
}

fn create_scratch_file(path: &Path) -> Result<File, GitArtifactFailure> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| GitArtifactFailure::Scratch)
}

struct PackReader<'a> {
    file: &'a mut File,
    position: u64,
    checksum_offset: u64,
    digest: DigestContext,
}

impl PackReader<'_> {
    fn read_exact_hashed(&mut self, bytes: &mut [u8]) -> Result<(), GitArtifactFailure> {
        let length = u64::try_from(bytes.len()).map_err(|_| GitArtifactFailure::StructureLimit)?;
        if self
            .position
            .checked_add(length)
            .is_none_or(|end| end > self.checksum_offset)
        {
            return Err(GitArtifactFailure::Pack);
        }
        self.file
            .read_exact(bytes)
            .map_err(|_| GitArtifactFailure::Pack)?;
        self.digest.update(bytes);
        self.position += length;
        Ok(())
    }

    fn read_byte(&mut self) -> Result<u8, GitArtifactFailure> {
        let mut byte = [0_u8; 1];
        self.read_exact_hashed(&mut byte)?;
        Ok(byte[0])
    }

    fn inflate_to(
        &mut self,
        mut destination: File,
        expected: u64,
        budget: &mut GitArtifactValidationBudget,
        cancelled: &AtomicBool,
    ) -> Result<u64, GitArtifactFailure> {
        let mut decompressor = Decompress::new(true);
        let mut input = [0_u8; COPY_BUFFER_BYTES];
        let mut output = [0_u8; COPY_BUFFER_BYTES];
        let mut observed = 0_u64;
        loop {
            check_cancelled(cancelled)?;
            let available = self.checksum_offset.saturating_sub(self.position);
            if available == 0 {
                return Err(GitArtifactFailure::Pack);
            }
            let requested = usize::try_from(available.min(COPY_BUFFER_BYTES as u64))
                .map_err(|_| GitArtifactFailure::StructureLimit)?;
            let start = self.position;
            let read = self
                .file
                .read(&mut input[..requested])
                .map_err(|_| GitArtifactFailure::Unavailable)?;
            if read == 0 {
                return Err(GitArtifactFailure::Pack);
            }
            let before_input = decompressor.total_in();
            let before_output = decompressor.total_out();
            let status = decompressor
                .decompress(&input[..read], &mut output, FlushDecompress::None)
                .map_err(|_| GitArtifactFailure::Pack)?;
            let consumed = usize::try_from(decompressor.total_in() - before_input)
                .map_err(|_| GitArtifactFailure::StructureLimit)?;
            let produced = usize::try_from(decompressor.total_out() - before_output)
                .map_err(|_| GitArtifactFailure::StructureLimit)?;
            if consumed == 0 && produced == 0 {
                return Err(GitArtifactFailure::Pack);
            }
            self.digest.update(&input[..consumed]);
            self.position = start
                .checked_add(
                    u64::try_from(consumed).map_err(|_| GitArtifactFailure::StructureLimit)?,
                )
                .ok_or(GitArtifactFailure::StructureLimit)?;
            self.file
                .seek(SeekFrom::Start(self.position))
                .map_err(|_| GitArtifactFailure::Unavailable)?;
            if produced != 0 {
                let produced =
                    u64::try_from(produced).map_err(|_| GitArtifactFailure::StructureLimit)?;
                let updated = observed
                    .checked_add(produced)
                    .filter(|updated| *updated <= expected)
                    .ok_or(GitArtifactFailure::Pack)?;
                budget.add_inflated(produced)?;
                destination
                    .write_all(
                        &output[..usize::try_from(produced)
                            .map_err(|_| GitArtifactFailure::StructureLimit)?],
                    )
                    .map_err(|_| GitArtifactFailure::Scratch)?;
                observed = updated;
            }
            if status == Status::StreamEnd {
                destination
                    .flush()
                    .map_err(|_| GitArtifactFailure::Scratch)?;
                return Ok(observed);
            }
        }
    }

    fn finish(self) -> Result<(), GitArtifactFailure> {
        if self.position != self.checksum_offset {
            return Err(GitArtifactFailure::Pack);
        }
        let expected = self.digest.finish();
        let mut checksum = [0_u8; 20];
        self.file
            .read_exact(&mut checksum)
            .map_err(|_| GitArtifactFailure::Pack)?;
        let mut trailing = [0_u8; 1];
        if self
            .file
            .read(&mut trailing)
            .map_err(|_| GitArtifactFailure::Unavailable)?
            != 0
        {
            return Err(GitArtifactFailure::Pack);
        }
        if expected.as_ref() != checksum {
            return Err(GitArtifactFailure::Checksum);
        }
        Ok(())
    }
}

fn read_entry_header(reader: &mut PackReader<'_>) -> Result<(u8, u64), GitArtifactFailure> {
    let mut byte = reader.read_byte()?;
    let kind = (byte >> 4) & 0x07;
    let mut size = u64::from(byte & 0x0f);
    let mut shift = 4_u32;
    while byte & 0x80 != 0 {
        if shift >= 64 {
            return Err(GitArtifactFailure::StructureLimit);
        }
        byte = reader.read_byte()?;
        size = size
            .checked_add(checked_shift(u64::from(byte & 0x7f), shift)?)
            .ok_or(GitArtifactFailure::StructureLimit)?;
        shift += 7;
    }
    if !matches!(kind, 1 | 2 | 3 | 4 | 6 | 7) {
        return Err(GitArtifactFailure::Pack);
    }
    Ok((kind, size))
}

fn read_offset_distance(reader: &mut PackReader<'_>) -> Result<u64, GitArtifactFailure> {
    let mut byte = reader.read_byte()?;
    let mut distance = u64::from(byte & 0x7f);
    while byte & 0x80 != 0 {
        byte = reader.read_byte()?;
        distance = distance
            .checked_add(1)
            .and_then(|value| value.checked_shl(7))
            .and_then(|value| value.checked_add(u64::from(byte & 0x7f)))
            .ok_or(GitArtifactFailure::StructureLimit)?;
    }
    (distance != 0)
        .then_some(distance)
        .ok_or(GitArtifactFailure::Pack)
}

fn object_record(
    kind: ObjectKind,
    path: PathBuf,
    size: u64,
    depth: u8,
    cancelled: &AtomicBool,
) -> Result<ObjectRecord, GitArtifactFailure> {
    let oid = hash_object(kind, &path, size, cancelled)?;
    Ok(ObjectRecord {
        kind,
        path,
        size,
        oid,
        depth,
    })
}

fn hash_object(
    kind: ObjectKind,
    path: &Path,
    size: u64,
    cancelled: &AtomicBool,
) -> Result<[u8; 20], GitArtifactFailure> {
    let mut context = DigestContext::new(&SHA1_FOR_LEGACY_USE_ONLY);
    context.update(kind.as_bytes());
    context.update(b" ");
    context.update(size.to_string().as_bytes());
    context.update(b"\0");
    let mut source = File::open(path).map_err(|_| GitArtifactFailure::Scratch)?;
    let mut observed = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        check_cancelled(cancelled)?;
        let read = source
            .read(&mut buffer)
            .map_err(|_| GitArtifactFailure::Scratch)?;
        if read == 0 {
            break;
        }
        context.update(&buffer[..read]);
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| GitArtifactFailure::StructureLimit)?)
            .ok_or(GitArtifactFailure::StructureLimit)?;
    }
    if observed != size {
        return Err(GitArtifactFailure::Pack);
    }
    let digest = context.finish();
    let mut oid = [0_u8; 20];
    oid.copy_from_slice(digest.as_ref());
    Ok(oid)
}

fn resolve_and_validate_objects(
    entries: &mut [PackEntry],
    descriptor: GitArtifactDescriptor<'_>,
    scratch: &Path,
    budget: &mut GitArtifactValidationBudget,
    cancelled: &AtomicBool,
) -> Result<(), GitArtifactFailure> {
    let mut objects = entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| match entry {
            PackEntry::Object(object) => Some((object.oid, index)),
            PackEntry::Delta(_) => None,
        })
        .collect::<BTreeMap<_, _>>();

    loop {
        let mut progress = false;
        for index in 0..entries.len() {
            check_cancelled(cancelled)?;
            let base_index = match &entries[index] {
                PackEntry::Object(_) => continue,
                PackEntry::Delta(delta) => match delta.base {
                    DeltaBase::Entry(base) => Some(base),
                    DeltaBase::Oid(oid) => objects.get(&oid).copied(),
                },
            };
            let Some(base_index) = base_index else {
                continue;
            };
            let PackEntry::Object(base) = &entries[base_index] else {
                continue;
            };
            let base_kind = base.kind;
            let base_path = base.path.clone();
            let base_size = base.size;
            let depth = base
                .depth
                .checked_add(1)
                .filter(|depth| *depth <= MAXIMUM_DELTA_DEPTH)
                .ok_or(GitArtifactFailure::StructureLimit)?;
            let PackEntry::Delta(delta) = &entries[index] else {
                continue;
            };
            let output_path = scratch.join(format!("object-{index:07}"));
            let result_size = apply_delta(
                &base_path,
                base_size,
                &delta.path,
                &output_path,
                budget,
                cancelled,
            )?;
            let object = object_record(base_kind, output_path, result_size, depth, cancelled)?;
            objects.insert(object.oid, index);
            entries[index] = PackEntry::Object(object);
            progress = true;
        }
        if !progress {
            break;
        }
    }

    let unresolved = entries
        .iter()
        .filter(|entry| matches!(entry, PackEntry::Delta(_)))
        .count();
    let head = parse_hex_oid(descriptor.head_oid).ok_or(GitArtifactFailure::Content)?;
    if let Some(index) = objects.get(&head).copied() {
        let PackEntry::Object(head) = &entries[index] else {
            return Err(GitArtifactFailure::Content);
        };
        validate_head_object(head, descriptor.tree_oid)?;
    } else if unresolved == 0 {
        return Err(GitArtifactFailure::Content);
    }
    Ok(())
}

fn validate_delta_program(
    delta_path: &Path,
    cancelled: &AtomicBool,
) -> Result<(), GitArtifactFailure> {
    let mut program = DeltaProgramReader::open(delta_path)?;
    while let Some(command) = program.next_command(cancelled)? {
        if let DeltaCommand::Insert(length) = command {
            program
                .delta
                .copy_exact(&mut std::io::sink(), length, cancelled)?;
        }
    }
    Ok(())
}

fn apply_delta(
    base_path: &Path,
    base_size: u64,
    delta_path: &Path,
    output_path: &Path,
    budget: &mut GitArtifactValidationBudget,
    cancelled: &AtomicBool,
) -> Result<u64, GitArtifactFailure> {
    let mut program = DeltaProgramReader::open(delta_path)?;
    if program.base_size != base_size {
        return Err(GitArtifactFailure::Pack);
    }
    budget.add_reconstructed(program.result_size)?;
    let mut output = create_scratch_file(output_path)?;
    while let Some(command) = program.next_command(cancelled)? {
        match command {
            DeltaCommand::Insert(length) => {
                program.delta.copy_exact(&mut output, length, cancelled)?;
            }
            DeltaCommand::Copy { offset, length } => {
                copy_base_range(base_path, offset, length, &mut output, cancelled)?;
            }
        }
    }
    output.flush().map_err(|_| GitArtifactFailure::Scratch)?;
    Ok(program.result_size)
}

enum DeltaCommand {
    Insert(u64),
    Copy { offset: u64, length: u64 },
}

struct DeltaProgramReader {
    delta: DeltaReader,
    base_size: u64,
    result_size: u64,
    produced: u64,
}

impl DeltaProgramReader {
    fn open(path: &Path) -> Result<Self, GitArtifactFailure> {
        let file = File::open(path).map_err(|_| GitArtifactFailure::Scratch)?;
        let mut delta = DeltaReader::new(file)?;
        let base_size = delta.read_varint()?;
        let result_size = delta.read_varint()?;
        Ok(Self {
            delta,
            base_size,
            result_size,
            produced: 0,
        })
    }

    fn next_command(
        &mut self,
        cancelled: &AtomicBool,
    ) -> Result<Option<DeltaCommand>, GitArtifactFailure> {
        check_cancelled(cancelled)?;
        if self.delta.remaining == 0 {
            return (self.produced == self.result_size)
                .then_some(None)
                .ok_or(GitArtifactFailure::Pack);
        }

        let command = self.delta.read_byte()?;
        if command == 0 {
            return Err(GitArtifactFailure::Pack);
        }
        if command & 0x80 == 0 {
            let length = u64::from(command);
            self.add_produced(length)?;
            return Ok(Some(DeltaCommand::Insert(length)));
        }

        let mut offset = 0_u64;
        for byte_index in 0..4_u32 {
            if command & (1 << byte_index) != 0 {
                offset |= u64::from(self.delta.read_byte()?) << (byte_index * 8);
            }
        }
        let mut length = 0_u64;
        for byte_index in 0..3_u32 {
            if command & (1 << (byte_index + 4)) != 0 {
                length |= u64::from(self.delta.read_byte()?) << (byte_index * 8);
            }
        }
        if length == 0 {
            length = 0x1_0000;
        }
        if offset
            .checked_add(length)
            .is_none_or(|end| end > self.base_size)
        {
            return Err(GitArtifactFailure::Pack);
        }
        self.add_produced(length)?;
        Ok(Some(DeltaCommand::Copy { offset, length }))
    }

    fn add_produced(&mut self, length: u64) -> Result<(), GitArtifactFailure> {
        self.produced = self
            .produced
            .checked_add(length)
            .filter(|produced| *produced <= self.result_size)
            .ok_or(GitArtifactFailure::Pack)?;
        Ok(())
    }
}

struct DeltaReader {
    file: File,
    remaining: u64,
}

impl DeltaReader {
    fn new(file: File) -> Result<Self, GitArtifactFailure> {
        let remaining = file
            .metadata()
            .map_err(|_| GitArtifactFailure::Scratch)?
            .len();
        Ok(Self { file, remaining })
    }

    fn read_byte(&mut self) -> Result<u8, GitArtifactFailure> {
        if self.remaining == 0 {
            return Err(GitArtifactFailure::Pack);
        }
        let mut byte = [0_u8; 1];
        self.file
            .read_exact(&mut byte)
            .map_err(|_| GitArtifactFailure::Scratch)?;
        self.remaining -= 1;
        Ok(byte[0])
    }

    fn read_varint(&mut self) -> Result<u64, GitArtifactFailure> {
        let mut value = 0_u64;
        let mut shift = 0_u32;
        loop {
            if shift >= 64 {
                return Err(GitArtifactFailure::StructureLimit);
            }
            let byte = self.read_byte()?;
            value = value
                .checked_add(checked_shift(u64::from(byte & 0x7f), shift)?)
                .ok_or(GitArtifactFailure::StructureLimit)?;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    fn copy_exact(
        &mut self,
        destination: &mut impl Write,
        length: u64,
        cancelled: &AtomicBool,
    ) -> Result<(), GitArtifactFailure> {
        if length > self.remaining {
            return Err(GitArtifactFailure::Pack);
        }
        copy_exact_bounded(&mut self.file, destination, length, cancelled)?;
        self.remaining -= length;
        Ok(())
    }
}

fn copy_base_range(
    path: &Path,
    offset: u64,
    length: u64,
    destination: &mut File,
    cancelled: &AtomicBool,
) -> Result<(), GitArtifactFailure> {
    let mut source = File::open(path).map_err(|_| GitArtifactFailure::Scratch)?;
    source
        .seek(SeekFrom::Start(offset))
        .map_err(|_| GitArtifactFailure::Scratch)?;
    copy_exact_bounded(&mut source, destination, length, cancelled)
}

fn copy_exact_bounded(
    source: &mut File,
    destination: &mut impl Write,
    mut remaining: u64,
    cancelled: &AtomicBool,
) -> Result<(), GitArtifactFailure> {
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    while remaining != 0 {
        check_cancelled(cancelled)?;
        let requested = usize::try_from(remaining.min(COPY_BUFFER_BYTES as u64))
            .map_err(|_| GitArtifactFailure::StructureLimit)?;
        let read = source
            .read(&mut buffer[..requested])
            .map_err(|_| GitArtifactFailure::Scratch)?;
        if read == 0 {
            return Err(GitArtifactFailure::Pack);
        }
        destination
            .write_all(&buffer[..read])
            .map_err(|_| GitArtifactFailure::Scratch)?;
        remaining -= u64::try_from(read).map_err(|_| GitArtifactFailure::StructureLimit)?;
    }
    Ok(())
}

fn validate_head_object(
    head: &ObjectRecord,
    expected_tree: &str,
) -> Result<(), GitArtifactFailure> {
    if head.kind != ObjectKind::Commit {
        return Err(GitArtifactFailure::Content);
    }
    let commit = File::open(&head.path).map_err(|_| GitArtifactFailure::Scratch)?;
    let mut first_line = Vec::new();
    commit
        .take(128)
        .read_to_end(&mut first_line)
        .map_err(|_| GitArtifactFailure::Scratch)?;
    let end = first_line
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or(GitArtifactFailure::Content)?;
    let expected = format!("tree {expected_tree}");
    if &first_line[..end] != expected.as_bytes() {
        return Err(GitArtifactFailure::Content);
    }
    Ok(())
}

fn checked_shift(value: u64, shift: u32) -> Result<u64, GitArtifactFailure> {
    if shift >= 64 || value > (u64::MAX >> shift) {
        return Err(GitArtifactFailure::StructureLimit);
    }
    Ok(value << shift)
}

fn parse_hex_oid(value: &str) -> Option<[u8; 20]> {
    if !is_lowercase_hex(value, 40) {
        return None;
    }
    let mut oid = [0_u8; 20];
    for (index, destination) in oid.iter_mut().enumerate() {
        let offset = index * 2;
        *destination = u8::from_str_radix(&value[offset..offset + 2], 16).ok()?;
    }
    Some(oid)
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), GitArtifactFailure> {
    if cancelled.load(Ordering::Acquire) {
        Err(GitArtifactFailure::Interrupted)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundle_profile_ignores_an_empty_prerequisite_comment() {
        let baseline = "0".repeat(40);
        let head = "1".repeat(40);
        let tree = "2".repeat(40);
        let header = format!("# v2 git bundle\n-{baseline} \n{head} refs/scherzo/head\n\n");
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("carrier.bundle");
        std::fs::write(&path, &header).unwrap();
        let mut file = File::open(path).unwrap();

        assert_eq!(
            validate_bundle_header(
                &mut file,
                GitArtifactDescriptor {
                    base_oid: &baseline,
                    head_oid: &head,
                    tree_oid: &tree,
                },
            ),
            Ok(u64::try_from(header.len()).unwrap()),
        );
    }
}
