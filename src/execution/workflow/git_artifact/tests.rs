use std::fs;
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::Path;

use flate2::{Compression, Decompress, FlushDecompress, Status, write::ZlibEncoder};
use ring::digest::{Context as DigestContext, SHA1_FOR_LEGACY_USE_ONLY, digest};

use super::*;

#[derive(Clone, Copy)]
pub(in crate::execution::workflow) enum BundleMutation {
    InvalidHeader,
    MismatchedProfile,
    TruncatedPack,
    BadChecksum,
    OversizedObject,
    OverdeepDeltaChain,
    ExternalDelta,
    MissingHeadObject,
    HeadReplacedByExternalDelta,
}

pub(in crate::execution::workflow) struct RealBundleFixture {
    _temporary: tempfile::TempDir,
    bytes: Vec<u8>,
    body_offset: usize,
    base_oid: String,
    head_oid: String,
    tree_oid: String,
}

impl RealBundleFixture {
    pub(in crate::execution::workflow) fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        run_git(None, &["init", "--quiet", repository.to_str().unwrap()]);
        run_git(Some(&repository), &["config", "user.name", "Scherzo Test"]);
        run_git(
            Some(&repository),
            &["config", "user.email", "test@example.invalid"],
        );

        fs::write(repository.join("tracked.txt"), b"baseline\n").unwrap();
        run_git(Some(&repository), &["add", "tracked.txt"]);
        run_git(Some(&repository), &["commit", "--quiet", "-m", "baseline"]);
        let base_oid = git_output(&repository, &["rev-parse", "HEAD"]);

        fs::write(repository.join("tracked.txt"), b"changed\n").unwrap();
        fs::write(repository.join("alpha.txt"), b"similar content alpha\n").unwrap();
        fs::write(repository.join("beta.txt"), b"similar content beta\n").unwrap();
        run_git(Some(&repository), &["add", "."]);
        run_git(Some(&repository), &["commit", "--quiet", "-m", "head"]);
        let head_oid = git_output(&repository, &["rev-parse", "HEAD"]);
        let tree_oid = git_output(&repository, &["rev-parse", "HEAD^{tree}"]);
        run_git(
            Some(&repository),
            &["update-ref", "refs/scherzo/head", &head_oid],
        );

        let bundle_path = temporary.path().join("carrier.bundle");
        let exclusion = format!("^{base_oid}");
        run_git(
            Some(&repository),
            &[
                "-c",
                "pack.window=0",
                "-c",
                "pack.depth=0",
                "bundle",
                "create",
                "--version=2",
                bundle_path.to_str().unwrap(),
                "refs/scherzo/head",
                &exclusion,
            ],
        );
        let bytes = fs::read(bundle_path).unwrap();
        let body_offset = bytes
            .windows(2)
            .position(|pair| pair == b"\n\n")
            .map(|offset| offset + 2)
            .unwrap();

        Self {
            _temporary: temporary,
            bytes,
            body_offset,
            base_oid,
            head_oid,
            tree_oid,
        }
    }

    pub(in crate::execution::workflow) fn failure(
        &self,
        mutation: BundleMutation,
    ) -> GitArtifactFailure {
        self.validate(&self.mutated(mutation)).unwrap_err()
    }

    fn validate(&self, bytes: &[u8]) -> Result<(), GitArtifactFailure> {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        validate_git_bundle(
            &mut file,
            self.descriptor(),
            &mut GitArtifactValidationBudget::default(),
            &AtomicBool::new(false),
        )
    }

    fn descriptor(&self) -> GitArtifactDescriptor<'_> {
        GitArtifactDescriptor {
            base_oid: &self.base_oid,
            head_oid: &self.head_oid,
            tree_oid: &self.tree_oid,
        }
    }

    fn mutated(&self, mutation: BundleMutation) -> Vec<u8> {
        let mut bytes = self.bytes.clone();
        match mutation {
            BundleMutation::InvalidHeader => {
                bytes[0] = b'!';
            }
            BundleMutation::MismatchedProfile => {
                let reference = b"refs/scherzo/head";
                let offset = bytes[..self.body_offset]
                    .windows(reference.len())
                    .position(|window| window == reference)
                    .unwrap();
                bytes[offset + reference.len() - 1] = b'x';
            }
            BundleMutation::TruncatedPack => {
                bytes.pop().unwrap();
            }
            BundleMutation::BadChecksum => {
                let checksum_byte = bytes.last_mut().unwrap();
                *checksum_byte ^= 0xff;
            }
            BundleMutation::OversizedObject => {
                let entries = parse_normal_entries(&bytes, self.body_offset);
                let entry = entries.first().unwrap();
                let replacement = encode_pack_entry_header(
                    entry.kind,
                    MAXIMUM_INFLATED_GIT_BYTES.saturating_add(1),
                );
                bytes.splice(entry.start..entry.data_start, replacement);
                rewrite_pack_checksum(&mut bytes, self.body_offset);
            }
            BundleMutation::OverdeepDeltaChain => {
                append_external_delta_chain(
                    &mut bytes,
                    self.body_offset,
                    usize::from(MAXIMUM_DELTA_DEPTH),
                );
            }
            BundleMutation::ExternalDelta => {
                append_external_delta_chain(&mut bytes, self.body_offset, 0);
            }
            BundleMutation::MissingHeadObject | BundleMutation::HeadReplacedByExternalDelta => {
                let entries = parse_normal_entries(&bytes, self.body_offset);
                let head = parse_hex_oid(&self.head_oid).unwrap();
                let entry = entries.iter().find(|entry| entry.oid == head).unwrap();
                let replacement = if matches!(mutation, BundleMutation::MissingHeadObject) {
                    normal_object_entry(3, b"replacement blob\n")
                } else {
                    external_delta_entry()
                };
                bytes.splice(entry.start..entry.end, replacement);
                rewrite_pack_checksum(&mut bytes, self.body_offset);
            }
        }
        bytes
    }

    fn with_resolvable_delta(&self, reference_base_by_oid: bool) -> Vec<u8> {
        let mut bytes = self.bytes.clone();
        let entries = parse_normal_entries(&bytes, self.body_offset);
        let blobs = entries
            .iter()
            .filter(|entry| entry.kind == 3)
            .collect::<Vec<_>>();
        let base = blobs.first().unwrap();
        let target = blobs.get(1).unwrap();
        let delta = literal_delta(base.size, &target.content);
        let mut replacement = encode_pack_entry_header(
            if reference_base_by_oid { 7 } else { 6 },
            u64::try_from(delta.len()).unwrap(),
        );
        if reference_base_by_oid {
            replacement.extend_from_slice(&base.oid);
        } else {
            replacement.extend_from_slice(&encode_offset_distance(target.start - base.start));
        }
        replacement.extend_from_slice(&zlib(&delta));
        bytes.splice(target.start..target.end, replacement);
        rewrite_pack_checksum(&mut bytes, self.body_offset);
        bytes
    }
}

struct RawPackEntry {
    start: usize,
    data_start: usize,
    end: usize,
    kind: u8,
    size: u64,
    oid: [u8; 20],
    content: Vec<u8>,
}

fn run_git(repository: Option<&Path>, arguments: &[&str]) {
    let mut command = crate::test_support::fixture_git_command("git");
    if let Some(repository) = repository {
        command.arg("-C").arg(repository);
    }
    let output = command
        .args(arguments)
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_output(repository: &Path, arguments: &[&str]) -> String {
    let mut command = crate::test_support::fixture_git_command("git");
    let output = command
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn parse_normal_entries(bundle: &[u8], body_offset: usize) -> Vec<RawPackEntry> {
    assert_eq!(&bundle[body_offset..body_offset + 4], b"PACK");
    let count = usize::try_from(u32::from_be_bytes(
        bundle[body_offset + 8..body_offset + 12]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let checksum_offset = bundle.len() - usize::try_from(PACK_CHECKSUM_BYTES).unwrap();
    let mut position = body_offset + 12;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let start = position;
        let (kind, size, data_start) = decode_pack_entry_header(bundle, position);
        assert!(
            matches!(kind, 1..=4),
            "real fixture unexpectedly used a delta"
        );
        position = data_start;

        let mut decompressor = Decompress::new(true);
        let mut content = Vec::new();
        loop {
            let before_input = decompressor.total_in();
            let before_output = decompressor.total_out();
            let mut output = [0_u8; 1024];
            let status = decompressor
                .decompress(
                    &bundle[position..checksum_offset],
                    &mut output,
                    FlushDecompress::None,
                )
                .unwrap();
            let consumed = usize::try_from(decompressor.total_in() - before_input).unwrap();
            let produced = usize::try_from(decompressor.total_out() - before_output).unwrap();
            position += consumed;
            content.extend_from_slice(&output[..produced]);
            assert!(consumed != 0 || produced != 0 || status == Status::StreamEnd);
            if status == Status::StreamEnd {
                break;
            }
        }
        assert_eq!(u64::try_from(content.len()).unwrap(), size);
        entries.push(RawPackEntry {
            start,
            data_start,
            end: position,
            kind,
            size,
            oid: object_oid(kind, &content),
            content,
        });
    }
    assert_eq!(position, checksum_offset);
    entries
}

fn decode_pack_entry_header(bytes: &[u8], start: usize) -> (u8, u64, usize) {
    let mut position = start;
    let mut byte = bytes[position];
    position += 1;
    let kind = (byte >> 4) & 0x07;
    let mut size = u64::from(byte & 0x0f);
    let mut shift = 4_u32;
    while byte & 0x80 != 0 {
        byte = bytes[position];
        position += 1;
        size |= u64::from(byte & 0x7f) << shift;
        shift += 7;
    }
    (kind, size, position)
}

fn object_oid(kind: u8, content: &[u8]) -> [u8; 20] {
    let name = match kind {
        1 => b"commit".as_slice(),
        2 => b"tree".as_slice(),
        3 => b"blob".as_slice(),
        4 => b"tag".as_slice(),
        _ => panic!("fixture object must not be a delta"),
    };
    let mut context = DigestContext::new(&SHA1_FOR_LEGACY_USE_ONLY);
    context.update(name);
    context.update(b" ");
    context.update(content.len().to_string().as_bytes());
    context.update(b"\0");
    context.update(content);
    let mut oid = [0_u8; 20];
    oid.copy_from_slice(context.finish().as_ref());
    oid
}

fn encode_pack_entry_header(kind: u8, mut size: u64) -> Vec<u8> {
    let mut bytes = vec![(kind << 4) | u8::try_from(size & 0x0f).unwrap()];
    size >>= 4;
    if size != 0 {
        bytes[0] |= 0x80;
    }
    while size != 0 {
        let mut byte = u8::try_from(size & 0x7f).unwrap();
        size >>= 7;
        if size != 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
    }
    bytes
}

fn encode_offset_distance(distance: usize) -> Vec<u8> {
    assert_ne!(distance, 0);
    let mut value = u64::try_from(distance).unwrap();
    let mut reversed = vec![u8::try_from(value & 0x7f).unwrap()];
    while value >> 7 != 0 {
        value = (value >> 7) - 1;
        reversed.push(0x80 | u8::try_from(value & 0x7f).unwrap());
    }
    reversed.reverse();
    reversed
}

fn encode_delta_varint(mut value: u64, destination: &mut Vec<u8>) {
    loop {
        let mut byte = u8::try_from(value & 0x7f).unwrap();
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        destination.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn literal_delta(base_size: u64, result: &[u8]) -> Vec<u8> {
    let mut delta = Vec::new();
    encode_delta_varint(base_size, &mut delta);
    encode_delta_varint(u64::try_from(result.len()).unwrap(), &mut delta);
    for chunk in result.chunks(127) {
        delta.push(u8::try_from(chunk.len()).unwrap());
        delta.extend_from_slice(chunk);
    }
    delta
}

fn zlib(bytes: &[u8]) -> Vec<u8> {
    let mut compressed = ZlibEncoder::new(Vec::new(), Compression::default());
    compressed.write_all(bytes).unwrap();
    compressed.finish().unwrap()
}

fn normal_object_entry(kind: u8, content: &[u8]) -> Vec<u8> {
    let mut entry = encode_pack_entry_header(kind, u64::try_from(content.len()).unwrap());
    entry.extend_from_slice(&zlib(content));
    entry
}

fn external_delta_entry() -> Vec<u8> {
    let delta = [0_u8, 0_u8];
    let mut entry = encode_pack_entry_header(7, u64::try_from(delta.len()).unwrap());
    entry.extend_from_slice(&[0xaa; 20]);
    entry.extend_from_slice(&zlib(&delta));
    entry
}

fn append_external_delta_chain(bundle: &mut Vec<u8>, body_offset: usize, offset_deltas: usize) {
    let checksum_offset = bundle.len() - usize::try_from(PACK_CHECKSUM_BYTES).unwrap();
    bundle.truncate(checksum_offset);
    let mut previous_offset = bundle.len();
    bundle.extend_from_slice(&external_delta_entry());
    for _ in 0..offset_deltas {
        let offset = bundle.len();
        let delta = [0_u8, 0_u8];
        bundle.extend_from_slice(&encode_pack_entry_header(
            6,
            u64::try_from(delta.len()).unwrap(),
        ));
        bundle.extend_from_slice(&encode_offset_distance(offset - previous_offset));
        bundle.extend_from_slice(&zlib(&delta));
        previous_offset = offset;
    }

    let count_offset = body_offset + 8;
    let original_count =
        u32::from_be_bytes(bundle[count_offset..count_offset + 4].try_into().unwrap());
    let added = u32::try_from(offset_deltas + 1).unwrap();
    bundle[count_offset..count_offset + 4]
        .copy_from_slice(&original_count.checked_add(added).unwrap().to_be_bytes());
    append_pack_checksum(bundle, body_offset);
}

fn rewrite_pack_checksum(bundle: &mut Vec<u8>, body_offset: usize) {
    bundle.truncate(bundle.len() - usize::try_from(PACK_CHECKSUM_BYTES).unwrap());
    append_pack_checksum(bundle, body_offset);
}

fn append_pack_checksum(bundle: &mut Vec<u8>, body_offset: usize) {
    let checksum = digest(&SHA1_FOR_LEGACY_USE_ONLY, &bundle[body_offset..]);
    bundle.extend_from_slice(checksum.as_ref());
}

#[test]
fn bundle_profile_ignores_an_empty_prerequisite_comment() {
    let baseline = "0".repeat(40);
    let head = "1".repeat(40);
    let tree = "2".repeat(40);
    let header = format!("# v2 git bundle\n-{baseline} \n{head} refs/scherzo/head\n\n");
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("carrier.bundle");
    fs::write(&path, &header).unwrap();
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

#[test]
fn real_git_bundle_round_trips_through_the_portable_parser() {
    let fixture = RealBundleFixture::new();

    assert_eq!(fixture.validate(&fixture.bytes), Ok(()));
}

#[test]
fn reference_and_offset_deltas_resolve_to_their_original_objects() {
    let fixture = RealBundleFixture::new();

    assert_eq!(
        fixture.validate(&fixture.with_resolvable_delta(true)),
        Ok(())
    );
    assert_eq!(
        fixture.validate(&fixture.with_resolvable_delta(false)),
        Ok(())
    );
}

#[test]
fn real_bundle_mutations_reach_each_pack_validation_failure() {
    let fixture = RealBundleFixture::new();

    for (mutation, expected) in [
        (BundleMutation::InvalidHeader, GitArtifactFailure::Header),
        (
            BundleMutation::MismatchedProfile,
            GitArtifactFailure::Profile,
        ),
        (BundleMutation::TruncatedPack, GitArtifactFailure::Pack),
        (BundleMutation::BadChecksum, GitArtifactFailure::Checksum),
        (
            BundleMutation::OversizedObject,
            GitArtifactFailure::StructureLimit,
        ),
        (
            BundleMutation::OverdeepDeltaChain,
            GitArtifactFailure::StructureLimit,
        ),
        (
            BundleMutation::MissingHeadObject,
            GitArtifactFailure::Content,
        ),
    ] {
        assert_eq!(fixture.failure(mutation), expected);
    }
}

#[test]
fn unresolved_external_delta_is_rejected_after_the_head_is_validated() {
    let fixture = RealBundleFixture::new();

    assert_eq!(
        fixture.failure(BundleMutation::ExternalDelta),
        GitArtifactFailure::Pack
    );
    assert_eq!(
        fixture.failure(BundleMutation::HeadReplacedByExternalDelta),
        GitArtifactFailure::Content
    );
}
