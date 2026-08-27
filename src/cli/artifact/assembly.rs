use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use rustix::fs::{RenameFlags, renameat_with};
use rustix::io::Errno;

use crate::api::{
    ArtifactApiError, ArtifactCapabilityMember, ArtifactInventoryPage, ArtifactMember,
    ArtifactSource,
};
use crate::public_id::valid_typed_id;

use crate::execution::workflow::portable_artifact::{
    PortableArtifactValidationFailure, validate_portable_artifact_set,
};

const INVENTORY_PAGE_LIMIT: u16 = 200;
const CAPABILITY_BATCH_SIZE: usize = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AssembledArtifact {
    pub(crate) artifact_set_id: String,
    pub(crate) member_count: usize,
    pub(crate) total_size_bytes: u64,
    pub(crate) destination: PathBuf,
}

#[derive(Debug)]
pub(crate) enum ArtifactAssemblyError {
    Api(ArtifactApiError),
    InvalidInventory,
    IntegrityMismatch,
    DestinationInvalid,
    DestinationExists,
    StagingUnavailable,
    CommitUnavailable,
}

impl std::fmt::Display for ArtifactAssemblyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(error) => error.fmt(formatter),
            Self::InvalidInventory => {
                formatter.write_str("the Artifact Set inventory is inconsistent")
            }
            Self::IntegrityMismatch => {
                formatter.write_str("the downloaded Artifact Set does not match its inventory")
            }
            Self::DestinationInvalid => formatter.write_str("the artifact output path is invalid"),
            Self::DestinationExists => {
                formatter.write_str("the artifact output path already exists")
            }
            Self::StagingUnavailable => formatter.write_str("prepare private artifact staging"),
            Self::CommitUnavailable => {
                formatter.write_str("atomically commit the downloaded Artifact Set")
            }
        }
    }
}

impl From<ArtifactApiError> for ArtifactAssemblyError {
    fn from(error: ArtifactApiError) -> Self {
        Self::Api(error)
    }
}

pub(crate) fn assemble_artifact_set(
    source: &mut impl ArtifactSource,
    organization: &str,
    run_id: &str,
    destination: &Path,
) -> Result<AssembledArtifact, ArtifactAssemblyError> {
    assemble_artifact_set_with_clock(
        source,
        organization,
        run_id,
        destination,
        crate::timing::utc_now,
    )
}

fn assemble_artifact_set_with_clock(
    source: &mut impl ArtifactSource,
    organization: &str,
    run_id: &str,
    destination: &Path,
    mut now: impl FnMut() -> time::OffsetDateTime,
) -> Result<AssembledArtifact, ArtifactAssemblyError> {
    let inventory = collect_inventory(source, organization, run_id)?;
    let (parent, destination_name) = destination_parent(destination)?;
    if std::fs::symlink_metadata(destination).is_ok() {
        return Err(ArtifactAssemblyError::DestinationExists);
    }
    let staging = tempfile::Builder::new()
        .prefix(".scherzo-artifact-download-")
        .tempdir_in(&parent)
        .map_err(|_| ArtifactAssemblyError::StagingUnavailable)?;
    std::fs::create_dir(staging.path().join("exports"))
        .map_err(|_| ArtifactAssemblyError::StagingUnavailable)?;

    for batch in capability_batches(&inventory.members) {
        download_capability_batch(
            source,
            organization,
            run_id,
            staging.path(),
            &inventory,
            batch,
            &mut now,
        )?;
    }

    let cancelled = AtomicBool::new(false);
    let validation = validate_portable_artifact_set(staging.path(), &cancelled)
        .map_err(classify_validation_failure)?;
    if !validation.is_valid() {
        return Err(ArtifactAssemblyError::IntegrityMismatch);
    }
    sync_staged_directory(staging.path())?;

    let staging_path = staging.keep();
    let staging_name = staging_path
        .file_name()
        .ok_or(ArtifactAssemblyError::CommitUnavailable)?;
    let parent_file = File::open(&parent).map_err(|_| ArtifactAssemblyError::CommitUnavailable)?;
    match renameat_with(
        &parent_file,
        staging_name,
        &parent_file,
        destination_name,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {}
        Err(Errno::EXIST | Errno::NOTEMPTY) => {
            let _ = std::fs::remove_dir_all(&staging_path);
            return Err(ArtifactAssemblyError::DestinationExists);
        }
        Err(_) => {
            let _ = std::fs::remove_dir_all(&staging_path);
            return Err(ArtifactAssemblyError::CommitUnavailable);
        }
    }
    parent_file
        .sync_all()
        .map_err(|_| ArtifactAssemblyError::CommitUnavailable)?;
    Ok(AssembledArtifact {
        artifact_set_id: inventory.artifact_set_id,
        member_count: inventory.member_count,
        total_size_bytes: inventory.total_size_bytes,
        destination: parent.join(destination_name),
    })
}

fn download_capability_batch(
    source: &mut impl ArtifactSource,
    organization: &str,
    run_id: &str,
    staging: &Path,
    inventory: &CompleteInventory,
    batch: &[ArtifactMember],
    now: &mut impl FnMut() -> time::OffsetDateTime,
) -> Result<(), ArtifactAssemblyError> {
    let retention_expires_at = parse_artifact_timestamp(&inventory.expires_at)?;
    let mut first_undownloaded = 0;
    while first_undownloaded < batch.len() {
        let remaining = &batch[first_undownloaded..];
        let paths = remaining
            .iter()
            .map(|member| member.path.clone())
            .collect::<Vec<_>>();
        let capabilities = source.issue_capabilities(organization, run_id, &paths)?;
        let capability_expires_at = parse_artifact_timestamp(&capabilities.capability_expires_at)?;
        if capabilities.artifact_set_id != inventory.artifact_set_id
            || capabilities.expires_at != inventory.expires_at
            || capability_expires_at > retention_expires_at
            || capabilities.members.len() != remaining.len()
            || capabilities
                .members
                .iter()
                .map(|capability| &capability.member)
                .ne(remaining.iter())
        {
            return Err(ArtifactAssemblyError::InvalidInventory);
        }

        let response_start = first_undownloaded;
        for capability in &capabilities.members {
            if now() >= capability_expires_at {
                if first_undownloaded == response_start {
                    return Err(ArtifactApiError::CapabilityUnavailable.into());
                }
                break;
            }
            download_member(source, staging, capability)?;
            first_undownloaded += 1;
        }
    }
    Ok(())
}

fn parse_artifact_timestamp(value: &str) -> Result<time::OffsetDateTime, ArtifactAssemblyError> {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| ArtifactAssemblyError::InvalidInventory)
}

struct CompleteInventory {
    artifact_set_id: String,
    expires_at: String,
    member_count: usize,
    total_size_bytes: u64,
    members: Vec<ArtifactMember>,
}

fn collect_inventory(
    source: &mut impl ArtifactSource,
    organization: &str,
    run_id: &str,
) -> Result<CompleteInventory, ArtifactAssemblyError> {
    let mut expected: Option<ArtifactInventoryPage> = None;
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut members = Vec::new();
    loop {
        let page = source.inventory_page(
            organization,
            run_id,
            INVENTORY_PAGE_LIMIT,
            cursor.as_deref(),
        )?;
        validate_page(&page, expected.as_ref(), members.last())?;
        if expected.is_none() {
            expected = Some(page.clone());
        }
        members.extend(page.members);
        let Some(next) = page.next_cursor else {
            break;
        };
        if next.is_empty() || !seen_cursors.insert(next.clone()) || members.is_empty() {
            return Err(ArtifactAssemblyError::InvalidInventory);
        }
        cursor = Some(next);
    }
    let expected = expected.ok_or(ArtifactAssemblyError::InvalidInventory)?;
    let total = members
        .iter()
        .try_fold(0_u64, |total, member| total.checked_add(member.size_bytes))
        .ok_or(ArtifactAssemblyError::InvalidInventory)?;
    if members.len() != expected.member_count
        || total != expected.total_size_bytes
        || members
            .last()
            .is_none_or(|member| member.path != "result.json")
    {
        return Err(ArtifactAssemblyError::InvalidInventory);
    }
    Ok(CompleteInventory {
        artifact_set_id: expected.artifact_set_id,
        expires_at: expected.expires_at,
        member_count: expected.member_count,
        total_size_bytes: expected.total_size_bytes,
        members,
    })
}

fn validate_page(
    page: &ArtifactInventoryPage,
    expected: Option<&ArtifactInventoryPage>,
    previous: Option<&ArtifactMember>,
) -> Result<(), ArtifactAssemblyError> {
    if !valid_typed_id(&page.artifact_set_id, "ats_")
        || page.member_count == 0
        || page.member_count > 4097
        || page.members.len() > usize::from(INVENTORY_PAGE_LIMIT)
    {
        return Err(ArtifactAssemblyError::InvalidInventory);
    }
    if let Some(expected) = expected
        && (page.artifact_set_id != expected.artifact_set_id
            || page.sealed_at != expected.sealed_at
            || page.expires_at != expected.expires_at
            || page.member_count != expected.member_count
            || page.total_size_bytes != expected.total_size_bytes)
    {
        return Err(ArtifactAssemblyError::InvalidInventory);
    }
    let mut prior_path = previous.map(|member| member.path.as_str());
    for member in &page.members {
        if !valid_member(member) || prior_path.is_some_and(|path| member.path.as_str() <= path) {
            return Err(ArtifactAssemblyError::InvalidInventory);
        }
        prior_path = Some(&member.path);
    }
    Ok(())
}

fn valid_member(member: &ArtifactMember) -> bool {
    if member.media_type.is_empty() {
        return false;
    }
    if member.path == "result.json" {
        return member.media_type == "application/json";
    }
    let Some(ordinal) = member.path.strip_prefix("exports/") else {
        return false;
    };
    ordinal.len() == 4
        && ordinal.bytes().all(|byte| byte.is_ascii_digit())
        && ordinal != "0000"
        && ordinal <= "4096"
}

fn capability_batches(members: &[ArtifactMember]) -> impl Iterator<Item = &[ArtifactMember]> {
    members.chunks(CAPABILITY_BATCH_SIZE)
}

fn download_member(
    source: &mut impl ArtifactSource,
    staging: &Path,
    capability: &ArtifactCapabilityMember,
) -> Result<(), ArtifactAssemblyError> {
    let path = staging.join(&capability.member.path);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| ArtifactAssemblyError::StagingUnavailable)?;
    let downloaded = source.download(capability, &mut file)?;
    file.flush()
        .map_err(|_| ArtifactAssemblyError::StagingUnavailable)?;
    file.sync_all()
        .map_err(|_| ArtifactAssemblyError::StagingUnavailable)?;
    if downloaded.size_bytes != capability.member.size_bytes
        || downloaded.sha256 != capability.member.sha256
    {
        return Err(ArtifactAssemblyError::IntegrityMismatch);
    }
    Ok(())
}

fn destination_parent(
    destination: &Path,
) -> Result<(PathBuf, &std::ffi::OsStr), ArtifactAssemblyError> {
    let name = destination
        .file_name()
        .ok_or(ArtifactAssemblyError::DestinationInvalid)?;
    if name == "." || name == ".." {
        return Err(ArtifactAssemblyError::DestinationInvalid);
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent =
        std::fs::canonicalize(parent).map_err(|_| ArtifactAssemblyError::DestinationInvalid)?;
    if !parent.is_dir() {
        return Err(ArtifactAssemblyError::DestinationInvalid);
    }
    Ok((parent, name))
}

fn sync_staged_directory(staging: &Path) -> Result<(), ArtifactAssemblyError> {
    File::open(staging.join("exports"))
        .and_then(|directory| directory.sync_all())
        .and_then(|()| File::open(staging))
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ArtifactAssemblyError::StagingUnavailable)
}

fn classify_validation_failure(
    failure: PortableArtifactValidationFailure,
) -> ArtifactAssemblyError {
    match failure {
        PortableArtifactValidationFailure::Interrupted => ArtifactAssemblyError::IntegrityMismatch,
        PortableArtifactValidationFailure::CurrentDirectoryUnavailable
        | PortableArtifactValidationFailure::ScratchUnavailable => {
            ArtifactAssemblyError::StagingUnavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, atomic::Ordering};

    use ring::digest::{SHA256, digest};
    use serde_json::json;

    use super::*;
    use crate::api::{ArtifactCapabilities, DownloadedMember};

    struct FakeSource {
        inventory: VecDeque<ArtifactInventoryPage>,
        bytes: Vec<(String, Vec<u8>)>,
        issue_count: usize,
        issued_paths: Vec<Vec<String>>,
        capability_expirations: VecDeque<String>,
        expire_after_download: Option<Arc<AtomicBool>>,
        fail_path: Option<String>,
    }

    impl ArtifactSource for FakeSource {
        fn inventory_page(
            &mut self,
            _organization: &str,
            _run_id: &str,
            _limit: u16,
            _cursor: Option<&str>,
        ) -> Result<ArtifactInventoryPage, ArtifactApiError> {
            self.inventory.pop_front().ok_or(ArtifactApiError::NotFound)
        }

        fn issue_capabilities(
            &mut self,
            _organization: &str,
            _run_id: &str,
            paths: &[String],
        ) -> Result<ArtifactCapabilities, ArtifactApiError> {
            self.issue_count += 1;
            self.issued_paths.push(paths.to_vec());
            let members = paths
                .iter()
                .map(|path| {
                    let bytes = self
                        .bytes
                        .iter()
                        .find(|(candidate, _)| candidate == path)
                        .map(|(_, bytes)| bytes)
                        .ok_or(ArtifactApiError::NotFound)?;
                    Ok(ArtifactCapabilityMember {
                        member: member(path, bytes),
                        url: format!("https://fixture.invalid/exact?path={path}"),
                    })
                })
                .collect::<Result<Vec<_>, ArtifactApiError>>()?;
            Ok(ArtifactCapabilities {
                artifact_set_id: "ats_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                expires_at: "2026-09-17T12:00:00Z".to_owned(),
                capability_expires_at: self
                    .capability_expirations
                    .pop_front()
                    .unwrap_or_else(|| "2026-08-17T12:05:00Z".to_owned()),
                members,
            })
        }

        fn download(
            &mut self,
            capability: &ArtifactCapabilityMember,
            destination: &mut dyn std::io::Write,
        ) -> Result<DownloadedMember, ArtifactApiError> {
            if self.fail_path.as_deref() == Some(&capability.member.path) {
                return Err(ArtifactApiError::CapabilityUnavailable);
            }
            let bytes = self
                .bytes
                .iter()
                .find(|(path, _)| path == &capability.member.path)
                .map(|(_, bytes)| bytes)
                .ok_or(ArtifactApiError::NotFound)?;
            destination
                .write_all(bytes)
                .map_err(|_| ArtifactApiError::LocalOutput)?;
            if let Some(expired) = &self.expire_after_download {
                expired.store(true, Ordering::SeqCst);
            }
            Ok(DownloadedMember {
                size_bytes: u64::try_from(bytes.len()).unwrap(),
                sha256: sha256(bytes),
            })
        }
    }

    #[test]
    fn metadata_only_set_commits_atomically_after_complete_validation() {
        let result = metadata_only_result();
        let page = page(vec![member("result.json", &result)], None);
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("artifact-set");
        let mut source = fake_source(
            VecDeque::from([page]),
            vec![("result.json".to_owned(), result)],
        );

        let assembled = assemble_fixture(&mut source, &destination).unwrap();

        assert_eq!(
            assembled.destination,
            std::fs::canonicalize(&destination).unwrap()
        );
        assert_eq!(assembled.member_count, 1);
        assert_eq!(source.issue_count, 1);
        assert!(destination.join("result.json").is_file());
        assert!(destination.join("exports").is_dir());
    }

    #[test]
    fn partial_download_never_commits_destination() {
        let (members, bytes) = one_carrier_fixture();
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("artifact-set");
        let mut source = fake_source(VecDeque::from([page(members, None)]), bytes);
        source.fail_path = Some("result.json".to_owned());

        assert!(assemble_fixture(&mut source, &destination).is_err());
        assert!(!destination.exists());
    }

    #[test]
    fn maximum_set_assembles_across_forty_one_capability_batches() {
        let mut exports = serde_json::Map::new();
        let mut bytes = Vec::with_capacity(4097);
        let mut members = Vec::with_capacity(4097);
        for ordinal in 1..=4096 {
            let path = format!("exports/{ordinal:04}");
            exports.insert(
                format!("export{ordinal:04}"),
                json!({
                    "state": "available", "kind": "file",
                    "mediaType": "application/octet-stream", "path": path.clone(),
                    "sizeBytes": 0,
                    "digest": {"algorithm": "sha256", "value": hex_sha256(&[])},
                }),
            );
            members.push(member(&path, &[]));
            bytes.push((path, Vec::new()));
        }
        let mut result = metadata_only_document();
        result["exports"] = serde_json::Value::Object(exports);
        let result = encode_result(&result);
        members.push(member("result.json", &result));
        bytes.push(("result.json".to_owned(), result));
        let member_count = members.len();
        let total_size_bytes = members.iter().map(|member| member.size_bytes).sum();
        let page_count = members.len().div_ceil(usize::from(INVENTORY_PAGE_LIMIT));
        let pages = members
            .chunks(usize::from(INVENTORY_PAGE_LIMIT))
            .enumerate()
            .map(|(index, page_members)| ArtifactInventoryPage {
                artifact_set_id: "ats_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                sealed_at: "2026-08-17T12:00:00Z".to_owned(),
                expires_at: "2026-09-17T12:00:00Z".to_owned(),
                member_count,
                total_size_bytes,
                members: page_members.to_vec(),
                next_cursor: (index + 1 < page_count).then(|| format!("cursor-{index}")),
            })
            .collect::<VecDeque<_>>();
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("artifact-set");
        let mut source = fake_source(pages, bytes);

        let assembled = assemble_fixture(&mut source, &destination).unwrap();

        assert_eq!(assembled.member_count, 4097);
        assert_eq!(source.issue_count, 41);
        assert!(destination.join("exports/4096").is_file());
    }

    #[test]
    fn expired_unstarted_capabilities_are_refreshed_for_remaining_members() {
        let (members, bytes) = one_carrier_fixture();
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("artifact-set");
        let expired = Arc::new(AtomicBool::new(false));
        let mut source = fake_source(VecDeque::from([page(members, None)]), bytes);
        source.capability_expirations = VecDeque::from([
            "2026-08-17T12:05:00Z".to_owned(),
            "2026-08-17T12:10:00Z".to_owned(),
        ]);
        source.expire_after_download = Some(expired.clone());
        let now = || {
            if expired.load(Ordering::SeqCst) {
                test_timestamp("2026-08-17T12:06:00Z")
            } else {
                test_timestamp("2026-08-17T12:04:00Z")
            }
        };

        let assembled =
            assemble_artifact_set_with_clock(&mut source, "acme", "run_fixture", &destination, now)
                .unwrap();

        assert_eq!(assembled.member_count, 2);
        assert_eq!(source.issue_count, 2);
        assert_eq!(
            source.issued_paths,
            vec![
                vec!["exports/0001".to_owned(), "result.json".to_owned()],
                vec!["result.json".to_owned()],
            ]
        );
    }

    fn fake_source(
        inventory: VecDeque<ArtifactInventoryPage>,
        bytes: Vec<(String, Vec<u8>)>,
    ) -> FakeSource {
        FakeSource {
            inventory,
            bytes,
            issue_count: 0,
            issued_paths: Vec::new(),
            capability_expirations: VecDeque::new(),
            expire_after_download: None,
            fail_path: None,
        }
    }

    fn one_carrier_fixture() -> (Vec<ArtifactMember>, Vec<(String, Vec<u8>)>) {
        let carrier = b"carrier".to_vec();
        let mut result_document = metadata_only_document();
        result_document["exports"] = json!({
            "artifact": {
                "state": "available",
                "kind": "file",
                "mediaType": "application/octet-stream",
                "path": "exports/0001",
                "sizeBytes": carrier.len(),
                "digest": {"algorithm": "sha256", "value": hex_sha256(&carrier)}
            }
        });
        let result = encode_result(&result_document);
        let members = vec![
            member("exports/0001", &carrier),
            member("result.json", &result),
        ];
        let bytes = vec![
            ("exports/0001".to_owned(), carrier),
            ("result.json".to_owned(), result),
        ];
        (members, bytes)
    }

    fn assemble_fixture(
        source: &mut FakeSource,
        destination: &Path,
    ) -> Result<AssembledArtifact, ArtifactAssemblyError> {
        assemble_artifact_set_with_clock(source, "acme", "run_fixture", destination, || {
            test_timestamp("2026-08-17T12:01:00Z")
        })
    }

    fn test_timestamp(value: &str) -> time::OffsetDateTime {
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).unwrap()
    }

    fn page(members: Vec<ArtifactMember>, next_cursor: Option<&str>) -> ArtifactInventoryPage {
        let total_size_bytes = members.iter().map(|member| member.size_bytes).sum();
        ArtifactInventoryPage {
            artifact_set_id: "ats_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            sealed_at: "2026-08-17T12:00:00Z".to_owned(),
            expires_at: "2026-09-17T12:00:00Z".to_owned(),
            member_count: members.len(),
            total_size_bytes,
            members,
            next_cursor: next_cursor.map(str::to_owned),
        }
    }

    fn member(path: &str, bytes: &[u8]) -> ArtifactMember {
        ArtifactMember {
            path: path.to_owned(),
            media_type: if path == "result.json" {
                "application/json"
            } else {
                "application/octet-stream"
            }
            .to_owned(),
            size_bytes: u64::try_from(bytes.len()).unwrap(),
            sha256: sha256(bytes),
        }
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        let observed = digest(&SHA256, bytes);
        let mut result = [0_u8; 32];
        result.copy_from_slice(observed.as_ref());
        result
    }

    fn metadata_only_result() -> Vec<u8> {
        encode_result(&metadata_only_document())
    }

    fn metadata_only_document() -> serde_json::Value {
        json!({
            "schemaVersion": 1,
            "attemptNumber": 1,
            "workflow": {
                "path": "workflow.yaml",
                "provenance": {
                    "kind": "cloud",
                    "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "objectFormat": "sha1",
                    "commitOid": "0123456789abcdef0123456789abcdef01234567"
                },
                "digest": {"algorithm": "sha256", "value": "1".repeat(64)}
            },
            "execution": {
                "maximumParallelSteps": 1,
                "capacity": {
                    "executionContract": "workflow_v1_inputless_cloud_artifacts@1",
                    "sourceClosureDigest": {
                        "algorithm": "sha256",
                        "value": "1".repeat(64)
                    },
                    "generalMaximumTransitions": 8,
                    "selectedMaximumTransitions": 7,
                    "maximumInvocations": 1,
                    "maximumRetainedBytesPerInvocation": 4_194_304,
                    "diagnosticRetentionBytes": 8_388_608,
                    "nativeSessionRetentionBytes": 4_194_304,
                    "aggregateRetentionBytes": 12_582_912,
                    "encodedOutboxBytes": 38_141_952
                },
                "startedAt": "2026-08-17T12:00:00Z",
                "finishedAt": "2026-08-17T12:00:01Z",
                "durationMilliseconds": 1000
            },
            "commandOutputPolicy": {
                "encoding": "base64",
                "maximumRetainedBytesPerStream": crate::execution::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
            },
            "outcome": "succeeded",
            "steps": [{
                "id": "produce", "role": "step", "kind": "agent",
                "failurePolicy": "required", "state": "succeeded",
                "startedAt": "2026-08-17T12:00:00Z", "durationMilliseconds": 1000
            }],
            "exports": {}
        })
    }

    fn encode_result(result: &serde_json::Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(result).unwrap();
        bytes.push(b'\n');
        bytes
    }

    fn hex_sha256(bytes: &[u8]) -> String {
        sha256(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}
