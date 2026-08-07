use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::OwnedFd;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use ring::digest::{Context as DigestContext, SHA256};
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, Stat, fstat, open, openat, statat};
use rustix::io::Errno;
use serde::Serialize;
use serde_json::{Map, Value};

use super::artifact_json::{self, ArtifactJsonFailure};
use super::presentation::visible_text;
use super::result_metadata::{self, ResultDocumentError};
use super::schema_common::{is_identifier, is_lowercase_hex, lowercase_hex};

const RESULT_FILE: &str = "result.json";
const EXPORT_DIRECTORY: &str = "exports";
const MAXIMUM_RESULT_BYTES: u64 = 64 * 1024 * 1024;
const ROOT_OVERFLOW_ENTRY: usize = 4_097;
const MAXIMUM_EXPORTS: usize = 4_096;
const MAXIMUM_CARRIERS: usize = 2_048;
const EXPORTS_OVERFLOW_ENTRY: usize = 2_049;
const MAXIMUM_CARRIER_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_TOTAL_CARRIER_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_DIAGNOSTICS: usize = 8_192;
const COPY_BUFFER_BYTES: usize = 64 * 1024;

const DIAGNOSTIC_CODES: &[&str] = &[
    "artifact_directory_invalid",
    "artifact_directory_unavailable",
    "artifact_directory_not_directory",
    "root_entry_limit_exceeded",
    "root_entry_unexpected",
    "boundary_name_invalid",
    "result_missing",
    "result_symbolic_link",
    "result_not_regular_file",
    "result_unavailable",
    "result_limit_exceeded",
    "result_encoding_invalid",
    "result_json_invalid",
    "result_schema_unsupported",
    "result_schema_invalid",
    "exports_directory_missing",
    "exports_directory_symbolic_link",
    "exports_directory_not_directory",
    "exports_directory_unavailable",
    "export_limit_exceeded",
    "export_entry_invalid",
    "export_media_type_invalid",
    "export_path_invalid",
    "export_ordinal_invalid",
    "alias_metadata_mismatch",
    "carrier_limit_exceeded",
    "carrier_missing",
    "carrier_symbolic_link",
    "carrier_not_regular_file",
    "carrier_unavailable",
    "carrier_size_limit_exceeded",
    "carrier_total_size_limit_exceeded",
    "carrier_size_mismatch",
    "carrier_digest_mismatch",
    "carrier_unreferenced",
    "text_encoding_invalid",
    "json_content_invalid",
    "json_content_noncanonical",
    "git_zero_delta_invalid",
    "git_bundle_header_invalid",
    "git_bundle_profile_invalid",
    "git_pack_invalid",
    "git_pack_checksum_mismatch",
    "git_content_invalid",
    "git_structure_limit_exceeded",
    "diagnostic_limit_exceeded",
];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ArtifactDiagnosticLocation {
    ArtifactDirectory,
    Boundary {
        path: String,
    },
    Result {
        #[serde(skip_serializing_if = "Option::is_none")]
        pointer: Option<String>,
    },
    Export {
        export: String,
    },
    Carrier {
        path: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ArtifactDiagnostic {
    code: &'static str,
    message: &'static str,
    location: ArtifactDiagnosticLocation,
}

impl ArtifactDiagnostic {
    pub(crate) fn code(&self) -> &'static str {
        self.code
    }

    pub(crate) fn message(&self) -> &'static str {
        self.message
    }

    pub(crate) fn human_location(&self) -> String {
        match &self.location {
            ArtifactDiagnosticLocation::ArtifactDirectory => "artifact directory".to_owned(),
            ArtifactDiagnosticLocation::Boundary { path } => {
                format!("boundary {}", visible_text(path))
            }
            ArtifactDiagnosticLocation::Result {
                pointer: Some(pointer),
            } => {
                format!("result {}", visible_text(pointer))
            }
            ArtifactDiagnosticLocation::Result { pointer: None } => "result".to_owned(),
            ArtifactDiagnosticLocation::Export { export } => {
                format!("export {}", visible_text(export))
            }
            ArtifactDiagnosticLocation::Carrier { path } => {
                format!("carrier {}", visible_text(path))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArtifactValidationSummary {
    pub(crate) declared_exports: u64,
    pub(crate) available_exports: u64,
    pub(crate) unavailable_exports: u64,
    pub(crate) referenced_carriers: u64,
    pub(crate) carrier_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct PortableArtifactValidation {
    pub(crate) artifact_directory: Option<String>,
    pub(crate) diagnostics: Vec<ArtifactDiagnostic>,
    pub(crate) summary: Option<ArtifactValidationSummary>,
}

impl PortableArtifactValidation {
    pub(crate) fn is_valid(&self) -> bool {
        self.diagnostics.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PortableArtifactValidationFailure {
    AccessBookkeepingUnavailable,
    CurrentDirectoryUnavailable,
    Interrupted,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DiagnosticOrder {
    stage: u8,
    major: Vec<u8>,
    check: usize,
    code: usize,
    minor: Vec<u8>,
}

#[derive(Default)]
struct Diagnostics {
    pending: BTreeMap<DiagnosticOrder, ArtifactDiagnostic>,
    public_orders: BTreeMap<(&'static str, ArtifactDiagnosticLocation), DiagnosticOrder>,
    limit_exceeded: bool,
}

impl Diagnostics {
    fn push(
        &mut self,
        order: DiagnosticOrder,
        code: &'static str,
        location: ArtifactDiagnosticLocation,
    ) {
        let public_key = (code, location.clone());
        if let Some(existing_order) = self.public_orders.get(&public_key) {
            if existing_order <= &order {
                return;
            }
            self.pending.remove(existing_order);
        }
        self.pending.insert(
            order.clone(),
            ArtifactDiagnostic {
                code,
                message: diagnostic_message(code),
                location,
            },
        );
        self.public_orders.insert(public_key, order);
        if self.pending.len() > MAXIMUM_DIAGNOSTICS {
            if let Some((_, removed)) = self.pending.pop_last() {
                self.public_orders.remove(&(removed.code, removed.location));
            }
            self.limit_exceeded = true;
        }
    }

    fn root(&mut self, code: &'static str, path: Option<&str>) {
        self.push(
            DiagnosticOrder {
                stage: 0,
                major: Vec::new(),
                check: code_rank(code),
                code: code_rank(code),
                minor: path.unwrap_or_default().as_bytes().to_vec(),
            },
            code,
            boundary_location(path),
        );
    }

    fn result(&mut self, code: &'static str, pointer: Option<&str>) {
        self.push(
            DiagnosticOrder {
                stage: 1,
                major: Vec::new(),
                check: code_rank(code),
                code: code_rank(code),
                minor: pointer.unwrap_or_default().as_bytes().to_vec(),
            },
            code,
            ArtifactDiagnosticLocation::Result {
                pointer: pointer.map(str::to_owned),
            },
        );
    }

    fn export(&mut self, code: &'static str, export: &str, check: usize) {
        self.push(
            DiagnosticOrder {
                stage: 2,
                major: export.as_bytes().to_vec(),
                check,
                code: code_rank(code),
                minor: Vec::new(),
            },
            code,
            ArtifactDiagnosticLocation::Export {
                export: export.to_owned(),
            },
        );
    }

    fn alias(&mut self, path: &str) {
        self.push(
            DiagnosticOrder {
                stage: 3,
                major: path.as_bytes().to_vec(),
                check: 0,
                code: code_rank("alias_metadata_mismatch"),
                minor: Vec::new(),
            },
            "alias_metadata_mismatch",
            ArtifactDiagnosticLocation::Carrier {
                path: path.to_owned(),
            },
        );
    }

    fn carrier_limit(&mut self) {
        self.push(
            DiagnosticOrder {
                stage: 4,
                major: Vec::new(),
                check: 0,
                code: code_rank("carrier_limit_exceeded"),
                minor: Vec::new(),
            },
            "carrier_limit_exceeded",
            ArtifactDiagnosticLocation::Result {
                pointer: Some("/exports".to_owned()),
            },
        );
    }

    fn carrier(&mut self, code: &'static str, path: &str, check: usize) {
        self.push(
            DiagnosticOrder {
                stage: 5,
                major: path.as_bytes().to_vec(),
                check,
                code: code_rank(code),
                minor: Vec::new(),
            },
            code,
            ArtifactDiagnosticLocation::Carrier {
                path: path.to_owned(),
            },
        );
    }

    fn exports_overflow(&mut self) {
        self.push(
            DiagnosticOrder {
                stage: 6,
                major: Vec::new(),
                check: 0,
                code: code_rank("carrier_limit_exceeded"),
                minor: Vec::new(),
            },
            "carrier_limit_exceeded",
            ArtifactDiagnosticLocation::ArtifactDirectory,
        );
    }

    fn inventory(&mut self, code: &'static str, path: Option<&str>) {
        self.push(
            DiagnosticOrder {
                stage: 6,
                major: path.unwrap_or_default().as_bytes().to_vec(),
                check: 1,
                code: code_rank(code),
                minor: Vec::new(),
            },
            code,
            boundary_location(path),
        );
    }

    fn finish(self) -> Vec<ArtifactDiagnostic> {
        let mut diagnostics = self.pending.into_values().collect::<Vec<_>>();
        if self.limit_exceeded {
            diagnostics.truncate(MAXIMUM_DIAGNOSTICS - 1);
            diagnostics.push(ArtifactDiagnostic {
                code: "diagnostic_limit_exceeded",
                message: diagnostic_message("diagnostic_limit_exceeded"),
                location: ArtifactDiagnosticLocation::ArtifactDirectory,
            });
        }
        diagnostics
    }
}

fn boundary_location(path: Option<&str>) -> ArtifactDiagnosticLocation {
    path.map_or(ArtifactDiagnosticLocation::ArtifactDirectory, |path| {
        ArtifactDiagnosticLocation::Boundary {
            path: path.to_owned(),
        }
    })
}

fn code_rank(code: &str) -> usize {
    DIAGNOSTIC_CODES
        .iter()
        .position(|candidate| *candidate == code)
        .unwrap_or(DIAGNOSTIC_CODES.len())
}

fn diagnostic_message(code: &str) -> &'static str {
    match code {
        "artifact_directory_invalid" => "The artifact directory path cannot be represented safely.",
        "artifact_directory_unavailable" => "The artifact directory is unavailable.",
        "artifact_directory_not_directory" => "The artifact directory path is not a directory.",
        "root_entry_limit_exceeded" => "The artifact directory contains too many entries.",
        "root_entry_unexpected" => "The artifact directory contains an unexpected entry.",
        "boundary_name_invalid" => "A directory entry name is not valid UTF-8.",
        "result_missing" => "The artifact set does not contain result.json.",
        "result_symbolic_link" => "result.json is a symbolic link.",
        "result_not_regular_file" => "result.json is not a regular file.",
        "result_unavailable" => "result.json could not be read safely.",
        "result_limit_exceeded" => "result.json exceeds the Artifact Set V1 limit.",
        "result_encoding_invalid" => "result.json does not use the required UTF-8 encoding.",
        "result_json_invalid" => "result.json is not one complete unique-member JSON value.",
        "result_schema_unsupported" => "result.json uses an unsupported schema version.",
        "result_schema_invalid" => "result.json violates Local Workflow Result Schema 1.",
        "exports_directory_missing" => "The artifact set does not contain exports.",
        "exports_directory_symbolic_link" => "exports is a symbolic link.",
        "exports_directory_not_directory" => "exports is not a directory.",
        "exports_directory_unavailable" => "The exports directory could not be read safely.",
        "export_limit_exceeded" => "result.json declares too many exports.",
        "export_entry_invalid" => "The export entry violates the closed Artifact Set V1 shape.",
        "export_media_type_invalid" => "The export media type is invalid for its artifact kind.",
        "export_path_invalid" => "The export carrier path is not a portable Artifact Set V1 path.",
        "export_ordinal_invalid" => {
            "The export carrier path does not use its physical owner's ordinal."
        }
        "alias_metadata_mismatch" => {
            "Exports sharing a carrier path do not repeat identical metadata."
        }
        "carrier_limit_exceeded" => "The artifact set exceeds the carrier limit.",
        "carrier_missing" => "A referenced carrier is missing.",
        "carrier_symbolic_link" => "A referenced carrier is a symbolic link.",
        "carrier_not_regular_file" => "A referenced carrier is not a regular file.",
        "carrier_unavailable" => "A referenced carrier could not be read safely.",
        "carrier_size_limit_exceeded" => "A carrier exceeds the per-carrier byte limit.",
        "carrier_total_size_limit_exceeded" => {
            "The artifact set exceeds the aggregate carrier byte limit."
        }
        "carrier_size_mismatch" => "The carrier size does not match result.json.",
        "carrier_digest_mismatch" => "The carrier digest does not match result.json.",
        "carrier_unreferenced" => "The exports directory contains an unreferenced entry.",
        "text_encoding_invalid" => "The text carrier is not valid UTF-8.",
        "json_content_invalid" => "The JSON carrier is not one valid RFC 8259 JSON value.",
        "json_content_noncanonical" => "The JSON carrier is not in compact ordered canonical form.",
        "git_zero_delta_invalid" => "The zero-delta Git artifact is invalid.",
        "git_bundle_header_invalid" => "The Git bundle header is invalid.",
        "git_bundle_profile_invalid" => "The Git bundle does not satisfy the Scherzo profile.",
        "git_pack_invalid" => "The Git pack stream is invalid.",
        "git_pack_checksum_mismatch" => "The Git pack checksum does not match.",
        "git_content_invalid" => "The Git bundle content does not match its descriptor.",
        "git_structure_limit_exceeded" => "The Git artifact exceeds a structural validation limit.",
        "diagnostic_limit_exceeded" => {
            "Additional artifact diagnostics were omitted at the V1 limit."
        }
        _ => "The artifact set is invalid.",
    }
}

pub(crate) fn validate_portable_artifact_set(
    argument: &Path,
    cancelled: &AtomicBool,
) -> Result<PortableArtifactValidation, PortableArtifactValidationFailure> {
    check_cancelled(cancelled)?;
    let initial_directory = std::env::current_dir()
        .map_err(|_| PortableArtifactValidationFailure::CurrentDirectoryUnavailable)?;
    let lexical = lexical_absolute(&initial_directory, argument);
    let mut diagnostics = Diagnostics::default();
    let mut artifact_directory = lexical.to_str().map(str::to_owned);

    let canonical = match fs::canonicalize(argument) {
        Ok(path) => path,
        Err(_) => {
            if artifact_directory.is_none() {
                diagnostics.root("artifact_directory_invalid", None);
            }
            diagnostics.root("artifact_directory_unavailable", None);
            return Ok(finish_report(
                artifact_directory,
                diagnostics,
                ArtifactValidationSummary {
                    declared_exports: 0,
                    available_exports: 0,
                    unavailable_exports: 0,
                    referenced_carriers: 0,
                    carrier_bytes: 0,
                },
            ));
        }
    };
    artifact_directory = canonical.to_str().map(str::to_owned);
    if artifact_directory.is_none() {
        diagnostics.root("artifact_directory_invalid", None);
    }

    ensure_access_bookkeeping_suppressed(&canonical)?;
    let root = match open(
        &canonical,
        access_preserving_flags(
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        ),
        Mode::empty(),
    ) {
        Ok(root) => root,
        Err(error) if no_atime_open_failed(error) => {
            return Err(PortableArtifactValidationFailure::AccessBookkeepingUnavailable);
        }
        Err(Errno::NOTDIR) => {
            diagnostics.root("artifact_directory_not_directory", None);
            return Ok(finish_report(
                artifact_directory,
                diagnostics,
                empty_summary(),
            ));
        }
        Err(_) => {
            diagnostics.root("artifact_directory_unavailable", None);
            return Ok(finish_report(
                artifact_directory,
                diagnostics,
                empty_summary(),
            ));
        }
    };

    inspect_root_boundary(&root, cancelled, &mut diagnostics)?;
    let result_bytes = read_result(&root, cancelled, &mut diagnostics)?;
    let metadata = result_bytes
        .as_deref()
        .map(|bytes| inspect_metadata(bytes, &mut diagnostics))
        .unwrap_or_default();
    let exports = open_exports_directory(&root, &mut diagnostics)?;
    let inventory = exports
        .as_ref()
        .map(|directory| enumerate_exports(directory, cancelled, &mut diagnostics))
        .transpose()?;

    inspect_aliases(&metadata.carriers, &mut diagnostics);
    if metadata.carriers.len() > MAXIMUM_CARRIERS {
        diagnostics.carrier_limit();
    }

    let mut total_bytes = 0_u64;
    for (path, group) in &metadata.carriers {
        check_cancelled(cancelled)?;
        validate_carrier(
            exports.as_ref(),
            path,
            group,
            &mut total_bytes,
            cancelled,
            &mut diagnostics,
        )?;
    }

    if let Some(inventory) = inventory {
        match inventory {
            EntryInventory::Overflow => diagnostics.exports_overflow(),
            EntryInventory::Complete(names) => {
                let expected = metadata
                    .carriers
                    .keys()
                    .filter_map(|path| path.strip_prefix("exports/"))
                    .map(|name| name.as_bytes().to_vec())
                    .collect::<BTreeSet<_>>();
                for name in names.difference(&expected) {
                    match std::str::from_utf8(name) {
                        Ok(name) => diagnostics
                            .inventory("carrier_unreferenced", Some(&format!("exports/{name}"))),
                        Err(_) => diagnostics.inventory("boundary_name_invalid", None),
                    }
                }
            }
        }
    }
    if let Some(exports) = &exports {
        recheck_exports_directory(&root, exports, &mut diagnostics);
    }

    let summary = ArtifactValidationSummary {
        declared_exports: usize_to_u64(metadata.declared_exports),
        available_exports: usize_to_u64(metadata.available_exports),
        unavailable_exports: usize_to_u64(metadata.unavailable_exports),
        referenced_carriers: usize_to_u64(metadata.carriers.len()),
        carrier_bytes: total_bytes,
    };
    Ok(finish_report(artifact_directory, diagnostics, summary))
}

fn empty_summary() -> ArtifactValidationSummary {
    ArtifactValidationSummary {
        declared_exports: 0,
        available_exports: 0,
        unavailable_exports: 0,
        referenced_carriers: 0,
        carrier_bytes: 0,
    }
}

fn finish_report(
    artifact_directory: Option<String>,
    diagnostics: Diagnostics,
    summary: ArtifactValidationSummary,
) -> PortableArtifactValidation {
    let diagnostics = diagnostics.finish();
    PortableArtifactValidation {
        artifact_directory,
        summary: diagnostics.is_empty().then_some(summary),
        diagnostics,
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn lexical_absolute(initial: &Path, argument: &Path) -> PathBuf {
    let combined = if argument.is_absolute() {
        argument.to_path_buf()
    } else {
        initial.join(argument)
    };
    let mut normalized = PathBuf::new();
    for component in combined.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    normalized
}

#[cfg(target_os = "linux")]
fn ensure_access_bookkeeping_suppressed(
    _path: &Path,
) -> Result<(), PortableArtifactValidationFailure> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn ensure_access_bookkeeping_suppressed(
    path: &Path,
) -> Result<(), PortableArtifactValidationFailure> {
    use nix::mount::MntFlags;

    let flags = nix::sys::statfs::statfs(path)
        .map_err(|_| PortableArtifactValidationFailure::AccessBookkeepingUnavailable)?
        .flags();
    flags
        .contains(MntFlags::MNT_NOATIME)
        .then_some(())
        .ok_or(PortableArtifactValidationFailure::AccessBookkeepingUnavailable)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn ensure_access_bookkeeping_suppressed(
    _path: &Path,
) -> Result<(), PortableArtifactValidationFailure> {
    Err(PortableArtifactValidationFailure::AccessBookkeepingUnavailable)
}

fn access_preserving_flags(flags: OFlags) -> OFlags {
    #[cfg(target_os = "linux")]
    {
        flags | OFlags::NOATIME
    }
    #[cfg(not(target_os = "linux"))]
    {
        flags
    }
}

fn no_atime_open_failed(error: Errno) -> bool {
    cfg!(target_os = "linux") && matches!(error, Errno::PERM | Errno::INVAL)
}

fn openat_access_preserving(
    directory: &OwnedFd,
    name: &str,
    flags: OFlags,
) -> Result<Option<OwnedFd>, PortableArtifactValidationFailure> {
    match openat(
        directory,
        name,
        access_preserving_flags(flags),
        Mode::empty(),
    ) {
        Ok(descriptor) => Ok(Some(descriptor)),
        Err(error) if no_atime_open_failed(error) => {
            Err(PortableArtifactValidationFailure::AccessBookkeepingUnavailable)
        }
        Err(_) => Ok(None),
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), PortableArtifactValidationFailure> {
    if cancelled.load(Ordering::Acquire) {
        Err(PortableArtifactValidationFailure::Interrupted)
    } else {
        Ok(())
    }
}

fn inspect_root_boundary(
    root: &OwnedFd,
    cancelled: &AtomicBool,
    diagnostics: &mut Diagnostics,
) -> Result<(), PortableArtifactValidationFailure> {
    match enumerate_names(root, ROOT_OVERFLOW_ENTRY, cancelled) {
        Ok(EntryInventory::Overflow) => diagnostics.root("root_entry_limit_exceeded", None),
        Ok(EntryInventory::Complete(names)) => {
            for name in names {
                if name == RESULT_FILE.as_bytes() || name == EXPORT_DIRECTORY.as_bytes() {
                    continue;
                }
                match std::str::from_utf8(&name) {
                    Ok(name) => diagnostics.root("root_entry_unexpected", Some(name)),
                    Err(_) => diagnostics.root("boundary_name_invalid", None),
                }
            }
        }
        Err(EnumerationFailure::Interrupted) => {
            return Err(PortableArtifactValidationFailure::Interrupted);
        }
        Err(EnumerationFailure::Unavailable) => {
            diagnostics.root("artifact_directory_unavailable", None);
        }
    }
    Ok(())
}

fn require_regular_result(stat: &Stat, diagnostics: &mut Diagnostics) -> bool {
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::RegularFile => true,
        FileType::Symlink => {
            diagnostics.result("result_symbolic_link", None);
            false
        }
        _ => {
            diagnostics.result("result_not_regular_file", None);
            false
        }
    }
}

fn read_result(
    root: &OwnedFd,
    cancelled: &AtomicBool,
    diagnostics: &mut Diagnostics,
) -> Result<Option<Vec<u8>>, PortableArtifactValidationFailure> {
    let named = match statat(root, RESULT_FILE, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(Errno::NOENT | Errno::NOTDIR) => {
            diagnostics.result("result_missing", None);
            return Ok(None);
        }
        Err(_) => {
            diagnostics.result("result_unavailable", None);
            return Ok(None);
        }
    };
    if !require_regular_result(&named, diagnostics) {
        return Ok(None);
    }
    let Some(descriptor) = openat_access_preserving(
        root,
        RESULT_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
    )?
    else {
        diagnostics.result("result_unavailable", None);
        return Ok(None);
    };
    let before = match fstat(&descriptor) {
        Ok(stat) if require_regular_result(&stat, diagnostics) => stat,
        Ok(_) => return Ok(None),
        Err(_) => {
            diagnostics.result("result_unavailable", None);
            return Ok(None);
        }
    };
    if !same_identity(&named, &before) {
        diagnostics.result("result_unavailable", None);
    }

    let mut file = File::from(descriptor);
    let bytes = match read_bounded(&mut file, MAXIMUM_RESULT_BYTES, cancelled) {
        Ok(BoundedRead::Complete(bytes)) => bytes,
        Ok(BoundedRead::LimitExceeded) => {
            diagnostics.result("result_limit_exceeded", None);
            return Ok(None);
        }
        Err(ReadFailure::Interrupted) => {
            return Err(PortableArtifactValidationFailure::Interrupted);
        }
        Err(ReadFailure::Unavailable) => {
            diagnostics.result("result_unavailable", None);
            return Ok(None);
        }
    };
    if retained_file_changed(root, RESULT_FILE, &file, &before) {
        diagnostics.result("result_unavailable", None);
    }
    Ok(Some(bytes))
}

fn read_bounded(
    file: &mut File,
    maximum: u64,
    cancelled: &AtomicBool,
) -> Result<BoundedRead, ReadFailure> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err(ReadFailure::Interrupted);
        }
        let observed = u64::try_from(bytes.len()).map_err(|_| ReadFailure::Unavailable)?;
        let permitted = usize::try_from(
            maximum
                .saturating_sub(observed)
                .saturating_add(1)
                .min(COPY_BUFFER_BYTES as u64),
        )
        .map_err(|_| ReadFailure::Unavailable)?;
        let read = file
            .read(&mut buffer[..permitted])
            .map_err(|_| ReadFailure::Unavailable)?;
        if read == 0 {
            return Ok(BoundedRead::Complete(bytes));
        }
        bytes.extend_from_slice(&buffer[..read]);
        if u64::try_from(bytes.len()).map_or(true, |length| length > maximum) {
            return Ok(BoundedRead::LimitExceeded);
        }
    }
}

enum BoundedRead {
    Complete(Vec<u8>),
    LimitExceeded,
}

enum ReadFailure {
    Interrupted,
    Unavailable,
}

#[derive(Default)]
struct MetadataInspection {
    declared_exports: usize,
    available_exports: usize,
    unavailable_exports: usize,
    carriers: BTreeMap<String, CarrierGroup>,
}

type MetadataFingerprint = [u8; 32];

struct CarrierGroup {
    owner_ordinal: usize,
    first_metadata: Option<MetadataFingerprint>,
    alias_metadata_mismatch: bool,
    kinds: BTreeSet<CarrierKind>,
    size_bytes: BTreeSet<u64>,
    digests: BTreeSet<String>,
}

impl CarrierGroup {
    fn new(
        owner_ordinal: usize,
        metadata: Option<MetadataFingerprint>,
        kind: Option<CarrierKind>,
        size_bytes: Option<u64>,
        digest: Option<String>,
    ) -> Self {
        let mut group = Self {
            owner_ordinal,
            first_metadata: metadata,
            alias_metadata_mismatch: false,
            kinds: BTreeSet::new(),
            size_bytes: BTreeSet::new(),
            digests: BTreeSet::new(),
        };
        group.record(metadata, kind, size_bytes, digest);
        group
    }

    fn record(
        &mut self,
        metadata: Option<MetadataFingerprint>,
        kind: Option<CarrierKind>,
        size_bytes: Option<u64>,
        digest: Option<String>,
    ) {
        self.alias_metadata_mismatch |= metadata != self.first_metadata;
        if let Some(kind) = kind {
            self.kinds.insert(kind);
        }
        if let Some(size_bytes) = size_bytes {
            self.size_bytes.insert(size_bytes);
        }
        if let Some(digest) = digest {
            self.digests.insert(digest);
        }
    }
}

struct FingerprintWriter<'a>(&'a mut DigestContext);

impl Write for FingerprintWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn metadata_fingerprint(metadata: &Value) -> Option<MetadataFingerprint> {
    let mut context = DigestContext::new(&SHA256);
    serde_json::to_writer(FingerprintWriter(&mut context), metadata).ok()?;
    let digest = context.finish();
    let mut fingerprint = [0_u8; 32];
    fingerprint.copy_from_slice(digest.as_ref());
    Some(fingerprint)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CarrierKind {
    File,
    Text,
    Json,
}

impl CarrierKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

fn inspect_metadata(bytes: &[u8], diagnostics: &mut Diagnostics) -> MetadataInspection {
    let mut document = match result_metadata::decode_document(bytes) {
        Ok(document) => document,
        Err(ResultDocumentError::Encoding) => {
            diagnostics.result("result_encoding_invalid", None);
            return MetadataInspection::default();
        }
        Err(ResultDocumentError::Json) => {
            diagnostics.result("result_json_invalid", None);
            return MetadataInspection::default();
        }
    };

    let mut supported_schema = false;
    match document.get("schemaVersion") {
        Some(Value::Number(version)) if version.as_u64() == Some(1) => {
            supported_schema = true;
        }
        Some(Value::Number(version))
            if version.as_i64().is_some() || version.as_u64().is_some() =>
        {
            diagnostics.result("result_schema_unsupported", Some("/schemaVersion"));
        }
        _ => diagnostics.result("result_schema_invalid", Some("/schemaVersion")),
    }
    if supported_schema && result_metadata::validate_document_envelope(&mut document).is_err() {
        diagnostics.result("result_schema_invalid", None);
    }

    let Some(exports) = document.get_mut("exports").and_then(Value::as_object_mut) else {
        return MetadataInspection::default();
    };
    inspect_exports(exports, diagnostics)
}

fn inspect_exports(
    exports: &mut Map<String, Value>,
    diagnostics: &mut Diagnostics,
) -> MetadataInspection {
    let mut inspection = MetadataInspection {
        declared_exports: exports.len(),
        ..MetadataInspection::default()
    };
    if exports.len() > MAXIMUM_EXPORTS {
        diagnostics.result("export_limit_exceeded", Some("/exports"));
    }

    exports.sort_keys();
    for (index, (name, entry)) in exports.iter().enumerate() {
        let ordinal = index + 1;
        let state = entry.get("state").and_then(Value::as_str);
        match state {
            Some("available") => inspection.available_exports += 1,
            Some("unavailable") => inspection.unavailable_exports += 1,
            _ => {}
        }

        let kind_name = entry.get("kind").and_then(Value::as_str);
        let kind = match kind_name {
            Some("file") => Some(CarrierKind::File),
            Some("text") => Some(CarrierKind::Text),
            Some("json") => Some(CarrierKind::Json),
            _ => None,
        };
        let entry_object = entry.as_object();
        let direct_path = entry_object
            .and_then(|object| object.get("path"))
            .and_then(Value::as_str);
        let direct_size = entry_object
            .and_then(|object| object.get("sizeBytes"))
            .and_then(Value::as_u64);
        let direct_digest = valid_digest(entry_object.and_then(|object| object.get("digest")));
        let nested_carrier = entry_object
            .and_then(|object| object.get("carrier"))
            .and_then(Value::as_object);
        let nested_path = nested_carrier
            .and_then(|carrier| carrier.get("path"))
            .and_then(Value::as_str);
        let nested_size = nested_carrier
            .and_then(|carrier| carrier.get("sizeBytes"))
            .and_then(Value::as_u64);
        let nested_digest = valid_digest(nested_carrier.and_then(|object| object.get("digest")));
        let fingerprint = metadata_fingerprint(entry);

        let shape_valid = if !is_identifier(name) {
            false
        } else {
            match state {
                Some("unavailable") => valid_unavailable_entry(entry),
                Some("available") if kind.is_some() => valid_available_entry(entry, kind),
                _ => false,
            }
        };
        if !shape_valid {
            diagnostics.export("export_entry_invalid", name, 0);
        }

        if let (Some(kind), Some(media_type)) =
            (kind, entry.get("mediaType").and_then(Value::as_str))
            && !result_metadata::valid_export_kind(kind.as_str(), media_type)
        {
            diagnostics.export("export_media_type_invalid", name, 1);
        }

        record_carrier_reference(
            &mut inspection,
            diagnostics,
            name,
            ordinal,
            direct_path,
            fingerprint,
            kind,
            direct_size,
            direct_digest,
        );
        record_carrier_reference(
            &mut inspection,
            diagnostics,
            name,
            ordinal,
            nested_path,
            fingerprint,
            None,
            nested_size,
            nested_digest,
        );
    }
    inspection
}

#[allow(
    clippy::too_many_arguments,
    reason = "keeps one bounded carrier-reference path"
)]
fn record_carrier_reference(
    inspection: &mut MetadataInspection,
    diagnostics: &mut Diagnostics,
    name: &str,
    ordinal: usize,
    path: Option<&str>,
    fingerprint: Option<MetadataFingerprint>,
    kind: Option<CarrierKind>,
    size_bytes: Option<u64>,
    digest: Option<String>,
) {
    let Some(path) = path else {
        return;
    };
    if result_metadata::parse_carrier_ordinal(path).is_none() {
        diagnostics.export("export_path_invalid", name, 2);
        return;
    }

    let group = match inspection.carriers.entry(path.to_owned()) {
        std::collections::btree_map::Entry::Vacant(vacant) => vacant.insert(CarrierGroup::new(
            ordinal,
            fingerprint,
            kind,
            size_bytes,
            digest,
        )),
        std::collections::btree_map::Entry::Occupied(occupied) => {
            let group = occupied.into_mut();
            group.record(fingerprint, kind, size_bytes, digest);
            group
        }
    };
    if result_metadata::parse_carrier_ordinal(path) != Some(group.owner_ordinal) {
        diagnostics.export("export_ordinal_invalid", name, 3);
    }
}

fn valid_unavailable_entry(entry: &Value) -> bool {
    let Some(object) = entry.as_object() else {
        return false;
    };
    exact_keys(object, &["state", "reason"])
        && matches!(
            object.get("reason").and_then(Value::as_str),
            Some("source_failed" | "source_blocked" | "source_not_run" | "source_cancelled")
        )
}

fn valid_available_entry(entry: &Value, kind: Option<CarrierKind>) -> bool {
    let Some(object) = entry.as_object() else {
        return false;
    };
    exact_keys(
        object,
        &["state", "kind", "mediaType", "path", "sizeBytes", "digest"],
    ) && kind.is_some()
        && object.get("mediaType").and_then(Value::as_str).is_some()
        && object.get("path").and_then(Value::as_str).is_some()
        && object.get("sizeBytes").and_then(Value::as_u64).is_some()
        && valid_digest(object.get("digest")).is_some()
}

fn exact_keys(object: &Map<String, Value>, expected: &[&str]) -> bool {
    object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
}

fn valid_digest(value: Option<&Value>) -> Option<String> {
    let object = value?.as_object()?;
    if !exact_keys(object, &["algorithm", "value"])
        || object.get("algorithm").and_then(Value::as_str) != Some("sha256")
    {
        return None;
    }
    let digest = object.get("value")?.as_str()?;
    is_lowercase_hex(digest, 64).then(|| digest.to_owned())
}

fn inspect_aliases(carriers: &BTreeMap<String, CarrierGroup>, diagnostics: &mut Diagnostics) {
    for (path, group) in carriers {
        if group.alias_metadata_mismatch {
            diagnostics.alias(path);
        }
    }
}

fn open_exports_directory(
    root: &OwnedFd,
    diagnostics: &mut Diagnostics,
) -> Result<Option<OwnedFd>, PortableArtifactValidationFailure> {
    let named = match statat(root, EXPORT_DIRECTORY, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(Errno::NOENT | Errno::NOTDIR) => {
            diagnostics.root("exports_directory_missing", Some(EXPORT_DIRECTORY));
            return Ok(None);
        }
        Err(_) => {
            diagnostics.root("exports_directory_unavailable", Some(EXPORT_DIRECTORY));
            return Ok(None);
        }
    };
    match FileType::from_raw_mode(named.st_mode) {
        FileType::Symlink => {
            diagnostics.root("exports_directory_symbolic_link", Some(EXPORT_DIRECTORY));
            return Ok(None);
        }
        FileType::Directory => {}
        _ => {
            diagnostics.root("exports_directory_not_directory", Some(EXPORT_DIRECTORY));
            return Ok(None);
        }
    }
    let Some(directory) = openat_access_preserving(
        root,
        EXPORT_DIRECTORY,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
    )?
    else {
        diagnostics.root("exports_directory_unavailable", Some(EXPORT_DIRECTORY));
        return Ok(None);
    };
    Ok(match fstat(&directory) {
        Ok(opened)
            if FileType::from_raw_mode(opened.st_mode) == FileType::Directory
                && same_identity(&named, &opened) =>
        {
            Some(directory)
        }
        Ok(opened) if FileType::from_raw_mode(opened.st_mode) != FileType::Directory => {
            diagnostics.root("exports_directory_not_directory", Some(EXPORT_DIRECTORY));
            None
        }
        Ok(_) | Err(_) => {
            diagnostics.root("exports_directory_unavailable", Some(EXPORT_DIRECTORY));
            None
        }
    })
}

fn recheck_exports_directory(root: &OwnedFd, exports: &OwnedFd, diagnostics: &mut Diagnostics) {
    let opened = fstat(exports);
    match statat(root, EXPORT_DIRECTORY, AtFlags::SYMLINK_NOFOLLOW) {
        Err(Errno::NOENT | Errno::NOTDIR) => {
            diagnostics.root("exports_directory_missing", Some(EXPORT_DIRECTORY));
        }
        Ok(named) => match FileType::from_raw_mode(named.st_mode) {
            FileType::Symlink => {
                diagnostics.root("exports_directory_symbolic_link", Some(EXPORT_DIRECTORY));
            }
            FileType::Directory
                if opened
                    .as_ref()
                    .is_ok_and(|opened| same_identity(&named, opened)) => {}
            FileType::Directory => {
                diagnostics.root("exports_directory_unavailable", Some(EXPORT_DIRECTORY));
            }
            _ => diagnostics.root("exports_directory_not_directory", Some(EXPORT_DIRECTORY)),
        },
        Err(_) => diagnostics.root("exports_directory_unavailable", Some(EXPORT_DIRECTORY)),
    }
}

fn enumerate_exports(
    exports: &OwnedFd,
    cancelled: &AtomicBool,
    diagnostics: &mut Diagnostics,
) -> Result<EntryInventory, PortableArtifactValidationFailure> {
    match enumerate_names(exports, EXPORTS_OVERFLOW_ENTRY, cancelled) {
        Ok(inventory) => Ok(inventory),
        Err(EnumerationFailure::Interrupted) => Err(PortableArtifactValidationFailure::Interrupted),
        Err(EnumerationFailure::Unavailable) => {
            diagnostics.root("exports_directory_unavailable", Some(EXPORT_DIRECTORY));
            Ok(EntryInventory::Complete(BTreeSet::new()))
        }
    }
}

enum EntryInventory {
    Complete(BTreeSet<Vec<u8>>),
    Overflow,
}

enum EnumerationFailure {
    Interrupted,
    Unavailable,
}

fn enumerate_names(
    directory: &OwnedFd,
    overflow_entry: usize,
    cancelled: &AtomicBool,
) -> Result<EntryInventory, EnumerationFailure> {
    let mut names = BTreeSet::new();
    for entry in Dir::read_from(directory).map_err(|_| EnumerationFailure::Unavailable)? {
        if cancelled.load(Ordering::Acquire) {
            return Err(EnumerationFailure::Interrupted);
        }
        let entry = entry.map_err(|_| EnumerationFailure::Unavailable)?;
        let name = entry.file_name().to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        names.insert(name.to_vec());
        if names.len() >= overflow_entry {
            return Ok(EntryInventory::Overflow);
        }
    }
    Ok(EntryInventory::Complete(names))
}

fn validate_carrier(
    exports: Option<&OwnedFd>,
    path: &str,
    group: &CarrierGroup,
    total_bytes: &mut u64,
    cancelled: &AtomicBool,
    diagnostics: &mut Diagnostics,
) -> Result<(), PortableArtifactValidationFailure> {
    let Some(exports) = exports else {
        diagnostics.carrier("carrier_missing", path, 0);
        return Ok(());
    };
    let Some(name) = path.strip_prefix("exports/") else {
        return Ok(());
    };
    let named = match statat(exports, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(Errno::NOENT | Errno::NOTDIR) => {
            diagnostics.carrier("carrier_missing", path, 0);
            return Ok(());
        }
        Err(_) => {
            diagnostics.carrier("carrier_unavailable", path, 0);
            return Ok(());
        }
    };
    if !require_regular_carrier(&named, path, diagnostics) {
        return Ok(());
    }
    let Some(descriptor) = openat_access_preserving(
        exports,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
    )?
    else {
        diagnose_current_carrier(exports, name, path, diagnostics);
        return Ok(());
    };
    let before = match fstat(&descriptor) {
        Ok(stat) if require_regular_carrier(&stat, path, diagnostics) => stat,
        Ok(_) => return Ok(()),
        Err(_) => {
            diagnostics.carrier("carrier_unavailable", path, 0);
            return Ok(());
        }
    };
    if !same_identity(&named, &before) {
        diagnostics.carrier("carrier_unavailable", path, 0);
    }

    let mut file = File::from(descriptor);
    let mut digest = DigestContext::new(&SHA256);
    let mut observed = 0_u64;
    let mut complete = true;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        check_cancelled(cancelled)?;
        let per_remaining = MAXIMUM_CARRIER_BYTES.saturating_sub(observed);
        let total_remaining = MAXIMUM_TOTAL_CARRIER_BYTES.saturating_sub(*total_bytes);
        let permitted = per_remaining
            .min(total_remaining)
            .saturating_add(1)
            .min(COPY_BUFFER_BYTES as u64);
        if permitted == 0 {
            diagnostics.carrier("carrier_total_size_limit_exceeded", path, 1);
            complete = false;
            break;
        }
        let read = match file
            .read(&mut buffer[..usize::try_from(permitted).unwrap_or(COPY_BUFFER_BYTES)])
        {
            Ok(read) => read,
            Err(_) => {
                diagnostics.carrier("carrier_unavailable", path, 0);
                complete = false;
                break;
            }
        };
        if read == 0 {
            break;
        }
        let read_u64 = u64::try_from(read).unwrap_or(u64::MAX);
        observed = observed.saturating_add(read_u64);
        *total_bytes = total_bytes.saturating_add(read_u64);
        if observed > MAXIMUM_CARRIER_BYTES {
            diagnostics.carrier("carrier_size_limit_exceeded", path, 1);
            complete = false;
            break;
        }
        if *total_bytes > MAXIMUM_TOTAL_CARRIER_BYTES {
            diagnostics.carrier("carrier_total_size_limit_exceeded", path, 1);
            complete = false;
            break;
        }
        digest.update(&buffer[..read]);
    }

    if complete {
        if group
            .size_bytes
            .iter()
            .any(|expected| *expected != observed)
        {
            diagnostics.carrier("carrier_size_mismatch", path, 2);
        }
        let observed_digest = lowercase_hex(digest.finish().as_ref());
        if group
            .digests
            .iter()
            .any(|expected| expected != &observed_digest)
        {
            diagnostics.carrier("carrier_digest_mismatch", path, 3);
        }

        for profile in &group.kinds {
            match profile {
                CarrierKind::File => {}
                CarrierKind::Text => {
                    let content = file
                        .seek(SeekFrom::Start(0))
                        .map_err(|_| TextContentFailure::Unavailable)
                        .and_then(|_| validate_utf8(&mut file, cancelled));
                    match content {
                        Ok(()) => {}
                        Err(TextContentFailure::Invalid) => {
                            diagnostics.carrier("text_encoding_invalid", path, 5);
                        }
                        Err(TextContentFailure::Unavailable) => {
                            diagnostics.carrier("carrier_unavailable", path, 0);
                        }
                        Err(TextContentFailure::Interrupted) => {
                            return Err(PortableArtifactValidationFailure::Interrupted);
                        }
                    }
                }
                CarrierKind::Json => {
                    let code = match validate_json_content(&mut file, cancelled) {
                        Ok(()) => None,
                        Err(JsonContentFailure::Invalid) => Some("json_content_invalid"),
                        Err(JsonContentFailure::Noncanonical) => Some("json_content_noncanonical"),
                        Err(JsonContentFailure::Interrupted) => {
                            return Err(PortableArtifactValidationFailure::Interrupted);
                        }
                        Err(JsonContentFailure::Unavailable) => {
                            diagnostics.carrier("carrier_unavailable", path, 0);
                            None
                        }
                    };
                    if let Some(code) = code {
                        diagnostics.carrier(code, path, 6);
                    }
                }
            }
        }
    }

    if retained_file_changed(exports, name, &file, &before) {
        diagnose_current_carrier(exports, name, path, diagnostics);
    }
    Ok(())
}

fn require_regular_carrier(stat: &Stat, path: &str, diagnostics: &mut Diagnostics) -> bool {
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::RegularFile => true,
        FileType::Symlink => {
            diagnostics.carrier("carrier_symbolic_link", path, 0);
            false
        }
        _ => {
            diagnostics.carrier("carrier_not_regular_file", path, 0);
            false
        }
    }
}

fn diagnose_current_carrier(
    exports: &OwnedFd,
    name: &str,
    path: &str,
    diagnostics: &mut Diagnostics,
) {
    match statat(exports, name, AtFlags::SYMLINK_NOFOLLOW) {
        Err(Errno::NOENT | Errno::NOTDIR) => diagnostics.carrier("carrier_missing", path, 0),
        Ok(stat) => match FileType::from_raw_mode(stat.st_mode) {
            FileType::Symlink => diagnostics.carrier("carrier_symbolic_link", path, 0),
            FileType::RegularFile => diagnostics.carrier("carrier_unavailable", path, 0),
            _ => diagnostics.carrier("carrier_not_regular_file", path, 0),
        },
        Err(_) => diagnostics.carrier("carrier_unavailable", path, 0),
    }
}

fn same_identity(left: &Stat, right: &Stat) -> bool {
    left.st_dev == right.st_dev && left.st_ino == right.st_ino
}

fn retained_file_changed(directory: &OwnedFd, name: &str, file: &File, before: &Stat) -> bool {
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

enum TextContentFailure {
    Invalid,
    Interrupted,
    Unavailable,
}

fn validate_utf8(reader: &mut impl Read, cancelled: &AtomicBool) -> Result<(), TextContentFailure> {
    let mut pending = Vec::with_capacity(4);
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err(TextContentFailure::Interrupted);
        }
        let read = reader
            .read(&mut buffer)
            .map_err(|_| TextContentFailure::Unavailable)?;
        if read == 0 {
            return pending
                .is_empty()
                .then_some(())
                .ok_or(TextContentFailure::Invalid);
        }
        pending.extend_from_slice(&buffer[..read]);
        match std::str::from_utf8(&pending) {
            Ok(_) => pending.clear(),
            Err(error) if error.error_len().is_some() => {
                return Err(TextContentFailure::Invalid);
            }
            Err(error) => {
                let suffix = pending.split_off(error.valid_up_to());
                if suffix.len() > 3 {
                    return Err(TextContentFailure::Invalid);
                }
                pending = suffix;
            }
        }
    }
}

enum JsonContentFailure {
    Invalid,
    Noncanonical,
    Interrupted,
    Unavailable,
}

fn validate_json_content(
    file: &mut File,
    cancelled: &AtomicBool,
) -> Result<(), JsonContentFailure> {
    let mut reader = CancellableReader::new(file, cancelled);
    let validation = artifact_json::validate(&mut reader);
    if reader.interrupted {
        return Err(JsonContentFailure::Interrupted);
    }
    validation.map_err(|failure| match failure {
        ArtifactJsonFailure::Invalid => JsonContentFailure::Invalid,
        ArtifactJsonFailure::Noncanonical => JsonContentFailure::Noncanonical,
        ArtifactJsonFailure::Unavailable => JsonContentFailure::Unavailable,
    })
}

struct CancellableReader<'a, Reader> {
    inner: &'a mut Reader,
    cancelled: &'a AtomicBool,
    interrupted: bool,
}

impl<'a, Reader> CancellableReader<'a, Reader> {
    fn new(inner: &'a mut Reader, cancelled: &'a AtomicBool) -> Self {
        Self {
            inner,
            cancelled,
            interrupted: false,
        }
    }

    fn ensure_active(&mut self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            self.interrupted = true;
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "validation interrupted",
            ))
        } else {
            Ok(())
        }
    }
}

impl<Reader: Read> Read for CancellableReader<'_, Reader> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.ensure_active()?;
        self.inner.read(bytes)
    }
}

impl<Reader: Seek> Seek for CancellableReader<'_, Reader> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.ensure_active()?;
        self.inner.seek(position)
    }
}
