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
use super::git_artifact::{
    GitArtifactDescriptor, GitArtifactFailure, GitArtifactValidationBudget, validate_git_bundle,
};
use super::presentation::visible_text;
use super::result_metadata::{self, ResultDocumentError};
use super::schema_common::{is_identifier, is_lowercase_hex, lowercase_hex};

const RESULT_FILE: &str = "result.json";
const EXPORT_DIRECTORY: &str = "exports";
const ROOT_OVERFLOW_ENTRY: usize = 4_097;
const MAXIMUM_EXPORTS: usize = 4_096;
const MAXIMUM_CARRIERS: usize = 4_096;
const EXPORTS_OVERFLOW_ENTRY: usize = 4_097;
const MAXIMUM_CARRIER_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_TOTAL_CARRIER_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_DIAGNOSTICS: usize = 8_192;
const COPY_BUFFER_BYTES: usize = 64 * 1024;

include!("portable_artifact_diagnostics_generated.rs");

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
pub struct ArtifactDiagnostic {
    code: ArtifactDiagnosticCode,
    message: &'static str,
    location: ArtifactDiagnosticLocation,
}

impl ArtifactDiagnostic {
    pub fn code(&self) -> &'static str {
        self.code.as_str()
    }

    pub fn message(&self) -> &'static str {
        self.message
    }

    pub fn missing_carrier_path(&self) -> Option<&str> {
        match &self.location {
            ArtifactDiagnosticLocation::Carrier { path }
                if self.code == ArtifactDiagnosticCode::CarrierMissing =>
            {
                Some(path)
            }
            _ => None,
        }
    }

    pub fn human_location(&self) -> String {
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
pub struct ArtifactValidationSummary {
    pub declared_exports: u64,
    pub(crate) available_exports: u64,
    pub(crate) unavailable_exports: u64,
    pub referenced_carriers: u64,
    pub carrier_bytes: u64,
}

#[derive(Debug)]
pub struct PortableArtifactValidation {
    pub artifact_directory: Option<String>,
    pub diagnostics: Vec<ArtifactDiagnostic>,
    pub summary: Option<ArtifactValidationSummary>,
}

impl PortableArtifactValidation {
    pub fn is_valid(&self) -> bool {
        self.diagnostics.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortableArtifactValidationFailure {
    CurrentDirectoryUnavailable,
    ScratchUnavailable,
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
    public_orders: BTreeMap<(ArtifactDiagnosticCode, ArtifactDiagnosticLocation), DiagnosticOrder>,
    limit_exceeded: bool,
}

impl Diagnostics {
    fn push(
        &mut self,
        order: DiagnosticOrder,
        code: ArtifactDiagnosticCode,
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

    fn root(&mut self, code: ArtifactDiagnosticCode, path: Option<&str>) {
        self.push(
            DiagnosticOrder {
                stage: 0,
                major: Vec::new(),
                check: code.rank(),
                code: code.rank(),
                minor: path.unwrap_or_default().as_bytes().to_vec(),
            },
            code,
            boundary_location(path),
        );
    }

    fn result(&mut self, code: ArtifactDiagnosticCode, pointer: Option<&str>) {
        self.push(
            DiagnosticOrder {
                stage: 1,
                major: Vec::new(),
                check: code.rank(),
                code: code.rank(),
                minor: pointer.unwrap_or_default().as_bytes().to_vec(),
            },
            code,
            ArtifactDiagnosticLocation::Result {
                pointer: pointer.map(str::to_owned),
            },
        );
    }

    fn export(&mut self, code: ArtifactDiagnosticCode, export: &str, check: usize) {
        self.push(
            DiagnosticOrder {
                stage: 2,
                major: export.as_bytes().to_vec(),
                check,
                code: code.rank(),
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
                code: ArtifactDiagnosticCode::AliasMetadataMismatch.rank(),
                minor: Vec::new(),
            },
            ArtifactDiagnosticCode::AliasMetadataMismatch,
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
                code: ArtifactDiagnosticCode::CarrierLimitExceeded.rank(),
                minor: Vec::new(),
            },
            ArtifactDiagnosticCode::CarrierLimitExceeded,
            ArtifactDiagnosticLocation::Result {
                pointer: Some("/exports".to_owned()),
            },
        );
    }

    fn carrier(&mut self, code: ArtifactDiagnosticCode, path: &str, check: usize) {
        self.push(
            DiagnosticOrder {
                stage: 5,
                major: path.as_bytes().to_vec(),
                check,
                code: code.rank(),
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
                code: ArtifactDiagnosticCode::CarrierLimitExceeded.rank(),
                minor: Vec::new(),
            },
            ArtifactDiagnosticCode::CarrierLimitExceeded,
            ArtifactDiagnosticLocation::ArtifactDirectory,
        );
    }

    fn inventory(&mut self, code: ArtifactDiagnosticCode, path: Option<&str>) {
        self.push(
            DiagnosticOrder {
                stage: 6,
                major: path.unwrap_or_default().as_bytes().to_vec(),
                check: 1,
                code: code.rank(),
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
                code: ArtifactDiagnosticCode::DiagnosticLimitExceeded,
                message: diagnostic_message(ArtifactDiagnosticCode::DiagnosticLimitExceeded),
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

fn diagnostic_message(code: ArtifactDiagnosticCode) -> &'static str {
    match code {
        ArtifactDiagnosticCode::ArtifactDirectoryInvalid => {
            "The artifact directory path cannot be represented safely."
        }
        ArtifactDiagnosticCode::ArtifactDirectoryUnavailable => {
            "The artifact directory is unavailable."
        }
        ArtifactDiagnosticCode::ArtifactDirectoryNotDirectory => {
            "The artifact directory path is not a directory."
        }
        ArtifactDiagnosticCode::RootEntryLimitExceeded => {
            "The artifact directory contains too many entries."
        }
        ArtifactDiagnosticCode::RootEntryUnexpected => {
            "The artifact directory contains an unexpected entry."
        }
        ArtifactDiagnosticCode::BoundaryNameInvalid => "A directory entry name is not valid UTF-8.",
        ArtifactDiagnosticCode::ResultMissing => "The artifact set does not contain result.json.",
        ArtifactDiagnosticCode::ResultSymbolicLink => "result.json is a symbolic link.",
        ArtifactDiagnosticCode::ResultNotRegularFile => "result.json is not a regular file.",
        ArtifactDiagnosticCode::ResultUnavailable => "result.json could not be read safely.",
        ArtifactDiagnosticCode::ResultLimitExceeded => {
            "result.json exceeds the artifact set limit."
        }
        ArtifactDiagnosticCode::ResultEncodingInvalid => {
            "result.json does not use the required UTF-8 encoding."
        }
        ArtifactDiagnosticCode::ResultJsonInvalid => {
            "result.json is not one complete unique-member JSON value."
        }
        ArtifactDiagnosticCode::ResultSchemaUnsupported => {
            "result.json uses an unsupported schema version."
        }
        ArtifactDiagnosticCode::RecoverySchemaUnsupported => {
            "result.json uses an unsupported recovery summary schema version."
        }
        ArtifactDiagnosticCode::ResultSchemaInvalid => {
            "result.json violates the portable workflow result contract."
        }
        ArtifactDiagnosticCode::ExportsDirectoryMissing => {
            "The artifact set does not contain exports."
        }
        ArtifactDiagnosticCode::ExportsDirectorySymbolicLink => "exports is a symbolic link.",
        ArtifactDiagnosticCode::ExportsDirectoryNotDirectory => "exports is not a directory.",
        ArtifactDiagnosticCode::ExportsDirectoryUnavailable => {
            "The exports directory could not be read safely."
        }
        ArtifactDiagnosticCode::ExportLimitExceeded => "result.json declares too many exports.",
        ArtifactDiagnosticCode::ExportEntryInvalid => {
            "The export entry violates the closed artifact set shape."
        }
        ArtifactDiagnosticCode::ExportMediaTypeInvalid => {
            "The export media type is invalid for its artifact kind."
        }
        ArtifactDiagnosticCode::ExportPathInvalid => {
            "The export carrier path is not a portable artifact path."
        }
        ArtifactDiagnosticCode::ExportOrdinalInvalid => {
            "The export carrier path does not use its physical owner's ordinal."
        }
        ArtifactDiagnosticCode::AliasMetadataMismatch => {
            "Exports sharing a carrier path do not repeat identical metadata."
        }
        ArtifactDiagnosticCode::CarrierLimitExceeded => {
            "The artifact set exceeds the carrier limit."
        }
        ArtifactDiagnosticCode::CarrierMissing => "A referenced carrier is missing.",
        ArtifactDiagnosticCode::CarrierSymbolicLink => "A referenced carrier is a symbolic link.",
        ArtifactDiagnosticCode::CarrierNotRegularFile => {
            "A referenced carrier is not a regular file."
        }
        ArtifactDiagnosticCode::CarrierUnavailable => {
            "A referenced carrier could not be read safely."
        }
        ArtifactDiagnosticCode::CarrierSizeLimitExceeded => {
            "A carrier exceeds the per-carrier byte limit."
        }
        ArtifactDiagnosticCode::CarrierTotalSizeLimitExceeded => {
            "The artifact set exceeds the aggregate carrier byte limit."
        }
        ArtifactDiagnosticCode::CarrierSizeMismatch => {
            "The carrier size does not match result.json."
        }
        ArtifactDiagnosticCode::CarrierDigestMismatch => {
            "The carrier digest does not match result.json."
        }
        ArtifactDiagnosticCode::CarrierUnreferenced => {
            "The exports directory contains an unreferenced entry."
        }
        ArtifactDiagnosticCode::TextEncodingInvalid => "The text carrier is not valid UTF-8.",
        ArtifactDiagnosticCode::JsonContentInvalid => {
            "The JSON carrier is not one valid RFC 8259 JSON value."
        }
        ArtifactDiagnosticCode::JsonContentNoncanonical => {
            "The JSON carrier is not in compact ordered canonical form."
        }
        ArtifactDiagnosticCode::GitZeroDeltaInvalid => "The zero-delta Git artifact is invalid.",
        ArtifactDiagnosticCode::GitBundleHeaderInvalid => "The Git bundle header is invalid.",
        ArtifactDiagnosticCode::GitBundleProfileInvalid => {
            "The Git bundle does not satisfy the Scherzo profile."
        }
        ArtifactDiagnosticCode::GitPackInvalid => "The Git pack stream is invalid.",
        ArtifactDiagnosticCode::GitPackChecksumMismatch => "The Git pack checksum does not match.",
        ArtifactDiagnosticCode::GitContentInvalid => {
            "The Git bundle content does not match its descriptor."
        }
        ArtifactDiagnosticCode::GitStructureLimitExceeded => {
            "The Git artifact exceeds a structural validation limit."
        }
        ArtifactDiagnosticCode::DiagnosticLimitExceeded => {
            "Additional artifact diagnostics were omitted at the report limit."
        }
    }
}

pub fn validate_portable_artifact_set(
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
                diagnostics.root(ArtifactDiagnosticCode::ArtifactDirectoryInvalid, None);
            }
            diagnostics.root(ArtifactDiagnosticCode::ArtifactDirectoryUnavailable, None);
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
        diagnostics.root(ArtifactDiagnosticCode::ArtifactDirectoryInvalid, None);
    }

    let root = match open(
        &canonical,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(root) => root,
        Err(Errno::NOTDIR) => {
            diagnostics.root(ArtifactDiagnosticCode::ArtifactDirectoryNotDirectory, None);
            return Ok(finish_report(
                artifact_directory,
                diagnostics,
                empty_summary(),
            ));
        }
        Err(_) => {
            diagnostics.root(ArtifactDiagnosticCode::ArtifactDirectoryUnavailable, None);
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
    let mut git_budget = GitArtifactValidationBudget::default();
    for (path, group) in &metadata.carriers {
        check_cancelled(cancelled)?;
        validate_carrier(
            exports.as_ref(),
            path,
            group,
            &mut total_bytes,
            &mut git_budget,
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
                        Ok(name) => diagnostics.inventory(
                            ArtifactDiagnosticCode::CarrierUnreferenced,
                            Some(&format!("exports/{name}")),
                        ),
                        Err(_) => {
                            diagnostics.inventory(ArtifactDiagnosticCode::BoundaryNameInvalid, None)
                        }
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

fn openat_read_only(directory: &OwnedFd, name: &str, flags: OFlags) -> Option<OwnedFd> {
    openat(directory, name, flags, Mode::empty()).ok()
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
        Ok(EntryInventory::Overflow) => {
            diagnostics.root(ArtifactDiagnosticCode::RootEntryLimitExceeded, None)
        }
        Ok(EntryInventory::Complete(names)) => {
            for name in names {
                if name == RESULT_FILE.as_bytes() || name == EXPORT_DIRECTORY.as_bytes() {
                    continue;
                }
                match std::str::from_utf8(&name) {
                    Ok(name) => {
                        diagnostics.root(ArtifactDiagnosticCode::RootEntryUnexpected, Some(name))
                    }
                    Err(_) => diagnostics.root(ArtifactDiagnosticCode::BoundaryNameInvalid, None),
                }
            }
        }
        Err(EnumerationFailure::Interrupted) => {
            return Err(PortableArtifactValidationFailure::Interrupted);
        }
        Err(EnumerationFailure::Unavailable) => {
            diagnostics.root(ArtifactDiagnosticCode::ArtifactDirectoryUnavailable, None);
        }
    }
    Ok(())
}

fn require_regular_result(stat: &Stat, diagnostics: &mut Diagnostics) -> bool {
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::RegularFile => true,
        FileType::Symlink => {
            diagnostics.result(ArtifactDiagnosticCode::ResultSymbolicLink, None);
            false
        }
        _ => {
            diagnostics.result(ArtifactDiagnosticCode::ResultNotRegularFile, None);
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
            diagnostics.result(ArtifactDiagnosticCode::ResultMissing, None);
            return Ok(None);
        }
        Err(_) => {
            diagnostics.result(ArtifactDiagnosticCode::ResultUnavailable, None);
            return Ok(None);
        }
    };
    if !require_regular_result(&named, diagnostics) {
        return Ok(None);
    }
    let Some(descriptor) = openat_read_only(
        root,
        RESULT_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
    ) else {
        diagnostics.result(ArtifactDiagnosticCode::ResultUnavailable, None);
        return Ok(None);
    };
    let before = match fstat(&descriptor) {
        Ok(stat) if require_regular_result(&stat, diagnostics) => stat,
        Ok(_) => return Ok(None),
        Err(_) => {
            diagnostics.result(ArtifactDiagnosticCode::ResultUnavailable, None);
            return Ok(None);
        }
    };
    if !same_identity(&named, &before) {
        diagnostics.result(ArtifactDiagnosticCode::ResultUnavailable, None);
    }

    let mut file = File::from(descriptor);
    let bytes = match read_bounded(
        &mut file,
        result_metadata::MAXIMUM_RESULT_JSON_BYTES,
        cancelled,
    ) {
        Ok(BoundedRead::Complete(bytes)) => bytes,
        Ok(BoundedRead::LimitExceeded) => {
            diagnostics.result(ArtifactDiagnosticCode::ResultLimitExceeded, None);
            return Ok(None);
        }
        Err(ReadFailure::Interrupted) => {
            return Err(PortableArtifactValidationFailure::Interrupted);
        }
        Err(ReadFailure::Unavailable) => {
            diagnostics.result(ArtifactDiagnosticCode::ResultUnavailable, None);
            return Ok(None);
        }
    };
    if retained_file_changed(root, RESULT_FILE, &file, &before) {
        diagnostics.result(ArtifactDiagnosticCode::ResultUnavailable, None);
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
    git_descriptors: BTreeSet<GitDescriptor>,
    size_bytes: BTreeSet<u64>,
    digests: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GitDescriptor {
    base_oid: String,
    head_oid: String,
    tree_oid: String,
}

impl CarrierGroup {
    fn new(
        owner_ordinal: usize,
        metadata: Option<MetadataFingerprint>,
        kind: Option<CarrierKind>,
        descriptor: Option<GitDescriptor>,
        size_bytes: Option<u64>,
        digest: Option<String>,
    ) -> Self {
        let mut group = Self {
            owner_ordinal,
            first_metadata: metadata,
            alias_metadata_mismatch: false,
            kinds: BTreeSet::new(),
            git_descriptors: BTreeSet::new(),
            size_bytes: BTreeSet::new(),
            digests: BTreeSet::new(),
        };
        group.record(metadata, kind, descriptor, size_bytes, digest);
        group
    }

    fn record(
        &mut self,
        metadata: Option<MetadataFingerprint>,
        kind: Option<CarrierKind>,
        descriptor: Option<GitDescriptor>,
        size_bytes: Option<u64>,
        digest: Option<String>,
    ) {
        self.alias_metadata_mismatch |= metadata != self.first_metadata;
        if let Some(kind) = kind {
            self.kinds.insert(kind);
        }
        if let Some(descriptor) = descriptor {
            self.git_descriptors.insert(descriptor);
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
    GitBranch,
}

impl CarrierKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Text => "text",
            Self::Json => "json",
            Self::GitBranch => "git_branch",
        }
    }
}

fn inspect_metadata(bytes: &[u8], diagnostics: &mut Diagnostics) -> MetadataInspection {
    let mut document = match result_metadata::decode_document(bytes) {
        Ok(document) => document,
        Err(ResultDocumentError::Encoding) => {
            diagnostics.result(ArtifactDiagnosticCode::ResultEncodingInvalid, None);
            return MetadataInspection::default();
        }
        Err(ResultDocumentError::Json) => {
            diagnostics.result(ArtifactDiagnosticCode::ResultJsonInvalid, None);
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
            diagnostics.result(
                ArtifactDiagnosticCode::ResultSchemaUnsupported,
                Some("/schemaVersion"),
            );
        }
        _ => diagnostics.result(
            ArtifactDiagnosticCode::ResultSchemaInvalid,
            Some("/schemaVersion"),
        ),
    }
    if supported_schema && result_metadata::dispatch_recovery_summary_versions(&document).is_err() {
        diagnostics.result(
            ArtifactDiagnosticCode::RecoverySchemaUnsupported,
            Some("/steps"),
        );
        supported_schema = false;
    }
    if supported_schema && result_metadata::validate_document_envelope(&mut document).is_err() {
        diagnostics.result(ArtifactDiagnosticCode::ResultSchemaInvalid, None);
    }

    let continuation = document.get("continuation").is_some();
    let output_producers = document.get("outputProducers").map_or_else(
        || Some(BTreeMap::new()),
        |producers| serde_json::from_value(producers.clone()).ok(),
    );
    let export_sources = document.get("exportSources").map_or_else(
        || Some(BTreeMap::new()),
        |sources| serde_json::from_value(sources.clone()).ok(),
    );
    let source_states = portable_source_states(&document);
    let Some(exports) = document.get_mut("exports").and_then(Value::as_object_mut) else {
        return MetadataInspection::default();
    };
    let sources_match = export_sources.as_ref().is_some_and(|sources| {
        if continuation {
            sources.keys().eq(exports.keys())
        } else {
            sources.is_empty()
        }
    });
    if !sources_match || source_states.is_none() {
        diagnostics.result(
            ArtifactDiagnosticCode::ResultSchemaInvalid,
            Some("/exportSources"),
        );
    }
    inspect_exports(
        exports,
        continuation,
        output_producers.as_ref(),
        export_sources.as_ref(),
        source_states.as_ref(),
        diagnostics,
    )
}

#[derive(Clone, Copy)]
struct PortableSourceState {
    role: super::publication::WorkflowNodeRoleV1,
    state: super::publication::WorkflowStepStateV1,
    inherited_prior_state: Option<super::evidence::InheritedPriorState>,
}

type PortableSourceStates = BTreeMap<String, PortableSourceState>;

fn portable_source_states(document: &Value) -> Option<PortableSourceStates> {
    let mut steps = serde_json::from_value::<Vec<super::publication::WorkflowStepV1>>(
        document.get("steps")?.clone(),
    )
    .ok()?;
    if let Some(finalizers) = document
        .get("finalization")
        .and_then(|finalization| finalization.get("finalizers"))
    {
        steps.extend(
            serde_json::from_value::<Vec<super::publication::WorkflowStepV1>>(finalizers.clone())
                .ok()?,
        );
    }
    let mut states = BTreeMap::new();
    for step in steps {
        let inherited_prior_state = match step.detail {
            Some(super::evidence::NodeDetail::Inherited(detail)) => Some(detail.prior_state),
            _ => None,
        };
        if states
            .insert(
                step.id,
                PortableSourceState {
                    role: step.role,
                    state: step.state,
                    inherited_prior_state,
                },
            )
            .is_some()
        {
            return None;
        }
    }
    Some(states)
}

fn inspect_exports(
    exports: &mut Map<String, Value>,
    continuation: bool,
    output_producers: Option<&BTreeMap<String, BTreeMap<String, super::runtime::OutputProducer>>>,
    export_sources: Option<&BTreeMap<String, super::publication::ExportSourceV1>>,
    source_states: Option<&PortableSourceStates>,
    diagnostics: &mut Diagnostics,
) -> MetadataInspection {
    let mut inspection = MetadataInspection {
        declared_exports: exports.len(),
        ..MetadataInspection::default()
    };
    if exports.len() > MAXIMUM_EXPORTS {
        diagnostics.result(
            ArtifactDiagnosticCode::ExportLimitExceeded,
            Some("/exports"),
        );
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
            Some("git_branch") => Some(CarrierKind::GitBranch),
            _ => None,
        };
        let descriptor = (kind == Some(CarrierKind::GitBranch))
            .then(|| git_descriptor(entry))
            .flatten();
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
        let invalid_zero_delta = descriptor.as_ref().is_some_and(|descriptor| {
            descriptor.base_oid == descriptor.head_oid && nested_carrier.is_some()
        });

        let source = export_sources.and_then(|sources| sources.get(name));
        let source_state = source.and_then(|source| {
            source_states
                .and_then(|states| states.get(&source.node.id))
                .filter(|state| state.role == source.node.role)
        });
        let origin = PortableExportOrigin {
            continuation,
            output_producers,
            source,
            source_state: source_state.map(|state| state.state),
            source_inherited_prior_state: source_state
                .and_then(|state| state.inherited_prior_state),
        };
        let shape_valid = if !is_identifier(name) {
            false
        } else {
            match state {
                Some("unavailable") => valid_unavailable_entry(entry, origin),
                Some("available") if kind == Some(CarrierKind::GitBranch) => {
                    valid_git_branch_entry(entry, descriptor.as_ref(), invalid_zero_delta, origin)
                }
                Some("available") if kind.is_some() => valid_available_entry(entry, kind, origin),
                _ => false,
            }
        };
        if !shape_valid {
            diagnostics.export(ArtifactDiagnosticCode::ExportEntryInvalid, name, 0);
        }
        if invalid_zero_delta {
            diagnostics.export(ArtifactDiagnosticCode::GitZeroDeltaInvalid, name, 4);
        }

        if let (Some(kind), Some(media_type)) =
            (kind, entry.get("mediaType").and_then(Value::as_str))
            && kind != CarrierKind::GitBranch
            && !result_metadata::valid_export_kind(kind.as_str(), media_type)
        {
            diagnostics.export(ArtifactDiagnosticCode::ExportMediaTypeInvalid, name, 1);
        }
        if kind == Some(CarrierKind::GitBranch)
            && nested_carrier
                .and_then(|carrier| carrier.get("mediaType"))
                .and_then(Value::as_str)
                .is_some_and(|media_type| media_type != "application/vnd.git.bundle")
        {
            diagnostics.export(ArtifactDiagnosticCode::ExportMediaTypeInvalid, name, 1);
        }

        record_carrier_reference(
            &mut inspection,
            diagnostics,
            name,
            ordinal,
            direct_path,
            fingerprint,
            kind.filter(|kind| *kind != CarrierKind::GitBranch),
            None,
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
            (kind == Some(CarrierKind::GitBranch)).then_some(CarrierKind::GitBranch),
            descriptor,
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
    descriptor: Option<GitDescriptor>,
    size_bytes: Option<u64>,
    digest: Option<String>,
) {
    let Some(path) = path else {
        return;
    };
    if result_metadata::parse_carrier_ordinal(path).is_none() {
        diagnostics.export(ArtifactDiagnosticCode::ExportPathInvalid, name, 2);
        return;
    }

    let group = match inspection.carriers.entry(path.to_owned()) {
        std::collections::btree_map::Entry::Vacant(vacant) => vacant.insert(CarrierGroup::new(
            ordinal,
            fingerprint,
            kind,
            descriptor,
            size_bytes,
            digest,
        )),
        std::collections::btree_map::Entry::Occupied(occupied) => {
            let group = occupied.into_mut();
            group.record(fingerprint, kind, descriptor, size_bytes, digest);
            group
        }
    };
    if result_metadata::parse_carrier_ordinal(path) != Some(group.owner_ordinal) {
        diagnostics.export(ArtifactDiagnosticCode::ExportOrdinalInvalid, name, 3);
    }
}

#[derive(Clone, Copy)]
struct PortableExportOrigin<'a> {
    continuation: bool,
    output_producers:
        Option<&'a BTreeMap<String, BTreeMap<String, super::runtime::OutputProducer>>>,
    source: Option<&'a super::publication::ExportSourceV1>,
    source_state: Option<super::publication::WorkflowStepStateV1>,
    source_inherited_prior_state: Option<super::evidence::InheritedPriorState>,
}

fn valid_unavailable_entry(entry: &Value, origin: PortableExportOrigin<'_>) -> bool {
    let Some(object) = entry.as_object() else {
        return false;
    };
    let Some(reason) = object
        .get("reason")
        .and_then(|reason| serde_json::from_value(reason.clone()).ok())
    else {
        return false;
    };
    if !exact_keys(object, &["state", "reason"]) {
        return false;
    }
    if !origin.continuation {
        return origin.source.is_none();
    }
    let (Some(source), Some(source_state), Some(output_producers)) =
        (origin.source, origin.source_state, origin.output_producers)
    else {
        return false;
    };
    result_metadata::unavailable_export_source_matches(
        output_producers,
        source,
        source_state,
        origin.source_inherited_prior_state,
        reason,
    )
}

fn valid_available_entry(
    entry: &Value,
    kind: Option<CarrierKind>,
    origin: PortableExportOrigin<'_>,
) -> bool {
    let Some(object) = entry.as_object() else {
        return false;
    };
    exact_export_keys(
        object,
        &["state", "kind", "mediaType", "path", "sizeBytes", "digest"],
        origin,
    ) && kind.is_some()
        && object.get("mediaType").and_then(Value::as_str).is_some()
        && object.get("path").and_then(Value::as_str).is_some()
        && object.get("sizeBytes").and_then(Value::as_u64).is_some()
        && valid_digest(object.get("digest")).is_some()
}

fn git_descriptor(entry: &Value) -> Option<GitDescriptor> {
    let object = entry.as_object()?;
    if object.get("artifactVersion").and_then(Value::as_u64) != Some(1)
        || object.get("objectFormat").and_then(Value::as_str) != Some("sha1")
    {
        return None;
    }
    let base_oid = object.get("baseOid")?.as_str()?;
    let head_oid = object.get("headOid")?.as_str()?;
    let tree_oid = object.get("treeOid")?.as_str()?;
    if !is_lowercase_hex(base_oid, 40)
        || !is_lowercase_hex(head_oid, 40)
        || !is_lowercase_hex(tree_oid, 40)
    {
        return None;
    }
    Some(GitDescriptor {
        base_oid: base_oid.to_owned(),
        head_oid: head_oid.to_owned(),
        tree_oid: tree_oid.to_owned(),
    })
}

fn valid_git_branch_entry(
    entry: &Value,
    descriptor: Option<&GitDescriptor>,
    invalid_zero_delta: bool,
    origin: PortableExportOrigin<'_>,
) -> bool {
    let (Some(object), Some(descriptor)) = (entry.as_object(), descriptor) else {
        return false;
    };
    if descriptor.base_oid == descriptor.head_oid && !invalid_zero_delta {
        exact_export_keys(
            object,
            &[
                "state",
                "kind",
                "artifactVersion",
                "objectFormat",
                "baseOid",
                "headOid",
                "treeOid",
            ],
            origin,
        )
    } else {
        exact_export_keys(
            object,
            &[
                "state",
                "kind",
                "artifactVersion",
                "objectFormat",
                "baseOid",
                "headOid",
                "treeOid",
                "carrier",
            ],
            origin,
        ) && object
            .get("carrier")
            .and_then(Value::as_object)
            .is_some_and(valid_git_carrier)
    }
}

fn exact_export_keys(
    object: &Map<String, Value>,
    base: &[&str],
    origin: PortableExportOrigin<'_>,
) -> bool {
    let provenance = object
        .get("provenance")
        .and_then(|value| serde_json::from_value(value.clone()).ok());
    let producer = object
        .get("producer")
        .and_then(|value| serde_json::from_value(value.clone()).ok());
    let origin_valid = if origin.continuation {
        let (Some(output_producers), Some(source), Some(source_state)) =
            (origin.output_producers, origin.source, origin.source_state)
        else {
            return false;
        };
        result_metadata::export_origin_matches(
            output_producers,
            source,
            source_state,
            provenance.as_ref(),
            producer.as_ref(),
        )
    } else {
        origin.source.is_none() && provenance.is_none() && producer.is_none()
    };
    let origin_fields = usize::from(provenance.is_some()) + usize::from(producer.is_some());
    origin_valid
        && object.len() == base.len() + origin_fields
        && base.iter().all(|key| object.contains_key(*key))
}

fn valid_git_carrier(carrier: &Map<String, Value>) -> bool {
    exact_keys(carrier, &["path", "mediaType", "sizeBytes", "digest"])
        && carrier.get("path").and_then(Value::as_str).is_some()
        && carrier.get("mediaType").and_then(Value::as_str) == Some("application/vnd.git.bundle")
        && carrier.get("sizeBytes").and_then(Value::as_u64).is_some()
        && valid_digest(carrier.get("digest")).is_some()
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
            diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectoryMissing,
                Some(EXPORT_DIRECTORY),
            );
            return Ok(None);
        }
        Err(_) => {
            diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectoryUnavailable,
                Some(EXPORT_DIRECTORY),
            );
            return Ok(None);
        }
    };
    match FileType::from_raw_mode(named.st_mode) {
        FileType::Symlink => {
            diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectorySymbolicLink,
                Some(EXPORT_DIRECTORY),
            );
            return Ok(None);
        }
        FileType::Directory => {}
        _ => {
            diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectoryNotDirectory,
                Some(EXPORT_DIRECTORY),
            );
            return Ok(None);
        }
    }
    let Some(directory) = openat_read_only(
        root,
        EXPORT_DIRECTORY,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
    ) else {
        diagnostics.root(
            ArtifactDiagnosticCode::ExportsDirectoryUnavailable,
            Some(EXPORT_DIRECTORY),
        );
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
            diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectoryNotDirectory,
                Some(EXPORT_DIRECTORY),
            );
            None
        }
        Ok(_) | Err(_) => {
            diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectoryUnavailable,
                Some(EXPORT_DIRECTORY),
            );
            None
        }
    })
}

fn recheck_exports_directory(root: &OwnedFd, exports: &OwnedFd, diagnostics: &mut Diagnostics) {
    let opened = fstat(exports);
    match statat(root, EXPORT_DIRECTORY, AtFlags::SYMLINK_NOFOLLOW) {
        Err(Errno::NOENT | Errno::NOTDIR) => {
            diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectoryMissing,
                Some(EXPORT_DIRECTORY),
            );
        }
        Ok(named) => match FileType::from_raw_mode(named.st_mode) {
            FileType::Symlink => {
                diagnostics.root(
                    ArtifactDiagnosticCode::ExportsDirectorySymbolicLink,
                    Some(EXPORT_DIRECTORY),
                );
            }
            FileType::Directory
                if opened
                    .as_ref()
                    .is_ok_and(|opened| same_identity(&named, opened)) => {}
            FileType::Directory => {
                diagnostics.root(
                    ArtifactDiagnosticCode::ExportsDirectoryUnavailable,
                    Some(EXPORT_DIRECTORY),
                );
            }
            _ => diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectoryNotDirectory,
                Some(EXPORT_DIRECTORY),
            ),
        },
        Err(_) => diagnostics.root(
            ArtifactDiagnosticCode::ExportsDirectoryUnavailable,
            Some(EXPORT_DIRECTORY),
        ),
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
            diagnostics.root(
                ArtifactDiagnosticCode::ExportsDirectoryUnavailable,
                Some(EXPORT_DIRECTORY),
            );
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
    git_budget: &mut GitArtifactValidationBudget,
    cancelled: &AtomicBool,
    diagnostics: &mut Diagnostics,
) -> Result<(), PortableArtifactValidationFailure> {
    let Some(exports) = exports else {
        diagnostics.carrier(ArtifactDiagnosticCode::CarrierMissing, path, 0);
        return Ok(());
    };
    let Some(name) = path.strip_prefix("exports/") else {
        return Ok(());
    };
    let named = match statat(exports, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(Errno::NOENT | Errno::NOTDIR) => {
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierMissing, path, 0);
            return Ok(());
        }
        Err(_) => {
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierUnavailable, path, 0);
            return Ok(());
        }
    };
    if !require_regular_carrier(&named, path, diagnostics) {
        return Ok(());
    }
    let Some(descriptor) = openat_read_only(
        exports,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
    ) else {
        diagnose_current_carrier(exports, name, path, diagnostics);
        return Ok(());
    };
    let before = match fstat(&descriptor) {
        Ok(stat) if require_regular_carrier(&stat, path, diagnostics) => stat,
        Ok(_) => return Ok(()),
        Err(_) => {
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierUnavailable, path, 0);
            return Ok(());
        }
    };
    if !same_identity(&named, &before) {
        diagnostics.carrier(ArtifactDiagnosticCode::CarrierUnavailable, path, 0);
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
            diagnostics.carrier(
                ArtifactDiagnosticCode::CarrierTotalSizeLimitExceeded,
                path,
                1,
            );
            complete = false;
            break;
        }
        let read = match file
            .read(&mut buffer[..usize::try_from(permitted).unwrap_or(COPY_BUFFER_BYTES)])
        {
            Ok(read) => read,
            Err(_) => {
                diagnostics.carrier(ArtifactDiagnosticCode::CarrierUnavailable, path, 0);
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
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierSizeLimitExceeded, path, 1);
            complete = false;
            break;
        }
        if *total_bytes > MAXIMUM_TOTAL_CARRIER_BYTES {
            diagnostics.carrier(
                ArtifactDiagnosticCode::CarrierTotalSizeLimitExceeded,
                path,
                1,
            );
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
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierSizeMismatch, path, 2);
        }
        let observed_digest = lowercase_hex(digest.finish().as_ref());
        if group
            .digests
            .iter()
            .any(|expected| expected != &observed_digest)
        {
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierDigestMismatch, path, 3);
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
                            diagnostics.carrier(
                                ArtifactDiagnosticCode::TextEncodingInvalid,
                                path,
                                5,
                            );
                        }
                        Err(TextContentFailure::Unavailable) => {
                            diagnostics.carrier(
                                ArtifactDiagnosticCode::CarrierUnavailable,
                                path,
                                0,
                            );
                        }
                        Err(TextContentFailure::Interrupted) => {
                            return Err(PortableArtifactValidationFailure::Interrupted);
                        }
                    }
                }
                CarrierKind::Json => {
                    let code = match validate_json_content(&mut file, cancelled) {
                        Ok(()) => None,
                        Err(JsonContentFailure::Invalid) => {
                            Some(ArtifactDiagnosticCode::JsonContentInvalid)
                        }
                        Err(JsonContentFailure::Noncanonical) => {
                            Some(ArtifactDiagnosticCode::JsonContentNoncanonical)
                        }
                        Err(JsonContentFailure::Interrupted) => {
                            return Err(PortableArtifactValidationFailure::Interrupted);
                        }
                        Err(JsonContentFailure::Unavailable) => {
                            diagnostics.carrier(
                                ArtifactDiagnosticCode::CarrierUnavailable,
                                path,
                                0,
                            );
                            None
                        }
                    };
                    if let Some(code) = code {
                        diagnostics.carrier(code, path, 6);
                    }
                }
                CarrierKind::GitBranch => {
                    for descriptor in &group.git_descriptors {
                        let result = validate_git_bundle(
                            &mut file,
                            GitArtifactDescriptor {
                                base_oid: &descriptor.base_oid,
                                head_oid: &descriptor.head_oid,
                                tree_oid: &descriptor.tree_oid,
                            },
                            git_budget,
                            cancelled,
                        );
                        if let Err(failure) = result {
                            diagnose_git_artifact_failure(failure, path, diagnostics)?;
                        }
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

fn diagnose_git_artifact_failure(
    failure: GitArtifactFailure,
    path: &str,
    diagnostics: &mut Diagnostics,
) -> Result<(), PortableArtifactValidationFailure> {
    let code = match failure {
        GitArtifactFailure::Header => ArtifactDiagnosticCode::GitBundleHeaderInvalid,
        GitArtifactFailure::Profile => ArtifactDiagnosticCode::GitBundleProfileInvalid,
        GitArtifactFailure::Pack => ArtifactDiagnosticCode::GitPackInvalid,
        GitArtifactFailure::Checksum => ArtifactDiagnosticCode::GitPackChecksumMismatch,
        GitArtifactFailure::Content => ArtifactDiagnosticCode::GitContentInvalid,
        GitArtifactFailure::StructureLimit => ArtifactDiagnosticCode::GitStructureLimitExceeded,
        GitArtifactFailure::Unavailable => {
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierUnavailable, path, 0);
            return Ok(());
        }
        GitArtifactFailure::Scratch => {
            return Err(PortableArtifactValidationFailure::ScratchUnavailable);
        }
        GitArtifactFailure::Interrupted => {
            return Err(PortableArtifactValidationFailure::Interrupted);
        }
    };
    diagnostics.carrier(code, path, 7);
    Ok(())
}

fn require_regular_carrier(stat: &Stat, path: &str, diagnostics: &mut Diagnostics) -> bool {
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::RegularFile => true,
        FileType::Symlink => {
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierSymbolicLink, path, 0);
            false
        }
        _ => {
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierNotRegularFile, path, 0);
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
        Err(Errno::NOENT | Errno::NOTDIR) => {
            diagnostics.carrier(ArtifactDiagnosticCode::CarrierMissing, path, 0)
        }
        Ok(stat) => match FileType::from_raw_mode(stat.st_mode) {
            FileType::Symlink => {
                diagnostics.carrier(ArtifactDiagnosticCode::CarrierSymbolicLink, path, 0)
            }
            FileType::RegularFile => {
                diagnostics.carrier(ArtifactDiagnosticCode::CarrierUnavailable, path, 0)
            }
            _ => diagnostics.carrier(ArtifactDiagnosticCode::CarrierNotRegularFile, path, 0),
        },
        Err(_) => diagnostics.carrier(ArtifactDiagnosticCode::CarrierUnavailable, path, 0),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::git_artifact::tests::{BundleMutation, RealBundleFixture};

    #[test]
    fn inherited_export_shape_requires_a_typed_recorded_producer() {
        let recorded = super::super::runtime::OutputProducer {
            attempt_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            attempt_number: 1,
            node: "produce".to_owned(),
            output: "message".to_owned(),
        };
        let output_producers = BTreeMap::from([(
            "produce".to_owned(),
            BTreeMap::from([("message".to_owned(), recorded.clone())]),
        )]);
        let source = super::super::publication::ExportSourceV1 {
            node: super::super::publication::WorkflowNodeV1 {
                id: "produce".to_owned(),
                role: super::super::publication::WorkflowNodeRoleV1::Step,
            },
            output: "message".to_owned(),
        };
        let base = ["state", "kind", "mediaType", "path", "sizeBytes", "digest"];
        let mut entry = serde_json::json!({
            "state": "available",
            "kind": "text",
            "mediaType": "text/plain; charset=utf-8",
            "path": "exports/0001",
            "sizeBytes": 4,
            "digest": {"algorithm": "sha256", "value": "0".repeat(64)},
            "provenance": "inherited",
            "producer": recorded
        });
        let origin = PortableExportOrigin {
            continuation: true,
            output_producers: Some(&output_producers),
            source: Some(&source),
            source_state: Some(super::super::publication::WorkflowStepStateV1::Inherited),
            source_inherited_prior_state: Some(
                super::super::evidence::InheritedPriorState::Succeeded,
            ),
        };
        let valid = |entry: &Value| exact_export_keys(entry.as_object().unwrap(), &base, origin);
        assert!(valid(&entry));

        entry["producer"]["attemptId"] = Value::Null;
        assert!(!valid(&entry));
        entry["producer"] = serde_json::json!({
            "attemptId": "00000000-0000-0000-0000-000000000002",
            "attemptNumber": 1,
            "node": "produce",
            "output": "message"
        });
        assert!(!valid(&entry));
        entry.as_object_mut().unwrap().remove("producer");
        entry.as_object_mut().unwrap().remove("provenance");
        assert!(!valid(&entry));
    }

    #[test]
    fn inherited_unavailable_shape_requires_a_resolved_skipped_source() {
        let source = super::super::publication::ExportSourceV1 {
            node: super::super::publication::WorkflowNodeV1 {
                id: "produce".to_owned(),
                role: super::super::publication::WorkflowNodeRoleV1::Step,
            },
            output: "message".to_owned(),
        };
        let entry = serde_json::json!({
            "state": "unavailable",
            "reason": "source_skipped"
        });
        let output_producers = BTreeMap::new();
        let origin = |prior_state| PortableExportOrigin {
            continuation: true,
            output_producers: Some(&output_producers),
            source: Some(&source),
            source_state: Some(super::super::publication::WorkflowStepStateV1::Inherited),
            source_inherited_prior_state: Some(prior_state),
        };

        assert!(!valid_unavailable_entry(
            &entry,
            origin(super::super::evidence::InheritedPriorState::Succeeded)
        ));
        assert!(valid_unavailable_entry(
            &entry,
            origin(super::super::evidence::InheritedPriorState::Skipped)
        ));
        assert!(valid_unavailable_entry(
            &entry,
            origin(super::super::evidence::InheritedPriorState::Inherited)
        ));

        let resolved_producer = super::super::runtime::OutputProducer {
            attempt_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            attempt_number: 1,
            node: "produce".to_owned(),
            output: "other".to_owned(),
        };
        let succeeded_producers = BTreeMap::from([(
            "produce".to_owned(),
            BTreeMap::from([("other".to_owned(), resolved_producer)]),
        )]);
        assert!(!valid_unavailable_entry(
            &entry,
            PortableExportOrigin {
                continuation: true,
                output_producers: Some(&succeeded_producers),
                source: Some(&source),
                source_state: Some(super::super::publication::WorkflowStepStateV1::Inherited),
                source_inherited_prior_state: Some(
                    super::super::evidence::InheritedPriorState::Inherited,
                ),
            }
        ));
    }

    #[test]
    fn real_bundle_failures_emit_their_portable_diagnostic_codes() {
        let fixture = RealBundleFixture::new();

        for (mutation, expected_code) in [
            (BundleMutation::InvalidHeader, "git_bundle_header_invalid"),
            (
                BundleMutation::MismatchedProfile,
                "git_bundle_profile_invalid",
            ),
            (BundleMutation::TruncatedPack, "git_pack_invalid"),
            (BundleMutation::BadChecksum, "git_pack_checksum_mismatch"),
            (BundleMutation::MissingHeadObject, "git_content_invalid"),
            (
                BundleMutation::OversizedObject,
                "git_structure_limit_exceeded",
            ),
            (
                BundleMutation::OverdeepDeltaChain,
                "git_structure_limit_exceeded",
            ),
        ] {
            let mut diagnostics = Diagnostics::default();
            diagnose_git_artifact_failure(
                fixture.failure(mutation),
                "exports/0001",
                &mut diagnostics,
            )
            .unwrap();
            let diagnostics = diagnostics.finish();

            assert_eq!(diagnostics.len(), 1);
            assert_eq!(diagnostics[0].code(), expected_code);
        }
    }
}
