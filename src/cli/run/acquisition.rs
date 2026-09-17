use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use serde::de::{self, MapAccess, Visitor};
use zeroize::Zeroizing;

use crate::api::{
    InputAttachmentMetadata, InputFileMetadata, InputScalarMetadata, NamedInputMetadata,
    RunInputManifest, RunInputUpload, digest_bytes, valid_integration_context,
};

const MAXIMUM_INPUTS: usize = 256;
const MAXIMUM_TEXT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_ATTACHMENTS: usize = 256;
const MAXIMUM_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
// Allows the maximum compact context to use six-byte Unicode escapes plus
// bounded formatting whitespace without permitting an unbounded source read.
const MAXIMUM_INTEGRATION_CONTEXT_SOURCE_BYTES: u64 = 128 * 1024;

pub(super) struct AcquiredInputObject {
    pub(super) member_id: String,
    bytes: Zeroizing<Vec<u8>>,
}

impl fmt::Debug for AcquiredInputObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AcquiredInputObject([redacted])")
    }
}

impl AcquiredInputObject {
    pub(super) fn upload(&self) -> RunInputUpload<'_> {
        RunInputUpload {
            member_id: &self.member_id,
            bytes: &self.bytes,
        }
    }
}

pub(super) struct AcquiredInputs {
    pub(super) manifest: RunInputManifest,
    pub(super) objects: Vec<AcquiredInputObject>,
}

impl fmt::Debug for AcquiredInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AcquiredInputs([redacted])")
    }
}

#[derive(Debug)]
pub(super) enum InputAcquisitionFailure {
    InvalidArguments,
    InvalidName,
    InvalidMediaType,
    InvalidUtf8 {
        path: Option<PathBuf>,
    },
    InvalidJson {
        path: Option<PathBuf>,
    },
    Read {
        path: Option<PathBuf>,
        source: io::Error,
    },
    NotRegular {
        path: PathBuf,
    },
    TooLarge {
        path: Option<PathBuf>,
    },
    AggregateTooLarge,
    TooManyInputs,
    TooManyAttachments,
    IntegrationContextRead {
        path: Option<PathBuf>,
        source: io::Error,
    },
    IntegrationContextNotRegular {
        path: PathBuf,
    },
    InvalidIntegrationContext {
        path: Option<PathBuf>,
        source: serde_json::Error,
    },
    IntegrationContextSourceTooLarge {
        path: Option<PathBuf>,
    },
    IntegrationContextLimits,
}

impl fmt::Display for InputAcquisitionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArguments => formatter.write_str("named input bindings conflict"),
            Self::InvalidName => formatter.write_str("a named input has an invalid name"),
            Self::InvalidMediaType => formatter.write_str("a named input media type is invalid"),
            Self::InvalidUtf8 { path: None } => {
                formatter.write_str("a named Text input is not valid UTF-8")
            }
            Self::InvalidUtf8 { path: Some(path) } => {
                write!(
                    formatter,
                    "a named Text input file is not valid UTF-8: {}",
                    path.display()
                )
            }
            Self::InvalidJson { path: None } => {
                formatter.write_str("a named JSON input is not strict JSON")
            }
            Self::InvalidJson { path: Some(path) } => {
                write!(
                    formatter,
                    "a named JSON input file is not strict JSON: {}",
                    path.display()
                )
            }
            Self::Read { path: None, source } => {
                write!(formatter, "read named input from standard input: {source}")
            }
            Self::Read {
                path: Some(path),
                source,
            } => write!(
                formatter,
                "read named input file {}: {source}",
                path.display()
            ),
            Self::NotRegular { path } => {
                write!(
                    formatter,
                    "named input source is not a regular file: {}",
                    path.display()
                )
            }
            Self::TooLarge { path: None } => {
                formatter.write_str("one named input exceeds its byte limit")
            }
            Self::TooLarge { path: Some(path) } => {
                write!(
                    formatter,
                    "named input file exceeds its byte limit: {}",
                    path.display()
                )
            }
            Self::AggregateTooLarge => {
                formatter.write_str("named inputs exceed the 256 MiB aggregate limit")
            }
            Self::TooManyInputs => formatter.write_str("named input count exceeds 256"),
            Self::TooManyAttachments => formatter.write_str("attachment member count exceeds 256"),
            Self::IntegrationContextRead { path: None, source } => {
                write!(
                    formatter,
                    "read integration context from standard input: {source}"
                )
            }
            Self::IntegrationContextRead {
                path: Some(path),
                source,
            } => write!(
                formatter,
                "read integration context file {}: {source}",
                path.display()
            ),
            Self::IntegrationContextNotRegular { path } => write!(
                formatter,
                "integration context source is not a regular file: {}",
                path.display()
            ),
            Self::InvalidIntegrationContext { path: None, source } => write!(
                formatter,
                "decode integration context from standard input: invalid JSON at line {}, column {}",
                source.line(),
                source.column()
            ),
            Self::InvalidIntegrationContext {
                path: Some(path),
                source,
            } => write!(
                formatter,
                "decode integration context file {}: invalid JSON at line {}, column {}",
                path.display(),
                source.line(),
                source.column()
            ),
            Self::IntegrationContextSourceTooLarge { path: None } => formatter.write_str(
                "integration context from standard input exceeds the 128 KiB source limit",
            ),
            Self::IntegrationContextSourceTooLarge { path: Some(path) } => write!(
                formatter,
                "integration context file exceeds the 128 KiB source limit: {}",
                path.display()
            ),
            Self::IntegrationContextLimits => formatter.write_str(
                "integration context exceeds its key, value, entry, or encoded-size limit",
            ),
        }
    }
}

#[derive(Debug)]
enum PlannedInput {
    TextInline(String),
    TextFile(PathBuf),
    JsonInline(Vec<u8>),
    JsonFile(PathBuf),
    File(PlannedFile),
    Attachments(Vec<PlannedFile>),
}

#[derive(Debug)]
struct PlannedFile {
    media_type: String,
    path: PathBuf,
}

pub(super) fn acquire_member_files(
    arguments: &[OsString],
) -> Result<Vec<AcquiredInputObject>, InputAcquisitionFailure> {
    let mut selected = BTreeMap::new();
    let mut total_bytes = 0_u64;
    for binding in bindings(arguments, 2)? {
        let member_id = string_argument(binding.first())?;
        if member_id.is_empty() {
            return Err(InputAcquisitionFailure::InvalidArguments);
        }
        let path = path_argument(binding.get(1))?;
        let bytes = read_regular_file(&path, MAXIMUM_OBJECT_BYTES)?;
        account_bytes(&mut total_bytes, &bytes, MAXIMUM_OBJECT_BYTES, Some(path))?;
        if selected
            .insert(
                member_id.to_owned(),
                AcquiredInputObject {
                    member_id: member_id.to_owned(),
                    bytes,
                },
            )
            .is_some()
        {
            return Err(InputAcquisitionFailure::InvalidArguments);
        }
    }
    if selected.is_empty() {
        return Err(InputAcquisitionFailure::InvalidArguments);
    }
    Ok(selected.into_values().collect())
}

pub(super) fn validate_standard_input_claims(
    arguments: &super::super::NamedInputArgs,
    integration_context_file: Option<&Path>,
) -> Result<(), InputAcquisitionFailure> {
    let mut claims = usize::from(integration_context_file == Some(Path::new("-")));
    for binding in bindings(&arguments.input_text_file, 2)?
        .into_iter()
        .chain(bindings(&arguments.input_json_file, 2)?)
    {
        if path_argument(binding.get(1))? == Path::new("-") {
            claims += 1;
        }
    }
    if claims > 1 {
        Err(InputAcquisitionFailure::InvalidArguments)
    } else {
        Ok(())
    }
}

pub(super) fn acquire_integration_context(
    path: Option<&Path>,
) -> Result<Option<BTreeMap<String, String>>, InputAcquisitionFailure> {
    let Some(path) = path else {
        return Ok(None);
    };
    let source_path = source_path(path);
    let context = if path == Path::new("-") {
        let input = io::stdin();
        read_integration_context(input.lock(), source_path)?
    } else {
        let file =
            super::super::open_regular_file_nonblocking(path).map_err(|error| match error {
                super::super::OpenRegularFileError::Open(source)
                | super::super::OpenRegularFileError::Metadata(source) => {
                    InputAcquisitionFailure::IntegrationContextRead {
                        path: source_path.clone(),
                        source,
                    }
                }
                super::super::OpenRegularFileError::NotRegular => {
                    InputAcquisitionFailure::IntegrationContextNotRegular {
                        path: path.to_owned(),
                    }
                }
            })?;
        read_integration_context(file, source_path)?
    };
    validate_integration_context(&context)?;
    Ok(Some(context))
}

pub(super) fn acquire(
    arguments: &super::super::NamedInputArgs,
) -> Result<AcquiredInputs, InputAcquisitionFailure> {
    let plan = plan(arguments)?;
    let mut total_bytes = 0_u64;
    let mut manifest = BTreeMap::new();
    let mut objects = Vec::new();
    for (name, input) in plan {
        match input {
            PlannedInput::TextInline(text) => acquire_scalar(
                name,
                Zeroizing::new(text.into_bytes()),
                None,
                ScalarKind::Text,
                &mut total_bytes,
                &mut manifest,
                &mut objects,
            )?,
            PlannedInput::TextFile(path) => {
                let bytes = read_scalar_source(&path, MAXIMUM_TEXT_BYTES)?;
                acquire_scalar(
                    name,
                    bytes,
                    source_path(&path),
                    ScalarKind::Text,
                    &mut total_bytes,
                    &mut manifest,
                    &mut objects,
                )?;
            }
            PlannedInput::JsonInline(source) => acquire_scalar(
                name,
                Zeroizing::new(source),
                None,
                ScalarKind::Json,
                &mut total_bytes,
                &mut manifest,
                &mut objects,
            )?,
            PlannedInput::JsonFile(path) => {
                let bytes = read_scalar_source(&path, MAXIMUM_TEXT_BYTES)?;
                acquire_scalar(
                    name,
                    bytes,
                    source_path(&path),
                    ScalarKind::Json,
                    &mut total_bytes,
                    &mut manifest,
                    &mut objects,
                )?;
            }
            PlannedInput::File(file) => {
                let (metadata, bytes) = acquire_file(file, &mut total_bytes)?;
                objects.push(AcquiredInputObject {
                    member_id: format!("inputs/{name}"),
                    bytes,
                });
                manifest.insert(name, NamedInputMetadata::File(metadata));
            }
            PlannedInput::Attachments(items) => {
                let mut metadata = Vec::with_capacity(items.len());
                for (index, item) in items.into_iter().enumerate() {
                    let (file, bytes) = acquire_file(item, &mut total_bytes)?;
                    metadata.push(InputAttachmentMetadata {
                        display_name: None,
                        media_type: file.media_type,
                        size_bytes: file.size_bytes,
                        sha256: file.sha256,
                    });
                    objects.push(AcquiredInputObject {
                        member_id: format!("inputs/{name}/{index:06}"),
                        bytes,
                    });
                }
                manifest.insert(name, NamedInputMetadata::Attachments(metadata));
            }
        }
    }
    let manifest = RunInputManifest { inputs: manifest };
    if !manifest.inputs.is_empty() {
        manifest
            .validate()
            .map_err(|_| InputAcquisitionFailure::InvalidArguments)?;
    }
    Ok(AcquiredInputs { manifest, objects })
}

#[derive(Clone, Copy)]
enum ScalarKind {
    Text,
    Json,
}

fn acquire_scalar(
    name: String,
    bytes: Zeroizing<Vec<u8>>,
    path: Option<PathBuf>,
    kind: ScalarKind,
    total_bytes: &mut u64,
    manifest: &mut BTreeMap<String, NamedInputMetadata>,
    objects: &mut Vec<AcquiredInputObject>,
) -> Result<(), InputAcquisitionFailure> {
    account_bytes(total_bytes, &bytes, MAXIMUM_TEXT_BYTES, path.clone())?;
    match kind {
        ScalarKind::Text if std::str::from_utf8(&bytes).is_err() => {
            return Err(InputAcquisitionFailure::InvalidUtf8 { path });
        }
        ScalarKind::Json if crate::workflow_contract::strict_json::from_slice(&bytes).is_err() => {
            return Err(InputAcquisitionFailure::InvalidJson { path });
        }
        ScalarKind::Text | ScalarKind::Json => {}
    }
    let metadata = scalar_metadata(&bytes);
    objects.push(AcquiredInputObject {
        member_id: format!("inputs/{name}"),
        bytes,
    });
    manifest.insert(
        name,
        match kind {
            ScalarKind::Text => NamedInputMetadata::Text(metadata),
            ScalarKind::Json => NamedInputMetadata::Json(metadata),
        },
    );
    Ok(())
}

fn acquire_file(
    file: PlannedFile,
    total_bytes: &mut u64,
) -> Result<(InputFileMetadata, Zeroizing<Vec<u8>>), InputAcquisitionFailure> {
    let bytes = read_regular_file(&file.path, MAXIMUM_OBJECT_BYTES)?;
    account_bytes(total_bytes, &bytes, MAXIMUM_OBJECT_BYTES, Some(file.path))?;
    Ok((file_metadata(file.media_type, &bytes), bytes))
}

fn plan(
    arguments: &super::super::NamedInputArgs,
) -> Result<BTreeMap<String, PlannedInput>, InputAcquisitionFailure> {
    let mut values = BTreeMap::new();
    let mut standard_input_claimed = false;
    for binding in bindings(&arguments.input_text, 2)? {
        let name = input_name(binding.first())?;
        let text = string_argument(binding.get(1))?;
        insert_scalar(&mut values, name, PlannedInput::TextInline(text.to_owned()))?;
    }
    for binding in bindings(&arguments.input_text_file, 2)? {
        let name = input_name(binding.first())?;
        let path = path_argument(binding.get(1))?;
        claim_standard_input(&path, &mut standard_input_claimed)?;
        insert_scalar(&mut values, name, PlannedInput::TextFile(path))?;
    }
    for binding in bindings(&arguments.input_json, 2)? {
        let name = input_name(binding.first())?;
        let json = string_argument(binding.get(1))?;
        insert_scalar(
            &mut values,
            name,
            PlannedInput::JsonInline(json.as_bytes().to_vec()),
        )?;
    }
    for binding in bindings(&arguments.input_json_file, 2)? {
        let name = input_name(binding.first())?;
        let path = path_argument(binding.get(1))?;
        claim_standard_input(&path, &mut standard_input_claimed)?;
        insert_scalar(&mut values, name, PlannedInput::JsonFile(path))?;
    }
    for binding in bindings(&arguments.input_file, 3)? {
        let name = input_name(binding.first())?;
        let media_type = media_type(binding.get(1))?;
        let path = path_argument(binding.get(2))?;
        insert_scalar(
            &mut values,
            name,
            PlannedInput::File(PlannedFile { media_type, path }),
        )?;
    }
    let attachment_bindings = bindings(&arguments.input_attachment, 3)?;
    if attachment_bindings.len() > MAXIMUM_ATTACHMENTS {
        return Err(InputAcquisitionFailure::TooManyAttachments);
    }
    for binding in attachment_bindings {
        let name = input_name(binding.first())?;
        let media_type = media_type(binding.get(1))?;
        let path = path_argument(binding.get(2))?;
        match values.entry(name.to_owned()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(PlannedInput::Attachments(vec![PlannedFile {
                    media_type,
                    path,
                }]));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let PlannedInput::Attachments(items) = entry.get_mut() else {
                    return Err(InputAcquisitionFailure::InvalidArguments);
                };
                items.push(PlannedFile { media_type, path });
            }
        }
    }
    for name in &arguments.input_attachments_empty {
        validate_name(name)?;
        insert_scalar(&mut values, name, PlannedInput::Attachments(Vec::new()))?;
    }
    if values.len() > MAXIMUM_INPUTS {
        return Err(InputAcquisitionFailure::TooManyInputs);
    }
    Ok(values)
}

fn bindings(
    values: &[OsString],
    width: usize,
) -> Result<Vec<&[OsString]>, InputAcquisitionFailure> {
    let chunks = values.chunks_exact(width);
    if !chunks.remainder().is_empty() {
        return Err(InputAcquisitionFailure::InvalidArguments);
    }
    Ok(chunks.collect())
}

fn input_name(value: Option<&OsString>) -> Result<&str, InputAcquisitionFailure> {
    let name = string_argument(value)?;
    validate_name(name)?;
    Ok(name)
}

fn validate_name(name: &str) -> Result<(), InputAcquisitionFailure> {
    if crate::execution::workflow::is_input_name(name) {
        Ok(())
    } else {
        Err(InputAcquisitionFailure::InvalidName)
    }
}

fn media_type(value: Option<&OsString>) -> Result<String, InputAcquisitionFailure> {
    let value = string_argument(value)?;
    if crate::execution::workflow::is_valid_media_type(value) {
        Ok(value.to_owned())
    } else {
        Err(InputAcquisitionFailure::InvalidMediaType)
    }
}

fn string_argument(value: Option<&OsString>) -> Result<&str, InputAcquisitionFailure> {
    value
        .and_then(|value| value.to_str())
        .ok_or(InputAcquisitionFailure::InvalidArguments)
}

fn path_argument(value: Option<&OsString>) -> Result<PathBuf, InputAcquisitionFailure> {
    value
        .map(PathBuf::from)
        .ok_or(InputAcquisitionFailure::InvalidArguments)
}

fn insert_scalar(
    values: &mut BTreeMap<String, PlannedInput>,
    name: &str,
    value: PlannedInput,
) -> Result<(), InputAcquisitionFailure> {
    if values.insert(name.to_owned(), value).is_some() {
        Err(InputAcquisitionFailure::InvalidArguments)
    } else {
        Ok(())
    }
}

fn claim_standard_input(path: &Path, claimed: &mut bool) -> Result<(), InputAcquisitionFailure> {
    if path != Path::new("-") {
        return Ok(());
    }
    if *claimed {
        return Err(InputAcquisitionFailure::InvalidArguments);
    }
    *claimed = true;
    Ok(())
}

fn read_scalar_source(
    path: &Path,
    maximum: u64,
) -> Result<Zeroizing<Vec<u8>>, InputAcquisitionFailure> {
    if path == Path::new("-") {
        let input = io::stdin();
        read_bounded(input.lock(), maximum, None)
    } else {
        read_regular_file(path, maximum)
    }
}

fn read_regular_file(
    path: &Path,
    maximum: u64,
) -> Result<Zeroizing<Vec<u8>>, InputAcquisitionFailure> {
    let file = super::super::open_regular_file_nonblocking(path).map_err(|error| match error {
        super::super::OpenRegularFileError::Open(source)
        | super::super::OpenRegularFileError::Metadata(source) => InputAcquisitionFailure::Read {
            path: Some(path.to_owned()),
            source,
        },
        super::super::OpenRegularFileError::NotRegular => InputAcquisitionFailure::NotRegular {
            path: path.to_owned(),
        },
    })?;
    read_bounded(file, maximum, Some(path.to_owned()))
}

fn read_bounded(
    mut source: impl Read,
    maximum: u64,
    path: Option<PathBuf>,
) -> Result<Zeroizing<Vec<u8>>, InputAcquisitionFailure> {
    let mut bytes = Zeroizing::new(Vec::with_capacity(
        usize::try_from(maximum.min(64 * 1024)).unwrap_or(0),
    ));
    source
        .by_ref()
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| InputAcquisitionFailure::Read {
            path: path.clone(),
            source,
        })?;
    if u64::try_from(bytes.len()).map_or(true, |length| length > maximum) {
        return Err(InputAcquisitionFailure::TooLarge { path });
    }
    Ok(bytes)
}

fn read_integration_context(
    source: impl Read,
    path: Option<PathBuf>,
) -> Result<BTreeMap<String, String>, InputAcquisitionFailure> {
    let mut document = Zeroizing::new(Vec::new());
    source
        .take(MAXIMUM_INTEGRATION_CONTEXT_SOURCE_BYTES + 1)
        .read_to_end(&mut document)
        .map_err(|source| InputAcquisitionFailure::IntegrationContextRead {
            path: path.clone(),
            source,
        })?;
    if document.len() as u64 > MAXIMUM_INTEGRATION_CONTEXT_SOURCE_BYTES {
        return Err(InputAcquisitionFailure::IntegrationContextSourceTooLarge { path });
    }
    decode_integration_context(&document, path)
}

fn decode_integration_context(
    document: &[u8],
    path: Option<PathBuf>,
) -> Result<BTreeMap<String, String>, InputAcquisitionFailure> {
    let mut decoder = serde_json::Deserializer::from_slice(document);
    let context = serde::de::Deserializer::deserialize_map(&mut decoder, IntegrationContextVisitor)
        .map_err(
            |source| InputAcquisitionFailure::InvalidIntegrationContext {
                path: path.clone(),
                source,
            },
        )?;
    decoder
        .end()
        .map_err(|source| InputAcquisitionFailure::InvalidIntegrationContext { path, source })?;
    Ok(context)
}

struct IntegrationContextVisitor;

impl<'de> Visitor<'de> for IntegrationContextVisitor {
    type Value = BTreeMap<String, String>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a flat JSON object with unique string members")
    }

    fn visit_map<A>(self, mut members: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut context = BTreeMap::new();
        while let Some(key) = members.next_key::<String>()? {
            if context.contains_key(&key) {
                return Err(de::Error::custom("duplicate integration context member"));
            }
            let value = members.next_value::<String>()?;
            context.insert(key, value);
        }
        Ok(context)
    }
}

fn validate_integration_context(
    context: &BTreeMap<String, String>,
) -> Result<(), InputAcquisitionFailure> {
    if !valid_integration_context(
        context.len(),
        context
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    ) {
        Err(InputAcquisitionFailure::IntegrationContextLimits)
    } else {
        Ok(())
    }
}

fn account_bytes(
    total: &mut u64,
    bytes: &[u8],
    maximum: u64,
    path: Option<PathBuf>,
) -> Result<(), InputAcquisitionFailure> {
    let size = u64::try_from(bytes.len())
        .map_err(|_| InputAcquisitionFailure::TooLarge { path: path.clone() })?;
    if size > maximum {
        return Err(InputAcquisitionFailure::TooLarge { path });
    }
    *total = total
        .checked_add(size)
        .filter(|total| *total <= MAXIMUM_TOTAL_BYTES)
        .ok_or(InputAcquisitionFailure::AggregateTooLarge)?;
    Ok(())
}

fn scalar_metadata(bytes: &[u8]) -> InputScalarMetadata {
    InputScalarMetadata {
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        sha256: digest_bytes(bytes),
    }
}

fn file_metadata(media_type: String, bytes: &[u8]) -> InputFileMetadata {
    InputFileMetadata {
        media_type,
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        sha256: digest_bytes(bytes),
    }
}

fn source_path(path: &Path) -> Option<PathBuf> {
    (path != Path::new("-")).then(|| path.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_context_enforces_closed_shape_and_exact_limits() {
        let mut exact_entries = BTreeMap::new();
        for index in 0..32 {
            exact_entries.insert(format!("key{index:02}"), String::new());
        }
        let mut exact_encoded = BTreeMap::new();
        for index in 0..15 {
            exact_encoded.insert(format!("{index:02}"), "v".repeat(1024));
        }
        exact_encoded.insert("15".to_owned(), "v".repeat(895));
        assert_eq!(serde_json::to_vec(&exact_encoded).unwrap().len(), 16 * 1024);
        let mut escape_sensitive_boundary = exact_encoded.clone();
        escape_sensitive_boundary
            .insert("15".to_owned(), format!("<>&\u{2028}{}", "v".repeat(889)));
        for context in [
            BTreeMap::new(),
            BTreeMap::from([("empty".to_owned(), String::new())]),
            exact_entries,
            BTreeMap::from([("k".repeat(64), "v".repeat(1024))]),
            exact_encoded.clone(),
            escape_sensitive_boundary,
        ] {
            validate_integration_context(&context).unwrap();
        }

        let mut too_many = BTreeMap::new();
        for index in 0..33 {
            too_many.insert(format!("key{index:02}"), String::new());
        }
        let mut encoded_over = exact_encoded;
        encoded_over.get_mut("15").unwrap().push('v');
        for context in [
            too_many,
            BTreeMap::from([(String::new(), "value".to_owned())]),
            BTreeMap::from([("k".repeat(65), "value".to_owned())]),
            BTreeMap::from([("key".to_owned(), "v".repeat(1025))]),
            BTreeMap::from([("nul\0key".to_owned(), "value".to_owned())]),
            BTreeMap::from([("key".to_owned(), "nul\0value".to_owned())]),
            encoded_over,
        ] {
            assert!(matches!(
                validate_integration_context(&context),
                Err(InputAcquisitionFailure::IntegrationContextLimits)
            ));
        }

        for document in [
            br#"{"key":"first","key":"second"}"#.as_slice(),
            br#"{"key":null}"#.as_slice(),
            br#"{"key":1}"#.as_slice(),
            br#"[]"#.as_slice(),
            br#"null"#.as_slice(),
        ] {
            assert!(matches!(
                decode_integration_context(document, None),
                Err(InputAcquisitionFailure::InvalidIntegrationContext { .. })
            ));
        }
    }

    #[test]
    fn integration_context_source_read_is_bounded_without_waiting_for_eof() {
        assert!(matches!(
            read_integration_context(io::repeat(b' '), None),
            Err(InputAcquisitionFailure::IntegrationContextSourceTooLarge { path: None })
        ));
    }

    #[test]
    fn mixed_input_acquisition_preserves_collection_order_and_empty_values() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("file.bin");
        let first = directory.path().join("first.bin");
        let second = directory.path().join("second.bin");
        std::fs::write(&file, []).unwrap();
        std::fs::write(&first, b"second logical member").unwrap();
        std::fs::write(&second, b"first logical member").unwrap();
        let arguments = super::super::super::NamedInputArgs {
            input_text: vec!["emptyText".into(), "".into()],
            input_text_file: Vec::new(),
            input_json: vec!["settings".into(), "{\"enabled\":true}".into()],
            input_json_file: Vec::new(),
            input_file: vec![
                "emptyFile".into(),
                "application/octet-stream".into(),
                file.into_os_string(),
            ],
            input_attachment: vec![
                "evidence".into(),
                "text/plain".into(),
                first.into_os_string(),
                "evidence".into(),
                "application/octet-stream".into(),
                second.into_os_string(),
            ],
            input_attachments_empty: vec!["emptyEvidence".to_owned()],
        };

        let acquired = acquire(&arguments).unwrap();

        assert_eq!(acquired.manifest.inputs.len(), 5);
        assert_eq!(acquired.objects.len(), 5);
        assert_eq!(acquired.objects[2].member_id, "inputs/evidence/000000");
        assert_eq!(acquired.objects[3].member_id, "inputs/evidence/000001");
        assert!(matches!(
            acquired.manifest.inputs.get("emptyEvidence"),
            Some(NamedInputMetadata::Attachments(items)) if items.is_empty()
        ));
    }
}
