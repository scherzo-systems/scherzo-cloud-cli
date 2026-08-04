use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use ring::digest::{Context, SHA256};
use serde_json::Value;

use super::result_validation::{ResultSchemaSupportFailure, RetainedResultSchema};
use super::validated::{RequiredImports, ValidatedMessageSource, ValidatedStep, ValidatedWorkflow};
use super::validation::{ValidationFailureKind, ValidationLocation};
use super::{DecodeFailureKind, decode, validation};
use crate::execution::workflow::document::Output;

const CONTENT_CLOSURE_DOMAIN: &[u8] = b"scherzo.workflow.content-closure.v1\0";
const MAX_SOURCE_CLOSURE_BYTES: u64 = 64 * 1024 * 1024;
const SHA256_ALGORITHM: &str = "sha256";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResolutionFailureKind {
    SourceRootUnavailable,
    SourceRootNotDirectory,
    LexicalSourceEscape,
    SourceUnavailable,
    SymbolicLinkEscape,
    SourceNotRegularFile,
    InvalidCanonicalPath,
    SourceChangedDuringResolution,
    InvalidWorkflowDocument(DecodeFailureKind),
    InvalidWorkflowDefinition(ValidationFailureKind),
    InvalidTextEncoding,
    InvalidResultSchemaEncoding,
    InvalidResultSchemaJson,
    InvalidResultSchemaDialect,
    InvalidResultSchemaReference,
    InvalidResultSchema,
    DigestInputTooLarge,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResolutionLocation {
    SourceRoot,
    Workflow,
    Semantic(ValidationLocation),
    SystemPrompt { step: String },
    MessageText { step: String, index: usize },
    MessageAttachment { step: String, index: usize },
    ResultSchema { step: String, output: String },
    ContentDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolutionFailure {
    kind: ResolutionFailureKind,
    location: ResolutionLocation,
    workflow_path: Option<String>,
}

impl ResolutionFailure {
    pub(crate) fn kind(&self) -> ResolutionFailureKind {
        self.kind
    }

    pub(crate) fn location(&self) -> &ResolutionLocation {
        &self.location
    }

    pub(crate) fn workflow_path(&self) -> Option<&str> {
        self.workflow_path.as_deref()
    }

    fn new(kind: ResolutionFailureKind, location: ResolutionLocation) -> Self {
        Self {
            kind,
            location,
            workflow_path: None,
        }
    }

    fn with_workflow_path(mut self, workflow_path: String) -> Self {
        self.workflow_path = Some(workflow_path);
        self
    }
}

impl fmt::Display for ResolutionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "workflow resolution failure at {:?}: {:?}",
            self.location, self.kind
        )
    }
}

impl std::error::Error for ResolutionFailure {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContentDigestAlgorithm {
    Sha256,
}

impl ContentDigestAlgorithm {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => SHA256_ALGORITHM,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowContentDigest {
    pub(crate) algorithm: ContentDigestAlgorithm,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowSourceProvenance {
    pub(crate) source_root: PathBuf,
    pub(crate) workflow_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedWorkflow {
    pub(crate) definition: ValidatedWorkflow,
    pub(crate) source_closure: BTreeMap<String, Arc<[u8]>>,
    result_schemas: BTreeMap<(String, String), RetainedResultSchema>,
    pub(crate) source: WorkflowSourceProvenance,
    pub(crate) content_digest: WorkflowContentDigest,
}

impl ResolvedWorkflow {
    pub(crate) fn required_imports(&self) -> RequiredImports {
        self.definition.required_imports
    }

    pub(crate) fn source_bytes(&self, canonical_path: &str) -> Option<&[u8]> {
        self.source_closure.get(canonical_path).map(AsRef::as_ref)
    }

    pub(crate) fn result_schema(&self, step: &str, output: &str) -> Option<&RetainedResultSchema> {
        self.result_schemas
            .get(&(step.to_owned(), output.to_owned()))
    }
}

pub(crate) fn resolve(
    source_root: &Path,
    selected_workflow: &Path,
) -> Result<ResolvedWorkflow, ResolutionFailure> {
    let mut sources = SourceResolver::new(source_root)?;
    let selected_candidate = sources.path_from_root(selected_workflow);
    let workflow_source = sources.load(&selected_candidate, ResolutionLocation::Workflow)?;

    let workflow_path = workflow_source.canonical_path.clone();
    resolve_loaded_workflow(sources, workflow_source)
        .map_err(|failure| failure.with_workflow_path(workflow_path))
}

fn resolve_loaded_workflow(
    mut sources: SourceResolver,
    workflow_source: LoadedSource,
) -> Result<ResolvedWorkflow, ResolutionFailure> {
    let document = decode(&workflow_source.bytes).map_err(|failure| {
        ResolutionFailure::new(
            ResolutionFailureKind::InvalidWorkflowDocument(failure.kind()),
            ResolutionLocation::Workflow,
        )
    })?;
    let mut definition = validation::validate(document).map_err(|failure| {
        ResolutionFailure::new(
            ResolutionFailureKind::InvalidWorkflowDefinition(failure.kind()),
            ResolutionLocation::Semantic(failure.location().clone()),
        )
    })?;

    let Some(workflow_directory) = workflow_source.canonical_host_path.parent() else {
        return Err(ResolutionFailure::new(
            ResolutionFailureKind::InvalidCanonicalPath,
            ResolutionLocation::Workflow,
        ));
    };
    let result_schemas = resolve_static_sources(&mut definition, workflow_directory, &mut sources)?;

    let source_root = sources.canonical_root.clone();
    let source_closure = sources.finish();
    let content_digest = digest_source_closure(&source_closure)?;
    Ok(ResolvedWorkflow {
        definition,
        source_closure,
        result_schemas,
        source: WorkflowSourceProvenance {
            source_root,
            workflow_path: workflow_source.canonical_path,
        },
        content_digest,
    })
}

struct LoadedSource {
    canonical_host_path: PathBuf,
    canonical_path: String,
    bytes: Arc<[u8]>,
}

struct SourceResolver {
    canonical_root: PathBuf,
    supplied_root: PathBuf,
    closure: BTreeMap<String, Arc<[u8]>>,
    retained_bytes: u64,
}

impl SourceResolver {
    fn new(source_root: &Path) -> Result<Self, ResolutionFailure> {
        let canonical_root = fs::canonicalize(source_root).map_err(|_| {
            ResolutionFailure::new(
                ResolutionFailureKind::SourceRootUnavailable,
                ResolutionLocation::SourceRoot,
            )
        })?;
        let metadata = fs::metadata(&canonical_root).map_err(|_| {
            ResolutionFailure::new(
                ResolutionFailureKind::SourceRootUnavailable,
                ResolutionLocation::SourceRoot,
            )
        })?;
        if !metadata.is_dir() {
            return Err(ResolutionFailure::new(
                ResolutionFailureKind::SourceRootNotDirectory,
                ResolutionLocation::SourceRoot,
            ));
        }

        let supplied_root = if source_root.is_absolute() {
            source_root.to_owned()
        } else {
            std::env::current_dir()
                .map_err(|_| {
                    ResolutionFailure::new(
                        ResolutionFailureKind::SourceRootUnavailable,
                        ResolutionLocation::SourceRoot,
                    )
                })?
                .join(source_root)
        };

        Ok(Self {
            canonical_root,
            supplied_root,
            closure: BTreeMap::new(),
            retained_bytes: 0,
        })
    }

    fn path_from_root(&self, path: &Path) -> PathBuf {
        if !path.is_absolute() {
            return self.canonical_root.join(path);
        }
        path.strip_prefix(&self.supplied_root)
            .map(|relative| self.canonical_root.join(relative))
            .unwrap_or_else(|_| path.to_owned())
    }

    fn load(
        &mut self,
        candidate: &Path,
        location: ResolutionLocation,
    ) -> Result<LoadedSource, ResolutionFailure> {
        if !lexically_within(&self.canonical_root, candidate) {
            return Err(ResolutionFailure::new(
                ResolutionFailureKind::LexicalSourceEscape,
                location,
            ));
        }

        let canonical_host_path = fs::canonicalize(candidate).map_err(|_| {
            ResolutionFailure::new(ResolutionFailureKind::SourceUnavailable, location.clone())
        })?;
        if !canonical_host_path.starts_with(&self.canonical_root) {
            return Err(ResolutionFailure::new(
                ResolutionFailureKind::SymbolicLinkEscape,
                location,
            ));
        }

        let mut file = open_source_file(&canonical_host_path).map_err(|_| {
            ResolutionFailure::new(ResolutionFailureKind::SourceUnavailable, location.clone())
        })?;
        let handle_metadata = file.metadata().map_err(|_| {
            ResolutionFailure::new(ResolutionFailureKind::SourceUnavailable, location.clone())
        })?;
        if !handle_metadata.is_file() {
            return Err(ResolutionFailure::new(
                ResolutionFailureKind::SourceNotRegularFile,
                location,
            ));
        }

        let rebound_path = fs::canonicalize(candidate).map_err(|_| {
            ResolutionFailure::new(
                ResolutionFailureKind::SourceChangedDuringResolution,
                location.clone(),
            )
        })?;
        if rebound_path != canonical_host_path
            || !rebound_path.starts_with(&self.canonical_root)
            || !path_identifies_file(&rebound_path, &handle_metadata).map_err(|_| {
                ResolutionFailure::new(
                    ResolutionFailureKind::SourceChangedDuringResolution,
                    location.clone(),
                )
            })?
        {
            return Err(ResolutionFailure::new(
                ResolutionFailureKind::SourceChangedDuringResolution,
                location,
            ));
        }

        let canonical_path = canonical_relative_path(&self.canonical_root, &canonical_host_path)
            .ok_or_else(|| {
                ResolutionFailure::new(
                    ResolutionFailureKind::InvalidCanonicalPath,
                    location.clone(),
                )
            })?;
        if let Some(bytes) = self.closure.get(&canonical_path) {
            return Ok(LoadedSource {
                canonical_host_path,
                canonical_path,
                bytes: Arc::clone(bytes),
            });
        }

        let remaining_bytes = MAX_SOURCE_CLOSURE_BYTES
            .checked_sub(self.retained_bytes)
            .ok_or_else(|| source_closure_too_large(location.clone()))?;
        if handle_metadata.len() > remaining_bytes {
            return Err(source_closure_too_large(location));
        }

        let mut bytes = Vec::new();
        file.by_ref()
            .take(remaining_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| {
                ResolutionFailure::new(ResolutionFailureKind::SourceUnavailable, location.clone())
            })?;
        let content_length =
            u64::try_from(bytes.len()).map_err(|_| source_closure_too_large(location.clone()))?;
        if content_length > remaining_bytes {
            return Err(source_closure_too_large(location));
        }
        self.retained_bytes += content_length;
        let bytes = Arc::<[u8]>::from(bytes);
        self.closure
            .insert(canonical_path.clone(), Arc::clone(&bytes));
        Ok(LoadedSource {
            canonical_host_path,
            canonical_path,
            bytes,
        })
    }

    fn finish(self) -> BTreeMap<String, Arc<[u8]>> {
        self.closure
    }
}

fn resolve_static_sources(
    definition: &mut ValidatedWorkflow,
    workflow_directory: &Path,
    sources: &mut SourceResolver,
) -> Result<BTreeMap<(String, String), RetainedResultSchema>, ResolutionFailure> {
    let mut result_schemas = BTreeMap::new();
    for (step_name, step) in &mut definition.steps {
        let ValidatedStep::Agent(agent_step) = step else {
            continue;
        };

        let system_location = ResolutionLocation::SystemPrompt {
            step: step_name.clone(),
        };
        agent_step.agent.system_prompt = resolve_text_source(
            &agent_step.agent.system_prompt,
            workflow_directory,
            sources,
            system_location,
        )?;

        resolve_message_files(
            step_name,
            &mut agent_step.agent.message.text,
            MessageFileKind::Text,
            workflow_directory,
            sources,
        )?;
        resolve_message_files(
            step_name,
            &mut agent_step.agent.message.attachments,
            MessageFileKind::Attachment,
            workflow_directory,
            sources,
        )?;

        for (output_name, output) in &mut agent_step.common.outputs {
            let Output::AgentResult { schema } = &mut output.definition else {
                continue;
            };
            let location = ResolutionLocation::ResultSchema {
                step: step_name.clone(),
                output: output_name.clone(),
            };
            let (canonical_path, retained) =
                resolve_result_schema(schema, workflow_directory, sources, location)?;
            *schema = canonical_path;
            result_schemas.insert((step_name.clone(), output_name.clone()), retained);
        }
    }
    Ok(result_schemas)
}

#[derive(Clone, Copy)]
enum MessageFileKind {
    Text,
    Attachment,
}

fn resolve_message_files(
    step: &str,
    message_sources: &mut [ValidatedMessageSource],
    kind: MessageFileKind,
    workflow_directory: &Path,
    sources: &mut SourceResolver,
) -> Result<(), ResolutionFailure> {
    for (index, source) in message_sources.iter_mut().enumerate() {
        let ValidatedMessageSource::File { path } = source else {
            continue;
        };
        let location = match kind {
            MessageFileKind::Text => ResolutionLocation::MessageText {
                step: step.to_owned(),
                index,
            },
            MessageFileKind::Attachment => ResolutionLocation::MessageAttachment {
                step: step.to_owned(),
                index,
            },
        };
        *path = match kind {
            MessageFileKind::Text => {
                resolve_text_source(path, workflow_directory, sources, location)?
            }
            MessageFileKind::Attachment => {
                resolve_binary_source(path, workflow_directory, sources, location)?
            }
        };
    }
    Ok(())
}

fn resolve_text_source(
    source_path: &str,
    workflow_directory: &Path,
    sources: &mut SourceResolver,
    location: ResolutionLocation,
) -> Result<String, ResolutionFailure> {
    load_utf8_static_source(
        source_path,
        workflow_directory,
        sources,
        &location,
        ResolutionFailureKind::InvalidTextEncoding,
    )
    .map(|loaded| loaded.canonical_path)
}

fn resolve_binary_source(
    source_path: &str,
    workflow_directory: &Path,
    sources: &mut SourceResolver,
    location: ResolutionLocation,
) -> Result<String, ResolutionFailure> {
    load_static_source(source_path, workflow_directory, sources, location)
        .map(|loaded| loaded.canonical_path)
}

fn resolve_result_schema(
    source_path: &str,
    workflow_directory: &Path,
    sources: &mut SourceResolver,
    location: ResolutionLocation,
) -> Result<(String, RetainedResultSchema), ResolutionFailure> {
    let loaded = load_utf8_static_source(
        source_path,
        workflow_directory,
        sources,
        &location,
        ResolutionFailureKind::InvalidResultSchemaEncoding,
    )?;
    let schema = Arc::new(serde_json::from_slice::<Value>(&loaded.bytes).map_err(|_| {
        ResolutionFailure::new(
            ResolutionFailureKind::InvalidResultSchemaJson,
            location.clone(),
        )
    })?);
    let retained =
        RetainedResultSchema::compile(Arc::clone(&loaded.bytes), schema).map_err(|failure| {
            let kind = match failure {
                ResultSchemaSupportFailure::Dialect => {
                    ResolutionFailureKind::InvalidResultSchemaDialect
                }
                ResultSchemaSupportFailure::Reference => {
                    ResolutionFailureKind::InvalidResultSchemaReference
                }
                ResultSchemaSupportFailure::Schema => ResolutionFailureKind::InvalidResultSchema,
            };
            ResolutionFailure::new(kind, location)
        })?;
    Ok((loaded.canonical_path, retained))
}

fn load_utf8_static_source(
    source_path: &str,
    workflow_directory: &Path,
    sources: &mut SourceResolver,
    location: &ResolutionLocation,
    encoding_failure: ResolutionFailureKind,
) -> Result<LoadedSource, ResolutionFailure> {
    let loaded = load_static_source(source_path, workflow_directory, sources, location.clone())?;
    std::str::from_utf8(&loaded.bytes)
        .map_err(|_| ResolutionFailure::new(encoding_failure, location.clone()))?;
    Ok(loaded)
}

fn load_static_source(
    source_path: &str,
    workflow_directory: &Path,
    sources: &mut SourceResolver,
    location: ResolutionLocation,
) -> Result<LoadedSource, ResolutionFailure> {
    sources.load(&workflow_directory.join(source_path), location)
}

fn lexically_within(root: &Path, candidate: &Path) -> bool {
    lexical_normalize(candidate).is_some_and(|normalized| normalized.starts_with(root))
}

fn lexical_normalize(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Some(normalized)
}

fn canonical_relative_path(root: &Path, canonical_path: &Path) -> Option<String> {
    let relative = canonical_path.strip_prefix(root).ok()?;
    let mut parts = Vec::new();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return None;
        };
        parts.push(part.to_str()?);
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join("/"))
}

fn open_source_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(source_open_flags());
    }
    options.open(path)
}

#[cfg(unix)]
#[expect(
    clippy::cast_possible_wrap,
    reason = "O_NOFOLLOW and O_NONBLOCK fit in the signed custom_flags value on Unix"
)]
fn source_open_flags() -> i32 {
    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32
}

fn path_identifies_file(path: &Path, handle_metadata: &fs::Metadata) -> io::Result<bool> {
    let path_metadata = fs::metadata(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(path_metadata.dev() == handle_metadata.dev()
            && path_metadata.ino() == handle_metadata.ino())
    }
    #[cfg(not(unix))]
    {
        Ok(path_metadata.is_file()
            && path_metadata.len() == handle_metadata.len()
            && path_metadata.modified().ok() == handle_metadata.modified().ok())
    }
}

fn source_closure_too_large(location: ResolutionLocation) -> ResolutionFailure {
    ResolutionFailure::new(ResolutionFailureKind::DigestInputTooLarge, location)
}

fn digest_source_closure(
    closure: &BTreeMap<String, Arc<[u8]>>,
) -> Result<WorkflowContentDigest, ResolutionFailure> {
    let mut context = Context::new(&SHA256);
    let mut framed_length = 0_u64;
    hash_bytes(&mut context, &mut framed_length, CONTENT_CLOSURE_DOMAIN)?;

    let entry_count = u64::try_from(closure.len()).map_err(|_| digest_too_large())?;
    hash_bytes(&mut context, &mut framed_length, &entry_count.to_be_bytes())?;
    for (path, content) in closure {
        hash_length_prefixed(&mut context, &mut framed_length, path.as_bytes())?;
        hash_length_prefixed(&mut context, &mut framed_length, content)?;
    }

    let digest = context.finish();
    Ok(WorkflowContentDigest {
        algorithm: ContentDigestAlgorithm::Sha256,
        value: lowercase_hex(digest.as_ref()),
    })
}

fn hash_length_prefixed(
    context: &mut Context,
    framed_length: &mut u64,
    bytes: &[u8],
) -> Result<(), ResolutionFailure> {
    let length = u64::try_from(bytes.len()).map_err(|_| digest_too_large())?;
    hash_bytes(context, framed_length, &length.to_be_bytes())?;
    hash_bytes(context, framed_length, bytes)
}

fn hash_bytes(
    context: &mut Context,
    framed_length: &mut u64,
    bytes: &[u8],
) -> Result<(), ResolutionFailure> {
    let length = u64::try_from(bytes.len()).map_err(|_| digest_too_large())?;
    *framed_length = framed_length
        .checked_add(length)
        .filter(|length| *length <= MAX_SOURCE_CLOSURE_BYTES)
        .ok_or_else(digest_too_large)?;
    context.update(bytes);
    Ok(())
}

fn digest_too_large() -> ResolutionFailure {
    ResolutionFailure::new(
        ResolutionFailureKind::DigestInputTooLarge,
        ResolutionLocation::ContentDigest,
    )
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests;
