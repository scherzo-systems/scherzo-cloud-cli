use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::redirect::Policy;
use ring::digest::{Context as DigestContext, SHA256, digest};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use url::Url;

use crate::execution::workflow::admission::{ResolvedAttachment, ResolvedImports};
use crate::execution::workflow::artifact::CaptureCancellation;
use crate::runner::credential::Credential;
use crate::runner_protocol::RunInputProjectionV1;

const MANIFEST_RESPONSE_LIMIT: usize = 1024 * 1024;
const CAPABILITY_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;
const PROVIDER_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAXIMUM_PROMPT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_ATTACHMENTS: usize = 256;
const MAXIMUM_ATTACHMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_AGGREGATE_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_CAPABILITY_MEMBERS: usize = 100;
const PROMPT_MEDIA_TYPE: &str = "text/plain; charset=utf-8";

#[derive(Clone, Copy)]
pub(super) struct PreparationDeadline {
    expires_at: OffsetDateTime,
    monotonic_deadline: Instant,
}

impl PreparationDeadline {
    pub(super) fn from_wire(
        value: &str,
        utc_now: OffsetDateTime,
        monotonic_now: Instant,
    ) -> Option<Self> {
        let expires_at = OffsetDateTime::parse(value, &Rfc3339).ok()?;
        if !value.ends_with('Z') || expires_at.offset() != UtcOffset::UTC || expires_at <= utc_now {
            return None;
        }
        let nanoseconds = (expires_at - utc_now).whole_nanoseconds();
        let nanoseconds = u64::try_from(nanoseconds).ok()?;
        Some(Self {
            expires_at,
            monotonic_deadline: monotonic_now.checked_add(Duration::from_nanos(nanoseconds))?,
        })
    }

    pub(super) fn remaining(self) -> Option<Duration> {
        self.remaining_at(crate::timing::monotonic_now())
    }

    pub(super) fn remaining_at(self, monotonic_now: Instant) -> Option<Duration> {
        self.monotonic_deadline
            .checked_duration_since(monotonic_now)
            .filter(|remaining| !remaining.is_zero())
    }

    #[cfg(test)]
    pub(super) fn elapsed_for_test() -> Self {
        Self {
            expires_at: OffsetDateTime::UNIX_EPOCH,
            monotonic_deadline: crate::timing::monotonic_now(),
        }
    }

    fn contains_expiry(self, expires_at: OffsetDateTime) -> bool {
        expires_at <= self.expires_at
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RunInputFailure {
    ServiceUnavailable,
    AssignmentFenced,
    EnvironmentUnavailable,
    InvalidProjection,
    ManifestMismatch,
    ContentUnavailable,
    ContentMismatch,
    PromptInvalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrokerFailure {
    Unavailable,
    Fenced,
    InvalidResponse,
    ContentUnavailable,
    ContentMismatch,
    Environment,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DigestV1 {
    algorithm: String,
    value: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PromptMember {
    size_bytes: u64,
    sha256: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttachmentMember {
    index: usize,
    display_name: Option<String>,
    media_type: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestV1 {
    schema_version: u64,
    prompt: Option<PromptMember>,
    attachments: Vec<AttachmentMember>,
}

#[derive(Clone)]
pub(super) struct ManifestEnvelope {
    schema_version: u64,
    input_set_id: String,
    manifest_digest: DigestV1,
    manifest: ManifestV1,
}

impl std::fmt::Debug for ManifestEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ManifestEnvelope(<redacted>)")
    }
}

#[derive(Clone)]
struct CapabilityMember {
    member_id: String,
    media_type: String,
    size_bytes: u64,
    sha256: String,
    url: String,
}

impl std::fmt::Debug for CapabilityMember {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CapabilityMember(<redacted>)")
    }
}

#[derive(Clone)]
pub(super) struct CapabilityEnvelope {
    schema_version: u64,
    input_set_id: String,
    capability_expires_at: String,
    members: Vec<CapabilityMember>,
}

impl std::fmt::Debug for CapabilityEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CapabilityEnvelope(<redacted>)")
    }
}

pub(super) trait RunInputBroker: Send + Sync {
    fn manifest(
        &self,
        assignment_id: &str,
        execution_spec_id: &str,
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
    ) -> Result<ManifestEnvelope, BrokerFailure>;

    fn capabilities(
        &self,
        assignment_id: &str,
        execution_spec_id: &str,
        members: &[String],
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
    ) -> Result<CapabilityEnvelope, BrokerFailure>;

    // The object-safe broker declaration intentionally mirrors its HTTP implementation.
    // jscpd:ignore-start
    fn download(
        &self,
        url: &str,
        expected_size: u64,
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
        consume: &mut dyn FnMut(&[u8]) -> Result<(), BrokerFailure>,
    ) -> Result<(), BrokerFailure>;
    // jscpd:ignore-end
}

#[derive(Clone)]
pub(super) struct HttpRunInputBroker {
    manifest_endpoint: Url,
    capability_endpoint: Url,
    runner_credential: Credential,
    boot_id: Arc<str>,
}

fn broker_operation_runtime(
    deadline: PreparationDeadline,
) -> Result<(reqwest::Client, tokio::runtime::Runtime), BrokerFailure> {
    crate::tls::install_provider();
    let timeout = deadline
        .remaining()
        .map(|remaining| remaining.min(PROVIDER_OPERATION_TIMEOUT))
        .ok_or(BrokerFailure::Fenced)?;
    let client = reqwest::Client::builder()
        .redirect(Policy::none())
        .timeout(timeout)
        .build()
        .map_err(|_| BrokerFailure::Unavailable)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| BrokerFailure::Unavailable)?;
    Ok((client, runtime))
}

async fn send_broker_request(
    request: reqwest::RequestBuilder,
    cancellation: &CaptureCancellation,
) -> Result<reqwest::Response, BrokerFailure> {
    tokio::select! {
        result = request.send() => result.map_err(|_| {
            if cancellation.is_cancelled() {
                BrokerFailure::Fenced
            } else {
                BrokerFailure::Unavailable
            }
        }),
        () = super::source::wait_for_cancellation(cancellation) => {
            Err(BrokerFailure::Fenced)
        }
    }
}

async fn consume_broker_chunk(
    response: &mut reqwest::Response,
    cancellation: &CaptureCancellation,
    consume: &mut dyn FnMut(&[u8]) -> Result<(), BrokerFailure>,
) -> Result<bool, BrokerFailure> {
    let chunk = tokio::select! {
        result = response.chunk() => result.map_err(|_| BrokerFailure::Unavailable)?,
        () = super::source::wait_for_cancellation(cancellation) => {
            return Err(BrokerFailure::Fenced);
        }
    };
    let Some(chunk) = chunk else {
        return Ok(false);
    };
    consume(&chunk)?;
    Ok(true)
}

impl HttpRunInputBroker {
    pub(super) fn new(
        endpoint: &Url,
        runner_credential: &Credential,
        boot_id: &str,
    ) -> Result<Self, ()> {
        let manifest_endpoint = super::source::private_runner_http_endpoint(
            endpoint,
            "/v1/runner/run-inputs/manifest",
        )?;
        let capability_endpoint = super::source::private_runner_http_endpoint(
            endpoint,
            "/v1/runner/run-inputs/download-capabilities",
        )?;
        Ok(Self {
            manifest_endpoint,
            capability_endpoint,
            runner_credential: runner_credential.clone(),
            boot_id: Arc::from(boot_id),
        })
    }

    fn request(
        &self,
        endpoint: &Url,
        body: Value,
        limit: usize,
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
    ) -> Result<Value, BrokerFailure> {
        ensure_current(cancellation, deadline)?;
        let (client, runtime) = broker_operation_runtime(deadline)?;
        let request = client
            .post(endpoint.clone())
            .bearer_auth(self.runner_credential.bearer_value())
            .json(&body);
        runtime.block_on(async {
            let mut response = send_broker_request(request, cancellation).await?;
            let status = response.status();
            if status == StatusCode::CONFLICT {
                return Err(BrokerFailure::Fenced);
            }
            if status != StatusCode::OK {
                return Err(classify_provider_status(status));
            }
            if response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok())
                != Some("private, no-store")
                || response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    != Some("application/json")
            {
                return Err(BrokerFailure::InvalidResponse);
            }
            let mut encoded = Vec::new();
            while consume_broker_chunk(&mut response, cancellation, &mut |chunk| {
                if encoded.len().saturating_add(chunk.len()) > limit {
                    return Err(BrokerFailure::InvalidResponse);
                }
                encoded.extend_from_slice(chunk);
                Ok(())
            })
            .await?
            {}
            ensure_current(cancellation, deadline)?;
            crate::execution::workflow::parse_strict_json(&encoded)
                .map_err(|_| BrokerFailure::InvalidResponse)
        })
    }
}

impl RunInputBroker for HttpRunInputBroker {
    fn manifest(
        &self,
        assignment_id: &str,
        execution_spec_id: &str,
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
    ) -> Result<ManifestEnvelope, BrokerFailure> {
        let value = self.request(
            &self.manifest_endpoint,
            serde_json::json!({
                "schemaVersion": 1,
                "bootId": self.boot_id.as_ref(),
                "assignmentId": assignment_id,
                "executionSpecId": execution_spec_id,
            }),
            MANIFEST_RESPONSE_LIMIT,
            cancellation,
            deadline,
        )?;
        parse_manifest_envelope(value)
    }

    fn capabilities(
        &self,
        assignment_id: &str,
        execution_spec_id: &str,
        members: &[String],
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
    ) -> Result<CapabilityEnvelope, BrokerFailure> {
        let value = self.request(
            &self.capability_endpoint,
            serde_json::json!({
                "schemaVersion": 1,
                "bootId": self.boot_id.as_ref(),
                "assignmentId": assignment_id,
                "executionSpecId": execution_spec_id,
                "members": members,
            }),
            CAPABILITY_RESPONSE_LIMIT,
            cancellation,
            deadline,
        )?;
        parse_capability_envelope(value)
    }

    fn download(
        &self,
        url: &str,
        expected_size: u64,
        cancellation: &CaptureCancellation,
        deadline: PreparationDeadline,
        consume: &mut dyn FnMut(&[u8]) -> Result<(), BrokerFailure>,
    ) -> Result<(), BrokerFailure> {
        ensure_current(cancellation, deadline)?;
        let url = validate_capability_url(url)?;
        let (client, runtime) = broker_operation_runtime(deadline)?;
        let request = client.get(url).header("Accept-Encoding", "identity");
        runtime.block_on(async {
            let mut response = send_broker_request(request, cancellation).await?;
            if response.status() != StatusCode::OK {
                return Err(classify_provider_status(response.status()));
            }
            if let Some(length) = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                && length != expected_size
            {
                return Err(BrokerFailure::ContentMismatch);
            }
            while consume_broker_chunk(&mut response, cancellation, consume).await? {}
            ensure_current(cancellation, deadline)
        })
    }
}

pub(super) fn materialize(
    broker: Option<&dyn RunInputBroker>,
    assignment_id: &str,
    execution_spec_id: &str,
    projection: Option<&RunInputProjectionV1>,
    deadline: PreparationDeadline,
    cancellation: &CaptureCancellation,
    private_root: &Path,
) -> Result<ResolvedImports, RunInputFailure> {
    let Some(projection) = projection else {
        return Ok(ResolvedImports::default());
    };
    validate_projection(projection)?;
    let broker = broker.ok_or(RunInputFailure::ServiceUnavailable)?;
    ensure_materialization_current(cancellation, deadline)?;
    let envelope = broker
        .manifest(assignment_id, execution_spec_id, cancellation, deadline)
        .map_err(manifest_broker_failure)?;
    let manifest = validate_manifest_envelope(&envelope, projection)?;

    // No capability or member request occurs until the fixed canonical bytes
    // have been compared with the immutable execution projection.
    let staging = tempfile::Builder::new()
        .prefix("run-inputs-")
        .tempdir_in(private_root)
        .map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
    let logical_members = logical_members(&manifest)?;
    let mut completed = Vec::with_capacity(logical_members.len());
    for member in &logical_members {
        ensure_materialization_current(cancellation, deadline)?;
        let capabilities = broker
            .capabilities(
                assignment_id,
                execution_spec_id,
                std::slice::from_ref(&member.member_id),
                cancellation,
                deadline,
            )
            .map_err(capability_broker_failure)?;
        validate_capabilities(
            &capabilities,
            projection,
            std::slice::from_ref(member),
            deadline,
        )?;
        let capability = capabilities
            .members
            .first()
            .ok_or(RunInputFailure::ManifestMismatch)?;
        completed.push(download_member(
            broker,
            staging.path(),
            member,
            capability,
            cancellation,
            deadline,
        )?);
    }
    let imports = construct_imports(&manifest, &completed)?;
    let _retained_staging: PathBuf = staging.keep();
    Ok(imports)
}

pub(super) fn validate_projection(
    projection: &RunInputProjectionV1,
) -> Result<(), RunInputFailure> {
    projection
        .input_set_id
        .parse::<crate::runner_protocol::generated::RunInputProjectionInputSetId>()
        .map_err(|_| RunInputFailure::InvalidProjection)?;
    if projection.manifest_digest.algorithm != "sha256"
        || !crate::execution::workflow::is_lowercase_hex(&projection.manifest_digest.value, 64)
    {
        return Err(RunInputFailure::InvalidProjection);
    }
    Ok(())
}

fn validate_manifest_envelope(
    envelope: &ManifestEnvelope,
    projection: &RunInputProjectionV1,
) -> Result<ManifestV1, RunInputFailure> {
    if envelope.schema_version != 1
        || envelope.input_set_id != projection.input_set_id
        || envelope.manifest_digest.algorithm != "sha256"
        || envelope.manifest_digest.value != projection.manifest_digest.value
    {
        return Err(RunInputFailure::ManifestMismatch);
    }
    validate_manifest(&envelope.manifest)?;
    let canonical = canonical_manifest(&envelope.manifest)?;
    let observed = lowercase_hex_bytes(digest(&SHA256, canonical.as_bytes()).as_ref());
    if observed != projection.manifest_digest.value {
        return Err(RunInputFailure::ManifestMismatch);
    }
    Ok(envelope.manifest.clone())
}

fn validate_manifest(manifest: &ManifestV1) -> Result<(), RunInputFailure> {
    if manifest.schema_version != 1
        || manifest.attachments.len() > MAXIMUM_ATTACHMENTS
        || (manifest.prompt.is_none() && manifest.attachments.is_empty())
    {
        return Err(RunInputFailure::ManifestMismatch);
    }
    let mut aggregate = 0_u64;
    if let Some(prompt) = &manifest.prompt {
        if prompt.size_bytes > MAXIMUM_PROMPT_BYTES
            || !crate::execution::workflow::is_lowercase_hex(&prompt.sha256, 64)
        {
            return Err(RunInputFailure::ManifestMismatch);
        }
        aggregate = prompt.size_bytes;
    }
    for (index, attachment) in manifest.attachments.iter().enumerate() {
        if attachment.index != index
            || attachment.size_bytes > MAXIMUM_ATTACHMENT_BYTES
            || !crate::execution::workflow::is_lowercase_hex(&attachment.sha256, 64)
            || !valid_display_name(attachment.display_name.as_deref())
            || !crate::execution::workflow::is_valid_media_type(&attachment.media_type)
        {
            return Err(RunInputFailure::ManifestMismatch);
        }
        aggregate = aggregate
            .checked_add(attachment.size_bytes)
            .filter(|total| *total <= MAXIMUM_AGGREGATE_BYTES)
            .ok_or(RunInputFailure::ManifestMismatch)?;
    }
    Ok(())
}

fn canonical_manifest(manifest: &ManifestV1) -> Result<String, RunInputFailure> {
    let mut canonical = String::new();
    canonical.push_str("{\"attachments\":[");
    for (index, attachment) in manifest.attachments.iter().enumerate() {
        if index > 0 {
            canonical.push(',');
        }
        let display_name = match &attachment.display_name {
            Some(name) => serde_json::to_string(name),
            None => Ok("null".to_owned()),
        }
        .map_err(|_| RunInputFailure::ManifestMismatch)?;
        write!(
            canonical,
            "{{\"displayName\":{display_name},\"index\":{},\"mediaType\":{},\"sha256\":{},\"sizeBytes\":{}}}",
            attachment.index,
            serde_json::to_string(&attachment.media_type)
                .map_err(|_| RunInputFailure::ManifestMismatch)?,
            serde_json::to_string(&attachment.sha256)
                .map_err(|_| RunInputFailure::ManifestMismatch)?,
            attachment.size_bytes,
        )
        .map_err(|_| RunInputFailure::ManifestMismatch)?;
    }
    canonical.push_str("],\"prompt\":");
    match &manifest.prompt {
        Some(prompt) => write!(
            canonical,
            "{{\"sha256\":{},\"sizeBytes\":{}}}",
            serde_json::to_string(&prompt.sha256).map_err(|_| RunInputFailure::ManifestMismatch)?,
            prompt.size_bytes,
        )
        .map_err(|_| RunInputFailure::ManifestMismatch)?,
        None => canonical.push_str("null"),
    }
    canonical.push_str(",\"schemaVersion\":1}");
    Ok(canonical)
}

struct LogicalMember {
    member_id: String,
    media_type: String,
    size_bytes: u64,
    sha256: String,
    final_name: String,
}

fn logical_members(manifest: &ManifestV1) -> Result<Vec<LogicalMember>, RunInputFailure> {
    let mut members =
        Vec::with_capacity(manifest.attachments.len() + usize::from(manifest.prompt.is_some()));
    if let Some(prompt) = &manifest.prompt {
        members.push(LogicalMember {
            member_id: "prompt".to_owned(),
            media_type: PROMPT_MEDIA_TYPE.to_owned(),
            size_bytes: prompt.size_bytes,
            sha256: prompt.sha256.clone(),
            final_name: "prompt".to_owned(),
        });
    }
    for attachment in &manifest.attachments {
        members.push(LogicalMember {
            member_id: format!("attachments/{:06}", attachment.index),
            media_type: attachment.media_type.clone(),
            size_bytes: attachment.size_bytes,
            sha256: attachment.sha256.clone(),
            final_name: format!("attachment-{:06}", attachment.index),
        });
    }
    Ok(members)
}

fn validate_capabilities(
    envelope: &CapabilityEnvelope,
    projection: &RunInputProjectionV1,
    requested: &[LogicalMember],
    deadline: PreparationDeadline,
) -> Result<(), RunInputFailure> {
    let expires_at = OffsetDateTime::parse(&envelope.capability_expires_at, &Rfc3339)
        .ok()
        .filter(|value| {
            envelope.capability_expires_at.ends_with('Z')
                && value.offset() == UtcOffset::UTC
                && *value > crate::timing::utc_now()
                && deadline.contains_expiry(*value)
        })
        .ok_or(RunInputFailure::ManifestMismatch)?;
    let _ = expires_at;
    if envelope.schema_version != 1
        || envelope.input_set_id != projection.input_set_id
        || envelope.members.len() != requested.len()
    {
        return Err(RunInputFailure::ManifestMismatch);
    }
    for (capability, expected) in envelope.members.iter().zip(requested) {
        if capability.member_id != expected.member_id
            || capability.media_type != expected.media_type
            || capability.size_bytes != expected.size_bytes
            || capability.sha256 != expected.sha256
            || validate_capability_url(&capability.url).is_err()
        {
            return Err(RunInputFailure::ManifestMismatch);
        }
    }
    Ok(())
}

fn download_member(
    broker: &dyn RunInputBroker,
    staging: &Path,
    member: &LogicalMember,
    capability: &CapabilityMember,
    cancellation: &CaptureCancellation,
    deadline: PreparationDeadline,
) -> Result<PathBuf, RunInputFailure> {
    let mut temporary = tempfile::Builder::new()
        .prefix("member-")
        .tempfile_in(staging)
        .map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
    let mut observed = 0_u64;
    let mut hash = DigestContext::new(&SHA256);
    broker
        .download(
            &capability.url,
            member.size_bytes,
            cancellation,
            deadline,
            &mut |bytes| {
                observed = observed
                    .checked_add(
                        u64::try_from(bytes.len()).map_err(|_| BrokerFailure::ContentMismatch)?,
                    )
                    .filter(|size| *size <= member.size_bytes)
                    .ok_or(BrokerFailure::ContentMismatch)?;
                hash.update(bytes);
                temporary
                    .as_file_mut()
                    .write_all(bytes)
                    .map_err(|_| BrokerFailure::Environment)
            },
        )
        .map_err(download_broker_failure)?;
    let observed_digest = hash.finish();
    let expected_digest = decode_hex(&member.sha256).ok_or(RunInputFailure::ManifestMismatch)?;
    if observed != member.size_bytes || observed_digest.as_ref() != expected_digest.as_slice() {
        return Err(RunInputFailure::ContentMismatch);
    }
    temporary
        .as_file()
        .sync_all()
        .and_then(|()| {
            temporary
                .as_file()
                .set_permissions(fs::Permissions::from_mode(0o400))
        })
        .map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
    let completed = staging.join(&member.final_name);
    temporary
        .persist_noclobber(&completed)
        .map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
    File::open(staging)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
    Ok(completed)
}

fn construct_imports(
    manifest: &ManifestV1,
    completed: &[PathBuf],
) -> Result<ResolvedImports, RunInputFailure> {
    let mut completed_index = 0;
    let prompt = if manifest.prompt.is_some() {
        let path = completed
            .get(completed_index)
            .ok_or(RunInputFailure::EnvironmentUnavailable)?;
        completed_index += 1;
        let bytes = fs::read(path).map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
        Some(Arc::<str>::from(
            String::from_utf8(bytes).map_err(|_| RunInputFailure::PromptInvalid)?,
        ))
    } else {
        None
    };
    let mut attachments = Vec::with_capacity(manifest.attachments.len());
    for attachment in &manifest.attachments {
        if !crate::execution::workflow::is_valid_media_type(&attachment.media_type) {
            return Err(RunInputFailure::ManifestMismatch);
        }
        let path = completed
            .get(completed_index)
            .ok_or(RunInputFailure::EnvironmentUnavailable)?;
        completed_index += 1;
        let bytes = fs::read(path).map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
        let mut resolved =
            ResolvedAttachment::new(Arc::from(attachment.media_type.as_str()), Arc::from(bytes));
        if let Some(name) = &attachment.display_name {
            resolved = resolved.with_diagnostic_source_name(Arc::from(name.as_str()));
        }
        attachments.push(resolved);
    }
    if completed_index != completed.len() {
        return Err(RunInputFailure::EnvironmentUnavailable);
    }
    Ok(ResolvedImports::new(prompt, Arc::from(attachments)))
}

fn required_u64(value: &Value, name: &str) -> Result<u64, BrokerFailure> {
    value[name].as_u64().ok_or(BrokerFailure::InvalidResponse)
}

fn required_string(value: &Value, name: &str) -> Result<String, BrokerFailure> {
    value[name]
        .as_str()
        .map(str::to_owned)
        .ok_or(BrokerFailure::InvalidResponse)
}

fn parse_manifest_envelope(value: Value) -> Result<ManifestEnvelope, BrokerFailure> {
    if !exact_object(
        &value,
        &["schemaVersion", "inputSetId", "manifestDigest", "manifest"],
    ) || !exact_object(&value["manifestDigest"], &["algorithm", "value"])
        || !exact_manifest_shape(&value["manifest"])
    {
        return Err(BrokerFailure::InvalidResponse);
    }
    Ok(ManifestEnvelope {
        schema_version: required_u64(&value, "schemaVersion")?,
        input_set_id: required_string(&value, "inputSetId")?,
        manifest_digest: serde_json::from_value(value["manifestDigest"].clone())
            .map_err(|_| BrokerFailure::InvalidResponse)?,
        manifest: serde_json::from_value(value["manifest"].clone())
            .map_err(|_| BrokerFailure::InvalidResponse)?,
    })
}

fn parse_capability_envelope(value: Value) -> Result<CapabilityEnvelope, BrokerFailure> {
    if !exact_object(
        &value,
        &[
            "schemaVersion",
            "inputSetId",
            "capabilityExpiresAt",
            "members",
        ],
    ) {
        return Err(BrokerFailure::InvalidResponse);
    }
    let members = value["members"]
        .as_array()
        .ok_or(BrokerFailure::InvalidResponse)?;
    if members.is_empty() || members.len() > MAXIMUM_CAPABILITY_MEMBERS {
        return Err(BrokerFailure::InvalidResponse);
    }
    let members = members
        .iter()
        .map(|member| {
            if !exact_object(
                member,
                &["memberId", "mediaType", "sizeBytes", "sha256", "url"],
            ) {
                return Err(BrokerFailure::InvalidResponse);
            }
            Ok(CapabilityMember {
                member_id: required_string(member, "memberId")?,
                media_type: required_string(member, "mediaType")?,
                size_bytes: required_u64(member, "sizeBytes")?,
                sha256: required_string(member, "sha256")?,
                url: required_string(member, "url")?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CapabilityEnvelope {
        schema_version: required_u64(&value, "schemaVersion")?,
        input_set_id: required_string(&value, "inputSetId")?,
        capability_expires_at: required_string(&value, "capabilityExpiresAt")?,
        members,
    })
}

fn exact_manifest_shape(value: &Value) -> bool {
    if !exact_object(value, &["schemaVersion", "prompt", "attachments"])
        || !(value["prompt"].is_null() || exact_object(&value["prompt"], &["sizeBytes", "sha256"]))
    {
        return false;
    }
    value["attachments"].as_array().is_some_and(|attachments| {
        attachments.iter().all(|attachment| {
            exact_object(
                attachment,
                &["index", "displayName", "mediaType", "sizeBytes", "sha256"],
            ) && (attachment["displayName"].is_null() || attachment["displayName"].is_string())
        })
    })
}

fn exact_object(value: &Value, names: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == names.len() && names.iter().all(|name| object.contains_key(*name))
    })
}

fn valid_display_name(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        !matches!(value, "" | "." | "..")
            && value.chars().count() <= 255
            && value
                .chars()
                .all(|character| !character.is_control() && character != '/' && character != '\\')
    })
}

fn validate_capability_url(raw: &str) -> Result<Url, BrokerFailure> {
    let parsed = Url::parse(raw).map_err(|_| BrokerFailure::InvalidResponse)?;
    let loopback_http = parsed.scheme() == "http"
        && parsed
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if parsed.username() != ""
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed.host_str().is_none()
        || !(parsed.scheme() == "https" || loopback_http)
    {
        return Err(BrokerFailure::InvalidResponse);
    }
    Ok(parsed)
}

fn ensure_current(
    cancellation: &CaptureCancellation,
    deadline: PreparationDeadline,
) -> Result<(), BrokerFailure> {
    if cancellation.is_cancelled() || deadline.remaining().is_none() {
        Err(BrokerFailure::Fenced)
    } else {
        Ok(())
    }
}

fn ensure_materialization_current(
    cancellation: &CaptureCancellation,
    deadline: PreparationDeadline,
) -> Result<(), RunInputFailure> {
    ensure_current(cancellation, deadline).map_err(|_| RunInputFailure::AssignmentFenced)
}

fn classify_provider_status(status: StatusCode) -> BrokerFailure {
    if status == StatusCode::NOT_FOUND {
        BrokerFailure::ContentUnavailable
    } else if status.is_client_error() || status.is_server_error() {
        BrokerFailure::Unavailable
    } else {
        BrokerFailure::ContentUnavailable
    }
}

fn manifest_broker_failure(failure: BrokerFailure) -> RunInputFailure {
    match failure {
        BrokerFailure::Fenced => RunInputFailure::AssignmentFenced,
        BrokerFailure::Unavailable => RunInputFailure::ServiceUnavailable,
        BrokerFailure::InvalidResponse => RunInputFailure::ManifestMismatch,
        BrokerFailure::ContentUnavailable => RunInputFailure::ContentUnavailable,
        BrokerFailure::ContentMismatch => RunInputFailure::ContentMismatch,
        BrokerFailure::Environment => RunInputFailure::EnvironmentUnavailable,
    }
}

fn capability_broker_failure(failure: BrokerFailure) -> RunInputFailure {
    match failure {
        BrokerFailure::InvalidResponse => RunInputFailure::ManifestMismatch,
        other => manifest_broker_failure(other),
    }
}

fn download_broker_failure(failure: BrokerFailure) -> RunInputFailure {
    match failure {
        BrokerFailure::Fenced => RunInputFailure::AssignmentFenced,
        BrokerFailure::Unavailable => RunInputFailure::ServiceUnavailable,
        BrokerFailure::ContentUnavailable => RunInputFailure::ContentUnavailable,
        BrokerFailure::ContentMismatch | BrokerFailure::InvalidResponse => {
            RunInputFailure::ContentMismatch
        }
        BrokerFailure::Environment => RunInputFailure::EnvironmentUnavailable,
    }
}

fn lowercase_hex_bytes(bytes: &[u8]) -> String {
    crate::execution::workflow::lowercase_hex(bytes)
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Some((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::runner_protocol::WorkflowSourceClosureDigestV1RunnerProjection;

    struct FixtureBroker {
        manifest: ManifestEnvelope,
        bodies: Mutex<VecDeque<Vec<u8>>>,
        manifest_calls: Mutex<usize>,
        capability_calls: Mutex<Vec<Vec<String>>>,
        download_calls: Mutex<usize>,
    }

    impl RunInputBroker for FixtureBroker {
        fn manifest(
            &self,
            _assignment_id: &str,
            _execution_spec_id: &str,
            _cancellation: &CaptureCancellation,
            _deadline: PreparationDeadline,
        ) -> Result<ManifestEnvelope, BrokerFailure> {
            *self.manifest_calls.lock().unwrap() += 1;
            Ok(self.manifest.clone())
        }

        fn capabilities(
            &self,
            _assignment_id: &str,
            _execution_spec_id: &str,
            members: &[String],
            _cancellation: &CaptureCancellation,
            deadline: PreparationDeadline,
        ) -> Result<CapabilityEnvelope, BrokerFailure> {
            self.capability_calls.lock().unwrap().push(members.to_vec());
            let logical = logical_members(&self.manifest.manifest)
                .unwrap()
                .into_iter()
                .filter(|member| members.contains(&member.member_id))
                .collect::<Vec<_>>();
            Ok(CapabilityEnvelope {
                schema_version: 1,
                input_set_id: self.manifest.input_set_id.clone(),
                capability_expires_at: deadline.expires_at.format(&Rfc3339).unwrap(),
                members: logical
                    .into_iter()
                    .map(|member| CapabilityMember {
                        member_id: member.member_id,
                        media_type: member.media_type,
                        size_bytes: member.size_bytes,
                        sha256: member.sha256,
                        url: "https://objects.example.test/exact?private=sentinel".to_owned(),
                    })
                    .collect(),
            })
        }

        fn download(
            &self,
            _url: &str,
            _expected_size: u64,
            _cancellation: &CaptureCancellation,
            _deadline: PreparationDeadline,
            consume: &mut dyn FnMut(&[u8]) -> Result<(), BrokerFailure>,
        ) -> Result<(), BrokerFailure> {
            *self.download_calls.lock().unwrap() += 1;
            let body = self
                .bodies
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(BrokerFailure::ContentUnavailable)?;
            for chunk in body.chunks(2) {
                consume(chunk)?;
            }
            Ok(())
        }
    }

    fn deadline() -> PreparationDeadline {
        let now = OffsetDateTime::parse("2099-01-01T00:00:00Z", &Rfc3339).unwrap();
        PreparationDeadline::from_wire(
            &((now + time::Duration::minutes(15))
                .format(&Rfc3339)
                .unwrap()),
            now,
            crate::timing::monotonic_now(),
        )
        .unwrap()
    }

    fn broker_for(
        manifest: ManifestV1,
        bodies: Vec<Vec<u8>>,
    ) -> (FixtureBroker, RunInputProjectionV1) {
        let digest = lowercase_hex_bytes(
            digest(&SHA256, canonical_manifest(&manifest).unwrap().as_bytes()).as_ref(),
        );
        let envelope = ManifestEnvelope {
            schema_version: 1,
            input_set_id: "ris_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            manifest_digest: DigestV1 {
                algorithm: "sha256".to_owned(),
                value: digest.clone(),
            },
            manifest,
        };
        let projection = RunInputProjectionV1 {
            input_set_id: envelope.input_set_id.clone(),
            manifest_digest: WorkflowSourceClosureDigestV1RunnerProjection {
                algorithm: "sha256".to_owned(),
                value: digest,
            },
        };
        (
            FixtureBroker {
                manifest: envelope,
                bodies: Mutex::new(bodies.into()),
                manifest_calls: Mutex::new(0),
                capability_calls: Mutex::new(Vec::new()),
                download_calls: Mutex::new(0),
            },
            projection,
        )
    }

    fn materialize_projection(
        broker: &FixtureBroker,
        projection: &RunInputProjectionV1,
        private_root: &Path,
    ) -> Result<ResolvedImports, RunInputFailure> {
        materialize(
            Some(broker),
            "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
            "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
            Some(projection),
            deadline(),
            &CaptureCancellation::default(),
            private_root,
        )
    }

    #[test]
    fn preparation_deadline_is_strict_and_fixed_at_receipt() {
        let now = OffsetDateTime::parse("2099-01-01T00:00:00Z", &Rfc3339).unwrap();
        let monotonic = crate::timing::monotonic_now();
        assert!(PreparationDeadline::from_wire("2099-01-01T00:00:00Z", now, monotonic,).is_none());
        let deadline =
            PreparationDeadline::from_wire("2099-01-01T00:15:00Z", now, monotonic).unwrap();
        assert!(deadline.remaining().is_some());
        assert!(deadline.contains_expiry(now + time::Duration::minutes(15)));
        assert!(!deadline.contains_expiry(now + time::Duration::minutes(16)));
    }

    #[test]
    fn canonical_encoder_matches_shared_digest_vectors() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/run-inputs/v1/manifest-digests.json"
        ))
        .unwrap();
        for vector in fixture["vectors"].as_array().unwrap() {
            let manifest: ManifestV1 = serde_json::from_value(vector["manifest"].clone()).unwrap();
            let canonical = canonical_manifest(&manifest).unwrap();
            assert_eq!(canonical, vector["canonical"].as_str().unwrap());
            assert_eq!(
                lowercase_hex_bytes(digest(&SHA256, canonical.as_bytes()).as_ref()),
                vector["sha256"].as_str().unwrap()
            );
        }
    }

    #[test]
    fn verifies_manifest_before_download_and_preserves_empty_prompt_and_order() {
        let empty = Vec::new();
        let second = b"second".to_vec();
        let first = b"first".to_vec();
        let manifest = ManifestV1 {
            schema_version: 1,
            prompt: Some(PromptMember {
                size_bytes: 0,
                sha256: lowercase_hex_bytes(digest(&SHA256, &empty).as_ref()),
            }),
            attachments: vec![
                AttachmentMember {
                    index: 0,
                    display_name: Some("reverse-upload-two".to_owned()),
                    media_type: "application/octet-stream".to_owned(),
                    size_bytes: u64::try_from(second.len()).unwrap(),
                    sha256: lowercase_hex_bytes(digest(&SHA256, &second).as_ref()),
                },
                AttachmentMember {
                    index: 1,
                    display_name: Some("reverse-upload-one".to_owned()),
                    media_type: "application/octet-stream".to_owned(),
                    size_bytes: u64::try_from(first.len()).unwrap(),
                    sha256: lowercase_hex_bytes(digest(&SHA256, &first).as_ref()),
                },
            ],
        };
        let (broker, projection) = broker_for(manifest, vec![empty, second.clone(), first.clone()]);
        let private = tempfile::tempdir().unwrap();
        let imports = materialize_projection(&broker, &projection, private.path()).unwrap();
        assert_eq!(imports.prompt(), Some(""));
        assert_eq!(imports.attachments()[0].bytes(), second);
        assert_eq!(imports.attachments()[1].bytes(), first);
        assert_eq!(
            imports.attachments()[0].diagnostic_source_name(),
            Some("reverse-upload-two")
        );
        assert_eq!(*broker.manifest_calls.lock().unwrap(), 1);
        assert_eq!(*broker.download_calls.lock().unwrap(), 3);

        let staging = fs::read_dir(private.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(staging.len(), 1);
        let names = fs::read_dir(&staging[0])
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            names,
            ["prompt", "attachment-000000", "attachment-000001"]
                .into_iter()
                .map(std::ffi::OsString::from)
                .collect()
        );
        assert!(!staging[0].join("reverse-upload-one").exists());
        for name in names {
            assert_eq!(
                fs::metadata(staging[0].join(name))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o400
            );
        }
    }

    #[test]
    fn digest_mismatch_prevents_capabilities_and_downloads_and_null_projection_is_call_free() {
        let manifest = ManifestV1 {
            schema_version: 1,
            prompt: Some(PromptMember {
                size_bytes: 0,
                sha256: lowercase_hex_bytes(digest(&SHA256, &[]).as_ref()),
            }),
            attachments: Vec::new(),
        };
        let (broker, mut projection) = broker_for(manifest, vec![Vec::new()]);
        projection.manifest_digest.value = "0".repeat(64);
        let private = tempfile::tempdir().unwrap();
        assert_eq!(
            materialize_projection(&broker, &projection, private.path()),
            Err(RunInputFailure::ManifestMismatch)
        );
        assert!(broker.capability_calls.lock().unwrap().is_empty());
        assert_eq!(*broker.download_calls.lock().unwrap(), 0);

        let empty = materialize(
            Some(&broker),
            "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
            "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
            None,
            deadline(),
            &CaptureCancellation::default(),
            private.path(),
        )
        .unwrap();
        assert_eq!(empty, ResolvedImports::default());
        assert_eq!(*broker.manifest_calls.lock().unwrap(), 1);
    }

    #[test]
    fn materialization_refreshes_authority_per_member_and_closes_content_failures() {
        let empty_digest = lowercase_hex_bytes(digest(&SHA256, &[]).as_ref());
        let attachments = (0..101)
            .map(|index| AttachmentMember {
                index,
                display_name: None,
                media_type: "application/octet-stream".to_owned(),
                size_bytes: 0,
                sha256: empty_digest.clone(),
            })
            .collect::<Vec<_>>();
        let (broker, projection) = broker_for(
            ManifestV1 {
                schema_version: 1,
                prompt: None,
                attachments,
            },
            vec![Vec::new(); 101],
        );
        let private = tempfile::tempdir().unwrap();
        let imports = materialize_projection(&broker, &projection, private.path()).unwrap();
        assert_eq!(imports.attachments().len(), 101);
        assert_eq!(
            broker
                .capability_calls
                .lock()
                .unwrap()
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![1; 101]
        );

        assert_eq!(
            classify_provider_status(StatusCode::FORBIDDEN),
            BrokerFailure::Unavailable
        );
        assert_eq!(
            classify_provider_status(StatusCode::NOT_FOUND),
            BrokerFailure::ContentUnavailable
        );

        for (body, expected) in [
            (vec![0xff], RunInputFailure::PromptInvalid),
            (b"wrong".to_vec(), RunInputFailure::ContentMismatch),
        ] {
            let declared = if expected == RunInputFailure::PromptInvalid {
                body.clone()
            } else {
                b"right".to_vec()
            };
            let (broker, projection) = broker_for(
                ManifestV1 {
                    schema_version: 1,
                    prompt: Some(PromptMember {
                        size_bytes: u64::try_from(declared.len()).unwrap(),
                        sha256: lowercase_hex_bytes(digest(&SHA256, &declared).as_ref()),
                    }),
                    attachments: Vec::new(),
                },
                vec![body],
            );
            let private = tempfile::tempdir().unwrap();
            assert_eq!(
                materialize_projection(&broker, &projection, private.path()),
                Err(expected)
            );
            assert!(fs::read_dir(private.path()).unwrap().next().is_none());
        }
    }

    #[test]
    fn materialized_imports_redact_private_values_from_debug() {
        let prompt = b"prompt-debug-privacy-sentinel".to_vec();
        let attachment = b"attachment-debug-privacy-sentinel".to_vec();
        let media_type = "application/x-debug-privacy-sentinel";
        let display_name = "display-debug-privacy-sentinel";
        let manifest = ManifestV1 {
            schema_version: 1,
            prompt: Some(PromptMember {
                size_bytes: u64::try_from(prompt.len()).unwrap(),
                sha256: lowercase_hex_bytes(digest(&SHA256, &prompt).as_ref()),
            }),
            attachments: vec![AttachmentMember {
                index: 0,
                display_name: Some(display_name.to_owned()),
                media_type: media_type.to_owned(),
                size_bytes: u64::try_from(attachment.len()).unwrap(),
                sha256: lowercase_hex_bytes(digest(&SHA256, &attachment).as_ref()),
            }],
        };
        let attachment_debug = format!("{attachment:?}");
        let (broker, projection) = broker_for(manifest, vec![prompt.clone(), attachment]);
        let private = tempfile::tempdir().unwrap();
        let imports = materialize_projection(&broker, &projection, private.path()).unwrap();
        let debug = format!("{imports:?}");

        for private_value in [
            std::str::from_utf8(&prompt).unwrap(),
            display_name,
            media_type,
            attachment_debug.as_str(),
        ] {
            assert!(
                !debug.contains(private_value),
                "materialized import Debug exposed a private Run Input value"
            );
        }
    }

    #[test]
    fn http_broker_refuses_redirects_without_contacting_the_target() {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;

        let target = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        target.set_nonblocking(true).unwrap();
        let target_url = format!("http://{}/private", target.local_addr().unwrap());
        let redirect = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let redirect_url = format!("http://{}/private", redirect.local_addr().unwrap());
        let worker = std::thread::spawn(move || {
            let (mut connection, _) = redirect.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = connection.read(&mut request).unwrap();
            write!(
                connection,
                "HTTP/1.1 302 Found\r\nLocation: {target_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            connection.flush().unwrap();
        });
        let endpoint = Url::parse("ws://127.0.0.1:1/v1/runner/connect").unwrap();
        let broker = HttpRunInputBroker::new(
            &endpoint,
            &crate::runner::credential::test_credential(),
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc",
        )
        .unwrap();
        let result = broker.download(
            &redirect_url,
            0,
            &CaptureCancellation::default(),
            deadline(),
            &mut |_| Ok(()),
        );
        worker.join().unwrap();
        assert_eq!(result, Err(BrokerFailure::ContentUnavailable));
        assert!(
            matches!(target.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }
}
