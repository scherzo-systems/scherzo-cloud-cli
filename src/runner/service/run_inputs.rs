use std::collections::BTreeMap;
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

use crate::execution::workflow::admission::{ResolvedAttachment, ResolvedInput, ResolvedInputs};
use crate::execution::workflow::artifact::CaptureCancellation;
use crate::runner::credential::Credential;
use crate::runner_protocol::RunInputProjectionV1;

const MANIFEST_RESPONSE_LIMIT: usize = 1024 * 1024;
const CAPABILITY_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;
const PROVIDER_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAXIMUM_INPUTS: usize = 256;
const MAXIMUM_TEXT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_ATTACHMENTS: usize = 256;
const MAXIMUM_ATTACHMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_AGGREGATE_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_CAPABILITY_MEMBERS: usize = 100;
const TEXT_MEDIA_TYPE: &str = "text/plain; charset=utf-8";

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
    TextInvalid,
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
struct AttachmentMember {
    index: usize,
    display_name: Option<String>,
    media_type: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ManifestInput {
    Text {
        #[serde(rename = "sizeBytes")]
        size_bytes: u64,
        sha256: String,
    },
    Attachments {
        items: Vec<AttachmentMember>,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestV1 {
    schema_version: u64,
    inputs: BTreeMap<String, ManifestInput>,
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
) -> Result<ResolvedInputs, RunInputFailure> {
    materialize_with_clock(
        broker,
        MaterializationIdentity {
            assignment_id,
            execution_spec_id,
        },
        projection,
        deadline,
        cancellation,
        private_root,
        crate::timing::utc_now,
    )
}

struct MaterializationIdentity<'a> {
    assignment_id: &'a str,
    execution_spec_id: &'a str,
}

fn materialize_with_clock(
    broker: Option<&dyn RunInputBroker>,
    identity: MaterializationIdentity<'_>,
    projection: Option<&RunInputProjectionV1>,
    deadline: PreparationDeadline,
    cancellation: &CaptureCancellation,
    private_root: &Path,
    mut utc_now: impl FnMut() -> OffsetDateTime,
) -> Result<ResolvedInputs, RunInputFailure> {
    let Some(projection) = projection else {
        return Ok(ResolvedInputs::default());
    };
    validate_projection(projection)?;
    let broker = broker.ok_or(RunInputFailure::ServiceUnavailable)?;
    ensure_materialization_current(cancellation, deadline)?;
    let envelope = broker
        .manifest(
            identity.assignment_id,
            identity.execution_spec_id,
            cancellation,
            deadline,
        )
        .map_err(manifest_broker_failure)?;
    let manifest = validate_manifest_envelope(&envelope, projection)?;

    // No capability or member request occurs until the canonical manifest bytes
    // have been compared with the immutable execution projection.
    let staging = tempfile::Builder::new()
        .prefix("run-inputs-")
        .tempdir_in(private_root)
        .map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
    let logical_members = logical_members(&manifest)?;
    let mut completed = Vec::with_capacity(logical_members.len());
    for batch in logical_members.chunks(MAXIMUM_CAPABILITY_MEMBERS) {
        let mut first_undownloaded = 0;
        while first_undownloaded < batch.len() {
            ensure_materialization_current(cancellation, deadline)?;
            let requested = &batch[first_undownloaded..];
            let member_ids = requested
                .iter()
                .map(|member| member.member_id.clone())
                .collect::<Vec<_>>();
            let capabilities = broker
                .capabilities(
                    identity.assignment_id,
                    identity.execution_spec_id,
                    &member_ids,
                    cancellation,
                    deadline,
                )
                .map_err(capability_broker_failure)?;
            let capability_expires_at =
                validate_capabilities(&capabilities, projection, requested, deadline, utc_now())?;
            let response_start = first_undownloaded;
            for (member, capability) in requested.iter().zip(&capabilities.members) {
                ensure_materialization_current(cancellation, deadline)?;
                if utc_now() >= capability_expires_at {
                    if first_undownloaded == response_start {
                        return Err(RunInputFailure::ServiceUnavailable);
                    }
                    break;
                }
                let completed_member = download_member(
                    broker,
                    staging.path(),
                    member,
                    capability,
                    cancellation,
                    deadline,
                )?;
                ensure_materialization_current(cancellation, deadline)?;
                completed.push(completed_member);
                first_undownloaded += 1;
            }
        }
    }
    let inputs = construct_inputs(&manifest, &completed)?;
    let _retained_staging: PathBuf = staging.keep();
    Ok(inputs)
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
        || manifest.inputs.is_empty()
        || manifest.inputs.len() > MAXIMUM_INPUTS
        || manifest
            .inputs
            .keys()
            .any(|name| !crate::execution::workflow::is_input_name(name))
    {
        return Err(RunInputFailure::ManifestMismatch);
    }
    let mut aggregate = 0_u64;
    let mut attachment_count = 0_usize;
    for input in manifest.inputs.values() {
        match input {
            ManifestInput::Text { size_bytes, sha256 } => {
                if *size_bytes > MAXIMUM_TEXT_BYTES
                    || !crate::execution::workflow::is_lowercase_hex(sha256, 64)
                {
                    return Err(RunInputFailure::ManifestMismatch);
                }
                aggregate = add_manifest_bytes(aggregate, *size_bytes)?;
            }
            ManifestInput::Attachments { items } => {
                attachment_count = attachment_count
                    .checked_add(items.len())
                    .filter(|count| *count <= MAXIMUM_ATTACHMENTS)
                    .ok_or(RunInputFailure::ManifestMismatch)?;
                for (index, attachment) in items.iter().enumerate() {
                    if attachment.index != index
                        || attachment.size_bytes > MAXIMUM_ATTACHMENT_BYTES
                        || !crate::execution::workflow::is_lowercase_hex(&attachment.sha256, 64)
                        || !valid_display_name(attachment.display_name.as_deref())
                        || !crate::execution::workflow::is_valid_media_type(&attachment.media_type)
                    {
                        return Err(RunInputFailure::ManifestMismatch);
                    }
                    aggregate = add_manifest_bytes(aggregate, attachment.size_bytes)?;
                }
            }
        }
    }
    Ok(())
}

fn add_manifest_bytes(total: u64, size: u64) -> Result<u64, RunInputFailure> {
    total
        .checked_add(size)
        .filter(|total| *total <= MAXIMUM_AGGREGATE_BYTES)
        .ok_or(RunInputFailure::ManifestMismatch)
}

fn canonical_manifest(manifest: &ManifestV1) -> Result<String, RunInputFailure> {
    let mut canonical = String::from("{\"inputs\":{");
    for (input_index, (name, input)) in manifest.inputs.iter().enumerate() {
        if input_index > 0 {
            canonical.push(',');
        }
        canonical
            .push_str(&serde_json::to_string(name).map_err(|_| RunInputFailure::ManifestMismatch)?);
        canonical.push(':');
        match input {
            ManifestInput::Text { size_bytes, sha256 } => {
                write!(
                    canonical,
                    "{{\"kind\":\"text\",\"sha256\":{},\"sizeBytes\":{size_bytes}}}",
                    serde_json::to_string(sha256).map_err(|_| RunInputFailure::ManifestMismatch)?,
                )
                .map_err(|_| RunInputFailure::ManifestMismatch)?;
            }
            ManifestInput::Attachments { items } => {
                canonical.push_str("{\"items\":[");
                for (index, attachment) in items.iter().enumerate() {
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
                canonical.push_str("],\"kind\":\"attachments\"}");
            }
        }
    }
    canonical.push_str("},\"schemaVersion\":1}");
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
    let mut members = Vec::new();
    for (name, input) in &manifest.inputs {
        match input {
            ManifestInput::Text { size_bytes, sha256 } => members.push(LogicalMember {
                member_id: format!("inputs/{name}"),
                media_type: TEXT_MEDIA_TYPE.to_owned(),
                size_bytes: *size_bytes,
                sha256: sha256.clone(),
                final_name: format!("member-{:06}", members.len()),
            }),
            ManifestInput::Attachments { items } => {
                for attachment in items {
                    members.push(LogicalMember {
                        member_id: format!("inputs/{name}/{:06}", attachment.index),
                        media_type: attachment.media_type.clone(),
                        size_bytes: attachment.size_bytes,
                        sha256: attachment.sha256.clone(),
                        final_name: format!("member-{:06}", members.len()),
                    });
                }
            }
        }
    }
    Ok(members)
}

fn validate_capabilities(
    envelope: &CapabilityEnvelope,
    projection: &RunInputProjectionV1,
    requested: &[LogicalMember],
    deadline: PreparationDeadline,
    utc_now: OffsetDateTime,
) -> Result<OffsetDateTime, RunInputFailure> {
    let expires_at = OffsetDateTime::parse(&envelope.capability_expires_at, &Rfc3339)
        .ok()
        .filter(|value| {
            envelope.capability_expires_at.ends_with('Z')
                && value.offset() == UtcOffset::UTC
                && *value > utc_now
                && deadline.contains_expiry(*value)
        })
        .ok_or(RunInputFailure::ManifestMismatch)?;
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
    Ok(expires_at)
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

fn construct_inputs(
    manifest: &ManifestV1,
    completed: &[PathBuf],
) -> Result<ResolvedInputs, RunInputFailure> {
    let mut completed_index = 0;
    let mut inputs = BTreeMap::new();
    for (name, input) in &manifest.inputs {
        let resolved = match input {
            ManifestInput::Text { .. } => {
                let path = completed
                    .get(completed_index)
                    .ok_or(RunInputFailure::EnvironmentUnavailable)?;
                completed_index += 1;
                let bytes = fs::read(path).map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
                ResolvedInput::Text(Arc::<str>::from(
                    String::from_utf8(bytes).map_err(|_| RunInputFailure::TextInvalid)?,
                ))
            }
            ManifestInput::Attachments { items } => {
                let mut attachments = Vec::with_capacity(items.len());
                for attachment in items {
                    let path = completed
                        .get(completed_index)
                        .ok_or(RunInputFailure::EnvironmentUnavailable)?;
                    completed_index += 1;
                    let bytes =
                        fs::read(path).map_err(|_| RunInputFailure::EnvironmentUnavailable)?;
                    let mut resolved = ResolvedAttachment::new(
                        Arc::from(attachment.media_type.as_str()),
                        Arc::from(bytes),
                    );
                    if let Some(display_name) = &attachment.display_name {
                        resolved =
                            resolved.with_diagnostic_source_name(Arc::from(display_name.as_str()));
                    }
                    attachments.push(resolved);
                }
                ResolvedInput::Attachments(Arc::from(attachments))
            }
        };
        inputs.insert(name.clone(), resolved);
    }
    if completed_index != completed.len() {
        return Err(RunInputFailure::EnvironmentUnavailable);
    }
    Ok(ResolvedInputs::new(inputs))
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
    if !exact_object(value, &["schemaVersion", "inputs"]) {
        return false;
    }
    value["inputs"].as_object().is_some_and(|inputs| {
        inputs.values().all(|input| match input["kind"].as_str() {
            Some("text") => exact_object(input, &["kind", "sizeBytes", "sha256"]),
            Some("attachments") => {
                exact_object(input, &["kind", "items"])
                    && input["items"].as_array().is_some_and(|items| {
                        items.iter().all(|attachment| {
                            exact_object(
                                attachment,
                                &["index", "displayName", "mediaType", "sizeBytes", "sha256"],
                            ) && (attachment["displayName"].is_null()
                                || attachment["displayName"].is_string())
                        })
                    })
            }
            _ => false,
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
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;
    use crate::runner_protocol::WorkflowSourceClosureDigestV1RunnerProjection;

    #[derive(Clone, Copy, Debug, Default)]
    enum CapabilityMutation {
        #[default]
        None,
        Missing,
        Duplicate,
        Extra,
        WrongMetadata,
        WrongSet,
        Expired,
    }

    struct FixtureBroker {
        manifest: ManifestEnvelope,
        bodies: Mutex<VecDeque<Vec<u8>>>,
        manifest_calls: Mutex<usize>,
        capability_calls: Mutex<Vec<Vec<String>>>,
        download_calls: Mutex<Vec<String>>,
        capability_mutation: CapabilityMutation,
        capability_failure: Option<BrokerFailure>,
        capability_expirations: Mutex<VecDeque<String>>,
        expire_after_download: Option<Arc<AtomicBool>>,
        cancel_after_download: Option<usize>,
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
            let issuance = {
                let mut calls = self.capability_calls.lock().unwrap();
                calls.push(members.to_vec());
                calls.len()
            };
            if let Some(failure) = self.capability_failure {
                return Err(failure);
            }
            let logical = logical_members(&self.manifest.manifest)
                .unwrap()
                .into_iter()
                .filter(|member| members.contains(&member.member_id))
                .collect::<Vec<_>>();
            let mut envelope = CapabilityEnvelope {
                schema_version: 1,
                input_set_id: self.manifest.input_set_id.clone(),
                capability_expires_at: self
                    .capability_expirations
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| deadline.expires_at.format(&Rfc3339).unwrap()),
                members: logical
                    .into_iter()
                    .map(|member| CapabilityMember {
                        url: fixture_member_url(&member.member_id, issuance),
                        member_id: member.member_id,
                        media_type: member.media_type,
                        size_bytes: member.size_bytes,
                        sha256: member.sha256,
                    })
                    .collect(),
            };
            match self.capability_mutation {
                CapabilityMutation::None => {}
                CapabilityMutation::Missing => {
                    envelope.members.pop();
                }
                CapabilityMutation::Duplicate => {
                    if let Some(first) = envelope.members.first().cloned()
                        && let Some(last) = envelope.members.last_mut()
                    {
                        *last = first;
                    }
                }
                CapabilityMutation::Extra => {
                    if let Some(last) = envelope.members.last().cloned() {
                        envelope.members.push(last);
                    }
                }
                CapabilityMutation::WrongMetadata => {
                    if let Some(last) = envelope.members.last_mut() {
                        last.size_bytes = last.size_bytes.saturating_add(1);
                    }
                }
                CapabilityMutation::WrongSet => {
                    envelope.input_set_id = "ris_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned();
                }
                CapabilityMutation::Expired => {
                    envelope.capability_expires_at =
                        OffsetDateTime::UNIX_EPOCH.format(&Rfc3339).unwrap();
                }
            }
            Ok(envelope)
        }

        fn download(
            &self,
            url: &str,
            _expected_size: u64,
            cancellation: &CaptureCancellation,
            _deadline: PreparationDeadline,
            consume: &mut dyn FnMut(&[u8]) -> Result<(), BrokerFailure>,
        ) -> Result<(), BrokerFailure> {
            let download_number = {
                let mut calls = self.download_calls.lock().unwrap();
                calls.push(url.to_owned());
                calls.len()
            };
            let body = self
                .bodies
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(BrokerFailure::ContentUnavailable)?;
            for chunk in body.chunks(2) {
                consume(chunk)?;
            }
            if let Some(expired) = &self.expire_after_download {
                expired.store(true, Ordering::SeqCst);
            }
            if self.cancel_after_download == Some(download_number) {
                cancellation.cancel();
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
                download_calls: Mutex::new(Vec::new()),
                capability_mutation: CapabilityMutation::None,
                capability_failure: None,
                capability_expirations: Mutex::new(VecDeque::new()),
                expire_after_download: None,
                cancel_after_download: None,
            },
            projection,
        )
    }

    fn fixture_member_url(member_id: &str, issuance: usize) -> String {
        format!("https://objects.example.test/{member_id}?issuance={issuance}")
    }

    fn attachment_body(index: usize) -> Vec<u8> {
        format!("attachment-{index:06}").into_bytes()
    }

    fn manifest_with_member_count(member_count: usize) -> (ManifestV1, Vec<Vec<u8>>) {
        assert!((1..=MAXIMUM_ATTACHMENTS + 1).contains(&member_count));
        let request = b"request".to_vec();
        let mut bodies = Vec::with_capacity(member_count);
        let mut attachments = Vec::with_capacity(member_count - 1);
        for index in 0..member_count - 1 {
            let body = attachment_body(index);
            attachments.push(AttachmentMember {
                index,
                display_name: None,
                media_type: "application/octet-stream".to_owned(),
                size_bytes: u64::try_from(body.len()).unwrap(),
                sha256: lowercase_hex_bytes(digest(&SHA256, &body).as_ref()),
            });
            bodies.push(body);
        }
        bodies.push(request.clone());
        (
            ManifestV1 {
                schema_version: 1,
                inputs: BTreeMap::from([
                    (
                        "request".to_owned(),
                        ManifestInput::Text {
                            size_bytes: u64::try_from(request.len()).unwrap(),
                            sha256: lowercase_hex_bytes(digest(&SHA256, &request).as_ref()),
                        },
                    ),
                    (
                        "evidence".to_owned(),
                        ManifestInput::Attachments { items: attachments },
                    ),
                ]),
            },
            bodies,
        )
    }

    fn expected_member_ids(member_count: usize) -> Vec<String> {
        (0..member_count.saturating_sub(1))
            .map(|index| format!("inputs/evidence/{index:06}"))
            .chain(std::iter::once("inputs/request".to_owned()))
            .collect()
    }

    fn text_value<'a>(inputs: &'a ResolvedInputs, name: &str) -> &'a str {
        let Some(ResolvedInput::Text(value)) = inputs.get(name) else {
            panic!("named Text input is missing");
        };
        value
    }

    fn attachment_values<'a>(inputs: &'a ResolvedInputs, name: &str) -> &'a [ResolvedAttachment] {
        let Some(ResolvedInput::Attachments(values)) = inputs.get(name) else {
            panic!("named attachment collection is missing");
        };
        values
    }

    fn assert_private_root_empty(private_root: &Path) {
        assert!(fs::read_dir(private_root).unwrap().next().is_none());
    }

    fn materialize_projection(
        broker: &FixtureBroker,
        projection: &RunInputProjectionV1,
        private_root: &Path,
    ) -> Result<ResolvedInputs, RunInputFailure> {
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
    fn verifies_manifest_before_download_and_preserves_empty_text_and_collection_order() {
        let empty = Vec::new();
        let second = b"second".to_vec();
        let first = b"first".to_vec();
        let manifest = ManifestV1 {
            schema_version: 1,
            inputs: BTreeMap::from([
                (
                    "request".to_owned(),
                    ManifestInput::Text {
                        size_bytes: 0,
                        sha256: lowercase_hex_bytes(digest(&SHA256, &empty).as_ref()),
                    },
                ),
                (
                    "evidence".to_owned(),
                    ManifestInput::Attachments {
                        items: vec![
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
                    },
                ),
            ]),
        };
        let (broker, projection) = broker_for(manifest, vec![second.clone(), first.clone(), empty]);
        let private = tempfile::tempdir().unwrap();
        let inputs = materialize_projection(&broker, &projection, private.path()).unwrap();
        assert_eq!(text_value(&inputs, "request"), "");
        let attachments = attachment_values(&inputs, "evidence");
        assert_eq!(attachments[0].bytes(), second);
        assert_eq!(attachments[1].bytes(), first);
        assert_eq!(
            attachments[0].diagnostic_source_name(),
            Some("reverse-upload-two")
        );
        assert_eq!(*broker.manifest_calls.lock().unwrap(), 1);
        assert_eq!(broker.download_calls.lock().unwrap().len(), 3);

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
            ["member-000000", "member-000001", "member-000002"]
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
            inputs: BTreeMap::from([(
                "request".to_owned(),
                ManifestInput::Text {
                    size_bytes: 0,
                    sha256: lowercase_hex_bytes(digest(&SHA256, &[]).as_ref()),
                },
            )]),
        };
        let (broker, mut projection) = broker_for(manifest, vec![Vec::new()]);
        projection.manifest_digest.value = "0".repeat(64);
        let private = tempfile::tempdir().unwrap();
        assert_eq!(
            materialize_projection(&broker, &projection, private.path()),
            Err(RunInputFailure::ManifestMismatch)
        );
        assert!(broker.capability_calls.lock().unwrap().is_empty());
        assert!(broker.download_calls.lock().unwrap().is_empty());

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
        assert_eq!(empty, ResolvedInputs::default());
        assert_eq!(*broker.manifest_calls.lock().unwrap(), 1);
    }

    #[test]
    fn materialization_batches_exact_members_at_boundaries_and_preserves_values() {
        for member_count in [99, 100, 101, MAXIMUM_ATTACHMENTS + 1] {
            let (manifest, bodies) = manifest_with_member_count(member_count);
            let (broker, projection) = broker_for(manifest, bodies);
            let private = tempfile::tempdir().unwrap();

            let inputs = materialize_projection(&broker, &projection, private.path()).unwrap();

            assert_eq!(text_value(&inputs, "request"), "request");
            let attachments = attachment_values(&inputs, "evidence");
            assert_eq!(attachments.len(), member_count - 1);
            for (index, attachment) in attachments.iter().enumerate() {
                assert_eq!(attachment.bytes(), attachment_body(index));
            }
            let expected_ids = expected_member_ids(member_count);
            let expected_batches = expected_ids
                .chunks(MAXIMUM_CAPABILITY_MEMBERS)
                .map(<[String]>::to_vec)
                .collect::<Vec<_>>();
            assert_eq!(*broker.capability_calls.lock().unwrap(), expected_batches);
            assert_eq!(
                *broker.download_calls.lock().unwrap(),
                expected_ids
                    .chunks(MAXIMUM_CAPABILITY_MEMBERS)
                    .enumerate()
                    .flat_map(|(batch_index, batch)| {
                        batch.iter().map(move |member_id| {
                            fixture_member_url(member_id, batch_index.saturating_add(1))
                        })
                    })
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn expired_batch_suffix_is_reissued_before_later_downloads() {
        let start = OffsetDateTime::parse("2099-01-01T00:00:00Z", &Rfc3339).unwrap();
        let first_expiry = start + time::Duration::minutes(5);
        let second_expiry = start + time::Duration::minutes(10);
        let expired = Arc::new(AtomicBool::new(false));
        let (manifest, bodies) = manifest_with_member_count(3);
        let (mut broker, projection) = broker_for(manifest, bodies);
        broker.capability_expirations = Mutex::new(VecDeque::from([
            first_expiry.format(&Rfc3339).unwrap(),
            second_expiry.format(&Rfc3339).unwrap(),
        ]));
        broker.expire_after_download = Some(Arc::clone(&expired));
        let private = tempfile::tempdir().unwrap();

        let inputs = materialize_with_clock(
            Some(&broker),
            MaterializationIdentity {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
                execution_spec_id: "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
            },
            Some(&projection),
            deadline(),
            &CaptureCancellation::default(),
            private.path(),
            || {
                if expired.load(Ordering::SeqCst) {
                    first_expiry
                } else {
                    start
                }
            },
        )
        .unwrap();

        assert_eq!(text_value(&inputs, "request"), "request");
        assert_eq!(attachment_values(&inputs, "evidence").len(), 2);
        assert_eq!(
            *broker.capability_calls.lock().unwrap(),
            vec![expected_member_ids(3), expected_member_ids(3)[1..].to_vec()]
        );
        assert_eq!(
            *broker.download_calls.lock().unwrap(),
            vec![
                fixture_member_url("inputs/evidence/000000", 1),
                fixture_member_url("inputs/evidence/000001", 2),
                fixture_member_url("inputs/request", 2),
            ]
        );
    }

    #[test]
    fn invalid_batch_entries_prevent_every_download_in_the_batch() {
        for mutation in [
            CapabilityMutation::Missing,
            CapabilityMutation::Duplicate,
            CapabilityMutation::Extra,
            CapabilityMutation::WrongMetadata,
            CapabilityMutation::WrongSet,
            CapabilityMutation::Expired,
        ] {
            let (manifest, bodies) = manifest_with_member_count(2);
            let (mut broker, projection) = broker_for(manifest, bodies);
            broker.capability_mutation = mutation;
            let private = tempfile::tempdir().unwrap();

            assert_eq!(
                materialize_projection(&broker, &projection, private.path()),
                Err(RunInputFailure::ManifestMismatch),
                "{mutation:?}"
            );
            assert_eq!(
                *broker.capability_calls.lock().unwrap(),
                vec![expected_member_ids(2)],
                "{mutation:?}"
            );
            assert!(
                broker.download_calls.lock().unwrap().is_empty(),
                "{mutation:?}"
            );
            assert_private_root_empty(private.path());
        }
    }

    #[test]
    fn cancellation_fencing_and_deadline_stop_acquisition_and_remove_staging() {
        let (manifest, bodies) = manifest_with_member_count(101);
        let (mut broker, projection) = broker_for(manifest, bodies);
        broker.cancel_after_download = Some(1);
        let private = tempfile::tempdir().unwrap();
        let cancellation = CaptureCancellation::default();
        assert_eq!(
            materialize(
                Some(&broker),
                "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
                Some(&projection),
                deadline(),
                &cancellation,
                private.path(),
            ),
            Err(RunInputFailure::AssignmentFenced)
        );
        assert_eq!(broker.capability_calls.lock().unwrap().len(), 1);
        assert_eq!(broker.download_calls.lock().unwrap().len(), 1);
        assert_private_root_empty(private.path());

        let (manifest, bodies) = manifest_with_member_count(1);
        let (mut broker, projection) = broker_for(manifest, bodies);
        broker.capability_failure = Some(BrokerFailure::Fenced);
        let private = tempfile::tempdir().unwrap();
        assert_eq!(
            materialize_projection(&broker, &projection, private.path()),
            Err(RunInputFailure::AssignmentFenced)
        );
        assert_eq!(broker.capability_calls.lock().unwrap().len(), 1);
        assert!(broker.download_calls.lock().unwrap().is_empty());
        assert_private_root_empty(private.path());

        let (manifest, bodies) = manifest_with_member_count(1);
        let (broker, projection) = broker_for(manifest, bodies);
        let private = tempfile::tempdir().unwrap();
        assert_eq!(
            materialize(
                Some(&broker),
                "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
                Some(&projection),
                PreparationDeadline::elapsed_for_test(),
                &CaptureCancellation::default(),
                private.path(),
            ),
            Err(RunInputFailure::AssignmentFenced)
        );
        assert_eq!(*broker.manifest_calls.lock().unwrap(), 0);
        assert!(broker.capability_calls.lock().unwrap().is_empty());
        assert!(broker.download_calls.lock().unwrap().is_empty());
        assert_private_root_empty(private.path());
    }

    #[test]
    fn content_failures_are_classified_and_remove_staging() {
        assert_eq!(
            classify_provider_status(StatusCode::FORBIDDEN),
            BrokerFailure::Unavailable
        );
        assert_eq!(
            classify_provider_status(StatusCode::NOT_FOUND),
            BrokerFailure::ContentUnavailable
        );

        for (body, expected) in [
            (vec![0xff], RunInputFailure::TextInvalid),
            (b"wrong".to_vec(), RunInputFailure::ContentMismatch),
        ] {
            let declared = if expected == RunInputFailure::TextInvalid {
                body.clone()
            } else {
                b"right".to_vec()
            };
            let (broker, projection) = broker_for(
                ManifestV1 {
                    schema_version: 1,
                    inputs: BTreeMap::from([(
                        "request".to_owned(),
                        ManifestInput::Text {
                            size_bytes: u64::try_from(declared.len()).unwrap(),
                            sha256: lowercase_hex_bytes(digest(&SHA256, &declared).as_ref()),
                        },
                    )]),
                },
                vec![body],
            );
            let private = tempfile::tempdir().unwrap();
            assert_eq!(
                materialize_projection(&broker, &projection, private.path()),
                Err(expected)
            );
            assert_private_root_empty(private.path());
        }
    }

    #[test]
    fn materialized_inputs_redact_private_values_from_debug() {
        let request = b"request-debug-privacy-sentinel".to_vec();
        let attachment = b"attachment-debug-privacy-sentinel".to_vec();
        let media_type = "application/x-debug-privacy-sentinel";
        let display_name = "display-debug-privacy-sentinel";
        let manifest = ManifestV1 {
            schema_version: 1,
            inputs: BTreeMap::from([
                (
                    "request".to_owned(),
                    ManifestInput::Text {
                        size_bytes: u64::try_from(request.len()).unwrap(),
                        sha256: lowercase_hex_bytes(digest(&SHA256, &request).as_ref()),
                    },
                ),
                (
                    "evidence".to_owned(),
                    ManifestInput::Attachments {
                        items: vec![AttachmentMember {
                            index: 0,
                            display_name: Some(display_name.to_owned()),
                            media_type: media_type.to_owned(),
                            size_bytes: u64::try_from(attachment.len()).unwrap(),
                            sha256: lowercase_hex_bytes(digest(&SHA256, &attachment).as_ref()),
                        }],
                    },
                ),
            ]),
        };
        let attachment_debug = format!("{attachment:?}");
        let (broker, projection) = broker_for(manifest, vec![attachment, request.clone()]);
        let private = tempfile::tempdir().unwrap();
        let inputs = materialize_projection(&broker, &projection, private.path()).unwrap();
        let debug = format!("{inputs:?}");

        for private_value in [
            std::str::from_utf8(&request).unwrap(),
            display_name,
            media_type,
            attachment_debug.as_str(),
        ] {
            assert!(
                !debug.contains(private_value),
                "materialized input Debug exposed a private Run Input value"
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
