use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Write as _};
use std::time::Duration;

use base64::Engine as _;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, IF_NONE_MATCH};
use reqwest::{Method, StatusCode, Url};
use ring::digest::{SHA256, digest};
use serde::de::DeserializeOwned;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use zeroize::Zeroizing;

use super::UnreachableCategory;
use super::generated::{apis, models};
use super::http_client::{HttpCancellation, SignedStorageGetError, SignedStorageRequestError};
use super::problem::JSON_MEDIA_TYPE;
use super::runs::{
    ReceivedResponse, RunApi, RunFailure, RunOperation, classify_failure, require_exact_header,
    require_media_type,
};

const TEXT_MEDIA_TYPE: &str = "text/plain; charset=utf-8";
const INPUT_JSON_MEDIA_TYPE: &str = "application/json";
const STORAGE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const CAPABILITY_BATCH_SIZE: usize = 100;
const MAXIMUM_INPUTS: usize = 256;
const MAXIMUM_TEXT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_ATTACHMENTS: usize = 256;
const MAXIMUM_TOTAL_BYTES: u64 = 256 * 1024 * 1024;

pub type RunInputSet = models::RunInputSet;
pub type RetainedRunInputs = models::RunInputRetainedInventory;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamedInputKind {
    Text,
    Json,
    File,
    Attachments,
}

impl NamedInputKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
            Self::File => "file",
            Self::Attachments => "attachments",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputScalarMetadata {
    pub size_bytes: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputFileMetadata {
    pub media_type: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputAttachmentMetadata {
    pub display_name: Option<String>,
    pub media_type: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamedInputMetadata {
    Text(InputScalarMetadata),
    Json(InputScalarMetadata),
    File(InputFileMetadata),
    Attachments(Vec<InputAttachmentMetadata>),
}

impl NamedInputMetadata {
    pub const fn kind(&self) -> NamedInputKind {
        match self {
            Self::Text(_) => NamedInputKind::Text,
            Self::Json(_) => NamedInputKind::Json,
            Self::File(_) => NamedInputKind::File,
            Self::Attachments(_) => NamedInputKind::Attachments,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunInputManifest {
    pub inputs: BTreeMap<String, NamedInputMetadata>,
}

impl RunInputManifest {
    pub fn validate(&self) -> Result<(), RunFailure> {
        validated_manifest(&self.to_wire())
            .filter(|validated| validated == self)
            .map(|_| ())
            .ok_or(RunFailure::InvalidInput)
    }

    pub fn objects(&self) -> Vec<RunInputObjectMetadata> {
        let mut objects = Vec::new();
        for (name, input) in &self.inputs {
            match input {
                NamedInputMetadata::Text(metadata) => objects.push(RunInputObjectMetadata {
                    member_id: format!("inputs/{name}"),
                    attachment_index: None,
                    display_name: None,
                    media_type: TEXT_MEDIA_TYPE.to_owned(),
                    size_bytes: metadata.size_bytes,
                    sha256: metadata.sha256,
                }),
                NamedInputMetadata::Json(metadata) => objects.push(RunInputObjectMetadata {
                    member_id: format!("inputs/{name}"),
                    attachment_index: None,
                    display_name: None,
                    media_type: INPUT_JSON_MEDIA_TYPE.to_owned(),
                    size_bytes: metadata.size_bytes,
                    sha256: metadata.sha256,
                }),
                NamedInputMetadata::File(metadata) => objects.push(RunInputObjectMetadata {
                    member_id: format!("inputs/{name}"),
                    attachment_index: None,
                    display_name: None,
                    media_type: metadata.media_type.clone(),
                    size_bytes: metadata.size_bytes,
                    sha256: metadata.sha256,
                }),
                NamedInputMetadata::Attachments(items) => {
                    for (index, metadata) in items.iter().enumerate() {
                        objects.push(RunInputObjectMetadata {
                            member_id: format!("inputs/{name}/{index:06}"),
                            attachment_index: i32::try_from(index).ok(),
                            display_name: metadata.display_name.clone(),
                            media_type: metadata.media_type.clone(),
                            size_bytes: metadata.size_bytes,
                            sha256: metadata.sha256,
                        });
                    }
                }
            }
        }
        objects
    }

    pub fn digest(&self) -> [u8; 32] {
        digest_bytes(canonical_manifest(self).as_bytes())
    }

    fn to_wire(&self) -> models::RunInputManifestV1 {
        models::RunInputManifestV1::new(
            1,
            self.inputs
                .iter()
                .map(|(name, input)| (name.clone(), input_to_wire(input)))
                .collect(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunInputObjectMetadata {
    pub member_id: String,
    pub attachment_index: Option<i32>,
    pub display_name: Option<String>,
    pub media_type: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
}

pub struct RunInputUpload<'a> {
    pub member_id: &'a str,
    pub bytes: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunInputUploadOutcome {
    pub accepted_members: usize,
    pub verification_required_members: usize,
}

pub struct RunInputDownloadCapability {
    pub metadata: RunInputObjectMetadata,
    url: Url,
}

pub struct ValidatedCapabilityBatch<C> {
    expires_at: OffsetDateTime,
    members: Vec<C>,
}

impl fmt::Debug for RunInputDownloadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunInputDownloadCapability([redacted])")
    }
}

impl RunApi<'_> {
    pub fn create_input_set(
        &self,
        organization: &str,
        idempotency_key: &str,
        project_id: &str,
        manifest: &RunInputManifest,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunInputSet, RunFailure> {
        manifest.validate()?;
        let request = models::CreateRunInputSetRequest::new(
            project_id.to_owned(),
            1,
            manifest.to_wire().inputs,
        );
        let body = serde_json::to_vec(&request).map_err(|_| RunFailure::protocol(false))?;
        let endpoint = input_set_collection_endpoint(&self.configuration.base_path, organization);
        let response = self.send_api_request(
            StatusCode::CREATED,
            Some(idempotency_key),
            begin_dispatch,
            || {
                self.request(Method::POST, &endpoint)
                    .header("Idempotency-Key", idempotency_key)
                    .header(CONTENT_TYPE, JSON_MEDIA_TYPE)
                    .body(body.clone())
            },
        )?;
        if response.status != StatusCode::CREATED {
            return Err(classify_failure(&response, RunOperation::Input));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.idempotency_keys.iter(), idempotency_key)?;
        let set = decode_input_set(&response)?;
        validate_input_set(
            &set,
            Some(project_id),
            Some(manifest),
            SetExpectation::Created,
        )?;
        let expected_location = format!(
            "/v1/organizations/{}/run-input-sets/{}",
            apis::urlencode(organization),
            apis::urlencode(&set.id)
        );
        require_exact_header(response.locations.iter(), &expected_location)?;
        Ok(set)
    }

    pub fn get_input_set(
        &self,
        organization: &str,
        input_set_id: &str,
    ) -> Result<RunInputSet, RunFailure> {
        let endpoint = input_set_endpoint(
            &self.configuration.base_path,
            organization,
            input_set_id,
            "",
        );
        let response = self.get_json_response(&endpoint, false)?;
        let set = decode_input_set(&response)?;
        if set.id != input_set_id {
            return Err(RunFailure::protocol(false));
        }
        validate_input_set(&set, None, None, SetExpectation::Any)?;
        Ok(set)
    }

    pub fn upload_input_members(
        &self,
        organization: &str,
        set: &RunInputSet,
        uploads: &[RunInputUpload<'_>],
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunInputUploadOutcome, RunFailure> {
        let manifest = validate_input_set(set, None, None, SetExpectation::Open)?;
        let objects = manifest.objects();
        let by_id = objects
            .iter()
            .map(|object| (object.member_id.as_str(), object))
            .collect::<HashMap<_, _>>();
        let mut selected = BTreeMap::new();
        for upload in uploads {
            let expected = by_id
                .get(upload.member_id)
                .ok_or(RunFailure::InvalidInput)?;
            if selected
                .insert(expected.member_id.clone(), (*expected, upload.bytes))
                .is_some()
                || u64::try_from(upload.bytes.len()).ok() != Some(expected.size_bytes)
                || digest_bytes(upload.bytes) != expected.sha256
            {
                return Err(RunFailure::InvalidInput);
            }
        }
        if selected.is_empty() {
            return if objects.is_empty() {
                Ok(RunInputUploadOutcome {
                    accepted_members: 0,
                    verification_required_members: 0,
                })
            } else {
                Err(RunFailure::InvalidInput)
            };
        }

        let selected = selected.into_values().collect::<Vec<_>>();
        let mut outcome = RunInputUploadOutcome {
            accepted_members: 0,
            verification_required_members: 0,
        };
        for batch in capability_batches(&selected) {
            transfer_capability_batch(
                batch,
                |remaining| {
                    let requested = remaining
                        .iter()
                        .map(|(metadata, _)| metadata.member_id.clone())
                        .collect::<Vec<_>>();
                    let issued = self.issue_upload_capabilities(
                        organization,
                        set,
                        &requested,
                        &begin_dispatch,
                    )?;
                    validate_upload_capabilities(
                        issued,
                        set,
                        &remaining
                            .iter()
                            .map(|(metadata, _)| (*metadata).clone())
                            .collect::<Vec<_>>(),
                        self.transport_policy,
                    )
                },
                |(_, bytes), capability| {
                    match self.upload_input_object(capability, bytes, &begin_dispatch)? {
                        StorageUploadOutcome::Accepted => outcome.accepted_members += 1,
                        StorageUploadOutcome::VerificationRequired => {
                            outcome.verification_required_members += 1;
                        }
                    }
                    Ok(())
                },
                scherzo_cloud_support::utc_now,
                || RunFailure::Unreachable(UnreachableCategory::Server),
            )?;
        }
        Ok(outcome)
    }

    pub fn seal_input_set(
        &self,
        organization: &str,
        idempotency_key: &str,
        expected: &RunInputSet,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunInputSet, RunFailure> {
        let manifest = validate_input_set(expected, None, None, SetExpectation::Open)?;
        let endpoint = input_set_endpoint(
            &self.configuration.base_path,
            organization,
            &expected.id,
            "/seal",
        );
        let response = self.send_api_request(
            StatusCode::OK,
            Some(idempotency_key),
            begin_dispatch,
            || {
                self.request(Method::POST, &endpoint)
                    .header("Idempotency-Key", idempotency_key)
            },
        )?;
        if response.status != StatusCode::OK {
            return Err(classify_failure(&response, RunOperation::Input));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.idempotency_keys.iter(), idempotency_key)?;
        let sealed = decode_input_set(&response)?;
        if sealed.id != expected.id {
            return Err(RunFailure::protocol(false));
        }
        validate_input_set(
            &sealed,
            Some(&expected.project_id),
            Some(&manifest),
            SetExpectation::Sealed,
        )?;
        Ok(sealed)
    }

    pub fn delete_input_set(
        &self,
        organization: &str,
        input_set_id: &str,
        idempotency_key: &str,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<(), RunFailure> {
        let endpoint = input_set_endpoint(
            &self.configuration.base_path,
            organization,
            input_set_id,
            "",
        );
        self.delete_inputs_resource(&endpoint, idempotency_key, begin_dispatch)
    }

    pub fn get_retained_inputs(
        &self,
        organization: &str,
        run_id: &str,
    ) -> Result<RetainedRunInputs, RunFailure> {
        let endpoint =
            retained_inputs_endpoint(&self.configuration.base_path, organization, run_id);
        let response = self.get_json_response(&endpoint, true)?;
        let inventory = decode_retained_inventory(&response.body)?;
        validate_retained_inputs(&inventory)?;
        Ok(inventory)
    }

    pub fn issue_input_download_capabilities(
        &self,
        organization: &str,
        run_id: &str,
        inventory: &RetainedRunInputs,
        requested: &[RunInputObjectMetadata],
    ) -> Result<ValidatedCapabilityBatch<RunInputDownloadCapability>, RunFailure> {
        if requested.is_empty() || requested.len() > CAPABILITY_BATCH_SIZE {
            return Err(RunFailure::InvalidInput);
        }
        validate_retained_inputs(inventory)?;
        let body = serde_json::to_vec(&models::RunInputDownloadCapabilityRequest::new(
            requested
                .iter()
                .map(|metadata| metadata.member_id.clone())
                .collect(),
        ))
        .map_err(|_| RunFailure::protocol(false))?;
        let endpoint = format!(
            "{}/download-capabilities",
            retained_inputs_endpoint(&self.configuration.base_path, organization, run_id)
        );
        let response = self.post_private_json_response(&endpoint, &body, || true)?;
        let issued = decode_download_capability_response(&response.body)?;
        validate_download_capabilities(issued, inventory, requested, self.transport_policy)
    }

    pub fn download_input_member(
        &self,
        capability: &RunInputDownloadCapability,
        cancellation: &HttpCancellation,
    ) -> Result<Zeroizing<Vec<u8>>, RunFailure> {
        let downloaded = self
            .storage_transport
            .signed_storage_get(
                &capability.url,
                capability.metadata.size_bytes,
                STORAGE_REQUEST_TIMEOUT,
                cancellation,
            )
            .map_err(|error| match error {
                SignedStorageGetError::InvalidRequest => RunFailure::protocol(false),
                SignedStorageGetError::Unreachable(category) => RunFailure::Unreachable(category),
                SignedStorageGetError::Interrupted => RunFailure::Interrupted,
                SignedStorageGetError::TooLarge | SignedStorageGetError::Rejected => {
                    RunFailure::InputDownloadRejected
                }
                SignedStorageGetError::Server => {
                    RunFailure::Unreachable(UnreachableCategory::Server)
                }
            })?;
        if downloaded.status != StatusCode::OK
            || downloaded
                .content_length
                .is_some_and(|length| length != capability.metadata.size_bytes)
            || u64::try_from(downloaded.bytes.len()).ok() != Some(capability.metadata.size_bytes)
            || digest_bytes(&downloaded.bytes) != capability.metadata.sha256
        {
            return Err(RunFailure::InputDownloadRejected);
        }
        Ok(downloaded.bytes)
    }

    pub fn delete_retained_inputs(
        &self,
        organization: &str,
        run_id: &str,
        idempotency_key: &str,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<(), RunFailure> {
        let endpoint =
            retained_inputs_endpoint(&self.configuration.base_path, organization, run_id);
        self.delete_inputs_resource(&endpoint, idempotency_key, begin_dispatch)
    }

    fn issue_upload_capabilities(
        &self,
        organization: &str,
        set: &RunInputSet,
        members: &[String],
        begin_dispatch: &impl Fn() -> bool,
    ) -> Result<models::RunInputUploadCapabilityResponse, RunFailure> {
        let body = serde_json::to_vec(&models::RunInputUploadCapabilityRequest::new(
            members.to_vec(),
        ))
        .map_err(|_| RunFailure::protocol(false))?;
        let endpoint = input_set_endpoint(
            &self.configuration.base_path,
            organization,
            &set.id,
            "/upload-capabilities",
        );
        let response = self.post_private_json_response(&endpoint, &body, begin_dispatch)?;
        decode_upload_capability_response(&response.body)
    }

    fn upload_input_object(
        &self,
        capability: &UploadCapability,
        bytes: &[u8],
        begin_dispatch: &impl Fn() -> bool,
    ) -> Result<StorageUploadOutcome, RunFailure> {
        let mut headers = HeaderMap::with_capacity(4);
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&capability.content_length)
                .map_err(|_| RunFailure::protocol(false))?,
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(&capability.content_type)
                .map_err(|_| RunFailure::protocol(false))?,
        );
        headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        headers.insert(
            "x-amz-checksum-sha256",
            HeaderValue::from_str(&capability.checksum_sha256)
                .map_err(|_| RunFailure::protocol(false))?,
        );
        if !begin_dispatch() {
            return Err(RunFailure::Interrupted);
        }
        let status =
            self.storage_transport
                .signed_storage_put(&capability.url, headers, bytes, STORAGE_REQUEST_TIMEOUT)
                .map_err(|error| match error {
                    SignedStorageRequestError::Build
                    | SignedStorageRequestError::InvalidRequest => RunFailure::protocol(false),
                    SignedStorageRequestError::Unreachable(category) => {
                        RunFailure::Unreachable(category)
                    }
                })?;
        if status == StatusCode::PRECONDITION_FAILED {
            return Ok(StorageUploadOutcome::VerificationRequired);
        }
        if status.is_server_error() {
            return Err(RunFailure::Unreachable(UnreachableCategory::Server));
        }
        if matches!(
            status,
            StatusCode::OK | StatusCode::CREATED | StatusCode::NO_CONTENT
        ) {
            Ok(StorageUploadOutcome::Accepted)
        } else {
            Err(RunFailure::InputUploadRejected)
        }
    }

    fn get_json_response(
        &self,
        endpoint: &str,
        private_no_store: bool,
    ) -> Result<ReceivedResponse, RunFailure> {
        let response = self.send_api_request(
            StatusCode::OK,
            None,
            || true,
            || self.request(Method::GET, endpoint),
        )?;
        if response.status != StatusCode::OK {
            return Err(classify_failure(&response, RunOperation::Input));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        if private_no_store {
            require_private_no_store(&response)?;
        }
        Ok(response)
    }

    fn post_private_json_response(
        &self,
        endpoint: &str,
        body: &[u8],
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<ReceivedResponse, RunFailure> {
        let response = self.send_api_request(StatusCode::OK, None, begin_dispatch, || {
            self.request(Method::POST, endpoint)
                .header(CONTENT_TYPE, JSON_MEDIA_TYPE)
                .body(body.to_vec())
        })?;
        if response.status != StatusCode::OK {
            return Err(classify_failure(&response, RunOperation::Input));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_private_no_store(&response)?;
        Ok(response)
    }

    fn delete_inputs_resource(
        &self,
        endpoint: &str,
        idempotency_key: &str,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<(), RunFailure> {
        let response = self.send_api_request(
            StatusCode::NO_CONTENT,
            Some(idempotency_key),
            begin_dispatch,
            || {
                self.request(Method::DELETE, endpoint)
                    .header("Idempotency-Key", idempotency_key)
            },
        )?;
        if response.status != StatusCode::NO_CONTENT {
            return Err(classify_failure(&response, RunOperation::Input));
        }
        require_exact_header(response.idempotency_keys.iter(), idempotency_key)?;
        if response.content_type.is_some() || !response.body.is_empty() {
            return Err(RunFailure::protocol(false));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum SetExpectation {
    Any,
    Created,
    Open,
    Sealed,
}

#[derive(Clone, Copy)]
enum StorageUploadOutcome {
    Accepted,
    VerificationRequired,
}

struct UploadCapability {
    url: Url,
    content_length: String,
    content_type: String,
    checksum_sha256: String,
}

impl fmt::Debug for UploadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UploadCapability([redacted])")
    }
}

pub fn capability_batches<T>(members: &[T]) -> impl Iterator<Item = &[T]> {
    members.chunks(CAPABILITY_BATCH_SIZE)
}

pub fn transfer_capability_batch<T, C, E>(
    batch: &[T],
    mut issue: impl FnMut(&[T]) -> Result<ValidatedCapabilityBatch<C>, E>,
    mut transfer: impl FnMut(&T, &C) -> Result<(), E>,
    mut utc_now: impl FnMut() -> OffsetDateTime,
    mut unavailable: impl FnMut() -> E,
) -> Result<(), E> {
    let mut first_untransferred = 0;
    while first_untransferred < batch.len() {
        let remaining = &batch[first_untransferred..];
        let capabilities = issue(remaining)?;
        if capabilities.members.len() != remaining.len() {
            return Err(unavailable());
        }
        let response_start = first_untransferred;
        for (member, capability) in remaining.iter().zip(&capabilities.members) {
            if utc_now() >= capabilities.expires_at {
                if first_untransferred == response_start {
                    return Err(unavailable());
                }
                break;
            }
            transfer(member, capability)?;
            first_untransferred += 1;
        }
    }
    Ok(())
}

fn input_set_collection_endpoint(base: &str, organization: &str) -> String {
    format!(
        "{}/v1/organizations/{}/run-input-sets",
        base.trim_end_matches('/'),
        apis::urlencode(organization)
    )
}

fn input_set_endpoint(base: &str, organization: &str, input_set_id: &str, suffix: &str) -> String {
    format!(
        "{}/{input_set_id}{suffix}",
        input_set_collection_endpoint(base, organization),
        input_set_id = apis::urlencode(input_set_id)
    )
}

fn retained_inputs_endpoint(base: &str, organization: &str, run_id: &str) -> String {
    format!(
        "{}/v1/organizations/{}/runs/{}/inputs",
        base.trim_end_matches('/'),
        apis::urlencode(organization),
        apis::urlencode(run_id)
    )
}

fn decode_input_set(response: &ReceivedResponse) -> Result<RunInputSet, RunFailure> {
    decode_input_set_body(&response.body)
}

fn decode_input_set_body(body: &[u8]) -> Result<RunInputSet, RunFailure> {
    decode_closed_model(body, closed_input_set_shape)
}

fn decode_retained_inventory(body: &[u8]) -> Result<RetainedRunInputs, RunFailure> {
    decode_closed_model(body, closed_retained_inventory_shape)
}

fn decode_upload_capability_response(
    body: &[u8],
) -> Result<models::RunInputUploadCapabilityResponse, RunFailure> {
    decode_closed_model(body, closed_upload_capability_shape)
}

fn decode_download_capability_response(
    body: &[u8],
) -> Result<models::RunInputDownloadCapabilityResponse, RunFailure> {
    decode_closed_model(body, closed_download_capability_shape)
}

fn decode_closed_model<T: DeserializeOwned>(
    body: &[u8],
    shape_is_valid: impl FnOnce(&serde_json::Value) -> bool,
) -> Result<T, RunFailure> {
    let value = scherzo_cloud_support::strict_json_from_slice(body)
        .map_err(|_| RunFailure::protocol(false))?;
    if !shape_is_valid(&value) {
        return Err(RunFailure::protocol(false));
    }
    serde_json::from_value(value).map_err(|_| RunFailure::protocol(false))
}

fn closed_object<'a>(
    value: &'a serde_json::Value,
    required: &[&str],
    optional: &[&str],
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    let object = value.as_object()?;
    if required.iter().all(|field| object.contains_key(*field))
        && object
            .keys()
            .all(|field| required.contains(&field.as_str()) || optional.contains(&field.as_str()))
    {
        Some(object)
    } else {
        None
    }
}

fn closed_digest_shape(value: &serde_json::Value) -> bool {
    closed_object(value, &["algorithm", "value"], &[]).is_some()
}

fn closed_manifest_shape(value: &serde_json::Value) -> bool {
    let Some(object) = closed_object(value, &["schemaVersion", "inputs"], &[]) else {
        return false;
    };
    object
        .get("inputs")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|inputs| inputs.values().all(closed_manifest_entry_shape))
}

fn closed_manifest_entry_shape(value: &serde_json::Value) -> bool {
    let Some(kind) = value
        .as_object()
        .and_then(|object| object.get("kind"))
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    match kind {
        "text" | "json" => closed_object(value, &["kind", "sizeBytes", "sha256"], &[]).is_some(),
        "file" => {
            closed_object(value, &["kind", "mediaType", "sizeBytes", "sha256"], &[]).is_some()
        }
        "attachments" => {
            let Some(object) = closed_object(value, &["kind", "items"], &[]) else {
                return false;
            };
            object
                .get("items")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|items| items.iter().all(closed_attachment_member_shape))
        }
        _ => false,
    }
}

fn closed_attachment_member_shape(value: &serde_json::Value) -> bool {
    closed_object(
        value,
        &["index", "displayName", "mediaType", "sizeBytes", "sha256"],
        &[],
    )
    .is_some()
}

fn closed_set_member_shape(value: &serde_json::Value) -> bool {
    closed_object(value, &["memberId", "uploadConfirmed"], &[]).is_some()
}

fn closed_input_set_shape(value: &serde_json::Value) -> bool {
    let Some(object) = closed_object(
        value,
        &[
            "id",
            "organizationId",
            "projectId",
            "boundsProfile",
            "manifest",
            "manifestDigest",
            "inputCount",
            "attachmentCount",
            "aggregateSizeBytes",
            "state",
            "createdAt",
            "openDeadlineAt",
            "members",
            "replayed",
        ],
        &["sealedAt", "sealedDeadlineAt"],
    ) else {
        return false;
    };
    ["sealedAt", "sealedDeadlineAt"]
        .into_iter()
        .all(|field| object.get(field).is_none_or(serde_json::Value::is_string))
        && object.get("manifest").is_some_and(closed_manifest_shape)
        && object
            .get("manifestDigest")
            .is_some_and(closed_digest_shape)
        && object
            .get("members")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|members| members.iter().all(closed_set_member_shape))
}

fn closed_retained_inventory_shape(value: &serde_json::Value) -> bool {
    let Some(object) = closed_object(
        value,
        &[
            "inputSetId",
            "manifest",
            "manifestDigest",
            "sealedAt",
            "contentExpiresAt",
            "inputCount",
            "attachmentCount",
            "aggregateSizeBytes",
            "availability",
        ],
        &[],
    ) else {
        return false;
    };
    object.get("manifest").is_some_and(closed_manifest_shape)
        && object
            .get("manifestDigest")
            .is_some_and(closed_digest_shape)
}

fn closed_upload_capability_shape(value: &serde_json::Value) -> bool {
    closed_capability_shape(value, closed_upload_capability_member_shape)
}

fn closed_capability_shape(
    value: &serde_json::Value,
    member_shape_is_valid: impl Fn(&serde_json::Value) -> bool,
) -> bool {
    let Some(object) = closed_object(
        value,
        &["inputSetId", "capabilityExpiresAt", "members"],
        &[],
    ) else {
        return false;
    };
    object
        .get("members")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|members| members.iter().all(member_shape_is_valid))
}

fn closed_upload_capability_member_shape(value: &serde_json::Value) -> bool {
    let Some(object) = closed_object(value, &["memberId", "url", "requiredHeaders"], &[]) else {
        return false;
    };
    object.get("requiredHeaders").is_some_and(|headers| {
        closed_object(
            headers,
            &[
                "contentLength",
                "contentType",
                "ifNoneMatch",
                "xAmzChecksumSha256",
            ],
            &[],
        )
        .is_some()
    })
}

fn closed_download_capability_shape(value: &serde_json::Value) -> bool {
    closed_capability_shape(value, closed_download_capability_member_shape)
}

fn closed_download_capability_member_shape(value: &serde_json::Value) -> bool {
    closed_object(
        value,
        &[
            "memberId",
            "attachmentIndex",
            "displayName",
            "mediaType",
            "sizeBytes",
            "sha256",
            "url",
        ],
        &[],
    )
    .is_some()
}

fn validate_input_set(
    set: &RunInputSet,
    expected_project: Option<&str>,
    expected_manifest: Option<&RunInputManifest>,
    expectation: SetExpectation,
) -> Result<RunInputManifest, RunFailure> {
    let manifest = validated_manifest(&set.manifest).ok_or_else(|| RunFailure::protocol(false))?;
    let metrics = manifest_metrics(&manifest).ok_or_else(|| RunFailure::protocol(false))?;
    let objects = &metrics.objects;
    let created_at = parse_timestamp(&set.created_at);
    let open_deadline_at = parse_timestamp(&set.open_deadline_at);
    let immutable_valid = scherzo_cloud_support::valid_typed_id(&set.id, "ris_")
        && scherzo_cloud_support::valid_typed_id(&set.organization_id, "org_")
        && scherzo_cloud_support::valid_typed_id(&set.project_id, "prj_")
        && expected_project.is_none_or(|project| project == set.project_id)
        && expected_manifest.is_none_or(|expected| expected == &manifest)
        && set.bounds_profile == 1
        && set.manifest_digest.algorithm
            == models::run_input_digest::Algorithm::RunInputDigestAlgorithmSha256
        && set.manifest_digest.value == lowercase_hex(manifest.digest())
        && usize::try_from(set.input_count).ok() == Some(manifest.inputs.len())
        && usize::try_from(set.attachment_count).ok() == Some(metrics.attachment_count)
        && u64::try_from(set.aggregate_size_bytes).ok() == Some(metrics.aggregate_size_bytes)
        && set.members.len() == objects.len()
        && set
            .members
            .iter()
            .zip(objects)
            .all(|(status, object)| status.member_id == object.member_id)
        && created_at
            .zip(open_deadline_at)
            .is_some_and(|(created, deadline)| created < deadline);
    let state_valid = match set.state {
        models::run_input_set::State::Open | models::run_input_set::State::Invalid => {
            set.sealed_at.is_none() && set.sealed_deadline_at.is_none()
        }
        models::run_input_set::State::Sealed => {
            let sealed_at = set.sealed_at.as_deref().and_then(parse_timestamp);
            let sealed_deadline_at = set.sealed_deadline_at.as_deref().and_then(parse_timestamp);
            sealed_at
                .zip(sealed_deadline_at)
                .is_some_and(|(sealed, deadline)| sealed < deadline)
                && set.members.iter().all(|member| member.upload_confirmed)
        }
    };
    let expectation_valid = match expectation {
        SetExpectation::Any => true,
        SetExpectation::Created => {
            set.state == models::run_input_set::State::Open
                && set.members.iter().all(|member| !member.upload_confirmed)
        }
        SetExpectation::Open => set.state == models::run_input_set::State::Open,
        SetExpectation::Sealed => set.state == models::run_input_set::State::Sealed,
    };
    if immutable_valid && state_valid && expectation_valid {
        Ok(manifest)
    } else {
        Err(RunFailure::protocol(false))
    }
}

pub fn retained_manifest(inventory: &RetainedRunInputs) -> Result<RunInputManifest, RunFailure> {
    validate_retained_inputs(inventory)
}

pub fn input_set_is_open(set: &RunInputSet) -> bool {
    set.state == models::run_input_set::State::Open
}

pub fn input_set_state_name(set: &RunInputSet) -> &'static str {
    match set.state {
        models::run_input_set::State::Open => "open",
        models::run_input_set::State::Sealed => "sealed",
        models::run_input_set::State::Invalid => "invalid",
    }
}

fn validate_retained_inputs(inventory: &RetainedRunInputs) -> Result<RunInputManifest, RunFailure> {
    let manifest =
        validated_manifest(&inventory.manifest).ok_or_else(|| RunFailure::protocol(false))?;
    let metrics = manifest_metrics(&manifest).ok_or_else(|| RunFailure::protocol(false))?;
    let sealed_at = parse_timestamp(&inventory.sealed_at);
    let content_expires_at = inventory.content_expires_at.as_deref().map(parse_timestamp);
    let valid = scherzo_cloud_support::valid_typed_id(&inventory.input_set_id, "ris_")
        && inventory.manifest_digest.algorithm
            == models::run_input_digest::Algorithm::RunInputDigestAlgorithmSha256
        && inventory.manifest_digest.value == lowercase_hex(manifest.digest())
        && usize::try_from(inventory.input_count).ok() == Some(manifest.inputs.len())
        && usize::try_from(inventory.attachment_count).ok() == Some(metrics.attachment_count)
        && u64::try_from(inventory.aggregate_size_bytes).ok() == Some(metrics.aggregate_size_bytes)
        && inventory.availability == models::run_input_retained_inventory::Availability::Available
        && sealed_at.is_some()
        && content_expires_at.is_none_or(|expires| {
            expires.is_some_and(|expires| sealed_at.is_some_and(|sealed| expires > sealed))
        });
    if valid {
        Ok(manifest)
    } else {
        Err(RunFailure::protocol(false))
    }
}

struct ManifestMetrics {
    objects: Vec<RunInputObjectMetadata>,
    attachment_count: usize,
    aggregate_size_bytes: u64,
}

fn manifest_metrics(manifest: &RunInputManifest) -> Option<ManifestMetrics> {
    let objects = manifest.objects();
    let attachment_count = manifest
        .inputs
        .values()
        .map(|input| match input {
            NamedInputMetadata::Attachments(items) => items.len(),
            NamedInputMetadata::Text(_)
            | NamedInputMetadata::Json(_)
            | NamedInputMetadata::File(_) => 0,
        })
        .sum::<usize>();
    let aggregate_size_bytes = objects
        .iter()
        .try_fold(0_u64, |total, member| total.checked_add(member.size_bytes))?;
    Some(ManifestMetrics {
        objects,
        attachment_count,
        aggregate_size_bytes,
    })
}

fn validated_manifest(wire: &models::RunInputManifestV1) -> Option<RunInputManifest> {
    if wire.schema_version != 1 || !(1..=MAXIMUM_INPUTS).contains(&wire.inputs.len()) {
        return None;
    }
    let mut aggregate = 0_u64;
    let mut attachment_count = 0_usize;
    let mut inputs = BTreeMap::new();
    for (name, input) in &wire.inputs {
        if !scherzo_cloud_support::is_identifier(name) {
            return None;
        }
        let metadata = match input {
            models::RunInputManifestEntry::Text(text) => {
                let metadata = scalar_metadata(text.size_bytes, &text.sha256, MAXIMUM_TEXT_BYTES)?;
                aggregate = aggregate.checked_add(metadata.size_bytes)?;
                NamedInputMetadata::Text(metadata)
            }
            models::RunInputManifestEntry::Json(json) => {
                let metadata = scalar_metadata(json.size_bytes, &json.sha256, MAXIMUM_TEXT_BYTES)?;
                aggregate = aggregate.checked_add(metadata.size_bytes)?;
                NamedInputMetadata::Json(metadata)
            }
            models::RunInputManifestEntry::File(file) => {
                let metadata = file_metadata(
                    file.size_bytes,
                    &file.sha256,
                    &file.media_type,
                    MAXIMUM_OBJECT_BYTES,
                )?;
                aggregate = aggregate.checked_add(metadata.size_bytes)?;
                NamedInputMetadata::File(metadata)
            }
            models::RunInputManifestEntry::Attachments(collection) => {
                let mut items = Vec::with_capacity(collection.items.len());
                for (index, item) in collection.items.iter().enumerate() {
                    if usize::try_from(item.index).ok() != Some(index)
                        || !scherzo_cloud_support::is_valid_input_display_name(
                            item.display_name.as_deref(),
                        )
                    {
                        return None;
                    }
                    let metadata = file_metadata(
                        item.size_bytes,
                        &item.sha256,
                        &item.media_type,
                        MAXIMUM_OBJECT_BYTES,
                    )?;
                    aggregate = aggregate.checked_add(metadata.size_bytes)?;
                    items.push(InputAttachmentMetadata {
                        display_name: item.display_name.clone(),
                        media_type: metadata.media_type,
                        size_bytes: metadata.size_bytes,
                        sha256: metadata.sha256,
                    });
                }
                attachment_count = attachment_count.checked_add(items.len())?;
                NamedInputMetadata::Attachments(items)
            }
        };
        inputs.insert(name.clone(), metadata);
    }
    if attachment_count > MAXIMUM_ATTACHMENTS || aggregate > MAXIMUM_TOTAL_BYTES {
        return None;
    }
    Some(RunInputManifest { inputs })
}

fn scalar_metadata(size: i64, sha256: &str, maximum: u64) -> Option<InputScalarMetadata> {
    let size_bytes = u64::try_from(size).ok().filter(|size| *size <= maximum)?;
    Some(InputScalarMetadata {
        size_bytes,
        sha256: decode_sha256(sha256)?,
    })
}

fn file_metadata(
    size: i64,
    sha256: &str,
    media_type: &str,
    maximum: u64,
) -> Option<InputFileMetadata> {
    let scalar = scalar_metadata(size, sha256, maximum)?;
    if !scherzo_cloud_support::is_valid_media_type(media_type) {
        return None;
    }
    Some(InputFileMetadata {
        media_type: media_type.to_owned(),
        size_bytes: scalar.size_bytes,
        sha256: scalar.sha256,
    })
}

fn input_to_wire(input: &NamedInputMetadata) -> models::RunInputManifestEntry {
    match input {
        NamedInputMetadata::Text(metadata) => {
            models::RunInputManifestEntry::Text(Box::new(models::RunInputTextEntry::new(
                models::run_input_text_entry::Kind::Text,
                i64::try_from(metadata.size_bytes).unwrap_or(i64::MAX),
                lowercase_hex(metadata.sha256),
            )))
        }
        NamedInputMetadata::Json(metadata) => {
            models::RunInputManifestEntry::Json(Box::new(models::RunInputJsonEntry::new(
                models::run_input_json_entry::Kind::Json,
                i64::try_from(metadata.size_bytes).unwrap_or(i64::MAX),
                lowercase_hex(metadata.sha256),
            )))
        }
        NamedInputMetadata::File(metadata) => {
            models::RunInputManifestEntry::File(Box::new(models::RunInputFileEntry::new(
                models::run_input_file_entry::Kind::File,
                metadata.media_type.clone(),
                i64::try_from(metadata.size_bytes).unwrap_or(i64::MAX),
                lowercase_hex(metadata.sha256),
            )))
        }
        NamedInputMetadata::Attachments(items) => models::RunInputManifestEntry::Attachments(
            Box::new(models::RunInputAttachmentsEntry::new(
                models::run_input_attachments_entry::Kind::Attachments,
                items
                    .iter()
                    .enumerate()
                    .map(|(index, metadata)| {
                        models::RunInputAttachmentMember::new(
                            i32::try_from(index).unwrap_or(i32::MAX),
                            metadata.display_name.clone(),
                            metadata.media_type.clone(),
                            i64::try_from(metadata.size_bytes).unwrap_or(i64::MAX),
                            lowercase_hex(metadata.sha256),
                        )
                    })
                    .collect(),
            )),
        ),
    }
}

fn canonical_manifest(manifest: &RunInputManifest) -> String {
    let mut canonical = String::from("{\"inputs\":{");
    for (index, (name, input)) in manifest.inputs.iter().enumerate() {
        if index > 0 {
            canonical.push(',');
        }
        canonical.push_str(&json_string(name));
        canonical.push(':');
        match input {
            NamedInputMetadata::Text(metadata) | NamedInputMetadata::Json(metadata) => {
                let _ = write!(
                    canonical,
                    "{{\"kind\":{},\"sha256\":\"{}\",\"sizeBytes\":{}}}",
                    json_string(input.kind().as_str()),
                    lowercase_hex(metadata.sha256),
                    metadata.size_bytes
                );
            }
            NamedInputMetadata::File(metadata) => {
                let _ = write!(
                    canonical,
                    "{{\"kind\":\"file\",\"mediaType\":{},\"sha256\":\"{}\",\"sizeBytes\":{}}}",
                    json_string(&metadata.media_type),
                    lowercase_hex(metadata.sha256),
                    metadata.size_bytes
                );
            }
            NamedInputMetadata::Attachments(items) => {
                canonical.push_str("{\"items\":[");
                for (item_index, metadata) in items.iter().enumerate() {
                    if item_index > 0 {
                        canonical.push(',');
                    }
                    let display_name = metadata
                        .display_name
                        .as_deref()
                        .map(json_string)
                        .unwrap_or_else(|| "null".to_owned());
                    let _ = write!(
                        canonical,
                        "{{\"displayName\":{display_name},\"index\":{item_index},\"mediaType\":{},\"sha256\":\"{}\",\"sizeBytes\":{}}}",
                        json_string(&metadata.media_type),
                        lowercase_hex(metadata.sha256),
                        metadata.size_bytes
                    );
                }
                canonical.push_str("],\"kind\":\"attachments\"}");
            }
        }
    }
    canonical.push_str("},\"schemaVersion\":1}");
    canonical
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}

fn validate_upload_capabilities(
    issued: models::RunInputUploadCapabilityResponse,
    set: &RunInputSet,
    requested: &[RunInputObjectMetadata],
    policy: super::HttpTransportPolicy,
) -> Result<ValidatedCapabilityBatch<UploadCapability>, RunFailure> {
    let open_deadline =
        parse_timestamp(&set.open_deadline_at).ok_or_else(|| RunFailure::protocol(false))?;
    validate_capability_envelope(
        &issued.input_set_id,
        &set.id,
        issued.members.len(),
        requested.len(),
    )?;
    let expires_at = parse_timestamp(&issued.capability_expires_at)
        .filter(|expires| *expires <= open_deadline)
        .ok_or_else(|| RunFailure::protocol(false))?;
    let members = capability_members(issued.members, requested)
        .map(|(member, expected)| {
            let headers = member.required_headers;
            let expected_checksum =
                base64::engine::general_purpose::STANDARD.encode(expected.sha256);
            if member.member_id != expected.member_id
                || headers.content_length != expected.size_bytes.to_string()
                || headers.content_type != expected.media_type
                || headers.if_none_match
                    != models::run_input_upload_required_headers::IfNoneMatch::Star
                || headers.x_amz_checksum_sha256 != expected_checksum
            {
                return Err(RunFailure::protocol(false));
            }
            let url = validate_capability_url(&member.url, policy)?;
            Ok(UploadCapability {
                url,
                content_length: headers.content_length,
                content_type: headers.content_type,
                checksum_sha256: headers.x_amz_checksum_sha256,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ValidatedCapabilityBatch {
        expires_at,
        members,
    })
}

fn validate_download_capabilities(
    issued: models::RunInputDownloadCapabilityResponse,
    inventory: &RetainedRunInputs,
    requested: &[RunInputObjectMetadata],
    policy: super::HttpTransportPolicy,
) -> Result<ValidatedCapabilityBatch<RunInputDownloadCapability>, RunFailure> {
    let expires = parse_timestamp(&issued.capability_expires_at)
        .ok_or_else(|| RunFailure::protocol(false))?;
    validate_capability_envelope(
        &issued.input_set_id,
        &inventory.input_set_id,
        issued.members.len(),
        requested.len(),
    )?;
    if inventory
        .content_expires_at
        .as_deref()
        .and_then(parse_timestamp)
        .is_some_and(|retention| expires > retention)
    {
        return Err(RunFailure::protocol(false));
    }
    let members = capability_members(issued.members, requested)
        .map(|(member, expected)| {
            let size_bytes =
                u64::try_from(member.size_bytes).map_err(|_| RunFailure::protocol(false))?;
            if member.member_id != expected.member_id
                || member.attachment_index != expected.attachment_index
                || member.display_name != expected.display_name
                || member.media_type != expected.media_type
                || size_bytes != expected.size_bytes
                || decode_sha256(&member.sha256) != Some(expected.sha256)
            {
                return Err(RunFailure::protocol(false));
            }
            Ok(RunInputDownloadCapability {
                metadata: expected.clone(),
                url: validate_capability_url(&member.url, policy)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ValidatedCapabilityBatch {
        expires_at: expires,
        members,
    })
}

fn capability_members<M>(
    members: Vec<M>,
    requested: &[RunInputObjectMetadata],
) -> impl Iterator<Item = (M, &RunInputObjectMetadata)> {
    members.into_iter().zip(requested)
}

fn validate_capability_envelope(
    input_set_id: &str,
    expected_input_set_id: &str,
    member_count: usize,
    expected_member_count: usize,
) -> Result<(), RunFailure> {
    if input_set_id == expected_input_set_id && member_count == expected_member_count {
        Ok(())
    } else {
        Err(RunFailure::protocol(false))
    }
}

fn validate_capability_url(
    raw: &str,
    policy: super::HttpTransportPolicy,
) -> Result<Url, RunFailure> {
    let url = Url::parse(raw).map_err(|_| RunFailure::protocol(false))?;
    if !policy.permits(&url)
        || url.host_str().is_none()
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_none()
    {
        return Err(RunFailure::protocol(false));
    }
    Ok(url)
}

fn require_private_no_store(response: &ReceivedResponse) -> Result<(), RunFailure> {
    let mut values = response.cache_controls.iter();
    let valid = values
        .next()
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("private, no-store"))
        && values.next().is_none();
    if valid {
        Ok(())
    } else {
        Err(RunFailure::protocol(false))
    }
}

pub fn digest_bytes(bytes: &[u8]) -> [u8; 32] {
    let observed = digest(&SHA256, bytes);
    let mut sha256 = [0_u8; 32];
    sha256.copy_from_slice(observed.as_ref());
    sha256
}

pub(crate) fn lowercase_hex(bytes: [u8; 32]) -> String {
    scherzo_cloud_support::lowercase_hex(&bytes)
}

fn decode_sha256(value: &str) -> Option<[u8; 32]> {
    if !scherzo_cloud_support::is_lowercase_hex(value, 64) {
        return None;
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = std::str::from_utf8(pair)
            .ok()
            .and_then(|pair| u8::from_str_radix(pair, 16).ok())?;
    }
    Some(decoded)
}

fn parse_timestamp(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    reason = "run-input API unit tests use Rust test assertions"
)]
mod tests {
    use super::*;

    #[test]
    fn mixed_manifest_digest_preserves_empty_and_ordered_values() {
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "emptyText".to_owned(),
            NamedInputMetadata::Text(InputScalarMetadata {
                size_bytes: 0,
                sha256: digest_bytes(&[]),
            }),
        );
        inputs.insert(
            "evidence".to_owned(),
            NamedInputMetadata::Attachments(vec![
                InputAttachmentMetadata {
                    display_name: None,
                    media_type: "application/octet-stream".to_owned(),
                    size_bytes: 1,
                    sha256: digest_bytes(b"b"),
                },
                InputAttachmentMetadata {
                    display_name: None,
                    media_type: "text/plain".to_owned(),
                    size_bytes: 1,
                    sha256: digest_bytes(b"a"),
                },
            ]),
        );
        inputs.insert(
            "nothing".to_owned(),
            NamedInputMetadata::Attachments(Vec::new()),
        );
        let manifest = RunInputManifest { inputs };

        assert!(manifest.validate().is_ok());
        assert_eq!(manifest.objects().len(), 3);
        assert_eq!(
            canonical_manifest(&manifest),
            format!(
                "{{\"inputs\":{{\"emptyText\":{{\"kind\":\"text\",\"sha256\":\"{}\",\"sizeBytes\":0}},\"evidence\":{{\"items\":[{{\"displayName\":null,\"index\":0,\"mediaType\":\"application/octet-stream\",\"sha256\":\"{}\",\"sizeBytes\":1}},{{\"displayName\":null,\"index\":1,\"mediaType\":\"text/plain\",\"sha256\":\"{}\",\"sizeBytes\":1}}],\"kind\":\"attachments\"}},\"nothing\":{{\"items\":[],\"kind\":\"attachments\"}}}},\"schemaVersion\":1}}",
                lowercase_hex(digest_bytes(&[])),
                lowercase_hex(digest_bytes(b"b")),
                lowercase_hex(digest_bytes(b"a")),
            )
        );
    }

    #[test]
    fn capabilities_are_batched_at_the_public_api_limit() {
        let members = [(); 201];
        assert_eq!(
            capability_batches(&members)
                .map(<[_]>::len)
                .collect::<Vec<_>>(),
            [100, 100, 1]
        );
    }

    #[test]
    fn expiry_after_one_transfer_reissues_only_remaining_members() {
        let members = ["first", "second"];
        let mut issued_for = Vec::new();
        let mut transferred = Vec::new();
        let mut expirations = [
            test_timestamp("2026-09-15T12:05:00Z"),
            test_timestamp("2026-09-15T12:10:00Z"),
        ]
        .into_iter();
        let mut observations = [
            test_timestamp("2026-09-15T12:04:00Z"),
            test_timestamp("2026-09-15T12:06:00Z"),
            test_timestamp("2026-09-15T12:06:00Z"),
        ]
        .into_iter();

        let result = transfer_capability_batch(
            &members,
            |remaining| {
                issued_for.push(remaining.to_vec());
                Ok(ValidatedCapabilityBatch {
                    expires_at: expirations.next().ok_or("missing expiration")?,
                    members: remaining.to_vec(),
                })
            },
            |member, capability| {
                assert_eq!(member, capability);
                transferred.push(*member);
                Ok(())
            },
            || observations.next().unwrap_or(OffsetDateTime::UNIX_EPOCH),
            || "fresh capabilities expired before transfer",
        );

        assert_eq!(result, Ok(()));
        assert_eq!(issued_for, vec![vec!["first", "second"], vec!["second"]]);
        assert_eq!(transferred, vec!["first", "second"]);
    }

    #[test]
    fn capabilities_expired_before_progress_are_not_reissued_indefinitely() {
        let members = ["only"];
        let expiry = test_timestamp("2026-09-15T12:05:00Z");
        let mut issue_count = 0;
        let mut transfer_count = 0;

        let result = transfer_capability_batch(
            &members,
            |remaining| {
                issue_count += 1;
                Ok(ValidatedCapabilityBatch {
                    expires_at: expiry,
                    members: remaining.to_vec(),
                })
            },
            |_, _| {
                transfer_count += 1;
                Ok(())
            },
            || expiry,
            || "unavailable",
        );

        assert_eq!(result, Err("unavailable"));
        assert_eq!(issue_count, 1);
        assert_eq!(transfer_count, 0);
    }

    #[test]
    fn closed_run_input_decoders_reject_unknown_fields_at_every_nested_shape() {
        let mut set_envelope = input_set_fixture();
        set_envelope["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(set_envelope, decode_input_set_body);

        let mut manifest = input_set_fixture();
        manifest["manifest"]["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(manifest, decode_input_set_body);

        let mut manifest_entry = input_set_fixture();
        manifest_entry["manifest"]["inputs"]["request"]["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(manifest_entry, decode_input_set_body);

        let mut attachment_member = input_set_fixture();
        attachment_member["manifest"]["inputs"]["request"] = serde_json::json!({
            "kind": "attachments",
            "items": [{
                "index": 0,
                "displayName": null,
                "mediaType": "application/octet-stream",
                "sizeBytes": 0,
                "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "unexpected": true
            }]
        });
        assert_closed_rejected(attachment_member, decode_input_set_body);

        let mut digest = input_set_fixture();
        digest["manifestDigest"]["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(digest, decode_input_set_body);

        let mut set_member = input_set_fixture();
        set_member["members"][0]["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(set_member, decode_input_set_body);

        let mut retained = retained_inventory_fixture();
        retained["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(retained, decode_retained_inventory);

        let mut upload_envelope = upload_capability_fixture();
        upload_envelope["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(upload_envelope, decode_upload_capability_response);

        let mut upload_member = upload_capability_fixture();
        upload_member["members"][0]["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(upload_member, decode_upload_capability_response);

        let mut upload_headers = upload_capability_fixture();
        upload_headers["members"][0]["requiredHeaders"]["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(upload_headers, decode_upload_capability_response);

        let mut download_envelope = download_capability_fixture();
        download_envelope["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(download_envelope, decode_download_capability_response);

        let mut download_member = download_capability_fixture();
        download_member["members"][0]["unexpected"] = serde_json::json!(true);
        assert_closed_rejected(download_member, decode_download_capability_response);
    }

    #[test]
    fn closed_run_input_decoders_retain_contract_null_optional_and_union_cases() {
        let mut set = input_set_fixture();
        set["manifest"]["inputs"]["request"] = serde_json::json!({
            "kind": "attachments",
            "items": [{
                "index": 0,
                "displayName": null,
                "mediaType": "application/octet-stream",
                "sizeBytes": 0,
                "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
            }]
        });
        set["manifestDigest"]["value"] =
            serde_json::json!(manifest_digest_fixture(&set["manifest"]));
        set["attachmentCount"] = serde_json::json!(1);
        set["members"][0]["memberId"] = serde_json::json!("inputs/request/000000");
        let decoded_set = decode_fixture(set, decode_input_set_body);
        assert!(
            decoded_set.as_ref().is_ok_and(|set| validate_input_set(
                set,
                None,
                None,
                SetExpectation::Any
            )
            .is_ok())
        );

        let decoded_inventory =
            decode_fixture(retained_inventory_fixture(), decode_retained_inventory);
        assert!(
            decoded_inventory
                .as_ref()
                .is_ok_and(|inventory| validate_retained_inputs(inventory).is_ok())
        );
        assert!(
            decode_fixture(
                upload_capability_fixture(),
                decode_upload_capability_response,
            )
            .is_ok()
        );
        assert!(
            decode_fixture(
                download_capability_fixture(),
                decode_download_capability_response,
            )
            .is_ok()
        );
    }

    fn test_timestamp(value: &str) -> OffsetDateTime {
        OffsetDateTime::parse(value, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH)
    }

    fn input_set_fixture() -> serde_json::Value {
        let manifest = manifest_fixture();
        let manifest_digest = manifest_digest_fixture(&manifest);
        serde_json::json!({
            "id": "ris_01k0z6r1w8f4jy2m7q9v3x5abc",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
            "boundsProfile": 1,
            "manifest": manifest,
            "manifestDigest": {
                "algorithm": "sha256",
                "value": manifest_digest
            },
            "inputCount": 1,
            "attachmentCount": 0,
            "aggregateSizeBytes": 0,
            "state": "open",
            "createdAt": "2026-09-15T12:00:00Z",
            "openDeadlineAt": "2026-09-16T12:00:00Z",
            "members": [{"memberId": "inputs/request", "uploadConfirmed": false}],
            "replayed": false
        })
    }

    fn manifest_fixture() -> serde_json::Value {
        serde_json::json!({
            "schemaVersion": 1,
            "inputs": {
                "request": {
                    "kind": "text",
                    "sizeBytes": 0,
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                }
            }
        })
    }

    fn retained_inventory_fixture() -> serde_json::Value {
        let manifest = manifest_fixture();
        let manifest_digest = manifest_digest_fixture(&manifest);
        serde_json::json!({
            "inputSetId": "ris_01k0z6r1w8f4jy2m7q9v3x5abc",
            "manifest": manifest,
            "manifestDigest": {
                "algorithm": "sha256",
                "value": manifest_digest
            },
            "sealedAt": "2026-09-15T12:00:00Z",
            "contentExpiresAt": null,
            "inputCount": 1,
            "attachmentCount": 0,
            "aggregateSizeBytes": 0,
            "availability": "available"
        })
    }

    fn manifest_digest_fixture(manifest: &serde_json::Value) -> String {
        let Ok(wire) = serde_json::from_value::<models::RunInputManifestV1>(manifest.clone())
        else {
            return String::new();
        };
        validated_manifest(&wire)
            .map(|manifest| lowercase_hex(manifest.digest()))
            .unwrap_or_default()
    }

    fn upload_capability_fixture() -> serde_json::Value {
        serde_json::json!({
            "inputSetId": "ris_01k0z6r1w8f4jy2m7q9v3x5abc",
            "capabilityExpiresAt": "2026-09-15T12:05:00Z",
            "members": [{
                "memberId": "inputs/request",
                "url": "https://storage.invalid/object?signature=private",
                "requiredHeaders": {
                    "contentLength": "0",
                    "contentType": "text/plain; charset=utf-8",
                    "ifNoneMatch": "*",
                    "xAmzChecksumSha256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
                }
            }]
        })
    }

    fn download_capability_fixture() -> serde_json::Value {
        serde_json::json!({
            "inputSetId": "ris_01k0z6r1w8f4jy2m7q9v3x5abc",
            "capabilityExpiresAt": "2026-09-15T12:05:00Z",
            "members": [{
                "memberId": "inputs/request",
                "attachmentIndex": null,
                "displayName": null,
                "mediaType": "text/plain; charset=utf-8",
                "sizeBytes": 0,
                "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "url": "https://storage.invalid/object?signature=private"
            }]
        })
    }

    fn decode_fixture<T>(
        value: serde_json::Value,
        decode: impl FnOnce(&[u8]) -> Result<T, RunFailure>,
    ) -> Result<T, RunFailure> {
        decode(&serde_json::to_vec(&value).unwrap_or_default())
    }

    fn assert_closed_rejected<T>(
        value: serde_json::Value,
        decode: impl FnOnce(&[u8]) -> Result<T, RunFailure>,
    ) {
        assert!(decode_fixture(value, decode).is_err());
    }
}
