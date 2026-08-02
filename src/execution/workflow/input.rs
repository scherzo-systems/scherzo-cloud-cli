use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use rustix::fs::{
    AtFlags, Dir, FileType, Mode, OFlags, chmodat, fchmod, fstat, mkdirat, openat, statat, unlinkat,
};
use rustix::io::Errno;
use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq, Serializer};
use serde_json::Value;

use super::admission::{AdmittedExecutionContext, ResolvedAttachment};
use super::artifact::{ArtifactReadFailure, ArtifactStaging, open_directory};
use super::validated::WorkflowValueType;
use super::value::CapturedValue;

const INPUT_NAME_MAX_BYTES: usize = 64;
const MAX_COLLECTION_ITEMS: usize = 1_000_000;
const IDENTITY_ATTEMPTS: usize = 16;
const MANIFEST_SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputStagingFailure {
    ExecutionRootUnavailable,
    StagingParentUnavailable,
    StagingParentExposed,
    IdentityUnavailable,
}

impl fmt::Display for InputStagingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "input staging failure: {self:?}")
    }
}

impl std::error::Error for InputStagingFailure {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputStagingReleaseFailure {
    CleanupUnavailable,
}

impl fmt::Display for InputStagingReleaseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "input staging release failure: {self:?}")
    }
}

impl std::error::Error for InputStagingReleaseFailure {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputPreparationFailureKind {
    InvalidInputName,
    ValueCountLimitExceeded,
    ValueSizeLimitExceeded,
    TotalSizeLimitExceeded,
    CollectionOrdinalLimitExceeded,
    ValueTypeMismatch,
    SourceUnavailable,
    StagingUnavailable,
    LiveLimitExceeded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InputPreparationFailure {
    input_identity: Option<Arc<str>>,
    collection_index: Option<usize>,
    kind: InputPreparationFailureKind,
}

impl InputPreparationFailure {
    pub(crate) fn input_identity(&self) -> Option<&str> {
        self.input_identity.as_deref()
    }

    pub(crate) fn collection_index(&self) -> Option<usize> {
        self.collection_index
    }

    pub(crate) fn kind(&self) -> InputPreparationFailureKind {
        self.kind
    }

    fn for_input(input_identity: &str, kind: InputPreparationFailureKind) -> Self {
        Self {
            input_identity: Some(Arc::from(input_identity)),
            collection_index: None,
            kind,
        }
    }

    fn for_item(
        input_identity: &str,
        collection_index: usize,
        kind: InputPreparationFailureKind,
    ) -> Self {
        Self {
            input_identity: Some(Arc::from(input_identity)),
            collection_index: Some(collection_index),
            kind,
        }
    }

    fn staging(kind: InputPreparationFailureKind) -> Self {
        Self {
            input_identity: None,
            collection_index: None,
            kind,
        }
    }
}

impl fmt::Display for InputPreparationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "input preparation failure for {:?} at {:?}: {:?}",
            self.input_identity, self.collection_index, self.kind
        )
    }
}

impl std::error::Error for InputPreparationFailure {}

pub(crate) enum InputValue<'a> {
    Prompt(&'a str),
    Attachments(&'a [ResolvedAttachment]),
    Captured {
        expected_type: WorkflowValueType,
        value: &'a CapturedValue,
    },
}

#[derive(Clone)]
pub(crate) struct InputStaging {
    inner: Arc<InputStagingInner>,
}

struct InputStagingInner {
    execution_root_device: u64,
    execution_root_inode: u64,
    staging_parent: OwnedFd,
    staging_root: OwnedFd,
    staging_path: PathBuf,
    staging_identity: Arc<str>,
    maximum_parallel_steps: usize,
    maximum_values: usize,
    maximum_value_bytes: u64,
    maximum_total_bytes: u64,
    lifecycle: RwLock<InputStagingLifecycle>,
    reservations: Mutex<ReservationLedger>,
    #[cfg(test)]
    cleanup_blocked: AtomicBool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum InputStagingLifecycle {
    Active,
    CleanupFailed,
    Released,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReservationUsage {
    views: usize,
    values: usize,
    bytes: u64,
}

#[derive(Default)]
struct ReservationLedger {
    active: BTreeMap<Arc<str>, ReservationUsage>,
    usage: ReservationUsage,
}

pub(crate) struct InputView {
    inner: Arc<InputStagingInner>,
    identity: Arc<str>,
    path: PathBuf,
    released: bool,
}

impl InputView {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        if self.inner.remove_view(&self.identity) {
            self.inner.release_reservation(&self.identity);
            self.released = true;
        }
    }
}

impl Drop for InputView {
    fn drop(&mut self) {
        self.release();
    }
}

impl InputStaging {
    pub(crate) fn create(
        execution: &AdmittedExecutionContext,
        staging_parent: &Path,
    ) -> Result<Self, InputStagingFailure> {
        let limits = execution.limits();
        Self::create_for_execution(
            execution.root(),
            staging_parent,
            limits.maximum_parallel_steps().get(),
            limits.maximum_input_values().get(),
            limits.maximum_input_value_bytes().get(),
            limits.maximum_total_input_bytes().get(),
        )
    }

    fn create_for_execution(
        execution_root: &Path,
        staging_parent: &Path,
        maximum_parallel_steps: usize,
        maximum_values: usize,
        maximum_value_bytes: u64,
        maximum_total_bytes: u64,
    ) -> Result<Self, InputStagingFailure> {
        let canonical_execution_root = fs::canonicalize(execution_root)
            .map_err(|_| InputStagingFailure::ExecutionRootUnavailable)?;
        let canonical_staging_parent = fs::canonicalize(staging_parent)
            .map_err(|_| InputStagingFailure::StagingParentUnavailable)?;
        if canonical_staging_parent.starts_with(&canonical_execution_root) {
            return Err(InputStagingFailure::StagingParentExposed);
        }
        let execution_metadata = fs::metadata(&canonical_execution_root)
            .map_err(|_| InputStagingFailure::ExecutionRootUnavailable)?;
        let staging_parent = open_directory(&canonical_staging_parent)
            .map_err(|_| InputStagingFailure::StagingParentUnavailable)?;
        let (staging_identity, staging_root) = create_staging_root(&staging_parent)?;
        let staging_path = canonical_staging_parent.join(staging_identity.as_ref());
        Ok(Self {
            inner: Arc::new(InputStagingInner {
                execution_root_device: execution_metadata.dev(),
                execution_root_inode: execution_metadata.ino(),
                staging_parent,
                staging_root,
                staging_path,
                staging_identity,
                maximum_parallel_steps,
                maximum_values,
                maximum_value_bytes,
                maximum_total_bytes,
                lifecycle: RwLock::new(InputStagingLifecycle::Active),
                reservations: Mutex::new(ReservationLedger::default()),
                #[cfg(test)]
                cleanup_blocked: AtomicBool::new(false),
            }),
        })
    }

    pub(super) fn is_bound_to(&self, execution: &AdmittedExecutionContext) -> bool {
        let limits = execution.limits();
        self.inner.maximum_parallel_steps == limits.maximum_parallel_steps().get()
            && self.inner.maximum_values == limits.maximum_input_values().get()
            && self.inner.maximum_value_bytes == limits.maximum_input_value_bytes().get()
            && self.inner.maximum_total_bytes == limits.maximum_total_input_bytes().get()
            && directory_identity(execution.root()).is_some_and(|(device, inode)| {
                device == self.inner.execution_root_device
                    && inode == self.inner.execution_root_inode
            })
    }

    pub(crate) fn materialize(
        &self,
        inputs: &BTreeMap<String, InputValue<'_>>,
        artifacts: &ArtifactStaging,
    ) -> Result<InputView, InputPreparationFailure> {
        let lifecycle = self.inner.lifecycle.read().map_err(|_| {
            InputPreparationFailure::staging(InputPreparationFailureKind::StagingUnavailable)
        })?;
        if *lifecycle != InputStagingLifecycle::Active {
            return Err(InputPreparationFailure::staging(
                InputPreparationFailureKind::StagingUnavailable,
            ));
        }
        let plan = MaterializationPlan::build(self, inputs)?;
        let (identity, path) = self.reserve(plan.usage)?;
        let materialized = self.materialize_reserved(&path, inputs, artifacts);
        match materialized {
            Ok(()) => Ok(InputView {
                inner: Arc::clone(&self.inner),
                identity,
                path,
                released: false,
            }),
            Err(failure) => {
                let cleaned = self.inner.remove_view_entry(&identity);
                drop(lifecycle);
                if cleaned {
                    self.inner.release_reservation(&identity);
                    Err(failure)
                } else {
                    self.inner.mark_cleanup_failed();
                    Err(InputPreparationFailure::staging(
                        InputPreparationFailureKind::StagingUnavailable,
                    ))
                }
            }
        }
    }

    pub(crate) fn release(&self) -> Result<(), InputStagingReleaseFailure> {
        self.inner.cleanup()
    }

    fn reserve(
        &self,
        requested: ReservationUsage,
    ) -> Result<(Arc<str>, PathBuf), InputPreparationFailure> {
        let mut reservations = lock_reservations(&self.inner.reservations);
        let maximum_live_values = self
            .inner
            .maximum_values
            .saturating_mul(self.inner.maximum_parallel_steps);
        let maximum_live_bytes = self
            .inner
            .maximum_total_bytes
            .saturating_mul(u64::try_from(self.inner.maximum_parallel_steps).unwrap_or(u64::MAX));
        let updated_views = reservations.usage.views.checked_add(1);
        let updated_values = reservations.usage.values.checked_add(requested.values);
        let updated_bytes = reservations.usage.bytes.checked_add(requested.bytes);
        if updated_views.is_none_or(|views| views > self.inner.maximum_parallel_steps)
            || updated_values.is_none_or(|values| values > maximum_live_values)
            || updated_bytes.is_none_or(|bytes| bytes > maximum_live_bytes)
        {
            return Err(InputPreparationFailure::staging(
                InputPreparationFailureKind::LiveLimitExceeded,
            ));
        }

        for _ in 0..IDENTITY_ATTEMPTS {
            let identity = Arc::<str>::from(format!(
                "view-{}",
                ulid::Ulid::generate().to_string().to_ascii_lowercase()
            ));
            let path = self.inner.staging_path.join(identity.as_ref());
            match mkdirat(&self.inner.staging_root, identity.as_ref(), Mode::RWXU) {
                Ok(()) => {
                    if chmodat(
                        &self.inner.staging_root,
                        identity.as_ref(),
                        Mode::RWXU,
                        AtFlags::empty(),
                    )
                    .is_err()
                    {
                        let _ = unlinkat(
                            &self.inner.staging_root,
                            identity.as_ref(),
                            AtFlags::REMOVEDIR,
                        );
                        return Err(InputPreparationFailure::staging(
                            InputPreparationFailureKind::StagingUnavailable,
                        ));
                    }
                    reservations.active.insert(identity.clone(), requested);
                    reservations.usage = ReservationUsage {
                        views: updated_views.unwrap_or(usize::MAX),
                        values: updated_values.unwrap_or(usize::MAX),
                        bytes: updated_bytes.unwrap_or(u64::MAX),
                    };
                    return Ok((identity, path));
                }
                Err(Errno::EXIST) => {}
                Err(_) => {
                    return Err(InputPreparationFailure::staging(
                        InputPreparationFailureKind::StagingUnavailable,
                    ));
                }
            }
        }
        Err(InputPreparationFailure::staging(
            InputPreparationFailureKind::StagingUnavailable,
        ))
    }

    fn materialize_reserved(
        &self,
        root: &Path,
        inputs: &BTreeMap<String, InputValue<'_>>,
        artifacts: &ArtifactStaging,
    ) -> Result<(), InputPreparationFailure> {
        let mut manifest_inputs = BTreeMap::new();
        let values_root = root.join("values");
        let collections_root = root.join("collections");
        let mut values_created = false;
        let mut collections_created = false;

        for (input_identity, value) in inputs {
            match value {
                InputValue::Prompt(text) => {
                    ensure_directory(&values_root, &mut values_created)?;
                    let relative_path = format!("values/{input_identity}");
                    write_bytes_read_only(&root.join(&relative_path), text.as_bytes())
                        .map_err(|_| staging_for_input(input_identity))?;
                    manifest_inputs.insert(
                        input_identity.clone(),
                        ManifestInput::scalar("text", "text/plain; charset=utf-8", relative_path),
                    );
                }
                InputValue::Attachments(attachments) => {
                    ensure_directory(&collections_root, &mut collections_created)?;
                    let relative_directory = format!("collections/{input_identity}");
                    let directory = root.join(&relative_directory);
                    create_directory(&directory)?;
                    let mut items = Vec::with_capacity(attachments.len());
                    for (index, attachment) in attachments.iter().enumerate() {
                        let ordinal = format!("{index:06}");
                        let relative_path = format!("{relative_directory}/{ordinal}");
                        write_bytes_read_only(&root.join(&relative_path), attachment.bytes())
                            .map_err(|_| staging_for_item(input_identity, index))?;
                        items.push(ManifestItem {
                            index,
                            media_type: attachment.media_type().to_owned(),
                            path: relative_path,
                        });
                    }
                    set_read_only_directory(&directory)?;
                    manifest_inputs.insert(
                        input_identity.clone(),
                        ManifestInput::collection(relative_directory, items),
                    );
                }
                InputValue::Captured {
                    expected_type,
                    value,
                } => {
                    ensure_directory(&values_root, &mut values_created)?;
                    let relative_path = format!("values/{input_identity}");
                    let destination = root.join(&relative_path);
                    let manifest = match (*expected_type, value) {
                        (WorkflowValueType::Text, CapturedValue::Text(text)) => {
                            write_bytes_read_only(&destination, text.as_bytes())
                                .map_err(|_| staging_for_input(input_identity))?;
                            ManifestInput::scalar(
                                "text",
                                "text/plain; charset=utf-8",
                                relative_path,
                            )
                        }
                        (WorkflowValueType::Json, CapturedValue::Json(json)) => {
                            write_canonical_json_read_only(&destination, json).map_err(|_| {
                                InputPreparationFailure::for_input(
                                    input_identity,
                                    InputPreparationFailureKind::StagingUnavailable,
                                )
                            })?;
                            ManifestInput::scalar("json", "application/json", relative_path)
                        }
                        (WorkflowValueType::File, CapturedValue::File(file)) => {
                            let mut destination = create_payload_file(&destination)
                                .map_err(|_| staging_for_input(input_identity))?;
                            let copied =
                                artifacts.copy_to(file.handle(), &mut destination).map_err(
                                    |failure| artifact_copy_failure(input_identity, failure),
                                )?;
                            if copied != file.size() {
                                return Err(InputPreparationFailure::for_input(
                                    input_identity,
                                    InputPreparationFailureKind::SourceUnavailable,
                                ));
                            }
                            finish_payload_file(destination)
                                .map_err(|_| staging_for_input(input_identity))?;
                            ManifestInput::scalar("file", file.media_type(), relative_path)
                        }
                        _ => {
                            return Err(InputPreparationFailure::for_input(
                                input_identity,
                                InputPreparationFailureKind::ValueTypeMismatch,
                            ));
                        }
                    };
                    manifest_inputs.insert(input_identity.clone(), manifest);
                }
            }
        }

        if values_created {
            set_read_only_directory(&values_root)?;
        }
        if collections_created {
            set_read_only_directory(&collections_root)?;
        }
        let manifest = Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            inputs: manifest_inputs,
        };
        write_manifest_read_only(&root.join("manifest.json"), &manifest).map_err(|_| {
            InputPreparationFailure::staging(InputPreparationFailureKind::StagingUnavailable)
        })?;
        set_read_only_directory(root)?;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn reservation_usage(&self) -> (usize, usize, u64) {
        let usage = lock_reservations(&self.inner.reservations).usage;
        (usage.views, usage.values, usage.bytes)
    }

    #[cfg(test)]
    pub(crate) fn active_view_count(&self) -> usize {
        lock_reservations(&self.inner.reservations).active.len()
    }

    #[cfg(test)]
    pub(crate) fn block_cleanup(&self) {
        self.inner.cleanup_blocked.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn unblock_cleanup(&self) {
        self.inner.cleanup_blocked.store(false, Ordering::Release);
    }
}

impl InputStagingInner {
    fn release_reservation(&self, identity: &str) {
        let mut reservations = lock_reservations(&self.reservations);
        let Some(released) = reservations.active.remove(identity) else {
            return;
        };
        reservations.usage.views = reservations.usage.views.saturating_sub(released.views);
        reservations.usage.values = reservations.usage.values.saturating_sub(released.values);
        reservations.usage.bytes = reservations.usage.bytes.saturating_sub(released.bytes);
    }

    fn remove_view(&self, identity: &str) -> bool {
        let Ok(lifecycle) = self.lifecycle.read() else {
            return false;
        };
        if *lifecycle == InputStagingLifecycle::Released {
            return true;
        }
        let cleaned = self.remove_view_entry(identity);
        drop(lifecycle);
        if !cleaned {
            self.mark_cleanup_failed();
        }
        cleaned
    }

    fn remove_view_entry(&self, identity: &str) -> bool {
        #[cfg(test)]
        if self.cleanup_blocked.load(Ordering::Acquire) {
            return false;
        }
        remove_tree_at(&self.staging_root, identity).is_ok()
    }

    fn mark_cleanup_failed(&self) {
        let Ok(mut lifecycle) = self.lifecycle.write() else {
            return;
        };
        if *lifecycle == InputStagingLifecycle::Active {
            *lifecycle = InputStagingLifecycle::CleanupFailed;
        }
    }

    fn cleanup(&self) -> Result<(), InputStagingReleaseFailure> {
        let mut lifecycle = self
            .lifecycle
            .write()
            .map_err(|_| InputStagingReleaseFailure::CleanupUnavailable)?;
        if *lifecycle == InputStagingLifecycle::Released {
            return Ok(());
        }

        let cleanup_result = self.cleanup_active();
        *lifecycle = if cleanup_result.is_ok() {
            InputStagingLifecycle::Released
        } else {
            InputStagingLifecycle::CleanupFailed
        };
        if cleanup_result.is_ok() {
            *lock_reservations(&self.reservations) = ReservationLedger::default();
        }
        cleanup_result
    }

    fn cleanup_active(&self) -> Result<(), InputStagingReleaseFailure> {
        #[cfg(test)]
        if self.cleanup_blocked.load(Ordering::Acquire) {
            return Err(InputStagingReleaseFailure::CleanupUnavailable);
        }
        remove_directory_contents(&self.staging_root)
            .map_err(|_| InputStagingReleaseFailure::CleanupUnavailable)?;

        let opened = fstat(&self.staging_root)
            .map_err(|_| InputStagingReleaseFailure::CleanupUnavailable)?;
        let named = statat(
            &self.staging_parent,
            self.staging_identity.as_ref(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| InputStagingReleaseFailure::CleanupUnavailable)?;
        if opened.st_dev != named.st_dev
            || opened.st_ino != named.st_ino
            || FileType::from_raw_mode(named.st_mode) != FileType::Directory
        {
            return Err(InputStagingReleaseFailure::CleanupUnavailable);
        }
        unlinkat(
            &self.staging_parent,
            self.staging_identity.as_ref(),
            AtFlags::REMOVEDIR,
        )
        .map_err(|_| InputStagingReleaseFailure::CleanupUnavailable)
    }
}

impl Drop for InputStagingInner {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

struct MaterializationPlan {
    usage: ReservationUsage,
}

impl MaterializationPlan {
    fn build(
        staging: &InputStaging,
        inputs: &BTreeMap<String, InputValue<'_>>,
    ) -> Result<Self, InputPreparationFailure> {
        let mut values = 0_usize;
        let mut total_bytes = 0_u64;
        for (input_identity, input) in inputs {
            if !valid_input_name(input_identity) {
                return Err(InputPreparationFailure::for_input(
                    input_identity,
                    InputPreparationFailureKind::InvalidInputName,
                ));
            }
            values = add_value_count(staging, values, 1, input_identity)?;
            match input {
                InputValue::Prompt(text) => add_payload_size(
                    staging,
                    &mut total_bytes,
                    byte_length(text.as_bytes(), input_identity)?,
                    input_identity,
                    None,
                )?,
                InputValue::Attachments(attachments) => {
                    if attachments.len() > MAX_COLLECTION_ITEMS {
                        return Err(InputPreparationFailure::for_input(
                            input_identity,
                            InputPreparationFailureKind::CollectionOrdinalLimitExceeded,
                        ));
                    }
                    values = add_value_count(staging, values, attachments.len(), input_identity)?;
                    for (index, attachment) in attachments.iter().enumerate() {
                        add_payload_size(
                            staging,
                            &mut total_bytes,
                            byte_length(attachment.bytes(), input_identity)?,
                            input_identity,
                            Some(index),
                        )?;
                    }
                }
                InputValue::Captured {
                    expected_type,
                    value,
                } => {
                    let size = match (*expected_type, value) {
                        (WorkflowValueType::Text, CapturedValue::Text(text)) => {
                            byte_length(text.as_bytes(), input_identity)?
                        }
                        (WorkflowValueType::Json, CapturedValue::Json(json)) => {
                            canonical_json_size(
                                json,
                                staging.inner.maximum_value_bytes,
                                input_identity,
                            )?
                        }
                        (WorkflowValueType::File, CapturedValue::File(file)) => file.size(),
                        _ => {
                            return Err(InputPreparationFailure::for_input(
                                input_identity,
                                InputPreparationFailureKind::ValueTypeMismatch,
                            ));
                        }
                    };
                    add_payload_size(staging, &mut total_bytes, size, input_identity, None)?;
                }
            }
        }
        Ok(Self {
            usage: ReservationUsage {
                views: 1,
                values,
                bytes: total_bytes,
            },
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    schema_version: u8,
    inputs: BTreeMap<String, ManifestInput>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ManifestInput {
    Scalar {
        kind: &'static str,
        #[serde(rename = "mediaType")]
        media_type: String,
        path: String,
    },
    Collection {
        kind: &'static str,
        path: String,
        items: Vec<ManifestItem>,
    },
}

impl ManifestInput {
    fn scalar(kind: &'static str, media_type: impl Into<String>, path: String) -> Self {
        Self::Scalar {
            kind,
            media_type: media_type.into(),
            path,
        }
    }

    fn collection(path: String, items: Vec<ManifestItem>) -> Self {
        Self::Collection {
            kind: "attachment_collection",
            path,
            items,
        }
    }
}

#[derive(Serialize)]
struct ManifestItem {
    index: usize,
    #[serde(rename = "mediaType")]
    media_type: String,
    path: String,
}

struct CanonicalJson<'a>(&'a Value);

impl Serialize for CanonicalJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(value) => serializer.serialize_bool(*value),
            Value::Number(value) => value.serialize(serializer),
            Value::String(value) => serializer.serialize_str(value),
            Value::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&CanonicalJson(value))?;
                }
                sequence.end()
            }
            Value::Object(values) => {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| {
                    left.as_bytes().cmp(right.as_bytes())
                });
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, &CanonicalJson(value))?;
                }
                map.end()
            }
        }
    }
}

struct LimitedCounter {
    bytes: u64,
    maximum: u64,
    exceeded: bool,
}

impl Write for LimitedCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| io::Error::other("canonical JSON size is unavailable"))?;
        let Some(updated) = self.bytes.checked_add(length) else {
            self.exceeded = true;
            return Err(io::Error::other("canonical JSON exceeds its limit"));
        };
        if updated > self.maximum {
            self.exceeded = true;
            return Err(io::Error::other("canonical JSON exceeds its limit"));
        }
        self.bytes = updated;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn canonical_json_size(
    value: &Value,
    maximum: u64,
    input_identity: &str,
) -> Result<u64, InputPreparationFailure> {
    let mut counter = LimitedCounter {
        bytes: 0,
        maximum,
        exceeded: false,
    };
    let result = serde_json::to_writer(&mut counter, &CanonicalJson(value));
    if result.is_err() {
        let kind = if counter.exceeded {
            InputPreparationFailureKind::ValueSizeLimitExceeded
        } else {
            InputPreparationFailureKind::SourceUnavailable
        };
        return Err(InputPreparationFailure::for_input(input_identity, kind));
    }
    Ok(counter.bytes)
}

fn write_canonical_json_read_only(path: &Path, value: &Value) -> io::Result<()> {
    let mut destination = create_payload_file(path)?;
    serde_json::to_writer(&mut destination, &CanonicalJson(value)).map_err(io::Error::other)?;
    finish_payload_file(destination)
}

fn add_value_count(
    staging: &InputStaging,
    current: usize,
    added: usize,
    input_identity: &str,
) -> Result<usize, InputPreparationFailure> {
    current
        .checked_add(added)
        .filter(|count| *count <= staging.inner.maximum_values)
        .ok_or_else(|| {
            InputPreparationFailure::for_input(
                input_identity,
                InputPreparationFailureKind::ValueCountLimitExceeded,
            )
        })
}

fn add_payload_size(
    staging: &InputStaging,
    total: &mut u64,
    size: u64,
    input_identity: &str,
    collection_index: Option<usize>,
) -> Result<(), InputPreparationFailure> {
    let failure = |kind| match collection_index {
        Some(index) => InputPreparationFailure::for_item(input_identity, index, kind),
        None => InputPreparationFailure::for_input(input_identity, kind),
    };
    if size > staging.inner.maximum_value_bytes {
        return Err(failure(InputPreparationFailureKind::ValueSizeLimitExceeded));
    }
    *total = total
        .checked_add(size)
        .filter(|total| *total <= staging.inner.maximum_total_bytes)
        .ok_or_else(|| failure(InputPreparationFailureKind::TotalSizeLimitExceeded))?;
    Ok(())
}

fn byte_length(bytes: &[u8], input_identity: &str) -> Result<u64, InputPreparationFailure> {
    u64::try_from(bytes.len()).map_err(|_| {
        InputPreparationFailure::for_input(
            input_identity,
            InputPreparationFailureKind::ValueSizeLimitExceeded,
        )
    })
}

fn artifact_copy_failure(
    input_identity: &str,
    failure: ArtifactReadFailure,
) -> InputPreparationFailure {
    let kind = match failure {
        ArtifactReadFailure::UnknownHandle | ArtifactReadFailure::Unavailable => {
            InputPreparationFailureKind::SourceUnavailable
        }
        ArtifactReadFailure::DestinationWrite => InputPreparationFailureKind::StagingUnavailable,
    };
    InputPreparationFailure::for_input(input_identity, kind)
}

fn valid_input_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= INPUT_NAME_MAX_BYTES
        && bytes[0].is_ascii_lowercase()
        && bytes[1..].iter().all(|byte| byte.is_ascii_alphanumeric())
}

fn create_staging_root(
    staging_parent: &OwnedFd,
) -> Result<(Arc<str>, OwnedFd), InputStagingFailure> {
    for _ in 0..IDENTITY_ATTEMPTS {
        let identity = Arc::<str>::from(format!(
            ".inputs-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        ));
        match mkdirat(staging_parent, identity.as_ref(), Mode::RWXU) {
            Ok(()) => {
                if chmodat(
                    staging_parent,
                    identity.as_ref(),
                    Mode::RWXU,
                    AtFlags::empty(),
                )
                .is_err()
                {
                    let _ = unlinkat(staging_parent, identity.as_ref(), AtFlags::REMOVEDIR);
                    return Err(InputStagingFailure::IdentityUnavailable);
                }
                match openat(
                    staging_parent,
                    identity.as_ref(),
                    input_directory_open_flags(),
                    Mode::empty(),
                ) {
                    Ok(directory) => return Ok((identity, directory)),
                    Err(_) => {
                        let _ = unlinkat(staging_parent, identity.as_ref(), AtFlags::REMOVEDIR);
                        return Err(InputStagingFailure::IdentityUnavailable);
                    }
                }
            }
            Err(Errno::EXIST) => {}
            Err(_) => return Err(InputStagingFailure::IdentityUnavailable),
        }
    }
    Err(InputStagingFailure::IdentityUnavailable)
}

fn input_directory_open_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

fn remove_tree_at(parent: &OwnedFd, identity: &str) -> Result<(), Errno> {
    let directory = match openat(
        parent,
        identity,
        input_directory_open_flags(),
        Mode::empty(),
    ) {
        Ok(directory) => directory,
        Err(Errno::NOENT) => return Ok(()),
        Err(failure) => return Err(failure),
    };
    remove_directory_contents(&directory)?;
    match unlinkat(parent, identity, AtFlags::REMOVEDIR) {
        Ok(()) | Err(Errno::NOENT) => Ok(()),
        Err(failure) => Err(failure),
    }
}

fn remove_directory_contents(directory: &OwnedFd) -> Result<(), Errno> {
    fchmod(directory, Mode::RWXU)?;
    let entries = Dir::read_from(directory)?
        .filter_map(|entry| match entry {
            Ok(entry) if !matches!(entry.file_name().to_bytes(), b"." | b"..") => {
                Some(Ok(entry.file_name().to_owned()))
            }
            Ok(_) => None,
            Err(failure) => Some(Err(failure)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    for name in entries {
        let metadata = statat(directory, &name, AtFlags::SYMLINK_NOFOLLOW)?;
        if FileType::from_raw_mode(metadata.st_mode) == FileType::Directory {
            let child = openat(
                directory,
                &name,
                input_directory_open_flags(),
                Mode::empty(),
            )?;
            remove_directory_contents(&child)?;
            unlinkat(directory, &name, AtFlags::REMOVEDIR)?;
        } else {
            unlinkat(directory, &name, AtFlags::empty())?;
        }
    }
    Ok(())
}

fn ensure_directory(path: &Path, created: &mut bool) -> Result<(), InputPreparationFailure> {
    if !*created {
        create_directory(path)?;
        *created = true;
    }
    Ok(())
}

fn create_directory(path: &Path) -> Result<(), InputPreparationFailure> {
    fs::create_dir(path)
        .and_then(|()| fs::set_permissions(path, fs::Permissions::from_mode(0o700)))
        .map_err(|_| {
            InputPreparationFailure::staging(InputPreparationFailureKind::StagingUnavailable)
        })
}

fn create_payload_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

fn finish_payload_file(mut file: File) -> io::Result<()> {
    file.flush()?;
    file.set_permissions(fs::Permissions::from_mode(0o400))
}

fn write_bytes_read_only(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = create_payload_file(path)?;
    file.write_all(bytes)?;
    finish_payload_file(file)
}

fn write_manifest_read_only(path: &Path, manifest: &Manifest) -> io::Result<()> {
    let mut file = create_payload_file(path)?;
    serde_json::to_writer(&mut file, manifest).map_err(io::Error::other)?;
    finish_payload_file(file)
}

fn set_read_only_directory(path: &Path) -> Result<(), InputPreparationFailure> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o500)).map_err(|_| {
        InputPreparationFailure::staging(InputPreparationFailureKind::StagingUnavailable)
    })
}

fn staging_for_input(input_identity: &str) -> InputPreparationFailure {
    InputPreparationFailure::for_input(
        input_identity,
        InputPreparationFailureKind::StagingUnavailable,
    )
}

fn staging_for_item(input_identity: &str, index: usize) -> InputPreparationFailure {
    InputPreparationFailure::for_item(
        input_identity,
        index,
        InputPreparationFailureKind::StagingUnavailable,
    )
}

fn directory_identity(path: &Path) -> Option<(u64, u64)> {
    let metadata = fs::metadata(path).ok()?;
    metadata
        .is_dir()
        .then_some((metadata.dev(), metadata.ino()))
}

fn lock_reservations(reservations: &Mutex<ReservationLedger>) -> MutexGuard<'_, ReservationLedger> {
    reservations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests;
