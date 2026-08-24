use std::error::Error;
use std::fmt;
use std::fs::{self, DirBuilder, File, Metadata, OpenOptions, Permissions};
use std::io::{self, IsTerminal, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use fs4::{FileExt, TryLockError};
use reqwest::{StatusCode, Url};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;

use crate::idempotency::generate_idempotency_key;

use super::validation::{valid_secret_syntax, valid_typed_id};

const ARTIFACT_LIMIT: u64 = 4096;
const CONFIG_LIMIT: u64 = 16 * 1024;
const STATE_LIMIT: u64 = 64 * 1024;
const RESPONSE_LIMIT: u64 = 64 * 1024;
const DIRECTORY_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
const JOURNAL_FILE: &str = ".runner-enrollment.json";
const LOCK_FILE: &str = ".runner-state.lock";
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const LOCK_RETRY: Duration = Duration::from_millis(25);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
#[expect(
    clippy::cast_possible_wrap,
    reason = "O_NOFOLLOW fits in the signed custom_flags value on supported Unix targets"
)]
const NOFOLLOW_FLAG: i32 = rustix::fs::OFlags::NOFOLLOW.bits() as i32;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ActivationArtifact {
    schema_version: u8,
    activation_url: String,
    #[serde(skip_serializing)]
    activation_token: String,
    runner_id: String,
    expires_at: String,
}

/// Owned activation fields supplied by the caller that received the Cloud
/// response. Translation from API DTOs happens at that boundary so the
/// runner component never references the human API client.
pub(crate) struct ActivationArtifactParts {
    pub(crate) activation_url: String,
    pub(crate) activation_token: String,
    pub(crate) runner_id: String,
    pub(crate) expires_at: String,
}

impl ActivationArtifact {
    pub(crate) fn from_parts(parts: ActivationArtifactParts) -> Self {
        Self {
            schema_version: 1,
            activation_url: parts.activation_url,
            activation_token: parts.activation_token,
            runner_id: parts.runner_id,
            expires_at: parts.expires_at,
        }
    }

    pub(crate) fn write_json(&self, output: &mut impl Write) -> Result<(), EnrollmentError> {
        let transferable = TransferableActivationArtifact::from(self);
        serde_json::to_writer_pretty(&mut *output, &transferable)
            .map_err(|_| EnrollmentError::ArtifactWrite)?;
        output
            .write_all(b"\n")
            .map_err(|_| EnrollmentError::ArtifactWrite)
    }

    pub(crate) fn runner_id(&self) -> &str {
        &self.runner_id
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransferableActivationArtifact<'a> {
    schema_version: u8,
    activation_url: &'a str,
    activation_token: &'a str,
    runner_id: &'a str,
    expires_at: &'a str,
}

impl<'a> From<&'a ActivationArtifact> for TransferableActivationArtifact<'a> {
    fn from(artifact: &'a ActivationArtifact) -> Self {
        Self {
            schema_version: artifact.schema_version,
            activation_url: &artifact.activation_url,
            activation_token: &artifact.activation_token,
            runner_id: &artifact.runner_id,
            expires_at: &artifact.expires_at,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DeploymentMode {
    Production,
    Development,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct OperatorConfig {
    schema_version: u8,
    deployment_mode: DeploymentMode,
    runner_state_path: PathBuf,
    control_socket_path: PathBuf,
    work_root: PathBuf,
}

// RunnerServiceConfiguration is the validated startup projection shared by
// enrollment and Runner Serve. Secret material remains private to the runner
// domain and is exposed only to construct the outbound Authorization header.
pub(crate) struct RunnerServiceConfiguration {
    pub(super) runner_id: String,
    pub(super) connection_url: String,
    pub(super) credential_id: String,
    pub(super) credential_secret: String,
    pub(super) pending_credential: Option<PendingCredential>,
    pub(super) state_access: RunnerStateAccess,
    pub(super) control_socket_path: PathBuf,
    pub(super) work_root: PathBuf,
}

#[derive(Clone)]
pub(crate) struct RunnerStateAccess {
    path: PathBuf,
    deployment_mode: DeploymentMode,
}

#[derive(Clone)]
pub(crate) struct PendingCredential {
    pub(super) runner_id: String,
    pub(super) connection_url: String,
    pub(super) credential_id: String,
    pub(super) credential_secret: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReloadStateError {
    RegistrationMismatch,
    StateUpdate,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EnrollmentJournal {
    schema_version: u8,
    activation_artifact: ActivationArtifact,
    #[serde(skip_serializing)]
    credential_secret: String,
    credential_secret_verifier: String,
    #[serde(skip_serializing)]
    idempotency_key: String,
    replace_credential: bool,
    staged_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedEnrollmentJournal<'a> {
    schema_version: u8,
    activation_artifact: TransferableActivationArtifact<'a>,
    credential_secret: &'a str,
    credential_secret_verifier: &'a str,
    idempotency_key: &'a str,
    replace_credential: bool,
    staged_at: &'a str,
}

impl<'a> From<&'a EnrollmentJournal> for PersistedEnrollmentJournal<'a> {
    fn from(journal: &'a EnrollmentJournal) -> Self {
        Self {
            schema_version: journal.schema_version,
            activation_artifact: TransferableActivationArtifact::from(&journal.activation_artifact),
            credential_secret: &journal.credential_secret,
            credential_secret_verifier: &journal.credential_secret_verifier,
            idempotency_key: &journal.idempotency_key,
            replace_credential: journal.replace_credential,
            staged_at: &journal.staged_at,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TerminalReceipt {
    schema_version: u8,
    activation_id: String,
    disposition: TerminalDisposition,
    observed_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum TerminalDisposition {
    Gone,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RunnerState {
    schema_version: u8,
    runner_id: String,
    connection_url: String,
    current_credential: StoredRunnerCredential,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_credential: Option<StoredRunnerCredential>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_promotion: Option<CredentialPromotion>,
    updated_at: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredRunnerCredential {
    id: String,
    #[serde(skip_serializing)]
    secret: String,
    activation_id: String,
    enrolled_at: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CredentialPromotion {
    credential_id: String,
    activation_id: String,
    promoted_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedRunnerState<'a> {
    schema_version: u8,
    runner_id: &'a str,
    connection_url: &'a str,
    current_credential: PersistedRunnerCredential<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_credential: Option<PersistedRunnerCredential<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_promotion: Option<&'a CredentialPromotion>,
    updated_at: &'a str,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedRunnerCredential<'a> {
    id: &'a str,
    secret: &'a str,
    activation_id: &'a str,
    enrolled_at: &'a str,
}

impl<'a> From<&'a StoredRunnerCredential> for PersistedRunnerCredential<'a> {
    fn from(credential: &'a StoredRunnerCredential) -> Self {
        Self {
            id: &credential.id,
            secret: &credential.secret,
            activation_id: &credential.activation_id,
            enrolled_at: &credential.enrolled_at,
        }
    }
}

impl<'a> From<&'a RunnerState> for PersistedRunnerState<'a> {
    fn from(state: &'a RunnerState) -> Self {
        Self {
            schema_version: state.schema_version,
            runner_id: &state.runner_id,
            connection_url: &state.connection_url,
            current_credential: PersistedRunnerCredential::from(&state.current_credential),
            pending_credential: state
                .pending_credential
                .as_ref()
                .map(PersistedRunnerCredential::from),
            last_promotion: state.last_promotion.as_ref(),
            updated_at: &state.updated_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct EnrollmentResponse {
    schema_version: u8,
    runner_id: String,
    runner_name: String,
    organization: EnrollmentOrganization,
    runner_pool: EnrollmentPool,
    credential_id: String,
    connection_url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EnrollmentOrganization {
    id: String,
    display_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EnrollmentPool {
    id: String,
    name: String,
}

impl EnrollmentResponse {
    pub(crate) fn runner_id(&self) -> &str {
        &self.runner_id
    }

    pub(crate) fn credential_id(&self) -> &str {
        &self.credential_id
    }

    pub(crate) fn runner_name(&self) -> &str {
        &self.runner_name
    }

    pub(crate) fn pool_name(&self) -> &str {
        &self.runner_pool.name
    }
}

#[derive(Debug)]
pub(crate) enum EnrollmentOutcome {
    Enrolled {
        response: EnrollmentResponse,
        replacement: bool,
    },
    ReplacementCredential {
        runner_id: String,
        credential_id: String,
    },
    Gone {
        activation_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplacementDisposition {
    Current,
    Pending,
    Missing,
}

pub(crate) fn write_activation_file(
    destination: &str,
    artifact: &ActivationArtifact,
) -> Result<(), EnrollmentError> {
    if destination == "-" {
        return artifact.write_json(&mut io::stdout().lock());
    }
    let path = Path::new(destination);
    let parent = artifact_parent(path);
    let metadata = fs::symlink_metadata(parent).map_err(|_| EnrollmentError::ArtifactWrite)?;
    if !metadata.file_type().is_dir() {
        return Err(EnrollmentError::UnsafePath(
            "activation artifact parent must be a non-symbolic-link directory",
        ));
    }
    let mut file = create_new_private_file(path).map_err(|_| EnrollmentError::ArtifactWrite)?;
    artifact.write_json(&mut file)?;
    file.sync_all()
        .map_err(|_| EnrollmentError::ArtifactWrite)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| EnrollmentError::ArtifactWrite)
}

pub(crate) fn enroll(
    activation_file: Option<&Path>,
    config_path: &Path,
    replace_credential: bool,
    resume: bool,
) -> Result<EnrollmentOutcome, EnrollmentError> {
    if !resume && activation_file.is_none() {
        return Err(EnrollmentError::InvalidCommand);
    }
    if resume && (activation_file.is_some() || replace_credential) {
        return Err(EnrollmentError::InvalidCommand);
    }
    if activation_file == Some(Path::new("-")) && io::stdin().is_terminal() {
        return Err(EnrollmentError::TerminalStdin);
    }

    let config = load_operator_config(config_path)?;
    let state_path = config.runner_state_path;
    let state_directory = state_path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .ok_or(EnrollmentError::UnsafePath(
            "runner state path has no parent directory",
        ))?;
    ensure_private_directory(state_directory)?;
    let lock_path = state_directory.join(LOCK_FILE);
    let lock = acquire_state_lock(&lock_path)?;
    let journal_path = state_directory.join(JOURNAL_FILE);

    let journal = if resume {
        read_journal(&journal_path)?
    } else {
        let artifact = read_activation_artifact_allow_expired(
            activation_file.ok_or(EnrollmentError::InvalidCommand)?,
            config.deployment_mode,
        )?;
        let existing_state = read_runner_state_if_present(&state_path, config.deployment_mode)?;
        match (&existing_state, replace_credential) {
            (Some(_), false) => return Err(EnrollmentError::StateAlreadyEnrolled),
            (None, true) => return Err(EnrollmentError::ReplacementStateMissing),
            (Some(state), true) => {
                if state.runner_id != artifact.runner_id {
                    return Err(EnrollmentError::RunnerMismatch);
                }
                let artifact_activation_id = activation_id(&artifact)?;
                let local_credential = if let Some(pending) = &state.pending_credential {
                    if pending.activation_id != artifact_activation_id {
                        return Err(EnrollmentError::PendingCredentialExists);
                    }
                    Some(pending)
                } else if state.last_promotion.as_ref().is_some_and(|promotion| {
                    promotion.credential_id == state.current_credential.id
                        && promotion.activation_id == artifact_activation_id
                }) {
                    Some(&state.current_credential)
                } else {
                    None
                };
                if let Some(credential) = local_credential {
                    require_resolved_journal(&journal_path)?;
                    let outcome = EnrollmentOutcome::ReplacementCredential {
                        runner_id: state.runner_id.clone(),
                        credential_id: credential.id.clone(),
                    };
                    drop(lock);
                    return Ok(outcome);
                }
            }
            (None, false) => {}
        }
        require_unexpired_artifact(&artifact)?;
        stage_journal(&journal_path, artifact, replace_credential)?
    };

    validate_journal(&journal, config.deployment_mode)?;
    if journal.replace_credential {
        validate_replacement_journal_local_state(&state_path, config.deployment_mode, &journal)?;
    }

    let request = EnrollmentRequest {
        schema_version: 1,
        credential_secret_verifier: &journal.credential_secret_verifier,
    };
    match send_enrollment(&journal, &request)? {
        EnrollmentHTTPOutcome::Success(response) => {
            validate_enrollment_response(
                &response,
                &journal.activation_artifact,
                config.deployment_mode,
            )?;
            persist_enrollment_state(&state_path, &journal, &response, config.deployment_mode)?;
            remove_and_sync(&journal_path)?;
            drop(lock);
            Ok(EnrollmentOutcome::Enrolled {
                response,
                replacement: journal.replace_credential,
            })
        }
        EnrollmentHTTPOutcome::Gone => {
            let activation_id = activation_id(&journal.activation_artifact)?;
            let receipt = TerminalReceipt {
                schema_version: 1,
                activation_id: activation_id.clone(),
                disposition: TerminalDisposition::Gone,
                observed_at: now_rfc3339()?,
            };
            atomic_write_json(&journal_path, &receipt)?;
            drop(lock);
            Ok(EnrollmentOutcome::Gone { activation_id })
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnrollmentRequest<'a> {
    schema_version: u8,
    credential_secret_verifier: &'a str,
}

enum EnrollmentHTTPOutcome {
    Success(EnrollmentResponse),
    Gone,
}

fn send_enrollment(
    journal: &EnrollmentJournal,
    request: &EnrollmentRequest<'_>,
) -> Result<EnrollmentHTTPOutcome, EnrollmentError> {
    crate::tls::install_provider();
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|_| EnrollmentError::NetworkAmbiguous)?;
    let body = serde_json::to_vec(request).map_err(|_| EnrollmentError::InvalidJournal)?;
    let mut response = client
        .post(&journal.activation_artifact.activation_url)
        .header(
            "Authorization",
            format!("Bearer {}", journal.activation_artifact.activation_token),
        )
        .header("Idempotency-Key", &journal.idempotency_key)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .map_err(|_| EnrollmentError::NetworkAmbiguous)?;
    let status = response.status();
    if status == StatusCode::GONE {
        return Ok(EnrollmentHTTPOutcome::Gone);
    }
    if status == StatusCode::UNAUTHORIZED {
        return Err(EnrollmentError::Unauthorized);
    }
    if status == StatusCode::CONFLICT {
        return Err(EnrollmentError::Conflict);
    }
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return Err(EnrollmentError::NetworkAmbiguous);
    }
    if status != StatusCode::CREATED {
        return Err(EnrollmentError::InvalidResponse);
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut response)
        .take(RESPONSE_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| EnrollmentError::NetworkAmbiguous)?;
    if bytes.len() as u64 > RESPONSE_LIMIT {
        return Err(EnrollmentError::InvalidResponse);
    }
    let decoded = serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidResponse)?;
    Ok(EnrollmentHTTPOutcome::Success(decoded))
}

pub(crate) fn load_runner_service_configuration(
    path: &Path,
) -> Result<RunnerServiceConfiguration, EnrollmentError> {
    let config = load_operator_config(path)?;
    let (lock, state) = load_locked_runner_state(&config)?;
    drop(lock);

    let pending_credential = state
        .pending_credential
        .map(|credential| PendingCredential {
            runner_id: state.runner_id.clone(),
            connection_url: state.connection_url.clone(),
            credential_id: credential.id,
            credential_secret: credential.secret,
        });
    Ok(RunnerServiceConfiguration {
        runner_id: state.runner_id,
        connection_url: state.connection_url,
        credential_id: state.current_credential.id,
        credential_secret: state.current_credential.secret,
        pending_credential,
        state_access: RunnerStateAccess {
            path: config.runner_state_path,
            deployment_mode: config.deployment_mode,
        },
        control_socket_path: config.control_socket_path,
        work_root: config.work_root,
    })
}

pub(crate) fn load_control_socket_path(path: &Path) -> Result<PathBuf, EnrollmentError> {
    Ok(load_operator_config(path)?.control_socket_path)
}

pub(crate) fn replacement_disposition(
    config_path: &Path,
    expected_runner_id: &str,
    expected_credential_id: &str,
) -> Result<ReplacementDisposition, EnrollmentError> {
    let config = load_operator_config(config_path)?;
    let (lock, state) = load_locked_runner_state(&config)?;
    if state.runner_id != expected_runner_id {
        return Err(EnrollmentError::RunnerMismatch);
    }
    let disposition = if state.current_credential.id == expected_credential_id {
        ReplacementDisposition::Current
    } else if state
        .pending_credential
        .as_ref()
        .is_some_and(|credential| credential.id == expected_credential_id)
    {
        ReplacementDisposition::Pending
    } else {
        ReplacementDisposition::Missing
    };
    drop(lock);
    Ok(disposition)
}

fn load_locked_runner_state(
    config: &OperatorConfig,
) -> Result<(StateLock, RunnerState), EnrollmentError> {
    let state_directory = config
        .runner_state_path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .ok_or(EnrollmentError::InvalidConfig)?;
    ensure_private_directory(state_directory)?;
    let lock = acquire_state_lock(&state_directory.join(LOCK_FILE))?;
    let state = read_runner_state_if_present(&config.runner_state_path, config.deployment_mode)?
        .ok_or(EnrollmentError::InvalidState)?;
    Ok((lock, state))
}

impl RunnerStateAccess {
    pub(crate) fn load_pending(
        &self,
        expected_runner_id: &str,
    ) -> Result<Option<PendingCredential>, ReloadStateError> {
        let directory = self.path.parent().ok_or(ReloadStateError::StateUpdate)?;
        ensure_private_directory(directory).map_err(|_| ReloadStateError::StateUpdate)?;
        let lock = acquire_state_lock(&directory.join(LOCK_FILE))
            .map_err(|_| ReloadStateError::StateUpdate)?;
        let state = read_runner_state_if_present(&self.path, self.deployment_mode)
            .map_err(|_| ReloadStateError::StateUpdate)?
            .ok_or(ReloadStateError::StateUpdate)?;
        drop(lock);
        if state.runner_id != expected_runner_id {
            return Err(ReloadStateError::RegistrationMismatch);
        }
        Ok(state
            .pending_credential
            .map(|credential| PendingCredential {
                runner_id: state.runner_id,
                connection_url: state.connection_url,
                credential_id: credential.id,
                credential_secret: credential.secret,
            }))
    }

    pub(crate) fn promote(
        &self,
        expected_runner_id: &str,
        expected_current_credential_id: &str,
        expected_pending_credential_id: &str,
    ) -> Result<(), ReloadStateError> {
        let directory = self.path.parent().ok_or(ReloadStateError::StateUpdate)?;
        ensure_private_directory(directory).map_err(|_| ReloadStateError::StateUpdate)?;
        let lock = acquire_state_lock(&directory.join(LOCK_FILE))
            .map_err(|_| ReloadStateError::StateUpdate)?;
        let mut state = read_runner_state_if_present(&self.path, self.deployment_mode)
            .map_err(|_| ReloadStateError::StateUpdate)?
            .ok_or(ReloadStateError::StateUpdate)?;
        if state.runner_id != expected_runner_id {
            return Err(ReloadStateError::RegistrationMismatch);
        }
        if state.current_credential.id != expected_current_credential_id {
            return Err(ReloadStateError::StateUpdate);
        }
        let pending = state
            .pending_credential
            .take()
            .filter(|credential| credential.id == expected_pending_credential_id)
            .ok_or(ReloadStateError::StateUpdate)?;
        let promoted_at = now_rfc3339().map_err(|_| ReloadStateError::StateUpdate)?;
        state.last_promotion = Some(CredentialPromotion {
            credential_id: pending.id.clone(),
            activation_id: pending.activation_id.clone(),
            promoted_at: promoted_at.clone(),
        });
        state.current_credential = pending;
        state.updated_at = promoted_at;
        atomic_write_json(&self.path, &PersistedRunnerState::from(&state))
            .map_err(|_| ReloadStateError::StateUpdate)?;
        drop(lock);
        Ok(())
    }
}

fn load_operator_config(path: &Path) -> Result<OperatorConfig, EnrollmentError> {
    let bytes = read_bounded_regular_file(path, CONFIG_LIMIT, false)
        .map_err(|_| EnrollmentError::InvalidConfig)?;
    let config: OperatorConfig =
        serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidConfig)?;
    if config.schema_version != 1
        || !config.runner_state_path.is_absolute()
        || !config.control_socket_path.is_absolute()
        || !config.work_root.is_absolute()
    {
        return Err(EnrollmentError::InvalidConfig);
    }
    Ok(config)
}

#[cfg(test)]
fn read_activation_artifact(
    path: &Path,
    mode: DeploymentMode,
) -> Result<ActivationArtifact, EnrollmentError> {
    let artifact = read_activation_artifact_allow_expired(path, mode)?;
    require_unexpired_artifact(&artifact)?;
    Ok(artifact)
}

fn read_activation_artifact_allow_expired(
    path: &Path,
    mode: DeploymentMode,
) -> Result<ActivationArtifact, EnrollmentError> {
    let bytes = if path == Path::new("-") {
        let mut bytes = Vec::new();
        io::stdin()
            .lock()
            .take(ARTIFACT_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| EnrollmentError::InvalidArtifact)?;
        bytes
    } else {
        read_bounded_regular_file(path, ARTIFACT_LIMIT, true)
            .map_err(|_| EnrollmentError::InvalidArtifact)?
    };
    if bytes.len() as u64 > ARTIFACT_LIMIT {
        return Err(EnrollmentError::InvalidArtifact);
    }
    let artifact = serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidArtifact)?;
    validate_artifact(&artifact, mode)?;
    Ok(artifact)
}

fn require_unexpired_artifact(artifact: &ActivationArtifact) -> Result<(), EnrollmentError> {
    if parse_rfc3339(&artifact.expires_at)
        .is_none_or(|expires_at| expires_at <= crate::timing::utc_now())
    {
        return Err(EnrollmentError::ExpiredArtifact);
    }
    Ok(())
}

fn validate_artifact(
    artifact: &ActivationArtifact,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    if artifact.schema_version != 1
        || !valid_typed_id(&artifact.runner_id, "rnr_")
        || parse_rfc3339(&artifact.expires_at).is_none()
    {
        return Err(EnrollmentError::InvalidArtifact);
    }
    let activation_id = activation_id(artifact)?;
    let url = validate_cloud_url(&artifact.activation_url, mode, CloudURLKind::Activation)?;
    if url.path() != format!("/v1/runner-enrollments/{activation_id}/activate") {
        return Err(EnrollmentError::InvalidArtifact);
    }
    Ok(())
}

fn activation_id(artifact: &ActivationArtifact) -> Result<String, EnrollmentError> {
    if artifact.activation_token.len() > 256 {
        return Err(EnrollmentError::InvalidArtifact);
    }
    let (activation_id, secret) = artifact
        .activation_token
        .split_once('.')
        .ok_or(EnrollmentError::InvalidArtifact)?;
    if secret.contains('.') || !valid_typed_id(activation_id, "rna_") || !valid_secret(secret) {
        return Err(EnrollmentError::InvalidArtifact);
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(secret)
        .map_err(|_| EnrollmentError::InvalidArtifact)?;
    if decoded.len() != 32 {
        return Err(EnrollmentError::InvalidArtifact);
    }
    Ok(activation_id.to_owned())
}

#[derive(Clone, Copy)]
enum CloudURLKind {
    Activation,
    Connection,
}

fn validate_cloud_url(
    value: &str,
    mode: DeploymentMode,
    kind: CloudURLKind,
) -> Result<Url, EnrollmentError> {
    let url = Url::parse(value).map_err(|_| EnrollmentError::InvalidURL)?;
    if url_authority_has_user_info(value)
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
        || url.host_str().is_none()
    {
        return Err(EnrollmentError::InvalidURL);
    }
    let (secure, insecure) = match kind {
        CloudURLKind::Activation => ("https", "http"),
        CloudURLKind::Connection => ("wss", "ws"),
    };
    if url.scheme() == secure {
        return Ok(url);
    }
    if mode == DeploymentMode::Development
        && url.scheme() == insecure
        && exact_loopback_host(value, &url)
    {
        return Ok(url);
    }
    Err(EnrollmentError::InvalidURL)
}

fn url_authority_has_user_info(value: &str) -> bool {
    value
        .split_once("://")
        .and_then(|(_, remainder)| remainder.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'))
}

fn exact_loopback_host(value: &str, url: &Url) -> bool {
    let Some(raw_host) = raw_url_host(value) else {
        return false;
    };
    match url.host() {
        Some(url::Host::Domain(host)) => host == "localhost" && raw_host == "localhost",
        Some(url::Host::Ipv4(address)) => address.is_loopback() && raw_host == address.to_string(),
        Some(url::Host::Ipv6(address)) => address.is_loopback() && raw_host == "[::1]",
        None => false,
    }
}

fn raw_url_host(value: &str) -> Option<&str> {
    let (_, remainder) = value.split_once("://")?;
    let authority = remainder.split(['/', '?', '#']).next()?;
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    if authority.starts_with('[') {
        let closing = authority.find(']')?;
        return Some(&authority[..=closing]);
    }
    Some(
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host),
    )
}

fn validate_enrollment_response(
    response: &EnrollmentResponse,
    artifact: &ActivationArtifact,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    if response.schema_version != 1
        || response.runner_id != artifact.runner_id
        || !valid_typed_id(&response.runner_id, "rnr_")
        || !valid_typed_id(&response.organization.id, "org_")
        || !valid_typed_id(&response.runner_pool.id, "rpl_")
        || !valid_typed_id(&response.credential_id, "rrc_")
        || !valid_name(&response.runner_name)
        || !valid_name(&response.runner_pool.name)
        || response.organization.display_name.is_empty()
    {
        return Err(EnrollmentError::InvalidResponse);
    }
    let url = validate_cloud_url(&response.connection_url, mode, CloudURLKind::Connection)?;
    if url.path() != "/v1/runner/connect" {
        return Err(EnrollmentError::InvalidResponse);
    }
    Ok(())
}

fn stage_journal(
    path: &Path,
    artifact: ActivationArtifact,
    replace_credential: bool,
) -> Result<EnrollmentJournal, EnrollmentError> {
    require_resolved_journal(path)?;
    let mut secret_bytes = [0_u8; 32];
    getrandom::fill(&mut secret_bytes).map_err(|_| EnrollmentError::EntropyUnavailable)?;
    let credential_secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret_bytes);
    let verifier = digest(&SHA256, &secret_bytes);
    let credential_secret_verifier =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(verifier.as_ref());
    let journal = EnrollmentJournal {
        schema_version: 1,
        activation_artifact: artifact,
        credential_secret,
        credential_secret_verifier,
        idempotency_key: generate_idempotency_key()
            .map_err(|_| EnrollmentError::EntropyUnavailable)?,
        replace_credential,
        staged_at: now_rfc3339()?,
    };
    atomic_write_json(path, &PersistedEnrollmentJournal::from(&journal))?;
    Ok(journal)
}

fn require_resolved_journal(path: &Path) -> Result<(), EnrollmentError> {
    if let Some(bytes) = read_private_file_if_present(path, STATE_LIMIT)? {
        let receipt = serde_json::from_slice::<TerminalReceipt>(&bytes)
            .map_err(|_| EnrollmentError::UnresolvedJournal)?;
        if !valid_terminal_receipt(&receipt) {
            return Err(EnrollmentError::UnresolvedJournal);
        }
    }
    Ok(())
}

fn read_journal(path: &Path) -> Result<EnrollmentJournal, EnrollmentError> {
    let bytes = read_private_file_if_present(path, STATE_LIMIT)
        .map_err(|_| EnrollmentError::InvalidJournal)?
        .ok_or(EnrollmentError::MissingJournal)?;
    serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidJournal)
}

fn validate_journal(
    journal: &EnrollmentJournal,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    validate_artifact(&journal.activation_artifact, mode)?;
    if journal.schema_version != 1
        || !valid_secret(&journal.credential_secret)
        || !valid_secret(&journal.credential_secret_verifier)
        || !journal_verifier_matches_secret(journal)
        || journal.idempotency_key.is_empty()
        || journal.idempotency_key.len() > 255
        || !journal
            .idempotency_key
            .bytes()
            .all(|byte| byte.is_ascii_graphic())
        || parse_rfc3339(&journal.staged_at).is_none()
    {
        return Err(EnrollmentError::InvalidJournal);
    }
    Ok(())
}

fn validate_replacement_journal_local_state(
    path: &Path,
    mode: DeploymentMode,
    journal: &EnrollmentJournal,
) -> Result<(), EnrollmentError> {
    let state = read_runner_state_if_present(path, mode)?
        .ok_or(EnrollmentError::ReplacementStateMissing)?;
    if state.runner_id != journal.activation_artifact.runner_id {
        return Err(EnrollmentError::RunnerMismatch);
    }
    let activation_id = activation_id(&journal.activation_artifact)?;
    let staged = state.pending_credential.as_ref().or_else(|| {
        state
            .last_promotion
            .as_ref()
            .filter(|promotion| promotion.activation_id == activation_id)
            .map(|_| &state.current_credential)
    });
    if staged.is_some_and(|credential| {
        credential.activation_id != activation_id || credential.secret != journal.credential_secret
    }) {
        return Err(EnrollmentError::PendingCredentialExists);
    }
    Ok(())
}

fn persist_enrollment_state(
    path: &Path,
    journal: &EnrollmentJournal,
    response: &EnrollmentResponse,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    let activation_id = activation_id(&journal.activation_artifact)?;
    let existing = read_runner_state_if_present(path, mode)?;
    if existing
        .as_ref()
        .is_some_and(|state| enrollment_already_persisted(state, journal, response, &activation_id))
    {
        return Ok(());
    }

    let enrolled_at = now_rfc3339()?;
    let credential = StoredRunnerCredential {
        id: response.credential_id.clone(),
        secret: journal.credential_secret.clone(),
        activation_id,
        enrolled_at: enrolled_at.clone(),
    };
    let state = if journal.replace_credential {
        let mut state = existing.ok_or(EnrollmentError::ReplacementStateMissing)?;
        if state.runner_id != response.runner_id {
            return Err(EnrollmentError::RunnerMismatch);
        }
        if state.pending_credential.is_some() {
            return Err(EnrollmentError::PendingCredentialExists);
        }
        state.connection_url.clone_from(&response.connection_url);
        state.pending_credential = Some(credential);
        state.updated_at = enrolled_at;
        state
    } else {
        if existing.is_some() {
            return Err(EnrollmentError::StateAlreadyEnrolled);
        }
        RunnerState {
            schema_version: 1,
            runner_id: response.runner_id.clone(),
            connection_url: response.connection_url.clone(),
            current_credential: credential,
            pending_credential: None,
            last_promotion: None,
            updated_at: enrolled_at,
        }
    };
    atomic_write_json(path, &PersistedRunnerState::from(&state))
}

fn enrollment_already_persisted(
    state: &RunnerState,
    journal: &EnrollmentJournal,
    response: &EnrollmentResponse,
    activation_id: &str,
) -> bool {
    if state.runner_id != response.runner_id || state.connection_url != response.connection_url {
        return false;
    }
    let persisted = if journal.replace_credential {
        state.pending_credential.as_ref().or_else(|| {
            state
                .last_promotion
                .as_ref()
                .filter(|promotion| {
                    promotion.credential_id == response.credential_id
                        && promotion.activation_id == activation_id
                })
                .map(|_| &state.current_credential)
        })
    } else if state.pending_credential.is_none() {
        Some(&state.current_credential)
    } else {
        None
    };
    persisted.is_some_and(|credential| {
        credential.id == response.credential_id
            && credential.secret == journal.credential_secret
            && credential.activation_id == activation_id
    })
}

fn read_runner_state_if_present(
    path: &Path,
    mode: DeploymentMode,
) -> Result<Option<RunnerState>, EnrollmentError> {
    let Some(bytes) = read_private_file_if_present(path, STATE_LIMIT)
        .map_err(|_| EnrollmentError::InvalidState)?
    else {
        return Ok(None);
    };
    let state: RunnerState =
        serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidState)?;
    if state.schema_version != 1
        || !valid_typed_id(&state.runner_id, "rnr_")
        || !valid_stored_credential(&state.current_credential)
        || state.pending_credential.as_ref().is_some_and(|credential| {
            !valid_stored_credential(credential) || credential.id == state.current_credential.id
        })
        || state.last_promotion.as_ref().is_some_and(|promotion| {
            promotion.credential_id != state.current_credential.id
                || promotion.activation_id != state.current_credential.activation_id
                || parse_rfc3339(&promotion.promoted_at).is_none()
        })
        || parse_rfc3339(&state.updated_at).is_none()
    {
        return Err(EnrollmentError::InvalidState);
    }
    let url = validate_cloud_url(&state.connection_url, mode, CloudURLKind::Connection)?;
    if url.path() != "/v1/runner/connect" {
        return Err(EnrollmentError::InvalidState);
    }
    Ok(Some(state))
}

fn valid_stored_credential(credential: &StoredRunnerCredential) -> bool {
    valid_typed_id(&credential.id, "rrc_")
        && valid_secret(&credential.secret)
        && valid_typed_id(&credential.activation_id, "rna_")
        && parse_rfc3339(&credential.enrolled_at).is_some()
}

fn journal_verifier_matches_secret(journal: &EnrollmentJournal) -> bool {
    let Ok(secret) =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&journal.credential_secret)
    else {
        return false;
    };
    let verifier = digest(&SHA256, &secret);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(verifier.as_ref())
        == journal.credential_secret_verifier
}

fn valid_terminal_receipt(receipt: &TerminalReceipt) -> bool {
    receipt.schema_version == 1
        && valid_typed_id(&receipt.activation_id, "rna_")
        && matches!(&receipt.disposition, TerminalDisposition::Gone)
        && parse_rfc3339(&receipt.observed_at).is_some()
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<(), EnrollmentError> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|_| EnrollmentError::StateWrite)?;
    bytes.push(b'\n');
    let parent = path.parent().ok_or(EnrollmentError::StateWrite)?;
    ensure_destination_safe(path)?;
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        ".runner-state.tmp.{}.{sequence}",
        std::process::id()
    ));
    let mut file = create_new_private_file(&temporary).map_err(|_| EnrollmentError::StateWrite)?;
    let result = (|| {
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| EnrollmentError::StateWrite)?;
        drop(file);
        ensure_destination_safe(path)?;
        fs::rename(&temporary, path).map_err(|_| EnrollmentError::StateWrite)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn remove_and_sync(path: &Path) -> Result<(), EnrollmentError> {
    let parent = path.parent().ok_or(EnrollmentError::StateWrite)?;
    fs::remove_file(path).map_err(|_| EnrollmentError::StateWrite)?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), EnrollmentError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| EnrollmentError::StateWrite)
}

fn acquire_state_lock(path: &Path) -> Result<StateLock, EnrollmentError> {
    let file = open_or_create_private_file(path).map_err(|_| EnrollmentError::StateLock)?;
    let start = crate::timing::monotonic_now();
    loop {
        match FileExt::try_lock(&file) {
            Ok(()) => return Ok(StateLock { file }),
            Err(TryLockError::WouldBlock) if crate::timing::elapsed(start) < LOCK_TIMEOUT => {
                crate::timing::sleep(LOCK_RETRY);
            }
            Err(TryLockError::WouldBlock | TryLockError::Error(_)) => {
                return Err(EnrollmentError::StateLock);
            }
        }
    }
}

struct StateLock {
    file: File,
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn artifact_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn ensure_private_directory(path: &Path) -> Result<(), EnrollmentError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_directory(&metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = DirBuilder::new();
            builder.mode(DIRECTORY_MODE);
            builder
                .create(path)
                .map_err(|_| EnrollmentError::UnsafePath("cannot create runner state directory"))?;
            fs::set_permissions(path, Permissions::from_mode(DIRECTORY_MODE)).map_err(|_| {
                EnrollmentError::UnsafePath("cannot protect runner state directory")
            })?;
            let metadata = fs::symlink_metadata(path).map_err(|_| {
                EnrollmentError::UnsafePath("cannot inspect runner state directory")
            })?;
            validate_private_directory(&metadata)
        }
        Err(_) => Err(EnrollmentError::UnsafePath(
            "cannot inspect runner state directory",
        )),
    }
}

fn validate_private_directory(metadata: &Metadata) -> Result<(), EnrollmentError> {
    if !metadata.file_type().is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o7777 != DIRECTORY_MODE
    {
        return Err(EnrollmentError::UnsafePath(
            "runner state directory must be owned by the current user with mode 0700",
        ));
    }
    Ok(())
}

fn validate_private_file(metadata: &Metadata) -> Result<(), EnrollmentError> {
    if !metadata.file_type().is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o7777 != FILE_MODE
        || metadata.nlink() != 1
    {
        return Err(EnrollmentError::UnsafePath(
            "runner state file must be owned by the current user with mode 0600",
        ));
    }
    Ok(())
}

// Enrollment state and human OAuth storage intentionally translate the same
// no-follow private-file primitive into different domain error contracts.
// jscpd:ignore-start
fn create_new_private_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(FILE_MODE)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)?;
    file.set_permissions(Permissions::from_mode(FILE_MODE))?;
    Ok(file)
}
// jscpd:ignore-end

fn open_or_create_private_file(path: &Path) -> Result<File, EnrollmentError> {
    match create_new_private_file(path) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path).map_err(|_| EnrollmentError::StateLock)?;
            validate_private_file(&metadata)?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(NOFOLLOW_FLAG)
                .open(path)
                .map_err(|_| EnrollmentError::StateLock)?;
            validate_private_file(&file.metadata().map_err(|_| EnrollmentError::StateLock)?)?;
            Ok(file)
        }
        Err(_) => Err(EnrollmentError::StateLock),
    }
}

fn ensure_destination_safe(path: &Path) -> Result<(), EnrollmentError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_file(&metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(EnrollmentError::StateWrite),
    }
}

fn read_private_file_if_present(
    path: &Path,
    limit: u64,
) -> Result<Option<Vec<u8>>, EnrollmentError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            validate_private_file(&metadata)?;
            let bytes = read_bounded_regular_file(path, limit, true)?;
            Ok(Some(bytes))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(EnrollmentError::UnsafePath(
            "cannot inspect runner state file",
        )),
    }
}

fn read_bounded_regular_file(
    path: &Path,
    limit: u64,
    private: bool,
) -> Result<Vec<u8>, EnrollmentError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| EnrollmentError::UnsafePath("cannot inspect input file"))?;
    if !metadata.file_type().is_file() {
        return Err(EnrollmentError::UnsafePath(
            "input must be a regular non-symbolic-link file",
        ));
    }
    if private {
        validate_private_file(&metadata)?;
    }
    if metadata.len() > limit {
        return Err(EnrollmentError::UnsafePath(
            "input file exceeds its size limit",
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
        .map_err(|_| EnrollmentError::UnsafePath("cannot open input file"))?;
    let opened = file
        .metadata()
        .map_err(|_| EnrollmentError::UnsafePath("cannot inspect opened input file"))?;
    if !opened.file_type().is_file() || private && validate_private_file(&opened).is_err() {
        return Err(EnrollmentError::UnsafePath("opened input file is unsafe"));
    }
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| EnrollmentError::UnsafePath("cannot read input file"))?;
    if bytes.len() as u64 > limit {
        return Err(EnrollmentError::UnsafePath(
            "input file exceeds its size limit",
        ));
    }
    Ok(bytes)
}

fn valid_secret(value: &str) -> bool {
    valid_secret_syntax(value)
        && base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value)
            .is_ok_and(|decoded| decoded.len() == 32)
}

fn parse_rfc3339(value: &str) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::parse(value, &Rfc3339).ok()
}

fn valid_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

fn now_rfc3339() -> Result<String, EnrollmentError> {
    crate::timing::utc_now()
        .format(&Rfc3339)
        .map_err(|_| EnrollmentError::StateWrite)
}

#[derive(Debug)]
pub(crate) enum EnrollmentError {
    InvalidCommand,
    TerminalStdin,
    InvalidConfig,
    InvalidArtifact,
    ExpiredArtifact,
    InvalidURL,
    ArtifactWrite,
    UnsafePath(&'static str),
    StateLock,
    UnresolvedJournal,
    MissingJournal,
    InvalidJournal,
    EntropyUnavailable,
    StateAlreadyEnrolled,
    ReplacementStateMissing,
    PendingCredentialExists,
    RunnerMismatch,
    InvalidState,
    StateWrite,
    NetworkAmbiguous,
    Unauthorized,
    Conflict,
    InvalidResponse,
}

impl fmt::Display for EnrollmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidCommand => "runner enrollment options are inconsistent",
            Self::TerminalStdin => "terminal stdin cannot supply an activation artifact",
            Self::InvalidConfig => "runner operator configuration is invalid",
            Self::InvalidArtifact => "activation artifact is invalid",
            Self::ExpiredArtifact => {
                "activation artifact has expired; issue a different activation"
            }
            Self::InvalidURL => "Cloud-issued runner URL is not permitted by deploymentMode",
            Self::ArtifactWrite => "activation artifact could not be written safely",
            Self::UnsafePath(requirement) => requirement,
            Self::StateLock => "runner state lock is unavailable",
            Self::UnresolvedJournal => "an unresolved enrollment journal already exists",
            Self::MissingJournal => "no unresolved enrollment journal is available to resume",
            Self::InvalidJournal => "enrollment journal is invalid",
            Self::EntropyUnavailable => "secure enrollment randomness is unavailable",
            Self::StateAlreadyEnrolled => "runner state already contains a current credential",
            Self::ReplacementStateMissing => {
                "replacement enrollment requires existing runner state"
            }
            Self::PendingCredentialExists => "runner state already contains a pending credential",
            Self::RunnerMismatch => "activation and local runner registration do not match",
            Self::InvalidState => "protected runner state is invalid",
            Self::StateWrite => "protected runner state could not be committed durably",
            Self::NetworkAmbiguous => {
                "enrollment outcome is ambiguous; resume the journal explicitly"
            }
            Self::Unauthorized => {
                "activation authentication was rejected; the journal remains unresolved"
            }
            Self::Conflict => {
                "enrollment request conflicts with Cloud state; the journal remains unresolved"
            }
            Self::InvalidResponse => {
                "enrollment response is invalid; the journal remains unresolved"
            }
        };
        formatter.write_str(message)
    }
}

impl Error for EnrollmentError {}

#[cfg(test)]
mod tests;
