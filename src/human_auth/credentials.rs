use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, DirBuilder, File, Metadata, OpenOptions, Permissions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fs4::{FileExt, TryLockError};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use zeroize::Zeroizing;

use super::deployment::DeploymentFingerprint;
use super::token::{SecretToken, TokenSource};

const CREDENTIALS_FILE_VARIABLE: &str = "SCHERZO_CLOUD_CREDENTIALS_FILE";
const HOME_VARIABLE: &str = "HOME";
const APPLICATION_HOME_DIRECTORY_NAME: &str = ".scherzo";
const NORMAL_FILE_NAME: &str = "credentials.json";
const SCHEMA_VERSION: u64 = 1;
const DIRECTORY_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
#[expect(
    clippy::cast_possible_wrap,
    reason = "O_NOFOLLOW fits in the signed custom_flags value on supported Unix targets"
)]
const NOFOLLOW_FLAG: i32 = rustix::fs::OFlags::NOFOLLOW.bits() as i32;
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const REFRESH_LOCK_TIMEOUT: Duration = Duration::from_secs(45);
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const TOKEN_EXPIRY_MARGIN: time::Duration = time::Duration::seconds(30);
pub(crate) const MAX_ACCESS_TOKEN_BYTES: usize = 64 * 1024;
const MAX_CREDENTIAL_FILE_BYTES: usize = 1024 * 1024;
const MAX_CREDENTIAL_FILE_BYTES_U64: u64 = 1024 * 1024;

static TEMPORARY_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) struct CredentialStore {
    path: PathBuf,
    lock_path: PathBuf,
    lock_timeout: Duration,
    refresh_lock_timeout: Duration,
}

#[derive(Clone)]
pub(crate) struct StoredCredential {
    access_token: SecretToken,
    expires_at: OffsetDateTime,
    refresh_token: SecretToken,
}

impl fmt::Debug for StoredCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredCredential")
            .field("access_token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("refresh_token", &"[REDACTED]")
            .finish()
    }
}

impl StoredCredential {
    // Stored and freshly issued credentials deliberately remain separate states;
    // their small secret-borrowing APIs are clearer than a shared token trait.
    // jscpd:ignore-start
    pub(crate) fn access_token(&self) -> &SecretToken {
        &self.access_token
    }

    pub(crate) fn refresh_token(&self) -> &SecretToken {
        &self.refresh_token
    }
    // jscpd:ignore-end

    pub(crate) fn needs_refresh(&self, now: OffsetDateTime) -> bool {
        let expiry_cutoff = now.checked_add(TOKEN_EXPIRY_MARGIN).unwrap_or(now);
        self.expires_at <= expiry_cutoff
    }

    #[cfg(test)]
    pub(crate) fn expires_at(&self) -> OffsetDateTime {
        self.expires_at
    }
}

pub(crate) struct RefreshAuthority {
    _lock: CredentialLock,
}

impl CredentialStore {
    pub(crate) fn from_environment() -> Result<Self, CredentialError> {
        Self::from_lookup(|name| env::var_os(name))
    }

    pub(crate) fn selected(
        &self,
        deployment: &DeploymentFingerprint,
    ) -> Result<Option<StoredCredential>, CredentialError> {
        let _lock = self.acquire_lock()?;
        let file = self.read_file()?;
        file.credentials
            .iter()
            .find(|credential| &credential.deployment == deployment)
            .map(stored_credential)
            .transpose()
    }

    pub(crate) fn replace(
        &self,
        deployment: &DeploymentFingerprint,
        access_token: &(impl TokenSource + ?Sized),
        expires_at: OffsetDateTime,
        refresh_token: &(impl TokenSource + ?Sized),
    ) -> Result<(), CredentialError> {
        let _authority = self.refresh_authority(deployment)?;
        self.replace_under_authority(deployment, access_token, expires_at, refresh_token)
    }

    pub(crate) fn replace_if_refresh_token_matches(
        &self,
        deployment: &DeploymentFingerprint,
        expected_refresh_token: &(impl TokenSource + ?Sized),
        access_token: &(impl TokenSource + ?Sized),
        expires_at: OffsetDateTime,
        refresh_token: &(impl TokenSource + ?Sized),
    ) -> Result<Option<StoredCredential>, CredentialError> {
        let replacement = credential_entry(deployment, access_token, expires_at, refresh_token)?;
        let _lock = self.acquire_lock()?;
        let mut file = self.read_file()?;
        let Some(index) = credential_index(&file, deployment) else {
            return Ok(None);
        };
        if file.credentials[index].refresh_token.expose() == expected_refresh_token.expose() {
            file.credentials[index] = replacement;
            self.write_file(&file)?;
        }
        stored_credential(&file.credentials[index]).map(Some)
    }

    #[cfg(test)]
    pub(crate) fn remove(
        &self,
        deployment: &DeploymentFingerprint,
    ) -> Result<bool, CredentialError> {
        let _authority = self.refresh_authority(deployment)?;
        self.remove_matching(deployment, |_| true)
    }

    pub(crate) fn remove_if_access_token_matches_under_authority(
        &self,
        deployment: &DeploymentFingerprint,
        access_token: &(impl TokenSource + ?Sized),
    ) -> Result<bool, CredentialError> {
        self.remove_matching(deployment, |credential| {
            credential.access_token.expose() == access_token.expose()
        })
    }

    pub(crate) fn remove_if_refresh_token_matches_under_authority(
        &self,
        deployment: &DeploymentFingerprint,
        refresh_token: &(impl TokenSource + ?Sized),
    ) -> Result<bool, CredentialError> {
        self.remove_matching(deployment, |credential| {
            credential.refresh_token.expose() == refresh_token.expose()
        })
    }

    pub(crate) fn take_under_authority(
        &self,
        deployment: &DeploymentFingerprint,
    ) -> Result<Option<StoredCredential>, CredentialError> {
        let _lock = self.acquire_lock()?;
        let mut file = self.read_file()?;
        let Some(index) = credential_index(&file, deployment) else {
            return Ok(None);
        };
        let credential = stored_credential(&file.credentials[index])?;
        file.credentials.remove(index);
        self.write_file(&file)?;
        Ok(Some(credential))
    }

    pub(crate) fn refresh_authority(
        &self,
        deployment: &DeploymentFingerprint,
    ) -> Result<RefreshAuthority, CredentialError> {
        let path = self.refresh_lock_path(deployment)?;
        self.acquire_lock_at(&path, self.refresh_lock_timeout, true)
            .map(|lock| RefreshAuthority { _lock: lock })
    }

    fn replace_under_authority(
        &self,
        deployment: &DeploymentFingerprint,
        access_token: &(impl TokenSource + ?Sized),
        expires_at: OffsetDateTime,
        refresh_token: &(impl TokenSource + ?Sized),
    ) -> Result<(), CredentialError> {
        let replacement = credential_entry(deployment, access_token, expires_at, refresh_token)?;
        let _lock = self.acquire_lock()?;
        let mut file = self.read_file()?;
        match credential_index(&file, deployment) {
            Some(index) => file.credentials[index] = replacement,
            None => file.credentials.push(replacement),
        }
        self.write_file(&file)
    }

    fn remove_matching<F>(
        &self,
        deployment: &DeploymentFingerprint,
        predicate: F,
    ) -> Result<bool, CredentialError>
    where
        F: Fn(&CredentialEntry) -> bool,
    {
        let _lock = self.acquire_lock()?;
        let mut file = self.read_file()?;
        let original_len = file.credentials.len();
        file.credentials
            .retain(|credential| &credential.deployment != deployment || !predicate(credential));
        let removed = file.credentials.len() != original_len;
        if removed {
            self.write_file(&file)?;
        }
        Ok(removed)
    }

    fn from_lookup<F>(lookup: F) -> Result<Self, CredentialError>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let path = match lookup(CREDENTIALS_FILE_VARIABLE) {
            Some(path) if path.is_empty() => {
                return Err(CredentialError::InvalidCredentialPath {
                    reason: "the credential-file override is empty",
                });
            }
            Some(path) => PathBuf::from(path),
            None => {
                let home = lookup(HOME_VARIABLE).ok_or(CredentialError::InvalidCredentialPath {
                    reason: "HOME is not set",
                })?;
                if home.is_empty() {
                    return Err(CredentialError::InvalidCredentialPath {
                        reason: "HOME is empty",
                    });
                }
                PathBuf::from(home)
                    .join(APPLICATION_HOME_DIRECTORY_NAME)
                    .join(NORMAL_FILE_NAME)
            }
        };
        let Some(file_name) = path.file_name() else {
            return Err(CredentialError::InvalidCredentialPath {
                reason: "the credential path has no file name",
            });
        };
        let lock_path = sibling_path(&path, file_name, ".lock");

        Ok(Self {
            path,
            lock_path,
            lock_timeout: LOCK_TIMEOUT,
            refresh_lock_timeout: REFRESH_LOCK_TIMEOUT,
        })
    }

    fn acquire_lock(&self) -> Result<CredentialLock, CredentialError> {
        self.acquire_lock_at(&self.lock_path, self.lock_timeout, false)
    }

    fn acquire_lock_at(
        &self,
        path: &Path,
        timeout: Duration,
        refresh: bool,
    ) -> Result<CredentialLock, CredentialError> {
        let directory = self.directory()?;
        ensure_private_directory(directory)?;
        let file = open_or_create_private_file(path)?;
        let start = crate::timing::monotonic_now();

        loop {
            match FileExt::try_lock(&file) {
                Ok(()) => return Ok(CredentialLock { file }),
                Err(TryLockError::WouldBlock) => {
                    let elapsed = crate::timing::elapsed(start);
                    if elapsed >= timeout {
                        return Err(if refresh {
                            CredentialError::RefreshLockTimeout
                        } else {
                            CredentialError::LockTimeout
                        });
                    }
                    crate::timing::sleep(LOCK_RETRY_INTERVAL.min(timeout.saturating_sub(elapsed)));
                }
                Err(TryLockError::Error(source)) => {
                    return Err(CredentialError::Io {
                        operation: if refresh {
                            "acquire refresh lock"
                        } else {
                            "acquire credential lock"
                        },
                        path: path.to_owned(),
                        source,
                    });
                }
            }
        }
    }

    fn refresh_lock_path(
        &self,
        deployment: &DeploymentFingerprint,
    ) -> Result<PathBuf, CredentialError> {
        let file_name = self
            .path
            .file_name()
            .ok_or(CredentialError::InvalidCredentialPath {
                reason: "the credential path has no file name",
            })?;
        let bytes = serde_json::to_vec(deployment).map_err(CredentialError::SerializeJson)?;
        let digest = ring::digest::digest(&ring::digest::SHA256, &bytes);
        let digest = digest
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(sibling_path(
            &self.path,
            file_name,
            &format!(".refresh.{digest}.lock"),
        ))
    }

    fn read_file(&self) -> Result<CredentialFile, CredentialError> {
        let metadata = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(CredentialFile::empty());
            }
            Err(source) => {
                return Err(CredentialError::Io {
                    operation: "inspect credential file",
                    path: self.path.clone(),
                    source,
                });
            }
        };
        validate_private_file(&self.path, &metadata)?;
        if metadata.len() > MAX_CREDENTIAL_FILE_BYTES_U64 {
            return Err(CredentialError::CredentialFileTooLarge);
        }

        let mut file = open_existing_private_file(&self.path)?;
        let metadata = file.metadata().map_err(|source| CredentialError::Io {
            operation: "inspect opened credential file",
            path: self.path.clone(),
            source,
        })?;
        validate_private_file(&self.path, &metadata)?;
        let initial_capacity =
            usize::try_from(metadata.len()).map_err(|_| CredentialError::CredentialFileTooLarge)?;
        let mut bytes = Zeroizing::new(Vec::with_capacity(initial_capacity));
        Read::by_ref(&mut file)
            .take(MAX_CREDENTIAL_FILE_BYTES_U64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| CredentialError::Io {
                operation: "read credential file",
                path: self.path.clone(),
                source,
            })?;
        if bytes.len() > MAX_CREDENTIAL_FILE_BYTES {
            return Err(CredentialError::CredentialFileTooLarge);
        }

        let file: CredentialFile =
            serde_json::from_slice(&bytes).map_err(CredentialError::MalformedJson)?;
        validate_credential_file(&file)?;
        Ok(file)
    }

    fn write_file(&self, file: &CredentialFile) -> Result<(), CredentialError> {
        validate_credential_file(file)?;
        ensure_destination_safe(&self.path)?;
        let mut bytes = Zeroizing::new(
            serde_json::to_vec_pretty(file).map_err(CredentialError::SerializeJson)?,
        );
        bytes.push(b'\n');
        let directory = self.directory()?;
        let temporary_path = temporary_path(&self.path)?;
        let mut temporary = TemporaryFile::new(temporary_path.clone());
        let mut output = create_private_file(&temporary_path)?;
        output
            .write_all(&bytes)
            .map_err(|source| CredentialError::Io {
                operation: "write temporary credential file",
                path: temporary_path.clone(),
                source,
            })?;
        output.sync_all().map_err(|source| CredentialError::Io {
            operation: "sync temporary credential file",
            path: temporary_path.clone(),
            source,
        })?;
        drop(output);

        ensure_destination_safe(&self.path)?;
        fs::rename(&temporary_path, &self.path).map_err(|source| CredentialError::Io {
            operation: "atomically replace credential file",
            path: self.path.clone(),
            source,
        })?;
        temporary.committed = true;
        File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| CredentialError::Io {
                operation: "sync credential directory",
                path: directory.to_owned(),
                source,
            })?;
        Ok(())
    }

    fn directory(&self) -> Result<&Path, CredentialError> {
        self.path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or(CredentialError::InvalidCredentialPath {
                reason: "the credential path has no parent directory",
            })
    }
}

#[derive(Debug)]
pub(crate) enum CredentialError {
    InvalidCredentialPath {
        reason: &'static str,
    },
    UnsafePath {
        path: PathBuf,
        requirement: &'static str,
    },
    LockTimeout,
    RefreshLockTimeout,
    CredentialFileTooLarge,
    MalformedJson(serde_json::Error),
    UnsupportedSchema(u64),
    InvalidCredentialFile {
        reason: &'static str,
    },
    SerializeJson(serde_json::Error),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCredentialPath { reason } => {
                write!(formatter, "invalid credential path: {reason}")
            }
            Self::UnsafePath { path, requirement } => {
                write!(
                    formatter,
                    "unsafe credential path {}: {requirement}",
                    path.display()
                )
            }
            Self::LockTimeout => {
                write!(formatter, "credential lock remained busy for five seconds")
            }
            Self::RefreshLockTimeout => {
                write!(formatter, "refresh lock remained busy for 45 seconds")
            }
            Self::CredentialFileTooLarge => {
                write!(formatter, "credential file exceeds the 1 MiB safety limit")
            }
            Self::MalformedJson(error) => {
                write!(formatter, "credential file is malformed: {error}")
            }
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "credential file uses unsupported schema version {version}"
                )
            }
            Self::InvalidCredentialFile { reason } => {
                write!(formatter, "credential file is invalid: {reason}")
            }
            Self::SerializeJson(error) => {
                write!(formatter, "serialize credential file: {error}")
            }
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
        }
    }
}

impl Error for CredentialError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MalformedJson(error) | Self::SerializeJson(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CredentialFile {
    schema_version: u64,
    credentials: Vec<CredentialEntry>,
}

impl CredentialFile {
    fn empty() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            credentials: Vec::new(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CredentialEntry {
    deployment: DeploymentFingerprint,
    access_token: SecretToken,
    expires_at: String,
    refresh_token: SecretToken,
}

struct CredentialLock {
    file: File,
}

impl Drop for CredentialLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

struct TemporaryFile {
    path: PathBuf,
    committed: bool,
}

impl TemporaryFile {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn validate_credential_file(file: &CredentialFile) -> Result<(), CredentialError> {
    if file.schema_version != SCHEMA_VERSION {
        return Err(CredentialError::UnsupportedSchema(file.schema_version));
    }

    let mut deployments = HashSet::new();
    for credential in &file.credentials {
        if !deployments.insert(&credential.deployment) {
            return Err(CredentialError::InvalidCredentialFile {
                reason: "a deployment fingerprint appears more than once",
            });
        }
        validate_access_token(credential.access_token.expose())?;
        parse_expiration(&credential.expires_at)?;
        validate_refresh_token(credential.refresh_token.expose())?;
    }

    Ok(())
}

fn validate_access_token(access_token: &str) -> Result<(), CredentialError> {
    if access_token.is_empty() {
        return Err(CredentialError::InvalidCredentialFile {
            reason: "an access token is empty",
        });
    }
    if access_token.len() > MAX_ACCESS_TOKEN_BYTES {
        return Err(CredentialError::InvalidCredentialFile {
            reason: "an access token exceeds 64 KiB",
        });
    }
    Ok(())
}

fn validate_refresh_token(refresh_token: &str) -> Result<(), CredentialError> {
    if refresh_token.is_empty() {
        return Err(CredentialError::InvalidCredentialFile {
            reason: "a refresh token is empty",
        });
    }
    if refresh_token.len() > MAX_ACCESS_TOKEN_BYTES {
        return Err(CredentialError::InvalidCredentialFile {
            reason: "a refresh token exceeds 64 KiB",
        });
    }
    Ok(())
}

fn parse_expiration(value: &str) -> Result<OffsetDateTime, CredentialError> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| CredentialError::InvalidCredentialFile {
        reason: "an access-token expiration is not valid RFC 3339",
    })
}

fn credential_entry(
    deployment: &DeploymentFingerprint,
    access_token: &(impl TokenSource + ?Sized),
    expires_at: OffsetDateTime,
    refresh_token: &(impl TokenSource + ?Sized),
) -> Result<CredentialEntry, CredentialError> {
    validate_access_token(access_token.expose())?;
    validate_refresh_token(refresh_token.expose())?;
    let expires_at =
        expires_at
            .format(&Rfc3339)
            .map_err(|_| CredentialError::InvalidCredentialFile {
                reason: "access-token expiration cannot be represented as RFC 3339",
            })?;
    Ok(CredentialEntry {
        deployment: deployment.clone(),
        access_token: access_token.to_secret(),
        expires_at,
        refresh_token: refresh_token.to_secret(),
    })
}

fn stored_credential(entry: &CredentialEntry) -> Result<StoredCredential, CredentialError> {
    Ok(StoredCredential {
        access_token: entry.access_token.clone(),
        expires_at: parse_expiration(&entry.expires_at)?,
        refresh_token: entry.refresh_token.clone(),
    })
}

fn credential_index(file: &CredentialFile, deployment: &DeploymentFingerprint) -> Option<usize> {
    file.credentials
        .iter()
        .position(|credential| &credential.deployment == deployment)
}

fn ensure_private_directory(path: &Path) -> Result<(), CredentialError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_directory(path, &metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = DirBuilder::new();
            builder.mode(DIRECTORY_MODE);
            match builder.create(path) {
                Ok(()) => {
                    fs::set_permissions(path, Permissions::from_mode(DIRECTORY_MODE)).map_err(
                        |source| CredentialError::Io {
                            operation: "set credential directory permissions",
                            path: path.to_owned(),
                            source,
                        },
                    )?;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(source) => {
                    return Err(CredentialError::Io {
                        operation: "create credential directory",
                        path: path.to_owned(),
                        source,
                    });
                }
            }
            let metadata = fs::symlink_metadata(path).map_err(|source| CredentialError::Io {
                operation: "inspect credential directory",
                path: path.to_owned(),
                source,
            })?;
            validate_private_directory(path, &metadata)
        }
        Err(source) => Err(CredentialError::Io {
            operation: "inspect credential directory",
            path: path.to_owned(),
            source,
        }),
    }
}

fn validate_private_directory(path: &Path, metadata: &Metadata) -> Result<(), CredentialError> {
    if !metadata.file_type().is_dir() {
        return Err(CredentialError::UnsafePath {
            path: path.to_owned(),
            requirement: "expected a non-symbolic-link directory",
        });
    }
    validate_owner(path, metadata)?;
    validate_mode(
        path,
        metadata,
        DIRECTORY_MODE,
        "directory mode must be 0700",
    )
}

fn validate_private_file(path: &Path, metadata: &Metadata) -> Result<(), CredentialError> {
    if !metadata.file_type().is_file() {
        return Err(CredentialError::UnsafePath {
            path: path.to_owned(),
            requirement: "expected a regular non-symbolic-link file",
        });
    }
    validate_owner(path, metadata)?;
    validate_mode(path, metadata, FILE_MODE, "file mode must be 0600")
}

fn validate_owner(path: &Path, metadata: &Metadata) -> Result<(), CredentialError> {
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(CredentialError::UnsafePath {
            path: path.to_owned(),
            requirement: "path must be owned by the current user",
        });
    }
    Ok(())
}

fn validate_mode(
    path: &Path,
    metadata: &Metadata,
    expected: u32,
    requirement: &'static str,
) -> Result<(), CredentialError> {
    if metadata.mode() & 0o7777 != expected {
        return Err(CredentialError::UnsafePath {
            path: path.to_owned(),
            requirement,
        });
    }
    Ok(())
}

fn open_existing_private_file(path: &Path) -> Result<File, CredentialError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
        .map_err(|source| CredentialError::Io {
            operation: "open credential file",
            path: path.to_owned(),
            source,
        })?;
    let metadata = file.metadata().map_err(|source| CredentialError::Io {
        operation: "inspect opened credential file",
        path: path.to_owned(),
        source,
    })?;
    validate_private_file(path, &metadata)?;
    Ok(file)
}

fn open_or_create_private_file(path: &Path) -> Result<File, CredentialError> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(FILE_MODE)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
    {
        Ok(file) => {
            file.set_permissions(Permissions::from_mode(FILE_MODE))
                .map_err(|source| CredentialError::Io {
                    operation: "set credential lock permissions",
                    path: path.to_owned(),
                    source,
                })?;
            Ok(file)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path).map_err(|source| CredentialError::Io {
                operation: "inspect credential lock",
                path: path.to_owned(),
                source,
            })?;
            validate_private_file(path, &metadata)?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(NOFOLLOW_FLAG)
                .open(path)
                .map_err(|source| CredentialError::Io {
                    operation: "open credential lock",
                    path: path.to_owned(),
                    source,
                })?;
            let metadata = file.metadata().map_err(|source| CredentialError::Io {
                operation: "inspect opened credential lock",
                path: path.to_owned(),
                source,
            })?;
            validate_private_file(path, &metadata)?;
            Ok(file)
        }
        Err(source) => Err(CredentialError::Io {
            operation: "create credential lock",
            path: path.to_owned(),
            source,
        }),
    }
}

fn create_private_file(path: &Path) -> Result<File, CredentialError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(FILE_MODE)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
        .map_err(|source| CredentialError::Io {
            operation: "create temporary credential file",
            path: path.to_owned(),
            source,
        })?;
    file.set_permissions(Permissions::from_mode(FILE_MODE))
        .map_err(|source| CredentialError::Io {
            operation: "set temporary credential file permissions",
            path: path.to_owned(),
            source,
        })?;
    Ok(file)
}

fn ensure_destination_safe(path: &Path) -> Result<(), CredentialError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_file(path, &metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(CredentialError::Io {
            operation: "inspect credential file before replacement",
            path: path.to_owned(),
            source,
        }),
    }
}

fn sibling_path(path: &Path, file_name: &OsStr, suffix: &str) -> PathBuf {
    let mut name = file_name.to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

fn temporary_path(path: &Path) -> Result<PathBuf, CredentialError> {
    let file_name = path
        .file_name()
        .ok_or(CredentialError::InvalidCredentialPath {
            reason: "the credential path has no file name",
        })?;
    let sequence = TEMPORARY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let suffix = format!(".tmp.{}.{sequence}", std::process::id());
    Ok(sibling_path(path, file_name, &suffix))
}

#[cfg(test)]
mod tests;
