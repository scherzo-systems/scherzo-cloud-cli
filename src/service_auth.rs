use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use zeroize::Zeroizing;

const MAX_API_KEY_BYTES: usize = 128;
const MAX_WORKLOAD_TOKEN_BYTES: usize = 64 * 1024;
const PRIVATE_FILE_MODE: u32 = 0o600;

pub(crate) struct ServiceApiKey(Zeroizing<String>);

impl ServiceApiKey {
    pub(crate) fn parse(value: Zeroizing<String>) -> Result<Self, ServiceApiKeyError> {
        if !valid_api_key(&value) {
            return Err(ServiceApiKeyError::Invalid {
                source: SecretSource::Issued,
                reason: "value does not use the canonical service API-key syntax",
            });
        }
        Ok(Self(value))
    }

    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for ServiceApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServiceApiKey([REDACTED])")
    }
}

pub(crate) struct WorkloadToken(Zeroizing<String>);

impl WorkloadToken {
    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for WorkloadToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkloadToken([REDACTED])")
    }
}

#[derive(Clone, Debug)]
pub(crate) enum SecretSource {
    File(PathBuf),
    Stdin,
    Issued,
    WorkloadFile(PathBuf),
    WorkloadStdin,
    ApiKeyStdout,
}

impl fmt::Display for SecretSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => write!(formatter, "service API-key file {}", path.display()),
            Self::Stdin => formatter.write_str("service API key from standard input"),
            Self::Issued => formatter.write_str("service API key returned by the deployment"),
            Self::WorkloadFile(path) => {
                write!(formatter, "workload identity-token file {}", path.display())
            }
            Self::WorkloadStdin => {
                formatter.write_str("workload identity token from standard input")
            }
            Self::ApiKeyStdout => formatter.write_str("standard output"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ServiceApiKeyError {
    Io {
        source_name: SecretSource,
        operation: &'static str,
        source: io::Error,
    },
    UnsafeFile {
        source_name: SecretSource,
        requirement: &'static str,
    },
    Invalid {
        source: SecretSource,
        reason: &'static str,
    },
}

impl ServiceApiKeyError {
    fn io(source_name: SecretSource, operation: &'static str, source: io::Error) -> Self {
        Self::Io {
            source_name,
            operation,
            source,
        }
    }
}

impl fmt::Display for ServiceApiKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                source_name,
                operation,
                source,
            } => write!(formatter, "{operation} {source_name}: {source}"),
            Self::UnsafeFile {
                source_name,
                requirement,
            } => write!(formatter, "unsafe {source_name}: {requirement}"),
            Self::Invalid { source, reason } => write!(formatter, "invalid {source}: {reason}"),
        }
    }
}

impl Error for ServiceApiKeyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::UnsafeFile { .. } | Self::Invalid { .. } => None,
        }
    }
}

pub(crate) fn read_api_key(path: &Path) -> Result<ServiceApiKey, ServiceApiKeyError> {
    let source = if path == Path::new("-") {
        SecretSource::Stdin
    } else {
        SecretSource::File(path.to_owned())
    };
    let mut bytes = read_secret_bytes(path, &source, MAX_API_KEY_BYTES)?;
    trim_one_newline(&mut bytes);
    let value = std::str::from_utf8(&bytes).map_err(|_| ServiceApiKeyError::Invalid {
        source: source.clone(),
        reason: "input is not UTF-8",
    })?;
    if !valid_api_key(value) {
        return Err(ServiceApiKeyError::Invalid {
            source,
            reason: "value does not use the canonical service API-key syntax",
        });
    }
    Ok(ServiceApiKey(Zeroizing::new(value.to_owned())))
}

pub(crate) fn read_workload_token(path: &Path) -> Result<WorkloadToken, ServiceApiKeyError> {
    let source = if path == Path::new("-") {
        SecretSource::WorkloadStdin
    } else {
        SecretSource::WorkloadFile(path.to_owned())
    };
    let mut bytes = read_secret_bytes(path, &source, MAX_WORKLOAD_TOKEN_BYTES)?;
    trim_one_newline(&mut bytes);
    let value = std::str::from_utf8(&bytes).map_err(|_| ServiceApiKeyError::Invalid {
        source: source.clone(),
        reason: "input is not UTF-8",
    })?;
    if value.is_empty()
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(ServiceApiKeyError::Invalid {
            source,
            reason: "value must be a nonempty token without whitespace or control characters",
        });
    }
    Ok(WorkloadToken(Zeroizing::new(value.to_owned())))
}

fn read_secret_bytes(
    path: &Path,
    source: &SecretSource,
    maximum_bytes: usize,
) -> Result<Zeroizing<Vec<u8>>, ServiceApiKeyError> {
    let mut bytes = Zeroizing::new(Vec::with_capacity(maximum_bytes.saturating_add(1)));
    if path == Path::new("-") {
        io::stdin()
            .lock()
            .take(u64::try_from(maximum_bytes + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut bytes)
            .map_err(|error| ServiceApiKeyError::io(source.clone(), "read", error))?;
    } else {
        read_private_file(path, source, maximum_bytes, &mut bytes)?;
    }
    if bytes.len() > maximum_bytes {
        return Err(ServiceApiKeyError::Invalid {
            source: source.clone(),
            reason: "input exceeds the supported secret size",
        });
    }
    Ok(bytes)
}

fn trim_one_newline(bytes: &mut Vec<u8>) {
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
}

fn read_private_file(
    path: &Path,
    source: &SecretSource,
    maximum_bytes: usize,
    bytes: &mut Vec<u8>,
) -> Result<(), ServiceApiKeyError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| ServiceApiKeyError::io(source.clone(), "inspect", error))?;
    validate_private_file(source, &metadata)?;
    if metadata.len() > u64::try_from(maximum_bytes).unwrap_or(u64::MAX) {
        return Err(ServiceApiKeyError::Invalid {
            source: source.clone(),
            reason: "file exceeds the supported secret size",
        });
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| ServiceApiKeyError::io(source.clone(), "open", error))?;
    let opened = file
        .metadata()
        .map_err(|error| ServiceApiKeyError::io(source.clone(), "inspect opened", error))?;
    validate_private_file(source, &opened)?;
    file.take(u64::try_from(maximum_bytes + 1).unwrap_or(u64::MAX))
        .read_to_end(bytes)
        .map_err(|error| ServiceApiKeyError::io(source.clone(), "read", error))?;
    Ok(())
}

fn validate_private_file(
    source: &SecretSource,
    metadata: &fs::Metadata,
) -> Result<(), ServiceApiKeyError> {
    if !metadata.file_type().is_file() {
        return Err(ServiceApiKeyError::UnsafeFile {
            source_name: source.clone(),
            requirement: "expected a regular non-symbolic-link file",
        });
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(ServiceApiKeyError::UnsafeFile {
            source_name: source.clone(),
            requirement: "file must be owned by the current user",
        });
    }
    if metadata.mode() & 0o7777 != PRIVATE_FILE_MODE {
        return Err(ServiceApiKeyError::UnsafeFile {
            source_name: source.clone(),
            requirement: "file mode must be 0600",
        });
    }
    Ok(())
}

fn valid_api_key(value: &str) -> bool {
    if value.trim() != value {
        return false;
    }
    let Some((credential_id, secret)) = value.split_once('.') else {
        return false;
    };
    if secret.contains('.')
        || !scherzo_cloud_support::valid_typed_id(credential_id, "crd_")
        || secret.len() != 43
    {
        return false;
    }
    // decode_slice may require the conservative 33-byte capacity for 43 unpadded characters;
    // canonical service secrets still decode to exactly 32 bytes.
    let mut decoded = Zeroizing::new([0_u8; 33]);
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode_slice(secret, decoded.as_mut_slice())
        .is_ok_and(|length| {
            length == 32
                && Zeroizing::new(
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&decoded[..length]),
                )
                .as_str()
                    == secret
        })
}

pub(crate) enum ApiKeyDestination {
    Stdout,
    File {
        path: PathBuf,
        file: File,
        cleanup_on_drop: bool,
    },
}

#[derive(Debug)]
pub(crate) enum ApiKeyCleanup {
    Removed,
    NotApplicable,
    Uncertain(ServiceApiKeyError),
}

impl ApiKeyCleanup {
    pub(crate) const fn status(&self) -> &'static str {
        match self {
            Self::Removed => "removed",
            Self::NotApplicable => "not_applicable",
            Self::Uncertain(_) => "uncertain",
        }
    }

    pub(crate) const fn error(&self) -> Option<&ServiceApiKeyError> {
        match self {
            Self::Uncertain(error) => Some(error),
            Self::Removed | Self::NotApplicable => None,
        }
    }
}

impl ApiKeyDestination {
    pub(crate) fn prepare(destination: &str) -> Result<Self, ServiceApiKeyError> {
        if destination == "-" {
            return Ok(Self::Stdout);
        }
        let path = PathBuf::from(destination);
        let source = SecretSource::File(path.clone());
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(PRIVATE_FILE_MODE)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|error| ServiceApiKeyError::io(source.clone(), "create", error))?;
        if let Err(error) = file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE)) {
            let _ = fs::remove_file(&path);
            return Err(ServiceApiKeyError::io(source, "set permissions on", error));
        }
        if let Err(error) = sync_prepared_destination(&path, &file) {
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        Ok(Self::File {
            path,
            file,
            cleanup_on_drop: true,
        })
    }

    pub(crate) fn display_path(&self) -> &str {
        match self {
            Self::Stdout => "-",
            Self::File { path, .. } => path.to_str().unwrap_or("<non-Unicode path>"),
        }
    }

    pub(crate) fn writes_stdout(&self) -> bool {
        matches!(self, Self::Stdout)
    }

    pub(crate) fn write(&mut self, api_key: &ServiceApiKey) -> Result<(), ServiceApiKeyError> {
        match self {
            Self::Stdout => {
                let mut output = io::stdout().lock();
                output
                    .write_all(api_key.expose().as_bytes())
                    .and_then(|()| output.write_all(b"\n"))
                    .and_then(|()| output.flush())
                    .map_err(|error| {
                        ServiceApiKeyError::io(
                            SecretSource::ApiKeyStdout,
                            "write API key to",
                            error,
                        )
                    })
            }
            Self::File {
                path,
                file,
                cleanup_on_drop,
            } => {
                let source = SecretSource::File(path.clone());
                file.write_all(api_key.expose().as_bytes())
                    .and_then(|()| file.write_all(b"\n"))
                    .map_err(|error| ServiceApiKeyError::io(source.clone(), "write", error))?;
                file.sync_all()
                    .map_err(|error| ServiceApiKeyError::io(source, "sync", error))?;
                *cleanup_on_drop = false;
                Ok(())
            }
        }
    }

    pub(crate) fn cleanup_after_delivery_failure(mut self) -> ApiKeyCleanup {
        match &mut self {
            Self::Stdout => ApiKeyCleanup::NotApplicable,
            Self::File {
                path,
                cleanup_on_drop,
                ..
            } => {
                let source = SecretSource::File(path.clone());
                let removal = fs::remove_file(path.as_path());
                // The explicit attempt owns the reported result. Do not let Drop retry after an
                // error and make the already-returned uncertainty classification inaccurate.
                *cleanup_on_drop = false;
                match removal {
                    Ok(()) => ApiKeyCleanup::Removed,
                    Err(error) => ApiKeyCleanup::Uncertain(ServiceApiKeyError::io(
                        source,
                        "remove incomplete",
                        error,
                    )),
                }
            }
        }
    }
}

fn sync_prepared_destination(path: &Path, file: &File) -> Result<(), ServiceApiKeyError> {
    let source = SecretSource::File(path.to_owned());
    file.sync_all()
        .map_err(|error| ServiceApiKeyError::io(source.clone(), "sync newly created", error))?;
    let parent_path = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(parent_path)
        .map_err(|error| {
            ServiceApiKeyError::io(source.clone(), "open parent directory for", error)
        })?;
    parent
        .sync_all()
        .map_err(|error| ServiceApiKeyError::io(source, "sync parent directory for", error))
}

impl Drop for ApiKeyDestination {
    fn drop(&mut self) {
        if let Self::File {
            path,
            cleanup_on_drop,
            ..
        } = self
            && *cleanup_on_drop
        {
            let _ = fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    #[test]
    fn service_api_key_validation_is_canonical_and_debug_is_redacted() {
        let key = ServiceApiKey::parse(Zeroizing::new(KEY.to_owned()))
            .expect("fixture key should be valid");
        assert_eq!(key.expose(), KEY);
        assert!(!format!("{key:?}").contains(KEY));

        for invalid in [
            "",
            " crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+",
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ] {
            assert!(ServiceApiKey::parse(Zeroizing::new(invalid.to_owned())).is_err());
        }
    }

    #[test]
    fn private_key_file_is_read_and_public_file_is_rejected() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("service.key");
        fs::write(&path, format!("{KEY}\n")).expect("fixture should be written");
        fs::set_permissions(&path, Permissions::from_mode(0o600))
            .expect("fixture mode should be set");

        let key = read_api_key(&path).expect("private file should be accepted");
        assert_eq!(key.expose(), KEY);

        fs::set_permissions(&path, Permissions::from_mode(0o644))
            .expect("fixture mode should be changed");
        assert!(matches!(
            read_api_key(&path),
            Err(ServiceApiKeyError::UnsafeFile { .. })
        ));
    }

    #[test]
    fn workload_token_is_zeroizing_and_rejects_whitespace() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("workload.token");
        fs::write(&path, "signed.workload.token\n").expect("fixture should be written");
        fs::set_permissions(&path, Permissions::from_mode(0o600))
            .expect("fixture mode should be set");

        let token = read_workload_token(&path).expect("private token file should be accepted");
        assert_eq!(token.expose(), "signed.workload.token");
        assert!(!format!("{token:?}").contains(token.expose()));

        fs::write(&path, "signed workload token").expect("fixture should be replaced");
        assert!(matches!(
            read_workload_token(&path),
            Err(ServiceApiKeyError::Invalid { .. })
        ));
    }

    #[test]
    fn api_key_destination_never_overwrites_and_removes_uncommitted_files() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let existing = directory.path().join("existing.key");
        fs::write(&existing, "keep").expect("fixture should be written");
        assert!(ApiKeyDestination::prepare(existing.to_str().unwrap()).is_err());
        assert_eq!(fs::read_to_string(&existing).unwrap(), "keep");

        let path = directory.path().join("service.key");
        {
            let destination = ApiKeyDestination::prepare(path.to_str().unwrap())
                .expect("destination should be reserved");
            assert!(path.exists());
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            drop(destination);
        }
        assert!(!path.exists());
    }

    fn destination_after_failed_write(path: &Path) -> ApiKeyDestination {
        let file = File::open(path).expect("read-only fixture handle should open");
        let mut destination = ApiKeyDestination::File {
            path: path.to_owned(),
            file,
            cleanup_on_drop: true,
        };
        let key = ServiceApiKey::parse(Zeroizing::new(KEY.to_owned()))
            .expect("fixture key should be valid");
        assert!(matches!(
            destination.write(&key),
            Err(ServiceApiKeyError::Io { .. })
        ));
        destination
    }

    #[test]
    fn api_key_destination_removes_a_file_after_delivery_write_failure() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("failed.key");
        fs::write(&path, b"").expect("fixture should be created");
        fs::set_permissions(&path, Permissions::from_mode(0o600))
            .expect("fixture mode should be set");
        let destination = destination_after_failed_write(&path);
        assert!(matches!(
            destination.cleanup_after_delivery_failure(),
            ApiKeyCleanup::Removed
        ));

        assert!(!path.exists());
    }

    #[test]
    fn delivery_failure_cleanup_is_uncertain_when_the_destination_cannot_be_unlinked() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("failed.key");
        fs::create_dir(&path).expect("cleanup obstacle should be created");
        let cleanup = destination_after_failed_write(&path).cleanup_after_delivery_failure();

        let ApiKeyCleanup::Uncertain(ServiceApiKeyError::Io {
            source_name,
            operation,
            source,
        }) = cleanup
        else {
            panic!("failed unlink should report uncertain cleanup");
        };
        assert!(matches!(source_name, SecretSource::File(source_path) if source_path == path));
        assert_eq!(operation, "remove incomplete");
        assert!(source.raw_os_error().is_some());
        assert!(path.is_dir());
    }
}
