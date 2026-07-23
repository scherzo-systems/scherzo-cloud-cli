use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

const MAX_CREDENTIAL_FILE_BYTES: u64 = 256;
const NOFOLLOW_FLAG: i32 = rustix::fs::OFlags::NOFOLLOW.bits() as i32;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum CredentialError {
    InvalidFile,
}

impl fmt::Display for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFile => formatter.write_str("invalid runner credential file"),
        }
    }
}

impl std::error::Error for CredentialError {}

// Credential is runner-only machine authentication material. Its Debug output
// deliberately omits the bearer value.
pub(crate) struct Credential {
    runner_id: String,
    value: String,
}

impl fmt::Debug for Credential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credential")
            .field("runner_id", &self.runner_id)
            .field("value", &"[redacted]")
            .finish()
    }
}

impl Credential {
    pub(crate) fn load(path: &Path) -> Result<Self, CredentialError> {
        let mut file = open_private_file(path)?;
        let mut bytes = Vec::new();
        file.by_ref()
            .take(MAX_CREDENTIAL_FILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| CredentialError::InvalidFile)?;
        if bytes.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
            return Err(CredentialError::InvalidFile);
        }
        if bytes.last() == Some(&b'\n') {
            bytes.pop();
        }
        let value = std::str::from_utf8(&bytes).map_err(|_| CredentialError::InvalidFile)?;
        let (runner_id, secret) = value.split_once('.').ok_or(CredentialError::InvalidFile)?;
        if secret.contains('.') || !valid_runner_id(runner_id) || !valid_secret(secret) {
            return Err(CredentialError::InvalidFile);
        }
        Ok(Self {
            runner_id: runner_id.to_owned(),
            value: value.to_owned(),
        })
    }

    pub(crate) fn runner_id(&self) -> &str {
        &self.runner_id
    }

    pub(crate) fn bearer_value(&self) -> &str {
        &self.value
    }
}

fn open_private_file(path: &Path) -> Result<File, CredentialError> {
    let initial = fs::symlink_metadata(path).map_err(|_| CredentialError::InvalidFile)?;
    validate_private_file(&initial)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
        .map_err(|_| CredentialError::InvalidFile)?;
    let opened = file.metadata().map_err(|_| CredentialError::InvalidFile)?;
    validate_private_file(&opened)?;
    Ok(file)
}

fn validate_private_file(metadata: &fs::Metadata) -> Result<(), CredentialError> {
    if !metadata.file_type().is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(CredentialError::InvalidFile);
    }
    Ok(())
}

fn valid_runner_id(value: &str) -> bool {
    let Some(suffix) = value.strip_prefix("rnr_") else {
        return false;
    };
    let bytes = suffix.as_bytes();
    bytes.len() == 26
        && matches!(bytes.first(), Some(b'0'..=b'7'))
        && bytes[1..].iter().all(|byte| {
            matches!(
                byte,
                b'0'..=b'9' | b'a'..=b'h' | b'j'..=b'k' | b'm'..=b'n' | b'p'..=b't' | b'v'..=b'z'
            )
        })
}

fn valid_secret(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[cfg(test)]
pub(crate) fn test_credential() -> Credential {
    use std::os::unix::fs::PermissionsExt;

    const VALUE: &str =
        "rnr_01k0z6r1w8f4jy2m7q9v3x5abd.abcdefghijklmnopqrstuvwxyzABCDEFG-012345678";
    let directory = tempfile::TempDir::new().expect("create credential directory");
    let path = directory.path().join("runner.credential");
    fs::write(&path, VALUE).expect("write credential");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("set credential mode");
    Credential::load(&path).expect("load credential")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;

    use super::{Credential, CredentialError};

    const VALUE: &str =
        "rnr_01k0z6r1w8f4jy2m7q9v3x5abd.abcdefghijklmnopqrstuvwxyzABCDEFG-012345678";

    #[test]
    fn reads_a_private_credential_without_exposing_its_value_in_debug() {
        let directory = TempDir::new().expect("create temporary directory");
        let path = directory.path().join("runner.credential");
        write_credential(&path, VALUE, 0o600);

        let credential = Credential::load(&path).expect("load credential");

        assert_eq!(credential.runner_id(), "rnr_01k0z6r1w8f4jy2m7q9v3x5abd");
        assert_eq!(credential.bearer_value(), VALUE);
        assert!(!format!("{credential:?}").contains(VALUE));
    }

    #[test]
    fn rejects_unsafe_or_malformed_files() {
        let oversized = "x".repeat(257);
        for (name, contents, mode) in [
            ("group-readable", VALUE, 0o640),
            ("oversized", oversized.as_str(), 0o600),
            ("malformed", "rnr_bad.not-a-credential", 0o600),
            (
                "whitespace",
                "rnr_01k0z6r1w8f4jy2m7q9v3x5abd.abcdefghijklmnopqrstuvwxyzABCDEFG-012345678 ",
                0o600,
            ),
        ] {
            let directory = TempDir::new().expect("create temporary directory");
            let path = directory.path().join(name);
            write_credential(&path, contents, mode);
            assert!(matches!(
                Credential::load(&path),
                Err(CredentialError::InvalidFile)
            ));
        }
    }

    #[test]
    fn rejects_a_credential_symlink() {
        let directory = TempDir::new().expect("create temporary directory");
        let target = directory.path().join("target");
        let link = directory.path().join("link");
        write_credential(&target, VALUE, 0o600);
        std::os::unix::fs::symlink(&target, &link).expect("create credential symlink");

        assert!(matches!(
            Credential::load(&link),
            Err(CredentialError::InvalidFile)
        ));
    }

    fn write_credential(path: &std::path::Path, value: &str, mode: u32) {
        fs::write(path, value).expect("write credential");
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("set credential mode");
    }
}
