use std::fmt;
use std::fs;
#[cfg(test)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Arc;

use url::Url;

use crate::execution::claude_code::ValidatedClaudeCodeInstallation;
use crate::execution::codex::ValidatedCodexInstallation;
use crate::execution::pi::ValidatedPiInstallation;
use crate::runner::credential::Credential;
use crate::runner::enrollment::{PendingCredential, RunnerStateAccess};

#[derive(Clone, Debug)]
pub(crate) struct AssignmentConfig {
    work_root: PathBuf,
}

impl AssignmentConfig {
    pub(crate) fn new(work_root: &Path) -> Result<Self, ConfigError> {
        Ok(Self {
            work_root: canonical_directory(work_root, ConfigError::WorkRootUnavailable)?,
        })
    }

    pub(crate) fn work_root(&self) -> &Path {
        &self.work_root
    }
}

#[derive(Clone)]
pub(crate) struct Config {
    endpoint: Url,
    credential: Credential,
    startup_pending: Option<PendingCredential>,
    state_access: Option<RunnerStateAccess>,
    control_socket_path: Option<PathBuf>,
    assignment: AssignmentConfig,
    #[cfg(test)]
    fixture_materialized_source: Option<(PathBuf, PathBuf)>,
    #[cfg(test)]
    fixture_work_root: Option<Arc<tempfile::TempDir>>,
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
    codex_installation: Option<ValidatedCodexInstallation>,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("endpoint", &self.endpoint)
            .field("credential", &self.credential)
            .field(
                "agent_capable",
                &(self.pi_installation.is_some()
                    || self.claude_code_installation.is_some()
                    || self.codex_installation.is_some()),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ConfigError {
    InvalidOperatorConfiguration,
    #[cfg(test)]
    InvalidGatewayUrl,
    #[cfg(test)]
    InsecureGatewayUrl,
    WorkRootUnavailable,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOperatorConfiguration => {
                formatter.write_str("runner operator configuration or protected state is invalid")
            }
            #[cfg(test)]
            Self::InvalidGatewayUrl => formatter.write_str("invalid runner gateway URL"),
            #[cfg(test)]
            Self::InsecureGatewayUrl => {
                formatter.write_str("insecure runner gateway URL is not allowed")
            }
            Self::WorkRootUnavailable => {
                formatter.write_str("runner work root is not an existing directory")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    pub(crate) fn load(path: &Path) -> Result<Self, ConfigError> {
        let enrolled = crate::runner::enrollment::load_runner_service_configuration(path)
            .map_err(|_| ConfigError::InvalidOperatorConfiguration)?;
        let credential = Credential::from_enrolled_state(
            &enrolled.runner_id,
            &enrolled.credential_id,
            &enrolled.credential_secret,
        )
        .map_err(|_| ConfigError::InvalidOperatorConfiguration)?;
        let endpoint = Url::parse(&enrolled.connection_url)
            .map_err(|_| ConfigError::InvalidOperatorConfiguration)?;
        if let Some(pending) = enrolled.pending_credential.clone() {
            let _ = validate_pending_credential(pending)?;
        }
        let startup_pending = enrolled.pending_credential;
        let assignment = AssignmentConfig::new(&enrolled.work_root)?;
        Ok(Self {
            endpoint,
            credential,
            startup_pending,
            state_access: Some(enrolled.state_access),
            control_socket_path: Some(enrolled.control_socket_path),
            assignment,
            #[cfg(test)]
            fixture_materialized_source: None,
            #[cfg(test)]
            fixture_work_root: None,
            pi_installation: None,
            claude_code_installation: None,
            codex_installation: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn new(
        gateway_url: &str,
        credential: Credential,
        allow_insecure_http: bool,
        assignment: AssignmentConfig,
    ) -> Result<Self, ConfigError> {
        let endpoint = Url::parse(gateway_url).map_err(|_| ConfigError::InvalidGatewayUrl)?;
        if endpoint.username() != ""
            || endpoint.password().is_some()
            || endpoint.host_str().is_none()
        {
            return Err(ConfigError::InvalidGatewayUrl);
        }
        match endpoint.scheme() {
            "wss" => {}
            "ws" if allow_insecure_http && crate::runner::is_loopback(&endpoint) => {}
            "ws" => return Err(ConfigError::InsecureGatewayUrl),
            _ => return Err(ConfigError::InvalidGatewayUrl),
        }
        Ok(Self {
            endpoint,
            credential,
            startup_pending: None,
            state_access: None,
            control_socket_path: None,
            assignment,
            fixture_materialized_source: None,
            fixture_work_root: None,
            pi_installation: None,
            claude_code_installation: None,
            codex_installation: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn fixture(
        gateway_url: &str,
        credential: Credential,
        allow_insecure_http: bool,
    ) -> Result<Self, ConfigError> {
        let manifest = canonical_directory(
            Path::new(env!("CARGO_MANIFEST_DIR")),
            ConfigError::WorkRootUnavailable,
        )?;
        // The test boundary preserves TMPDIR, which may be inside this checkout.
        // Runner fixtures must not follow it back into the source tree.
        let work_root =
            Arc::new(tempfile::tempdir_in("/tmp").map_err(|_| ConfigError::WorkRootUnavailable)?);
        fs::set_permissions(work_root.path(), fs::Permissions::from_mode(0o700))
            .map_err(|_| ConfigError::WorkRootUnavailable)?;
        let assignment = AssignmentConfig::new(work_root.path())?;
        if assignment.work_root().starts_with(&manifest) {
            return Err(ConfigError::WorkRootUnavailable);
        }
        Self::new(gateway_url, credential, allow_insecure_http, assignment).map(|mut config| {
            config.fixture_work_root = Some(work_root);
            config.with_materialized_source_fixture(
                manifest.join("tests"),
                PathBuf::from("missing-workflow-fixture.yaml"),
            )
        })
    }

    pub(crate) fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    pub(crate) fn credential(&self) -> &Credential {
        &self.credential
    }

    pub(crate) fn startup_pending(&self) -> Option<&PendingCredential> {
        self.startup_pending.as_ref()
    }

    pub(crate) fn state_access(&self) -> Option<&RunnerStateAccess> {
        self.state_access.as_ref()
    }

    pub(crate) fn control_socket_path(&self) -> Option<&Path> {
        self.control_socket_path.as_deref()
    }

    pub(crate) fn with_pending_credential(
        &self,
        pending: PendingCredential,
    ) -> Result<Self, ConfigError> {
        let (endpoint, credential) = validate_pending_credential(pending)?;
        let mut config = self.clone();
        config.endpoint = endpoint;
        config.credential = credential;
        config.startup_pending = None;
        Ok(config)
    }

    pub(crate) fn assignment(&self) -> &AssignmentConfig {
        &self.assignment
    }

    #[cfg(test)]
    pub(crate) fn with_materialized_source_fixture(
        mut self,
        source_root: PathBuf,
        workflow_path: PathBuf,
    ) -> Self {
        self.fixture_materialized_source = Some((source_root, workflow_path));
        self
    }

    #[cfg(test)]
    pub(crate) fn fixture_materialized_source(&self) -> Option<&(PathBuf, PathBuf)> {
        self.fixture_materialized_source.as_ref()
    }

    pub(crate) fn pi_installation(&self) -> Option<&ValidatedPiInstallation> {
        self.pi_installation.as_ref()
    }

    pub(crate) fn with_pi_installation(mut self, installation: ValidatedPiInstallation) -> Self {
        self.pi_installation = Some(installation);
        self
    }

    // Runner service configuration snapshots and workflow admission intentionally retain
    // parallel typed builders; sharing their containers would merge separate lifecycle layers.
    // jscpd:ignore-start
    pub(crate) fn claude_code_installation(&self) -> Option<&ValidatedClaudeCodeInstallation> {
        self.claude_code_installation.as_ref()
    }

    pub(crate) fn with_claude_code_installation(
        mut self,
        installation: ValidatedClaudeCodeInstallation,
    ) -> Self {
        self.claude_code_installation = Some(installation);
        self
    }

    pub(crate) fn codex_installation(&self) -> Option<&ValidatedCodexInstallation> {
        self.codex_installation.as_ref()
    }

    pub(crate) fn with_codex_installation(
        mut self,
        installation: ValidatedCodexInstallation,
    ) -> Self {
        self.codex_installation = Some(installation);
        self
    }
    // jscpd:ignore-end
}

fn validate_pending_credential(
    pending: PendingCredential,
) -> Result<(Url, Credential), ConfigError> {
    let endpoint = Url::parse(&pending.connection_url)
        .map_err(|_| ConfigError::InvalidOperatorConfiguration)?;
    let credential = Credential::from_enrolled_state(
        &pending.runner_id,
        &pending.credential_id,
        &pending.credential_secret,
    )
    .map_err(|_| ConfigError::InvalidOperatorConfiguration)?;
    Ok((endpoint, credential))
}

fn canonical_directory(path: &Path, failure: ConfigError) -> Result<PathBuf, ConfigError> {
    let canonical = fs::canonicalize(path).map_err(|_| failure)?;
    if !fs::metadata(&canonical).is_ok_and(|metadata| metadata.is_dir()) {
        return Err(failure);
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use tempfile::TempDir;

    use super::{AssignmentConfig, Config, ConfigError};
    use crate::runner::credential::test_credential;
    use crate::runner::service::workspace::WorkRootLease;

    const FIXTURE_BOOT_ID: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";

    fn assignment_fixture(temporary: &TempDir) -> AssignmentConfig {
        let work = temporary.path().join("work");
        fs::create_dir_all(&work).unwrap();
        AssignmentConfig::new(&work).unwrap()
    }

    #[test]
    fn permits_wss_and_explicit_loopback_ws_only() {
        let temporary = tempfile::tempdir().unwrap();
        assert!(
            Config::new(
                "wss://gateway.example.test/v1/runner/connect",
                test_credential(),
                false,
                assignment_fixture(&temporary),
            )
            .is_ok()
        );
        assert!(
            Config::new(
                "ws://127.0.0.1:8081/v1/runner/connect",
                test_credential(),
                true,
                assignment_fixture(&temporary),
            )
            .is_ok()
        );
        assert_eq!(
            Config::new(
                "ws://127.0.0.1:8081/v1/runner/connect",
                test_credential(),
                false,
                assignment_fixture(&temporary),
            )
            .unwrap_err(),
            ConfigError::InsecureGatewayUrl,
        );
        assert_eq!(
            Config::new(
                "ws://gateway.example.test/v1/runner/connect",
                test_credential(),
                true,
                assignment_fixture(&temporary),
            )
            .unwrap_err(),
            ConfigError::InsecureGatewayUrl,
        );
    }

    #[test]
    fn requires_an_existing_work_root() {
        let temporary = tempfile::tempdir().unwrap();
        assert_eq!(
            AssignmentConfig::new(&temporary.path().join("missing")).unwrap_err(),
            ConfigError::WorkRootUnavailable,
        );
    }

    #[test]
    fn fixture_boot_root_is_removed_with_temporary_config() {
        let source_root = fs::canonicalize(env!("CARGO_MANIFEST_DIR")).unwrap();
        let boot_path = {
            let config = Config::fixture(
                "ws://127.0.0.1:1/v1/runner/connect",
                test_credential(),
                true,
            )
            .unwrap();
            let work_root = config.assignment().work_root().to_owned();
            assert!(
                !work_root.starts_with(&source_root),
                "fixture work root leaked into {}",
                source_root.display()
            );

            let lease = WorkRootLease::acquire(&work_root, FIXTURE_BOOT_ID).unwrap();
            let boot_path = lease.boot_path().to_owned();
            assert_eq!(boot_path, work_root.join(FIXTURE_BOOT_ID));
            assert!(boot_path.is_dir());
            boot_path
        };

        assert!(!boot_path.exists());
    }
}
