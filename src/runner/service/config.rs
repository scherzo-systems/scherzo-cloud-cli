use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};

use url::Url;

use crate::execution::claude_code::ValidatedClaudeCodeInstallation;
use crate::execution::pi::ValidatedPiInstallation;
use crate::runner::credential::Credential;
use crate::runner::enrollment::{PendingCredential, RunnerStateAccess};

#[derive(Clone)]
pub(crate) struct AssignmentConfig {
    workflow_id: String,
    workflow_source_root: PathBuf,
    workflow_path: PathBuf,
    work_root: PathBuf,
}

impl fmt::Debug for AssignmentConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssignmentConfig")
            .field("workflow_id", &self.workflow_id)
            .finish_non_exhaustive()
    }
}

impl AssignmentConfig {
    pub(crate) fn new(
        workflow_id: String,
        workflow_source_root: &Path,
        workflow_path: &Path,
        work_root: &Path,
    ) -> Result<Self, ConfigError> {
        if workflow_id
            .parse::<crate::runner_protocol::generated::WorkflowId>()
            .is_err()
        {
            return Err(ConfigError::InvalidWorkflowId);
        }
        let workflow_source_root = canonical_directory(
            workflow_source_root,
            ConfigError::WorkflowSourceRootUnavailable,
        )?;
        let work_root = canonical_directory(work_root, ConfigError::WorkRootUnavailable)?;
        if workflow_source_root.starts_with(&work_root)
            || work_root.starts_with(&workflow_source_root)
        {
            return Err(ConfigError::OverlappingRoots);
        }
        let workflow_path =
            normalized_relative_path(workflow_path).ok_or(ConfigError::InvalidWorkflowPath)?;
        Ok(Self {
            workflow_id,
            workflow_source_root,
            workflow_path,
            work_root,
        })
    }

    pub(crate) fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    pub(crate) fn workflow_source_root(&self) -> &Path {
        &self.workflow_source_root
    }

    pub(crate) fn workflow_path(&self) -> &Path {
        &self.workflow_path
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
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("endpoint", &self.endpoint)
            .field("credential", &self.credential)
            .field("workflow_id", &self.assignment.workflow_id)
            .field(
                "agent_capable",
                &(self.pi_installation.is_some() || self.claude_code_installation.is_some()),
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
    InvalidWorkflowId,
    WorkflowSourceRootUnavailable,
    InvalidWorkflowPath,
    WorkRootUnavailable,
    OverlappingRoots,
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
            Self::InvalidWorkflowId => formatter.write_str("invalid registered workflow ID"),
            Self::WorkflowSourceRootUnavailable => {
                formatter.write_str("workflow source root is not an existing directory")
            }
            Self::InvalidWorkflowPath => {
                formatter.write_str("workflow path must remain within the workflow source root")
            }
            Self::WorkRootUnavailable => {
                formatter.write_str("runner work root is not an existing directory")
            }
            Self::OverlappingRoots => formatter.write_str(
                "workflow source root and runner work root must be separate directory trees",
            ),
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
        let assignment = AssignmentConfig::new(
            enrolled.workflow_id,
            &enrolled.workflow_source_root,
            &enrolled.workflow_path,
            &enrolled.work_root,
        )?;
        Ok(Self {
            endpoint,
            credential,
            startup_pending,
            state_access: Some(enrolled.state_access),
            control_socket_path: Some(enrolled.control_socket_path),
            assignment,
            pi_installation: None,
            claude_code_installation: None,
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
            pi_installation: None,
            claude_code_installation: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn fixture(
        gateway_url: &str,
        credential: Credential,
        allow_insecure_http: bool,
    ) -> Result<Self, ConfigError> {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let assignment = AssignmentConfig::new(
            "wfl_01k0z6r1w8f4jy2m7q9v3x5abs".to_owned(),
            &manifest.join("schemas"),
            Path::new("workflow-v1.schema.json"),
            &manifest.join("tests"),
        )?;
        Self::new(gateway_url, credential, allow_insecure_http, assignment)
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

    pub(crate) fn pi_installation(&self) -> Option<&ValidatedPiInstallation> {
        self.pi_installation.as_ref()
    }

    pub(crate) fn with_pi_installation(mut self, installation: ValidatedPiInstallation) -> Self {
        self.pi_installation = Some(installation);
        self
    }

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

fn normalized_relative_path(path: &Path) -> Option<PathBuf> {
    if path.is_absolute() {
        return None;
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                parts.pop()?;
            }
            Component::Normal(part) => parts.push(part.to_owned()),
            Component::Prefix(_) | Component::RootDir => return None,
        }
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use tempfile::TempDir;

    use super::{AssignmentConfig, Config, ConfigError};
    use crate::runner::credential::test_credential;

    fn assignment_fixture(temporary: &TempDir) -> AssignmentConfig {
        let source = temporary.path().join("source");
        let work = temporary.path().join("work");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&work).unwrap();
        AssignmentConfig::new(
            "wfl_01k0z6r1w8f4jy2m7q9v3x5abr".to_owned(),
            &source,
            Path::new("workflows/../workflow.yaml"),
            &work,
        )
        .unwrap()
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
    fn validates_the_registered_workflow_mapping_without_resolving_it() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let work = temporary.path().join("work");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&work).unwrap();

        let mapping = AssignmentConfig::new(
            "wfl_01k0z6r1w8f4jy2m7q9v3x5abr".to_owned(),
            &source,
            Path::new("missing/../workflow.yaml"),
            &work,
        )
        .unwrap();
        assert_eq!(mapping.workflow_path(), Path::new("workflow.yaml"));

        assert_eq!(
            AssignmentConfig::new(
                "not-a-workflow".to_owned(),
                &source,
                Path::new("workflow.yaml"),
                &work,
            )
            .unwrap_err(),
            ConfigError::InvalidWorkflowId,
        );
        assert_eq!(
            AssignmentConfig::new(
                "wfl_01k0z6r1w8f4jy2m7q9v3x5abr".to_owned(),
                &source,
                Path::new("../workflow.yaml"),
                &work,
            )
            .unwrap_err(),
            ConfigError::InvalidWorkflowPath,
        );
        assert_eq!(
            AssignmentConfig::new(
                "wfl_01k0z6r1w8f4jy2m7q9v3x5abr".to_owned(),
                &source,
                Path::new("workflow.yaml"),
                &source,
            )
            .unwrap_err(),
            ConfigError::OverlappingRoots,
        );
    }
}
