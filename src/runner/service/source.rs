use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use nix::fcntl::{FcntlArg, FdFlag, fcntl};
use reqwest::StatusCode;
use serde::Deserialize;
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use url::Url;
use zeroize::Zeroize as _;

use crate::execution::workflow::admission::EnvironmentSnapshot;
use crate::execution::workflow::artifact::CaptureCancellation;
use crate::execution::workflow::git_capture::CloudGitCaptureProjection;
use crate::execution::workflow::resolution::{self, ResolvedWorkflow};
use crate::process::ManagedProcessGroup;
use crate::runner::credential::Credential;
use crate::runner_protocol::ExecutionSpecV1RunnerProjection;

const SOURCE_CREDENTIAL_RESPONSE_LIMIT: usize = 128 * 1024;
const PROVIDER_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const GIT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const FETCH_DIAGNOSTIC_LIMIT: u64 = 4 * 1024;
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub(super) trait SourceCredentialBroker: Send + Sync {
    fn issue(
        &self,
        assignment_id: &str,
        cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CredentialBrokerFailure {
    Unavailable,
    Fenced,
    InvalidResponse,
}

pub(super) struct ProviderCredential {
    repository_url: Arc<str>,
    token: ProviderSecret,
    expires_at: OffsetDateTime,
}

impl std::fmt::Debug for ProviderCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderCredential")
            .field("token", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

struct ProviderSecret(Vec<u8>);

impl Drop for ProviderSecret {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Clone)]
pub(super) struct HttpSourceCredentialBroker {
    endpoint: Url,
    runner_credential: Credential,
    boot_id: Arc<str>,
}

impl HttpSourceCredentialBroker {
    pub(super) fn new(
        endpoint: &Url,
        runner_credential: &Credential,
        boot_id: &str,
    ) -> Result<Self, ()> {
        let mut endpoint = endpoint.clone();
        match endpoint.scheme() {
            "wss" => endpoint.set_scheme("https")?,
            "ws" => endpoint.set_scheme("http")?,
            _ => return Err(()),
        }
        endpoint.set_path("/v1/runner/source-credentials");
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        Ok(Self {
            endpoint,
            runner_credential: runner_credential.clone(),
            boot_id: Arc::from(boot_id),
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CredentialResponse {
    schema_version: u64,
    repository_url: String,
    token: String,
    expires_at: String,
}

struct CredentialRequest<'a> {
    assignment_id: &'a str,
}

impl HttpSourceCredentialBroker {
    fn issue_current(
        &self,
        request: CredentialRequest<'_>,
        cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        ensure_broker_current(cancellation)?;
        crate::tls::install_provider();
        let client = reqwest::Client::builder()
            .timeout(PROVIDER_OPERATION_TIMEOUT)
            .build()
            .map_err(|_| CredentialBrokerFailure::Unavailable)?;
        let request = client
            .post(self.endpoint.clone())
            .bearer_auth(self.runner_credential.bearer_value())
            .json(&serde_json::json!({
                "schemaVersion": 1,
                "bootId": self.boot_id.as_ref(),
                "assignmentId": request.assignment_id,
            }));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| CredentialBrokerFailure::Unavailable)?;
        let (status, encoded) = runtime.block_on(async {
            let mut response = tokio::select! {
                result = request.send() => {
                    result.map_err(|_| CredentialBrokerFailure::Unavailable)?
                }
                () = wait_for_cancellation(cancellation) => {
                    return Err(CredentialBrokerFailure::Fenced);
                }
            };
            let status = response.status();
            let mut encoded = ProviderSecret(Vec::new());
            if status == StatusCode::OK {
                loop {
                    let chunk = tokio::select! {
                        result = response.chunk() => {
                            result.map_err(|_| CredentialBrokerFailure::Unavailable)?
                        }
                        () = wait_for_cancellation(cancellation) => {
                            return Err(CredentialBrokerFailure::Fenced);
                        }
                    };
                    let Some(chunk) = chunk else {
                        break;
                    };
                    if encoded.0.len().saturating_add(chunk.len())
                        > SOURCE_CREDENTIAL_RESPONSE_LIMIT
                    {
                        return Err(CredentialBrokerFailure::InvalidResponse);
                    }
                    encoded.0.extend_from_slice(&chunk);
                }
            }
            Ok((status, encoded))
        })?;
        if status == StatusCode::CONFLICT {
            return Err(CredentialBrokerFailure::Fenced);
        }
        if status != StatusCode::OK {
            return Err(CredentialBrokerFailure::Unavailable);
        }
        ensure_broker_current(cancellation)?;
        let parsed: CredentialResponse = serde_json::from_slice(&encoded.0)
            .map_err(|_| CredentialBrokerFailure::InvalidResponse)?;
        let CredentialResponse {
            schema_version,
            repository_url,
            token,
            expires_at,
        } = parsed;
        let token = ProviderSecret(token.into_bytes());
        if schema_version != 1
            || token.0.is_empty()
            || token.0.len() > 64 * 1024
            || !token.0.iter().all(|byte| (0x21..=0x7e).contains(byte))
        {
            return Err(CredentialBrokerFailure::InvalidResponse);
        }
        let utc_spelling = expires_at.ends_with('Z');
        let expires_at = OffsetDateTime::parse(&expires_at, &Rfc3339)
            .ok()
            .filter(|value| {
                utc_spelling
                    && value.offset() == UtcOffset::UTC
                    && *value > crate::timing::utc_now()
            })
            .ok_or(CredentialBrokerFailure::InvalidResponse)?;
        let repository_url = validate_repository_url(&repository_url)?;
        Ok(ProviderCredential {
            repository_url: Arc::from(repository_url),
            token,
            expires_at,
        })
    }
}

impl SourceCredentialBroker for HttpSourceCredentialBroker {
    fn issue(
        &self,
        assignment_id: &str,
        cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        ensure_broker_current(cancellation)?;
        let broker = self.clone();
        let assignment_id = assignment_id.to_owned();
        let worker_cancellation = cancellation.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("runner-source-credential".to_owned())
            .spawn(move || {
                let result = broker.issue_current(
                    CredentialRequest {
                        assignment_id: &assignment_id,
                    },
                    &worker_cancellation,
                );
                let _ = sender.send(result);
            })
            .map_err(|_| CredentialBrokerFailure::Unavailable)?;
        loop {
            ensure_broker_current(cancellation)?;
            match receiver.try_recv() {
                Ok(result) => {
                    ensure_broker_current(cancellation)?;
                    return result;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    crate::timing::sleep(PROCESS_POLL_INTERVAL);
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(CredentialBrokerFailure::Unavailable);
                }
            }
        }
    }
}

fn ensure_broker_current(
    cancellation: &CaptureCancellation,
) -> Result<(), CredentialBrokerFailure> {
    if cancellation.is_cancelled() {
        Err(CredentialBrokerFailure::Fenced)
    } else {
        Ok(())
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "the private credential-request runtime must yield while polling its fence"
)]
async fn wait_for_cancellation(cancellation: &CaptureCancellation) {
    while !cancellation.is_cancelled() {
        tokio::time::sleep(PROCESS_POLL_INTERVAL).await;
    }
}

fn validate_repository_url(raw: &str) -> Result<String, CredentialBrokerFailure> {
    let mut parsed = Url::parse(raw).map_err(|_| CredentialBrokerFailure::InvalidResponse)?;
    let loopback_http = parsed.scheme() == "http"
        && parsed
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    #[cfg(test)]
    let fixture_file = parsed.scheme() == "file";
    #[cfg(not(test))]
    let fixture_file = false;
    if parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !(parsed.scheme() == "https" || loopback_http || fixture_file)
        || parsed.path().is_empty()
    {
        return Err(CredentialBrokerFailure::InvalidResponse);
    }
    parsed.set_query(None);
    parsed.set_fragment(None);
    Ok(parsed.to_string())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MaterializationFailure {
    ProviderUnavailable,
    AssignmentFenced,
    EnvironmentUnavailable,
    CommitUnavailable,
    CommitMismatch,
    UnsupportedObjectFormat,
    DirtyCheckout,
    WorkflowUnavailable,
    WorkflowDigestMismatch,
}

pub(super) struct MaterializedSource {
    pub(super) workflow: ResolvedWorkflow,
    pub(super) execution_root: PathBuf,
    pub(super) git_capture: Option<CloudGitCaptureProjection>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MaterializationRequest {
    pub(super) repository_connection_id: String,
    object_format: String,
    commit_oid: String,
    workflow_path: String,
    workflow_source_closure_digest: WorkflowSourceClosureDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkflowSourceClosureDigest {
    algorithm: String,
    value: String,
}

impl MaterializationRequest {
    pub(super) fn from_validated(execution_spec: &ExecutionSpecV1RunnerProjection) -> Self {
        let workflow = &execution_spec.workflow_definition_source;
        Self {
            repository_connection_id: execution_spec
                .primary_workspace_source
                .repository_connection_id
                .clone(),
            object_format: workflow.object_format.clone(),
            commit_oid: workflow.commit_oid.clone(),
            workflow_path: workflow.workflow_path.clone(),
            workflow_source_closure_digest: WorkflowSourceClosureDigest {
                algorithm: workflow.workflow_source_closure_digest.algorithm.clone(),
                value: workflow.workflow_source_closure_digest.value.clone(),
            },
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "source materialization binds explicit assignment authority and three confined roots"
)]
pub(super) fn materialize(
    broker: Arc<dyn SourceCredentialBroker>,
    environment: &EnvironmentSnapshot,
    assignment_id: &str,
    request: &MaterializationRequest,
    cancellation: &CaptureCancellation,
    source_root: &Path,
    ordinary_execution_root: &Path,
    private_root: &Path,
) -> Result<MaterializedSource, MaterializationFailure> {
    if request.object_format != "sha1" {
        return Err(MaterializationFailure::UnsupportedObjectFormat);
    }
    ensure_current(cancellation)?;
    let source_metadata = fs::symlink_metadata(source_root)
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    if !source_metadata.file_type().is_dir()
        || fs::read_dir(source_root)
            .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?
            .next()
            .is_some()
    {
        return Err(MaterializationFailure::EnvironmentUnavailable);
    }
    let credential = broker
        .issue(assignment_id, cancellation)
        .map_err(map_broker_failure)?;
    ensure_current(cancellation)?;
    let repository_url = Arc::clone(&credential.repository_url);

    run_git_status(
        source_root,
        environment,
        &["init", "--quiet", "--object-format=sha1"],
        None,
        cancellation,
    )?;
    run_git_status(
        source_root,
        environment,
        &["remote", "add", "origin", repository_url.as_ref()],
        None,
        cancellation,
    )?;
    {
        let askpass = EphemeralAskpass::create(private_root, &credential.token.0)?;
        verify_remote_object_format(
            source_root,
            environment,
            private_root,
            &askpass,
            cancellation,
        )?;
        fetch_pinned_commit(
            source_root,
            environment,
            private_root,
            &askpass,
            &request.commit_oid,
            cancellation,
        )?;
    }
    drop(credential);
    ensure_current(cancellation)?;
    verify_fetched_commit(source_root, environment, request, cancellation)?;
    checkout_pinned_commit(source_root, environment, &request.commit_oid, cancellation)?;

    verify_checkout(source_root, environment, request, cancellation)?;
    ensure_current(cancellation)?;
    let workflow = resolution::resolve(source_root, Path::new(&request.workflow_path))
        .map_err(|_| MaterializationFailure::WorkflowUnavailable)?;
    ensure_current(cancellation)?;
    if workflow.source.workflow_path != request.workflow_path {
        return Err(MaterializationFailure::WorkflowUnavailable);
    }
    if workflow.content_digest.algorithm.as_str()
        != request.workflow_source_closure_digest.algorithm
        || workflow.content_digest.value != request.workflow_source_closure_digest.value
    {
        return Err(MaterializationFailure::WorkflowDigestMismatch);
    }

    if workflow.requires_git_capture() {
        fs::remove_dir(ordinary_execution_root)
            .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
        let git_capture = CloudGitCaptureProjection::new(
            Arc::from(request.commit_oid.as_str()),
            Arc::from(request.workflow_source_closure_digest.value.as_str()),
            cancellation.clone(),
        );
        Ok(MaterializedSource {
            workflow,
            execution_root: source_root.to_owned(),
            git_capture: Some(git_capture),
        })
    } else {
        Ok(MaterializedSource {
            workflow,
            execution_root: ordinary_execution_root.to_owned(),
            git_capture: None,
        })
    }
}

fn ensure_current(cancellation: &CaptureCancellation) -> Result<(), MaterializationFailure> {
    if cancellation.is_cancelled() {
        Err(MaterializationFailure::AssignmentFenced)
    } else {
        Ok(())
    }
}

fn map_broker_failure(failure: CredentialBrokerFailure) -> MaterializationFailure {
    match failure {
        CredentialBrokerFailure::Fenced => MaterializationFailure::AssignmentFenced,
        CredentialBrokerFailure::Unavailable | CredentialBrokerFailure::InvalidResponse => {
            MaterializationFailure::ProviderUnavailable
        }
    }
}

fn verify_remote_object_format(
    root: &Path,
    environment: &EnvironmentSnapshot,
    private_root: &Path,
    askpass: &EphemeralAskpass,
    cancellation: &CaptureCancellation,
) -> Result<(), MaterializationFailure> {
    let mut captured = tempfile::tempfile_in(private_root)
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    let output = captured
        .try_clone()
        .map(Stdio::from)
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    let mut command = git_command(
        root,
        environment,
        &["ls-remote", "origin", "HEAD"],
        Some(askpass),
    );
    command
        .stdin(Stdio::null())
        .stdout(output)
        .stderr(Stdio::null());
    ensure_git_success(
        &mut command,
        cancellation,
        MaterializationFailure::ProviderUnavailable,
    )?;
    captured
        .rewind()
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    let mut bytes = Vec::new();
    captured
        .take(129)
        .read_to_end(&mut bytes)
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    let oid = bytes
        .strip_suffix(b"\tHEAD\n")
        .ok_or(MaterializationFailure::ProviderUnavailable)?;
    if oid.len() == 40 && oid.iter().all(u8::is_ascii_hexdigit) {
        Ok(())
    } else if oid.len() == 64 && oid.iter().all(u8::is_ascii_hexdigit) {
        Err(MaterializationFailure::UnsupportedObjectFormat)
    } else {
        Err(MaterializationFailure::ProviderUnavailable)
    }
}

fn fetch_pinned_commit(
    root: &Path,
    environment: &EnvironmentSnapshot,
    private_root: &Path,
    askpass: &EphemeralAskpass,
    commit_oid: &str,
    cancellation: &CaptureCancellation,
) -> Result<(), MaterializationFailure> {
    run_git_status_with_failure(
        root,
        environment,
        &[
            "fetch",
            "--quiet",
            "--force",
            "--no-write-fetch-head",
            "origin",
            "+refs/heads/*:refs/remotes/origin/*",
            "+refs/tags/*:refs/tags/*",
        ],
        Some(askpass),
        cancellation,
        MaterializationFailure::ProviderUnavailable,
    )?;
    if commit_is_available(root, environment, commit_oid, cancellation)? {
        return Ok(());
    }

    let mut diagnostic = tempfile::tempfile_in(private_root)
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    let stderr = diagnostic
        .try_clone()
        .map(Stdio::from)
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    let mut command = git_command(
        root,
        environment,
        &[
            "fetch",
            "--quiet",
            "--force",
            "--no-write-fetch-head",
            "origin",
            commit_oid,
        ],
        Some(askpass),
    );
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(stderr);
    let status = run_managed_source_git(
        &mut command,
        cancellation,
        MaterializationFailure::ProviderUnavailable,
    )?;
    if status.success() {
        return Ok(());
    }
    let diagnostic_length = diagnostic
        .metadata()
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?
        .len();
    diagnostic
        .seek(SeekFrom::Start(
            diagnostic_length.saturating_sub(FETCH_DIAGNOSTIC_LIMIT),
        ))
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    let mut bytes = Vec::new();
    diagnostic
        .take(FETCH_DIAGNOSTIC_LIMIT)
        .read_to_end(&mut bytes)
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    if provider_reports_missing_object(&bytes, commit_oid) {
        Err(MaterializationFailure::CommitUnavailable)
    } else {
        Err(MaterializationFailure::ProviderUnavailable)
    }
}

fn commit_is_available(
    root: &Path,
    environment: &EnvironmentSnapshot,
    commit_oid: &str,
    cancellation: &CaptureCancellation,
) -> Result<bool, MaterializationFailure> {
    let commit = format!("{commit_oid}^{{commit}}");
    let mut command = git_command(root, environment, &["cat-file", "-e", &commit], None);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    run_managed_source_git(
        &mut command,
        cancellation,
        MaterializationFailure::EnvironmentUnavailable,
    )
    .map(|status| status.success())
}

fn provider_reports_missing_object(diagnostic: &[u8], commit_oid: &str) -> bool {
    [
        format!("upload-pack: not our ref {commit_oid}"),
        format!("Server does not allow request for unadvertised object {commit_oid}"),
        format!("couldn't find remote ref {commit_oid}"),
    ]
    .iter()
    .any(|evidence| {
        diagnostic
            .windows(evidence.len())
            .any(|window| window == evidence.as_bytes())
    })
}

fn verify_fetched_commit(
    root: &Path,
    environment: &EnvironmentSnapshot,
    request: &MaterializationRequest,
    cancellation: &CaptureCancellation,
) -> Result<(), MaterializationFailure> {
    let object_format = run_git_output(
        root,
        environment,
        &["rev-parse", "--show-object-format"],
        cancellation,
    )?;
    if object_format != b"sha1\n" {
        return Err(MaterializationFailure::UnsupportedObjectFormat);
    }
    let object_type = run_git_output(
        root,
        environment,
        &["cat-file", "-t", &request.commit_oid],
        cancellation,
    )?;
    if object_type != b"commit\n" {
        return Err(MaterializationFailure::CommitUnavailable);
    }
    Ok(())
}

fn checkout_pinned_commit(
    root: &Path,
    environment: &EnvironmentSnapshot,
    commit_oid: &str,
    cancellation: &CaptureCancellation,
) -> Result<(), MaterializationFailure> {
    run_git_status(
        root,
        environment,
        &["checkout", "--quiet", "--detach", "--force", commit_oid],
        None,
        cancellation,
    )
}

fn verify_checkout(
    root: &Path,
    environment: &EnvironmentSnapshot,
    request: &MaterializationRequest,
    cancellation: &CaptureCancellation,
) -> Result<(), MaterializationFailure> {
    verify_fetched_commit(root, environment, request, cancellation)?;
    let head = run_git_output(
        root,
        environment,
        &["rev-parse", "--verify", "HEAD^{commit}"],
        cancellation,
    )?;
    if head != format!("{}\n", request.commit_oid).as_bytes() {
        return Err(MaterializationFailure::CommitMismatch);
    }
    let mut detached = git_command(root, environment, &["symbolic-ref", "-q", "HEAD"], None);
    detached
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let detached = run_managed_source_git(
        &mut detached,
        cancellation,
        MaterializationFailure::EnvironmentUnavailable,
    )?;
    match detached.code() {
        Some(1) => {}
        Some(0) => return Err(MaterializationFailure::CommitMismatch),
        _ => return Err(MaterializationFailure::EnvironmentUnavailable),
    }
    let status = run_git_output(
        root,
        environment,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=normal",
            "--ignore-submodules=none",
        ],
        cancellation,
    )?;
    if !status.is_empty() {
        return Err(MaterializationFailure::DirtyCheckout);
    }
    Ok(())
}

fn run_git_status(
    root: &Path,
    environment: &EnvironmentSnapshot,
    arguments: &[&str],
    askpass: Option<&EphemeralAskpass>,
    cancellation: &CaptureCancellation,
) -> Result<(), MaterializationFailure> {
    run_git_status_with_failure(
        root,
        environment,
        arguments,
        askpass,
        cancellation,
        MaterializationFailure::EnvironmentUnavailable,
    )
}

fn run_git_status_with_failure(
    root: &Path,
    environment: &EnvironmentSnapshot,
    arguments: &[&str],
    askpass: Option<&EphemeralAskpass>,
    cancellation: &CaptureCancellation,
    failure: MaterializationFailure,
) -> Result<(), MaterializationFailure> {
    let mut command = git_command(root, environment, arguments, askpass);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    ensure_git_success(&mut command, cancellation, failure)
}

fn ensure_git_success(
    command: &mut Command,
    cancellation: &CaptureCancellation,
    failure: MaterializationFailure,
) -> Result<(), MaterializationFailure> {
    let status = run_managed_source_git(command, cancellation, failure)?;
    if status.success() {
        Ok(())
    } else {
        Err(failure)
    }
}

fn run_git_output(
    root: &Path,
    environment: &EnvironmentSnapshot,
    arguments: &[&str],
    cancellation: &CaptureCancellation,
) -> Result<Vec<u8>, MaterializationFailure> {
    ensure_current(cancellation)?;
    let output = git_command(root, environment, arguments, None)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
    ensure_current(cancellation)?;
    if !output.status.success() || output.stdout.len() > 64 * 1024 {
        return Err(MaterializationFailure::EnvironmentUnavailable);
    }
    Ok(output.stdout)
}

fn git_command(
    root: &Path,
    environment: &EnvironmentSnapshot,
    arguments: &[&str],
    askpass: Option<&EphemeralAskpass>,
) -> Command {
    let mut command = Command::new("git");
    command
        .current_dir(root)
        .args([
            "--no-pager",
            "--no-optional-locks",
            "--no-replace-objects",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
            "-c",
            "fetch.writeCommitGraph=false",
        ])
        .args(arguments)
        .env_clear()
        .env("LC_ALL", "C")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_TERMINAL_PROMPT", "0");
    if let Some(path) = environment.variable(OsStr::new("PATH")) {
        command.env("PATH", path);
    }
    if let Some(askpass) = askpass {
        askpass.bind(&mut command);
    }
    command
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManagedGitFailure {
    Cancelled,
    Spawn,
    Timeout,
    Wait,
}

fn run_managed_source_git(
    command: &mut Command,
    cancellation: &CaptureCancellation,
    timeout_failure: MaterializationFailure,
) -> Result<ExitStatus, MaterializationFailure> {
    match run_managed_git(command, Some(cancellation)) {
        Ok(status) => Ok(status),
        Err(ManagedGitFailure::Cancelled) => Err(MaterializationFailure::AssignmentFenced),
        Err(ManagedGitFailure::Timeout) => Err(timeout_failure),
        Err(ManagedGitFailure::Spawn | ManagedGitFailure::Wait) => {
            Err(MaterializationFailure::EnvironmentUnavailable)
        }
    }
}

fn run_managed_git(
    command: &mut Command,
    cancellation: Option<&CaptureCancellation>,
) -> Result<ExitStatus, ManagedGitFailure> {
    command.process_group(0);
    let mut child = ManagedProcessGroup::spawn(command).map_err(|_| ManagedGitFailure::Spawn)?;
    let started = crate::timing::monotonic_now();
    loop {
        if cancellation.is_some_and(CaptureCancellation::is_cancelled) {
            return Err(ManagedGitFailure::Cancelled);
        }
        if crate::timing::elapsed(started) >= GIT_OPERATION_TIMEOUT {
            return Err(ManagedGitFailure::Timeout);
        }
        match child.try_wait().map_err(|_| ManagedGitFailure::Wait)? {
            Some(status) => return Ok(status),
            None => crate::timing::sleep(PROCESS_POLL_INTERVAL),
        }
    }
}

struct EphemeralAskpass {
    helper: tempfile::TempPath,
    token: File,
    token_length: usize,
}

impl EphemeralAskpass {
    fn create(parent: &Path, token: &[u8]) -> Result<Self, MaterializationFailure> {
        let script = b"#!/bin/sh\ncase \"${1-}\" in\n  *[Uu]sername*) printf '%s\\n' 'x-access-token' ;;\n  *) IFS= read -r token < \"/dev/fd/$SCHERZO_SOURCE_TOKEN_FD\"; printf '%s\\n' \"$token\"; unset token ;;\nesac\n";
        let mut helper = tempfile::Builder::new()
            .prefix("source-askpass-")
            .tempfile_in(parent)
            .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
        helper
            .write_all(script)
            .and_then(|()| helper.flush())
            .and_then(|()| {
                helper
                    .as_file()
                    .set_permissions(fs::Permissions::from_mode(0o500))
            })
            .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
        let helper = helper.into_temp_path();
        let mut token_output = tempfile::tempfile_in(parent)
            .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
        token_output
            .set_permissions(fs::Permissions::from_mode(0o600))
            .and_then(|()| token_output.write_all(token))
            .and_then(|()| token_output.flush())
            .and_then(|()| token_output.rewind())
            .map_err(|_| MaterializationFailure::EnvironmentUnavailable)?;
        close_on_exec(&token_output)?;
        Ok(Self {
            helper,
            token: token_output,
            token_length: token.len(),
        })
    }

    fn helper_path(&self) -> &Path {
        &self.helper
    }

    fn bind(&self, command: &mut Command) {
        command
            .env("GIT_ASKPASS", self.helper_path())
            .env("GIT_ASKPASS_REQUIRE", "force")
            .env(
                "SCHERZO_SOURCE_TOKEN_FD",
                self.token.as_raw_fd().to_string(),
            );
        inherit_for_git_child(command, self.token.as_raw_fd());
    }
}

impl Drop for EphemeralAskpass {
    fn drop(&mut self) {
        let zeros = vec![0_u8; self.token_length];
        let _ = self
            .token
            .rewind()
            .and_then(|()| self.token.write_all(&zeros))
            .and_then(|()| self.token.flush());
    }
}

fn close_on_exec(file: &File) -> Result<(), MaterializationFailure> {
    fcntl(file, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))
        .map(|_| ())
        .map_err(|_| MaterializationFailure::EnvironmentUnavailable)
}

#[allow(
    unsafe_code,
    reason = "the pre-exec hook prepares only the assignment-owned token descriptor"
)]
fn inherit_for_git_child(command: &mut Command, descriptor: libc::c_int) {
    // SAFETY: `lseek` and `fcntl(F_SETFD)` are async-signal-safe, the descriptor
    // remains open through spawn, and the closure performs no allocation or
    // process-global work.
    unsafe {
        command.pre_exec(move || {
            if libc::lseek(descriptor, 0, libc::SEEK_SET) == -1
                || libc::fcntl(descriptor, libc::F_SETFD, 0) == -1
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(test)]
fn fixture_credential(repository_url: String, token: &str) -> ProviderCredential {
    ProviderCredential {
        repository_url: Arc::from(repository_url),
        token: ProviderSecret(token.as_bytes().to_vec()),
        expires_at: OffsetDateTime::UNIX_EPOCH + Duration::from_secs(3600),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::ffi::OsStrExt as _;
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Mutex, mpsc};

    use base64::Engine as _;

    use super::*;
    use crate::execution::workflow::admission::{
        CancellationPolicy, CancellationSource, ExecutionContext, ExecutionRootLifecycle,
        ResolvedImports, admit_workflow, default_execution_policy_limits,
    };
    use crate::execution::workflow::artifact::ArtifactStaging;
    use crate::execution::workflow::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
    use crate::execution::workflow::document::FailurePolicy;
    use crate::execution::workflow::publication::{
        CloudCarrierBody, WorkflowRunResult, WorkflowRunStep, WorkflowRunStepKind,
        WorkflowRunTiming, WorkflowStepTiming, prepare_cloud_workflow_result,
    };
    use crate::execution::workflow::runtime::{ExportValue, RunOutcome, StepState};
    use crate::execution::workflow::validated::WorkflowNodeRole;
    use crate::execution::workflow::value::CapturedValue;
    use crate::runner::credential::test_credential;

    struct FixtureBroker {
        repository_url: String,
        token: String,
        calls: Mutex<usize>,
    }

    impl SourceCredentialBroker for FixtureBroker {
        fn issue(
            &self,
            _assignment_id: &str,
            cancellation: &CaptureCancellation,
        ) -> Result<ProviderCredential, CredentialBrokerFailure> {
            ensure_broker_current(cancellation)?;
            *self.calls.lock().unwrap() += 1;
            Ok(fixture_credential(self.repository_url.clone(), &self.token))
        }
    }

    struct RepositoryFixture {
        _temporary: tempfile::TempDir,
        repository: PathBuf,
        commit: String,
        parent_commit: String,
        digest: String,
    }

    #[derive(Clone, Copy, Default)]
    struct AuthenticatedRequestEvidence {
        challenges: usize,
        accepted: usize,
        helper_observed: bool,
        token_observed_in_private_root: bool,
    }

    // This timeout only bounds anti-hang failure at the external Git/socket
    // boundary; request progress has no in-process readiness signal.
    const AUTHENTICATED_GIT_FIXTURE_IO_TIMEOUT: Duration = Duration::from_secs(10);

    struct AuthenticatedGitServer {
        _temporary: tempfile::TempDir,
        repository_url: String,
        evidence: Arc<Mutex<AuthenticatedRequestEvidence>>,
        failure: Arc<Mutex<Option<String>>>,
        shutdown: Arc<AtomicBool>,
        worker: Option<std::thread::JoinHandle<()>>,
    }

    impl AuthenticatedGitServer {
        fn start(
            repository: &Path,
            private_root: &Path,
            accepted_token: &str,
            observed_token: &str,
            authorized_gate: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
        ) -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let bare_repository = temporary.path().join("repository.git");
            assert!(
                fixture_git_command()
                    .args(["clone", "--quiet", "--bare"])
                    .arg(repository)
                    .arg(&bare_repository)
                    .status()
                    .unwrap()
                    .success()
            );
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let address = listener.local_addr().unwrap();
            let repository_url = format!("http://{address}/repository.git");
            let project_root = temporary.path().to_owned();
            let private_root = private_root.to_owned();
            let expected_authorization = format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("x-access-token:{accepted_token}"))
            );
            let observed_token = observed_token.as_bytes().to_vec();
            let evidence = Arc::new(Mutex::new(AuthenticatedRequestEvidence::default()));
            let worker_evidence = Arc::clone(&evidence);
            let failure = Arc::new(Mutex::new(None));
            let worker_failure = Arc::clone(&failure);
            let shutdown = Arc::new(AtomicBool::new(false));
            let worker_shutdown = Arc::clone(&shutdown);
            let worker = std::thread::spawn(move || {
                let mut authorized_gate = authorized_gate;
                loop {
                    let Ok((mut stream, _)) = listener.accept() else {
                        *worker_failure.lock().unwrap() =
                            Some("authenticated Git fixture could not accept a request".to_owned());
                        break;
                    };
                    if worker_shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    let result = stream
                        .set_read_timeout(Some(AUTHENTICATED_GIT_FIXTURE_IO_TIMEOUT))
                        .and_then(|()| {
                            stream.set_write_timeout(Some(AUTHENTICATED_GIT_FIXTURE_IO_TIMEOUT))
                        })
                        .and_then(|()| {
                            serve_authenticated_git_request(
                                &mut stream,
                                &project_root,
                                &private_root,
                                &expected_authorization,
                                &observed_token,
                                &worker_evidence,
                                &mut authorized_gate,
                            )
                        });
                    if let Err(error) = result {
                        *worker_failure.lock().unwrap() = Some(error.to_string());
                        break;
                    }
                }
            });
            Self {
                _temporary: temporary,
                repository_url,
                evidence,
                failure,
                shutdown,
                worker: Some(worker),
            }
        }

        fn evidence(&self) -> AuthenticatedRequestEvidence {
            *self.evidence.lock().unwrap()
        }
    }

    impl Drop for AuthenticatedGitServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Release);
            let _ = TcpStream::connect(
                self.repository_url
                    .strip_prefix("http://")
                    .and_then(|url| url.split('/').next())
                    .unwrap_or_default(),
            );
            if let Some(worker) = self.worker.take() {
                let result = worker.join();
                if !std::thread::panicking() {
                    assert!(result.is_ok(), "authenticated Git fixture worker panicked");
                    assert_eq!(
                        self.failure.lock().unwrap().as_deref(),
                        None,
                        "authenticated Git fixture failed"
                    );
                }
            }
        }
    }

    struct HttpRequest {
        method: String,
        target: String,
        authorization: Option<String>,
        content_type: Option<String>,
        body: Vec<u8>,
    }

    fn serve_authenticated_git_request(
        stream: &mut TcpStream,
        project_root: &Path,
        private_root: &Path,
        expected_authorization: &str,
        observed_token: &[u8],
        evidence: &Mutex<AuthenticatedRequestEvidence>,
        authorized_gate: &mut Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    ) -> std::io::Result<()> {
        let request = read_http_request(stream)?;
        let private_entries = fs::read_dir(private_root)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        let mut request_evidence = evidence.lock().unwrap();
        request_evidence.helper_observed |= private_entries.iter().any(|path| {
            path.file_name()
                .is_some_and(|name| name.as_bytes().starts_with(b"source-askpass-"))
        });
        request_evidence.token_observed_in_private_root |= private_entries
            .iter()
            .any(|path| path_contains(path, observed_token));
        if request.authorization.as_deref() != Some(expected_authorization) {
            request_evidence.challenges += 1;
            drop(request_evidence);
            stream.write_all(
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\nWWW-Authenticate: Basic realm=\"fixture\"\r\n\r\n",
            )?;
            return stream.flush();
        }
        request_evidence.accepted += 1;
        drop(request_evidence);

        if let Some((started, proceed)) = authorized_gate.take() {
            started.send(()).map_err(|_| {
                std::io::Error::other("cancellation fixture did not observe the request")
            })?;
            proceed.recv().map_err(|_| {
                std::io::Error::other("cancellation fixture did not release the request")
            })?;
            // The client is being cancelled and may close without reading a response.
            return Ok(());
        }

        let (path_info, query) = request
            .target
            .split_once('?')
            .unwrap_or((&request.target, ""));
        let mut backend = fixture_git_command();
        backend
            .arg("http-backend")
            .env("GIT_HTTP_EXPORT_ALL", "1")
            .env("GIT_PROJECT_ROOT", project_root)
            .env("PATH_INFO", path_info)
            .env("QUERY_STRING", query)
            .env("REQUEST_METHOD", &request.method)
            .env("CONTENT_LENGTH", request.body.len().to_string())
            .env("REMOTE_USER", "x-access-token")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(content_type) = request.content_type {
            backend.env("CONTENT_TYPE", content_type);
        }
        let mut backend = backend.spawn()?;
        if let Some(mut stdin) = backend.stdin.take() {
            stdin.write_all(&request.body)?;
        }
        let output = backend.wait_with_output()?;
        if !output.status.success() {
            stream.write_all(
                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )?;
            return stream.flush();
        }
        write_cgi_response(stream, &output.stdout)
    }

    fn read_http_request(stream: &mut TcpStream) -> std::io::Result<HttpRequest> {
        const MAXIMUM_REQUEST_BYTES: usize = 32 * 1024 * 1024;
        let mut encoded = Vec::new();
        let header_end = loop {
            if let Some(position) = encoded.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break position + 4;
            }
            if encoded.len() >= MAXIMUM_REQUEST_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "fixture request exceeded its bound",
                ));
            }
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "fixture request ended before its headers",
                ));
            }
            encoded.extend_from_slice(&buffer[..read]);
        };
        let headers = std::str::from_utf8(&encoded[..header_end]).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "fixture request headers were not UTF-8",
            )
        })?;
        let mut lines = headers.split("\r\n");
        let mut request_line = lines.next().unwrap_or_default().split_whitespace();
        let method = request_line.next().unwrap_or_default().to_owned();
        let target = request_line.next().unwrap_or_default().to_owned();
        let mut authorization = None;
        let mut content_type = None;
        let mut content_length = 0_usize;
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim();
            if name.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.to_owned());
            } else if name.eq_ignore_ascii_case("content-type") {
                content_type = Some(value.to_owned());
            } else if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "fixture request had an invalid content length",
                    )
                })?;
            }
        }
        if header_end.saturating_add(content_length) > MAXIMUM_REQUEST_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "fixture request body exceeded its bound",
            ));
        }
        while encoded.len() < header_end + content_length {
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "fixture request ended before its body",
                ));
            }
            encoded.extend_from_slice(&buffer[..read]);
        }
        Ok(HttpRequest {
            method,
            target,
            authorization,
            content_type,
            body: encoded[header_end..header_end + content_length].to_vec(),
        })
    }

    fn write_cgi_response(stream: &mut TcpStream, encoded: &[u8]) -> std::io::Result<()> {
        let (separator, separator_length) = encoded
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .map(|position| (position, 4))
            .or_else(|| {
                encoded
                    .windows(2)
                    .position(|bytes| bytes == b"\n\n")
                    .map(|position| (position, 2))
            })
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "git http-backend omitted its header boundary",
                )
            })?;
        let headers = std::str::from_utf8(&encoded[..separator]).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "git http-backend emitted invalid headers",
            )
        })?;
        let body = &encoded[separator + separator_length..];
        let mut status = "200 OK";
        let mut forwarded = String::new();
        for line in headers.lines() {
            let line = line.trim_end_matches('\r');
            if let Some(value) = line.strip_prefix("Status: ") {
                status = value;
            } else if !line.to_ascii_lowercase().starts_with("content-length:") {
                forwarded.push_str(line);
                forwarded.push_str("\r\n");
            }
        }
        stream.write_all(
            format!(
                "HTTP/1.1 {status}\r\n{forwarded}Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )?;
        stream.write_all(body)?;
        stream.flush()
    }

    fn path_contains(path: &Path, needle: &[u8]) -> bool {
        path.as_os_str()
            .as_bytes()
            .windows(needle.len())
            .any(|bytes| bytes == needle)
            || fs::read(path)
                .is_ok_and(|contents| contents.windows(needle.len()).any(|bytes| bytes == needle))
    }

    fn tree_contains(root: &Path, needle: &[u8]) -> bool {
        let mut pending = vec![root.to_owned()];
        while let Some(path) = pending.pop() {
            if path_contains(&path, needle) {
                return true;
            }
            if let Ok(entries) = fs::read_dir(&path) {
                pending.extend(entries.filter_map(Result::ok).map(|entry| entry.path()));
            }
        }
        false
    }

    fn command_contains(command: &Command, needle: &[u8]) -> bool {
        command
            .get_program()
            .as_bytes()
            .windows(needle.len())
            .any(|bytes| bytes == needle)
            || command.get_args().any(|argument| {
                argument
                    .as_bytes()
                    .windows(needle.len())
                    .any(|bytes| bytes == needle)
            })
            || command.get_envs().any(|(name, value)| {
                name.as_bytes()
                    .windows(needle.len())
                    .any(|bytes| bytes == needle)
                    || value.is_some_and(|value| {
                        value
                            .as_bytes()
                            .windows(needle.len())
                            .any(|bytes| bytes == needle)
                    })
            })
    }

    fn assert_credential_material_removed(assignment: &tempfile::TempDir, token: &str) {
        assert_eq!(
            fs::read_dir(assignment.path().join("private"))
                .unwrap()
                .count(),
            0
        );
        assert!(!tree_contains(assignment.path(), token.as_bytes()));
    }

    fn fixture_git_command() -> Command {
        crate::test_support::fixture_git_command("git")
    }

    fn run_fixture_git(repository: &Path, arguments: &[&str]) {
        assert!(
            fixture_git_command()
                .current_dir(repository)
                .args(arguments)
                .status()
                .unwrap()
                .success()
        );
    }

    fn initialize_repository(repository: &Path, object_format: &str) {
        fs::create_dir(repository).unwrap();
        let init = format!("--object-format={object_format}");
        run_fixture_git(repository, &["init", "--quiet", &init]);
        run_fixture_git(repository, &["config", "user.name", "Scherzo Fixture"]);
        run_fixture_git(
            repository,
            &["config", "user.email", "fixture@scherzo.invalid"],
        );
    }

    fn commit_repository(repository: &Path) {
        run_fixture_git(repository, &["add", "."]);
        run_fixture_git(repository, &["commit", "--quiet", "-m", "fixture"]);
    }

    fn empty_assignment() -> tempfile::TempDir {
        let assignment = tempfile::tempdir().unwrap();
        fs::create_dir(assignment.path().join("execution")).unwrap();
        fs::create_dir(assignment.path().join("private")).unwrap();
        fs::create_dir(assignment.path().join("source")).unwrap();
        assignment
    }

    fn fixture_broker(repository_url: String, token: &str) -> Arc<FixtureBroker> {
        Arc::new(FixtureBroker {
            repository_url,
            token: token.to_owned(),
            calls: Mutex::new(0),
        })
    }

    fn assignment_fixture(repository: &Path) -> (tempfile::TempDir, Arc<FixtureBroker>) {
        let assignment = empty_assignment();
        let broker = fixture_broker(
            Url::from_file_path(repository).unwrap().to_string(),
            "fixture-provider-token",
        );
        (assignment, broker)
    }

    fn fixture_environment() -> EnvironmentSnapshot {
        EnvironmentSnapshot::new([(OsString::from("PATH"), std::env::var_os("PATH").unwrap())])
    }

    fn materialization_result(
        assignment: &tempfile::TempDir,
        broker: Arc<FixtureBroker>,
        projection: &MaterializationRequest,
    ) -> Result<MaterializedSource, MaterializationFailure> {
        materialization_result_with_cancellation(
            assignment,
            broker,
            projection,
            &CaptureCancellation::default(),
        )
    }

    fn materialization_result_with_cancellation(
        assignment: &tempfile::TempDir,
        broker: Arc<FixtureBroker>,
        projection: &MaterializationRequest,
        cancellation: &CaptureCancellation,
    ) -> Result<MaterializedSource, MaterializationFailure> {
        materialize(
            broker,
            &fixture_environment(),
            "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
            projection,
            cancellation,
            &assignment.path().join("source"),
            &assignment.path().join("execution"),
            &assignment.path().join("private"),
        )
    }

    fn repository_fixture(workflow: &str) -> RepositoryFixture {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        initialize_repository(&repository, "sha1");
        fs::write(repository.join("history.txt"), b"history\n").unwrap();
        commit_repository(&repository);
        let parent_commit = String::from_utf8(
            fixture_git_command()
                .current_dir(&repository)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        run_fixture_git(&repository, &["branch", "fixture-history", &parent_commit]);
        run_fixture_git(&repository, &["tag", "fixture-history", &parent_commit]);
        fs::create_dir(repository.join("workflows")).unwrap();
        fs::write(repository.join("workflows/workflow.yaml"), workflow).unwrap();
        commit_repository(&repository);
        let commit = fixture_git_command()
            .current_dir(&repository)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        let digest = resolution::resolve(&repository, Path::new("workflows/workflow.yaml"))
            .unwrap()
            .content_digest
            .value;
        RepositoryFixture {
            _temporary: temporary,
            repository,
            commit: String::from_utf8(commit.stdout).unwrap().trim().to_owned(),
            parent_commit,
            digest,
        }
    }

    fn projection(fixture: &RepositoryFixture) -> MaterializationRequest {
        MaterializationRequest {
            repository_connection_id: "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            object_format: "sha1".to_owned(),
            commit_oid: fixture.commit.clone(),
            workflow_path: "workflows/workflow.yaml".to_owned(),
            workflow_source_closure_digest: WorkflowSourceClosureDigest {
                algorithm: "sha256".to_owned(),
                value: fixture.digest.clone(),
            },
        }
    }

    fn materialize_fixture(
        fixture: &RepositoryFixture,
        projection: &MaterializationRequest,
    ) -> (tempfile::TempDir, MaterializedSource, Arc<FixtureBroker>) {
        let (assignment, broker) = assignment_fixture(&fixture.repository);
        let materialized = materialization_result(&assignment, broker.clone(), projection).unwrap();
        (assignment, materialized, broker)
    }

    #[test]
    fn askpass_uses_a_named_executable_with_an_anonymous_inherited_token() {
        const TOKEN: &[u8] = b"synthetic-provider-token";
        let private = tempfile::tempdir().unwrap();
        let askpass = EphemeralAskpass::create(private.path(), TOKEN).unwrap();
        let private_entries = fs::read_dir(private.path())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(private_entries.len(), 1);
        assert_eq!(private_entries[0].path(), askpass.helper_path());
        assert_eq!(
            fs::metadata(askpass.helper_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o500
        );
        assert!(!path_contains(askpass.helper_path(), TOKEN));
        let unrelated = Command::new("sh")
            .args(["-c", "test ! -e /dev/fd/$TOKEN_FD"])
            .env("TOKEN_FD", askpass.token.as_raw_fd().to_string())
            .status()
            .unwrap();
        assert!(unrelated.success());
        let provider_git = git_command(
            private.path(),
            &fixture_environment(),
            &[
                "ls-remote",
                "https://provider.invalid/repository.git",
                "HEAD",
            ],
            Some(&askpass),
        );
        assert!(!command_contains(&provider_git, TOKEN));
        let mut helper_command = Command::new(askpass.helper_path());
        helper_command.arg("Password for repository");
        askpass.bind(&mut helper_command);
        assert!(!command_contains(&helper_command, TOKEN));

        let output = helper_command.output().unwrap();

        assert!(output.status.success());
        assert_eq!(output.stdout, b"synthetic-provider-token\n");
        drop(askpass);
        assert_eq!(fs::read_dir(private.path()).unwrap().count(), 0);
    }

    #[test]
    fn materializes_the_exact_detached_clean_commit_and_removes_credential_helpers() {
        const TOKEN: &str = "fixture-provider-token";
        let workflow = "schemaVersion: 1\nsteps:\n  capture:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      first:\n        kind: git_branch\n      second:\n        kind: git_branch\nexports:\n  alias:\n    ref: outputs.capture.first\n  first:\n    ref: outputs.capture.first\n  second:\n    ref: outputs.capture.second\n";
        let fixture = repository_fixture(workflow);
        let projection = projection(&fixture);
        let assignment = empty_assignment();
        let server = AuthenticatedGitServer::start(
            &fixture.repository,
            &assignment.path().join("private"),
            TOKEN,
            TOKEN,
            None,
        );
        let broker = fixture_broker(server.repository_url.clone(), TOKEN);
        let materialized =
            materialization_result(&assignment, Arc::clone(&broker), &projection).unwrap();

        assert_eq!(
            materialized.execution_root,
            assignment.path().join("source")
        );
        assert!(materialized.git_capture.is_some());
        assert!(!assignment.path().join("execution").exists());
        assert_eq!(*broker.calls.lock().unwrap(), 1);
        assert_credential_material_removed(&assignment, TOKEN);
        let request_evidence = server.evidence();
        assert!(request_evidence.challenges > 0);
        assert!(request_evidence.accepted > 0);
        assert!(request_evidence.helper_observed);
        assert!(!request_evidence.token_observed_in_private_root);
        let configuration = fixture_git_command()
            .current_dir(&materialized.execution_root)
            .args(["config", "--local", "--list", "--show-origin"])
            .output()
            .unwrap();
        assert!(configuration.status.success());
        assert!(
            !configuration
                .stdout
                .windows(TOKEN.len())
                .any(|bytes| bytes == TOKEN.as_bytes())
        );
        let head = fixture_git_command()
            .current_dir(&materialized.execution_root)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8(head.stdout).unwrap().trim(),
            fixture.commit
        );
        let shallow = fixture_git_command()
            .current_dir(&materialized.execution_root)
            .args(["rev-parse", "--is-shallow-repository"])
            .output()
            .unwrap();
        assert!(shallow.status.success());
        assert_eq!(shallow.stdout, b"false\n");
        for reference in [
            "HEAD^",
            "refs/remotes/origin/fixture-history",
            "refs/tags/fixture-history",
        ] {
            let resolved = fixture_git_command()
                .current_dir(&materialized.execution_root)
                .args(["rev-parse", reference])
                .output()
                .unwrap();
            assert!(resolved.status.success());
            assert_eq!(
                String::from_utf8(resolved.stdout).unwrap().trim(),
                fixture.parent_commit
            );
        }
        assert!(
            !materialized
                .execution_root
                .join(".git/objects/info/alternates")
                .exists()
        );
        assert!(
            !fixture_git_command()
                .current_dir(&materialized.execution_root)
                .args(["symbolic-ref", "-q", "HEAD"])
                .status()
                .unwrap()
                .success()
        );

        let expected_root = fs::canonicalize(&materialized.execution_root).unwrap();
        let workflow_environment = fixture_environment();
        assert!(
            workflow_environment
                .variables()
                .iter()
                .all(|(name, value)| {
                    !name
                        .as_bytes()
                        .windows(TOKEN.len())
                        .any(|bytes| bytes == TOKEN.as_bytes())
                        && !value
                            .as_bytes()
                            .windows(TOKEN.len())
                            .any(|bytes| bytes == TOKEN.as_bytes())
                })
        );
        assert!(
            workflow_environment
                .variable(OsStr::new("GIT_ASKPASS"))
                .is_none()
        );
        assert!(
            workflow_environment
                .variable(OsStr::new("SCHERZO_SOURCE_TOKEN_FD"))
                .is_none()
        );
        let context = ExecutionContext::new(
            materialized.execution_root.clone(),
            ExecutionRootLifecycle::CallerOwnedRetained,
            default_execution_policy_limits(1),
            workflow_environment,
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        )
        .with_cloud_git_capture(materialized.git_capture.unwrap());
        let admitted =
            admit_workflow(materialized.workflow, ResolvedImports::default(), context).unwrap();
        let git = admitted.git_capture().unwrap();
        assert_eq!(admitted.execution().root(), expected_root);
        assert_eq!(
            admitted.execution().root_lifecycle(),
            ExecutionRootLifecycle::CallerOwnedRetained
        );
        assert_eq!(git.baseline_oid(), fixture.commit);
        assert_eq!(git.workflow_digest(), Some(fixture.digest.as_str()));
        assert_eq!(
            git.carrier_limits(),
            (1024, 64 * 1024 * 1024, 256 * 1024 * 1024)
        );

        let artifacts =
            ArtifactStaging::create(admitted.execution(), &assignment.path().join("private"))
                .unwrap();
        let capture = |identity: &str| {
            git.capture(identity, &artifacts, &CaptureCancellation::default())
                .unwrap()
                .commit()
                .remove(identity)
                .unwrap()
        };
        let prepare = |first: CapturedValue, second: CapturedValue| {
            let outputs =
                BTreeMap::from([("first".to_owned(), first), ("second".to_owned(), second)]);
            let exports = admitted
                .workflow()
                .definition
                .exports
                .iter()
                .map(|(name, source)| {
                    (
                        name.clone(),
                        ExportValue::Available {
                            output: outputs[&source.output].clone(),
                        },
                    )
                })
                .collect();
            let run = WorkflowRunResult {
                run_directory: assignment.path().to_owned(),
                attempt_number: 1,
                workflow_path: admitted.workflow().source.workflow_path.clone(),
                source_root: admitted.execution().root().to_owned(),
                content_digest: admitted.workflow().content_digest.clone(),
                execution_root: admitted.execution().root().to_owned(),
                maximum_parallel_steps: admitted.execution().limits().maximum_parallel_steps(),
                timing: WorkflowRunTiming {
                    started_at: OffsetDateTime::UNIX_EPOCH,
                    finished_at: OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND,
                    duration: Duration::from_secs(1),
                },
                outcome: RunOutcome::Succeeded,
                cancellation: None,
                steps: vec![WorkflowRunStep {
                    id: "capture".to_owned(),
                    role: WorkflowNodeRole::Step,
                    kind: WorkflowRunStepKind::Command,
                    failure_policy: FailurePolicy::Required,
                    state: StepState::Succeeded { outputs },
                    timing: Some(WorkflowStepTiming {
                        started_at: OffsetDateTime::UNIX_EPOCH,
                        duration: Duration::from_secs(1),
                    }),
                    command_output: Some(StepDiagnostic::from_streams(
                        CapturedDiagnosticStream::from_parts(Arc::<[u8]>::from([]), 0, true),
                        CapturedDiagnosticStream::from_parts(Arc::<[u8]>::from([]), 0, true),
                    )),
                    recovery: None,
                    invocations: Vec::new(),
                }],
                finalization: None,
                exports,
                export_sources: admitted.workflow().definition.exports.clone(),
            };
            prepare_cloud_workflow_result(
                &run,
                "prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                projection.repository_connection_id.clone(),
                projection.object_format.clone(),
                projection.commit_oid.clone(),
            )
            .unwrap()
        };

        let zero_delta = prepare(capture("first"), capture("second"));
        assert!(zero_delta.carriers.is_empty());
        let zero_result: serde_json::Value =
            serde_json::from_slice(&zero_delta.result_json).unwrap();
        for name in ["alias", "first", "second"] {
            let export = &zero_result["exports"][name];
            assert_eq!(export["baseOid"], fixture.commit);
            assert_eq!(export["headOid"], fixture.commit);
            assert!(export.get("carrier").is_none());
        }

        run_fixture_git(
            admitted.execution().root(),
            &["config", "user.name", "Scherzo Test"],
        );
        run_fixture_git(
            admitted.execution().root(),
            &["config", "user.email", "test@example.invalid"],
        );
        fs::write(
            admitted.execution().root().join("captured.txt"),
            b"captured\n",
        )
        .unwrap();
        run_fixture_git(admitted.execution().root(), &["add", "captured.txt"]);
        run_fixture_git(
            admitted.execution().root(),
            &["commit", "--quiet", "-m", "capture branch"],
        );
        let changed_head = String::from_utf8(
            fixture_git_command()
                .current_dir(admitted.execution().root())
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        let changed = prepare(capture("first"), capture("second"));
        assert_eq!(
            changed
                .carriers
                .iter()
                .map(|carrier| carrier.portable_owner_path.as_str())
                .collect::<Vec<_>>(),
            ["exports/0001", "exports/0003"]
        );
        let changed_result: serde_json::Value =
            serde_json::from_slice(&changed.result_json).unwrap();
        assert_eq!(changed_result["workflow"]["provenance"]["kind"], "cloud");
        assert!(changed_result["execution"].get("executionRoot").is_none());
        assert_eq!(
            changed_result["exports"]["alias"]["carrier"]["path"],
            "exports/0001"
        );
        assert_eq!(
            changed_result["exports"]["first"]["carrier"]["path"],
            "exports/0001"
        );
        assert_eq!(
            changed_result["exports"]["second"]["carrier"]["path"],
            "exports/0003"
        );
        for name in ["alias", "first", "second"] {
            let export = &changed_result["exports"][name];
            assert_eq!(export["baseOid"], fixture.commit);
            assert_eq!(export["headOid"], changed_head);
        }
        let encoded_result = String::from_utf8_lossy(&changed.result_json);
        assert!(!encoded_result.contains(expected_root.to_string_lossy().as_ref()));
        assert!(!encoded_result.contains(&server.repository_url));

        let portable = tempfile::tempdir().unwrap();
        fs::create_dir(portable.path().join("exports")).unwrap();
        fs::write(portable.path().join("result.json"), &changed.result_json).unwrap();
        for carrier in &changed.carriers {
            let destination = portable.path().join(&carrier.portable_owner_path);
            match &carrier.body {
                CloudCarrierBody::Staged(staged) => {
                    let mut file = File::create(destination).unwrap();
                    artifacts.copy_to(staged.handle(), &mut file).unwrap();
                }
                CloudCarrierBody::Bytes(bytes) => fs::write(destination, bytes).unwrap(),
            }
        }
        let validation =
            crate::execution::workflow::portable_artifact::validate_portable_artifact_set(
                portable.path(),
                &AtomicBool::new(false),
            )
            .unwrap();
        assert!(validation.is_valid());
        assert_eq!(*broker.calls.lock().unwrap(), 1);
    }

    #[test]
    fn credential_rejection_fails_closed_and_removes_the_askpass_helper() {
        const REJECTED_TOKEN: &str = "rejected-provider-token";
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let fixture = repository_fixture(workflow);
        let projection = projection(&fixture);
        let assignment = empty_assignment();
        let server = AuthenticatedGitServer::start(
            &fixture.repository,
            &assignment.path().join("private"),
            "accepted-provider-token",
            REJECTED_TOKEN,
            None,
        );
        let broker = fixture_broker(server.repository_url.clone(), REJECTED_TOKEN);

        let result = materialization_result(&assignment, broker, &projection);

        assert!(matches!(
            result,
            Err(MaterializationFailure::ProviderUnavailable)
        ));
        let request_evidence = server.evidence();
        assert!(request_evidence.challenges > 0);
        assert!(request_evidence.helper_observed);
        assert!(!request_evidence.token_observed_in_private_root);
        assert_credential_material_removed(&assignment, REJECTED_TOKEN);
    }

    #[test]
    fn cancellation_removes_the_askpass_helper_before_materialization_returns() {
        const TOKEN: &str = "cancelled-provider-token";
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let fixture = repository_fixture(workflow);
        let projection = projection(&fixture);
        let assignment = empty_assignment();
        let (authorized_sender, authorized_receiver) = mpsc::sync_channel(1);
        let (proceed_sender, proceed_receiver) = mpsc::sync_channel(1);
        let server = AuthenticatedGitServer::start(
            &fixture.repository,
            &assignment.path().join("private"),
            TOKEN,
            TOKEN,
            Some((authorized_sender, proceed_receiver)),
        );
        let broker = fixture_broker(server.repository_url.clone(), TOKEN);
        let cancellation = CaptureCancellation::default();
        let cancel = cancellation.clone();
        let canceller = std::thread::spawn(move || {
            authorized_receiver.recv().unwrap();
            cancel.cancel();
            proceed_sender.send(()).unwrap();
        });

        let result = materialization_result_with_cancellation(
            &assignment,
            broker,
            &projection,
            &cancellation,
        );

        let request_evidence = server.evidence();
        drop(server);
        canceller.join().unwrap();
        assert!(matches!(
            result,
            Err(MaterializationFailure::AssignmentFenced)
        ));
        assert!(request_evidence.helper_observed);
        assert!(!request_evidence.token_observed_in_private_root);
        assert_credential_material_removed(&assignment, TOKEN);
    }

    #[test]
    fn outputless_workflow_retains_the_checkout_for_workspace_lease_release() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let fixture = repository_fixture(workflow);
        let projection = projection(&fixture);
        let (assignment, materialized, _) = materialize_fixture(&fixture, &projection);

        assert_eq!(
            materialized.execution_root,
            assignment.path().join("execution")
        );
        assert!(materialized.git_capture.is_none());
        assert!(assignment.path().join("source").exists());
        assert_eq!(
            materialized
                .workflow
                .source_bytes("workflows/workflow.yaml")
                .unwrap(),
            workflow.as_bytes(),
        );
    }

    #[test]
    fn rejects_a_non_commit_pinned_oid_as_an_integrity_failure() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let fixture = repository_fixture(workflow);
        let blob = fixture_git_command()
            .current_dir(&fixture.repository)
            .args(["rev-parse", "HEAD:workflows/workflow.yaml"])
            .output()
            .unwrap();
        let mut projection = projection(&fixture);
        projection.commit_oid = String::from_utf8(blob.stdout).unwrap().trim().to_owned();
        let (assignment, broker) = assignment_fixture(&fixture.repository);

        assert!(matches!(
            materialization_result(&assignment, broker, &projection),
            Err(MaterializationFailure::CommitUnavailable)
        ));
    }

    #[test]
    fn missing_pinned_commit_is_an_immutable_source_failure() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let fixture = repository_fixture(workflow);
        let mut projection = projection(&fixture);
        projection.commit_oid = "0".repeat(40);
        let (assignment, broker) = assignment_fixture(&fixture.repository);

        assert_eq!(
            materialization_result(&assignment, broker, &projection).map(|_| ()),
            Err(MaterializationFailure::CommitUnavailable)
        );
    }

    #[test]
    fn failed_local_checkout_is_a_runner_environment_failure() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let fixture = repository_fixture(workflow);
        fs::write(fixture.repository.join(".git/index.lock"), b"fixture lock").unwrap();

        assert_eq!(
            checkout_pinned_commit(
                &fixture.repository,
                &fixture_environment(),
                &fixture.commit,
                &CaptureCancellation::default(),
            ),
            Err(MaterializationFailure::EnvironmentUnavailable)
        );
    }

    #[test]
    fn failed_local_verification_is_a_runner_environment_failure() {
        let temporary = tempfile::tempdir().unwrap();
        let empty_repository = temporary.path().join("repository");
        initialize_repository(&empty_repository, "sha1");

        assert_eq!(
            run_git_output(
                &empty_repository,
                &fixture_environment(),
                &["rev-parse", "--verify", "HEAD^{commit}"],
                &CaptureCancellation::default(),
            ),
            Err(MaterializationFailure::EnvironmentUnavailable)
        );
    }

    #[test]
    fn rejects_a_non_sha1_repository_before_fetching_the_pinned_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("repository");
        initialize_repository(&repository, "sha256");
        fs::write(
            repository.join("workflow.yaml"),
            "schemaVersion: 1\nsteps: {}\n",
        )
        .unwrap();
        commit_repository(&repository);

        let (assignment, broker) = assignment_fixture(&repository);
        let projection = MaterializationRequest {
            repository_connection_id: "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            object_format: "sha1".to_owned(),
            commit_oid: "0".repeat(40),
            workflow_path: "workflow.yaml".to_owned(),
            workflow_source_closure_digest: WorkflowSourceClosureDigest {
                algorithm: "sha256".to_owned(),
                value: "0".repeat(64),
            },
        };

        assert!(matches!(
            materialization_result(&assignment, broker, &projection),
            Err(MaterializationFailure::UnsupportedObjectFormat)
        ));
        assert_eq!(
            fs::read_dir(assignment.path().join("private"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "wall time only bounds the credential fixture's request-readiness message"
    )]
    fn broker_request_is_cancelled_when_the_assignment_is_fenced() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let (request_sender, request_receiver) = std::sync::mpsc::sync_channel(1);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let _ = request_sender.send(());
                    break;
                }
            }
            while stream.read(&mut buffer).is_ok_and(|read| read != 0) {}
        });
        let endpoint = Url::parse(&format!("ws://{address}/v1/runner/connect")).unwrap();
        let broker = HttpSourceCredentialBroker::new(
            &endpoint,
            &test_credential(),
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc",
        )
        .unwrap();
        let cancellation = CaptureCancellation::default();
        let cancel = cancellation.clone();
        let canceller = std::thread::spawn(move || {
            assert!(
                request_receiver
                    .recv_timeout(Duration::from_secs(1))
                    .is_ok()
            );
            cancel.cancel();
        });
        let started = crate::timing::monotonic_now();

        let result = broker.issue("asn_01k0z6r1w8f4jy2m7q9v3x5abc", &cancellation);

        assert!(matches!(result, Err(CredentialBrokerFailure::Fenced)));
        assert!(crate::timing::elapsed(started) < Duration::from_secs(2));
        canceller.join().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn source_git_processes_are_terminated_when_capture_is_fenced() {
        let temporary = tempfile::tempdir().unwrap();
        let ready = temporary.path().join("provider-operation-ready");
        let cancellation = CaptureCancellation::default();
        let cancel = cancellation.clone();
        let worker_ready = ready.clone();
        let canceller = std::thread::spawn(move || {
            let started = crate::timing::monotonic_now();
            // The child-process filesystem marker is the only available readiness boundary.
            while !worker_ready.is_file() {
                assert!(crate::timing::elapsed(started) < Duration::from_secs(2));
                crate::timing::sleep(PROCESS_POLL_INTERVAL);
            }
            cancel.cancel();
        });
        let started = crate::timing::monotonic_now();
        let mut command = Command::new("sh");
        command
            .args(["-c", "printf ready > \"$READY\"; exec sleep 60"])
            .env("READY", ready)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        assert_eq!(
            run_managed_git(&mut command, Some(&cancellation)),
            Err(ManagedGitFailure::Cancelled)
        );
        assert!(crate::timing::elapsed(started) < Duration::from_secs(2));
        canceller.join().unwrap();
    }

    #[test]
    fn rejects_a_pinned_digest_mismatch_after_checkout() {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let fixture = repository_fixture(workflow);
        let mut projection = projection(&fixture);
        projection.workflow_source_closure_digest.value = "0".repeat(64);
        let (assignment, broker) = assignment_fixture(&fixture.repository);
        assert!(matches!(
            materialization_result(&assignment, broker, &projection),
            Err(MaterializationFailure::WorkflowDigestMismatch)
        ));
    }
}
