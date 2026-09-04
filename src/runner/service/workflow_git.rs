use std::fs::{self, OpenOptions, Permissions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _, symlink};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use opentelemetry::KeyValue;
use time::OffsetDateTime;
use url::Url;
use zeroize::Zeroize as _;

use super::Sleeper;
use super::assignment::LeaseAuthority;
use super::lease_clock::LeaseClock;
use super::source::{
    ProviderCredential, SourceCredentialBroker, WorkflowGitRevocation, WorkflowGitRevocationOutcome,
};
use super::workspace::ProcessQuiescence;
use crate::execution::workflow::admission::EnvironmentSnapshot;
use crate::execution::workflow::artifact::CaptureCancellation;

const INTERNAL_HELPER_ENVIRONMENT: &str = "SCHERZO_INTERNAL_WORKFLOW_GIT_HELPER";
const INTERNAL_HELPER_SOCKET_ENVIRONMENT: &str = "SCHERZO_INTERNAL_WORKFLOW_GIT_SOCKET";
const INTERNAL_HELPER_VERSION: &str = "1";
const HELPER_FILE: &str = "workflow-git-credential";
const SOCKET_FILE: &str = "workflow-git.sock";
const SOCKET_ALIAS_ROOT: &str = "/tmp";
const SOCKET_ALIAS_LINK: &str = "private";
const MAXIMUM_CREDENTIAL_REQUEST_BYTES: usize = 16 * 1024;
const MAXIMUM_TOKEN_BYTES: usize = 64 * 1024;
const HELPER_IO_TIMEOUT: Duration = Duration::from_secs(35);
const TOKEN_REFRESH_MARGIN: time::Duration = time::Duration::seconds(30);

pub(crate) fn internal_helper_requested() -> bool {
    std::env::var(INTERNAL_HELPER_ENVIRONMENT).as_deref() == Ok(INTERNAL_HELPER_VERSION)
}

pub(crate) fn run_internal_helper() -> bool {
    run_helper_process().is_ok()
}

fn run_helper_process() -> anyhow::Result<()> {
    let operation = std::env::args_os()
        .nth(1)
        .ok_or_else(|| anyhow!("workflow Git helper operation is missing"))?;
    if operation != "get" {
        return Ok(());
    }
    let socket = std::env::var_os(INTERNAL_HELPER_SOCKET_ENVIRONMENT)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("workflow Git helper socket is missing"))?;
    let mut request = Vec::new();
    io::stdin()
        .lock()
        .take(
            u64::try_from(MAXIMUM_CREDENTIAL_REQUEST_BYTES + 1)
                .context("bound workflow Git credential request")?,
        )
        .read_to_end(&mut request)
        .context("read workflow Git credential request")?;
    if request.len() > MAXIMUM_CREDENTIAL_REQUEST_BYTES {
        return Err(anyhow!("workflow Git credential request is oversized"));
    }
    let mut stream = UnixStream::connect(socket).context("connect workflow Git authority")?;
    stream
        .set_read_timeout(Some(HELPER_IO_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(HELPER_IO_TIMEOUT)))
        .context("bound workflow Git helper I/O")?;
    write_frame(&mut stream, &request).context("send workflow Git credential request")?;
    let mut status = [0_u8; 1];
    stream
        .read_exact(&mut status)
        .context("read workflow Git credential disposition")?;
    if status == [0] {
        return Ok(());
    }
    if status != [1] {
        return Err(anyhow!("workflow Git credential disposition is invalid"));
    }
    let mut token = read_frame(&mut stream, MAXIMUM_TOKEN_BYTES)
        .context("read workflow Git credential response")?;
    if token.is_empty() || token.contains(&b'\n') || token.contains(&b'\r') {
        token.zeroize();
        return Err(anyhow!("workflow Git credential response is invalid"));
    }
    let mut output = io::stdout().lock();
    let result = output
        .write_all(b"username=x-access-token\npassword=")
        .and_then(|()| output.write_all(&token))
        .and_then(|()| output.write_all(b"\n\n"))
        .and_then(|()| output.flush())
        .context("write workflow Git credential response");
    token.zeroize();
    result
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RevocationObservation {
    Settled(WorkflowGitRevocation),
    LocallyExpired { provider_expires_at: OffsetDateTime },
    UnconfirmedUntilExpiry { provider_expires_at: OffsetDateTime },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RevocationSummary {
    NoIssuance,
    FullyRevoked,
    FullyRevokedWithReplay,
    Expired,
    SettledByRevocationAndExpiry,
    PartialResidualUntilExpiry,
    ResidualUntilExpiry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WorkflowGitTeardownReport {
    pub(super) observations: Vec<RevocationObservation>,
    pub(super) summary: RevocationSummary,
    pub(super) local_state_destroyed: bool,
}

impl WorkflowGitTeardownReport {
    fn from_observations(
        observations: Vec<RevocationObservation>,
        local_state_destroyed: bool,
    ) -> Self {
        let settled = observations
            .iter()
            .filter_map(|observation| match observation {
                RevocationObservation::Settled(revocation) => Some(revocation.outcome),
                RevocationObservation::LocallyExpired { .. } => {
                    Some(WorkflowGitRevocationOutcome::Expired)
                }
                RevocationObservation::UnconfirmedUntilExpiry { .. } => None,
            });
        let outcomes = settled.collect::<Vec<_>>();
        let residual_count = observations
            .len()
            .saturating_sub(outcomes.len())
            .saturating_add(
                outcomes
                    .iter()
                    .filter(|outcome| {
                        **outcome == WorkflowGitRevocationOutcome::ResidualUntilExpiry
                    })
                    .count(),
            );
        let settled_without_residual = outcomes
            .iter()
            .filter(|outcome| **outcome != WorkflowGitRevocationOutcome::ResidualUntilExpiry)
            .count();
        let summary = if observations.is_empty() {
            RevocationSummary::NoIssuance
        } else if residual_count > 0 && settled_without_residual > 0 {
            RevocationSummary::PartialResidualUntilExpiry
        } else if residual_count > 0 {
            RevocationSummary::ResidualUntilExpiry
        } else if outcomes
            .iter()
            .all(|outcome| *outcome == WorkflowGitRevocationOutcome::Expired)
        {
            RevocationSummary::Expired
        } else if outcomes.iter().all(|outcome| {
            matches!(
                outcome,
                WorkflowGitRevocationOutcome::Revoked
                    | WorkflowGitRevocationOutcome::AlreadyRevoked
            )
        }) {
            if outcomes.contains(&WorkflowGitRevocationOutcome::AlreadyRevoked) {
                RevocationSummary::FullyRevokedWithReplay
            } else {
                RevocationSummary::FullyRevoked
            }
        } else {
            RevocationSummary::SettledByRevocationAndExpiry
        };
        Self {
            observations,
            summary,
            local_state_destroyed,
        }
    }
}

#[derive(Clone)]
pub(super) struct WorkflowGitAuthority {
    inner: Arc<WorkflowGitAuthorityInner>,
}

struct WorkflowGitAuthorityInner {
    assignment_id: Arc<str>,
    origin: Url,
    origin_text: Arc<str>,
    workspace: PathBuf,
    environment: EnvironmentSnapshot,
    helper_path: PathBuf,
    socket_path: PathBuf,
    socket_address: PathBuf,
    alias_directory: PathBuf,
    alias_link: PathBuf,
    broker: Arc<dyn SourceCredentialBroker>,
    clock: Arc<dyn Sleeper>,
    recorder: Option<Arc<crate::runner::telemetry::Recorder>>,
    stop: AtomicBool,
    state: Mutex<AuthorityState>,
    teardown_changed: Condvar,
}

struct AuthorityState {
    lifecycle: AuthorityLifecycle,
    active: Option<ActiveLease>,
    worker: Option<JoinHandle<()>>,
    issuances: Vec<ProviderCredential>,
    current_issuance: Option<usize>,
    expired_observations: Vec<RevocationObservation>,
    teardown_report: Option<WorkflowGitTeardownReport>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthorityLifecycle {
    Installed,
    Active,
    Disabled,
    TearingDown,
    Destroyed,
}

struct ActiveLease {
    lease_clock: LeaseClock,
    authority: tokio::sync::watch::Receiver<LeaseAuthority>,
    issuance_cancellation: CaptureCancellation,
}

pub(super) struct WorkflowGitInstall<'a> {
    pub(super) broker: Arc<dyn SourceCredentialBroker>,
    pub(super) assignment_id: &'a str,
    pub(super) origin: Arc<str>,
    pub(super) workspace: &'a Path,
    pub(super) private_root: &'a Path,
    pub(super) environment: &'a EnvironmentSnapshot,
    pub(super) helper_executable: &'a Path,
    pub(super) clock: Arc<dyn Sleeper>,
    pub(super) recorder: Option<Arc<crate::runner::telemetry::Recorder>>,
    pub(super) cancellation: &'a CaptureCancellation,
}

impl WorkflowGitAuthority {
    pub(super) fn install(request: WorkflowGitInstall<'_>) -> anyhow::Result<Self> {
        let WorkflowGitInstall {
            broker,
            assignment_id,
            origin,
            workspace,
            private_root,
            environment,
            helper_executable,
            clock,
            recorder,
            cancellation,
        } = request;
        let parsed_origin = Url::parse(&origin).context("parse workflow Git origin")?;
        if parsed_origin.username() != ""
            || parsed_origin.password().is_some()
            || parsed_origin.query().is_some()
            || parsed_origin.fragment().is_some()
            || parsed_origin.path().is_empty()
        {
            return Err(anyhow!("workflow Git origin is not credential-free"));
        }
        let helper_path = private_root.join(HELPER_FILE);
        let socket_path = private_root.join(SOCKET_FILE);
        let (alias_directory, alias_link, socket_address) =
            create_socket_alias(private_root).context("create workflow Git socket alias")?;
        let helper = helper_script(helper_executable, &socket_address)
            .context("construct workflow Git helper")?;
        if let Err(error) = write_executable(&helper_path, helper.as_bytes()) {
            let _ = remove_socket_alias(&alias_directory, &alias_link);
            return Err(error).context("write workflow Git helper");
        }
        let helper_value = format!("!f() {{ exec {} \"$@\"; }}; f", shell_quote(&helper_path)?);
        let scoped_helper_key = format!("credential.{}.helper", origin);
        let configured = set_local_config(
            workspace,
            environment,
            "credential.helper",
            "",
            cancellation,
        )
        .and_then(|()| {
            set_local_config(
                workspace,
                environment,
                "credential.useHttpPath",
                "true",
                cancellation,
            )
        })
        .and_then(|()| {
            set_local_config(
                workspace,
                environment,
                &scoped_helper_key,
                &helper_value,
                cancellation,
            )
        });
        if let Err(error) = configured {
            let _ = unset_workflow_git_config(workspace, environment, &scoped_helper_key);
            let _ = fs::remove_file(&helper_path);
            let _ = remove_socket_alias(&alias_directory, &alias_link);
            return Err(error).context("configure workflow Git helper");
        }
        Ok(Self {
            inner: Arc::new(WorkflowGitAuthorityInner {
                assignment_id: Arc::from(assignment_id),
                origin: parsed_origin,
                origin_text: origin,
                workspace: workspace.to_owned(),
                environment: environment.clone(),
                helper_path,
                socket_path,
                socket_address,
                alias_directory,
                alias_link,
                broker,
                clock,
                recorder,
                stop: AtomicBool::new(false),
                state: Mutex::new(AuthorityState {
                    lifecycle: AuthorityLifecycle::Installed,
                    active: None,
                    worker: None,
                    issuances: Vec::new(),
                    current_issuance: None,
                    expired_observations: Vec::new(),
                    teardown_report: None,
                }),
                teardown_changed: Condvar::new(),
            }),
        })
    }

    pub(super) fn activate(
        &self,
        lease_clock: LeaseClock,
        authority: tokio::sync::watch::Receiver<LeaseAuthority>,
    ) -> anyhow::Result<()> {
        {
            let state = self.lock();
            if state.lifecycle != AuthorityLifecycle::Installed
                || !lease_is_current(&lease_clock, &authority)
            {
                return Err(anyhow!("workflow Git authority is not activatable"));
            }
        }
        let listener = UnixListener::bind(&self.inner.socket_address)
            .context("bind workflow Git authority socket")?;
        if let Err(error) =
            fs::set_permissions(&self.inner.socket_path, Permissions::from_mode(0o600))
        {
            drop(listener);
            let _ = fs::remove_file(&self.inner.socket_path);
            return Err(error).context("protect workflow Git authority socket");
        }
        let active = ActiveLease {
            lease_clock,
            authority,
            issuance_cancellation: CaptureCancellation::default(),
        };
        let inner = Arc::clone(&self.inner);
        let worker = std::thread::Builder::new()
            .name("runner-workflow-git-helper".to_owned())
            .spawn(move || serve_helper(listener, inner))
            .context("start workflow Git authority worker")?;
        let mut state = self.lock();
        if state.lifecycle != AuthorityLifecycle::Installed {
            self.inner.stop.store(true, Ordering::Release);
            drop(state);
            wake_helper(&self.inner.socket_address);
            let _ = worker.join();
            return Err(anyhow!(
                "workflow Git authority was fenced during activation"
            ));
        }
        state.active = Some(active);
        state.worker = Some(worker);
        state.lifecycle = AuthorityLifecycle::Active;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn is_active(&self) -> bool {
        let state = self.lock();
        state.lifecycle == AuthorityLifecycle::Active
            && state
                .active
                .as_ref()
                .is_some_and(|active| lease_is_current(&active.lease_clock, &active.authority))
    }

    pub(super) fn disable(&self) {
        let mut state = self.lock();
        if matches!(
            state.lifecycle,
            AuthorityLifecycle::Installed | AuthorityLifecycle::Active
        ) {
            if let Some(active) = &state.active {
                active.issuance_cancellation.cancel();
            }
            state.lifecycle = AuthorityLifecycle::Disabled;
            self.inner.stop.store(true, Ordering::Release);
            drop(state);
            wake_helper(&self.inner.socket_address);
        }
    }

    pub(super) fn teardown(&self, quiescence: ProcessQuiescence) -> WorkflowGitTeardownReport {
        let worker = {
            let mut state = self.lock();
            loop {
                match state.lifecycle {
                    AuthorityLifecycle::Destroyed => {
                        return state.teardown_report.clone().unwrap_or_else(|| {
                            WorkflowGitTeardownReport::from_observations(Vec::new(), false)
                        });
                    }
                    AuthorityLifecycle::TearingDown => {
                        state = self
                            .inner
                            .teardown_changed
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                    AuthorityLifecycle::Installed
                    | AuthorityLifecycle::Active
                    | AuthorityLifecycle::Disabled => {
                        if let Some(active) = &state.active {
                            active.issuance_cancellation.cancel();
                        }
                        state.lifecycle = AuthorityLifecycle::TearingDown;
                        self.inner.stop.store(true, Ordering::Release);
                        break state.worker.take();
                    }
                }
            }
        };
        wake_helper(&self.inner.socket_address);
        let worker_stopped = worker.is_none_or(|worker| worker.join().is_ok());

        let scoped_helper_key = format!("credential.{}.helper", self.inner.origin_text);
        let mut config_removed = unset_workflow_git_config(
            &self.inner.workspace,
            &self.inner.environment,
            &scoped_helper_key,
        );
        if !config_removed
            && quiescence == ProcessQuiescence::Proven
            && remove_file_if_present(&self.inner.workspace.join(".git/config.lock"))
        {
            config_removed = unset_workflow_git_config(
                &self.inner.workspace,
                &self.inner.environment,
                &scoped_helper_key,
            );
        }
        let socket_removed = remove_file_if_present(&self.inner.socket_path);
        let helper_removed = remove_file_if_present(&self.inner.helper_path);
        let alias_removed =
            remove_socket_alias(&self.inner.alias_directory, &self.inner.alias_link);

        let mut state = self.lock();
        retire_expired_issuances(&mut state, self.inner.clock.utc_now());
        let mut observations = std::mem::take(&mut state.expired_observations);
        observations.reserve(state.issuances.len());
        for credential in &state.issuances {
            let observation = if credential.expires_at <= self.inner.clock.utc_now() {
                RevocationObservation::LocallyExpired {
                    provider_expires_at: credential.expires_at,
                }
            } else if quiescence == ProcessQuiescence::Proven {
                match self
                    .inner
                    .broker
                    .revoke_workflow_git(&self.inner.assignment_id, &credential.token.0)
                {
                    Ok(revocation) if revocation.provider_expires_at == credential.expires_at => {
                        RevocationObservation::Settled(revocation)
                    }
                    Ok(_) | Err(_) => RevocationObservation::UnconfirmedUntilExpiry {
                        provider_expires_at: credential.expires_at,
                    },
                }
            } else {
                RevocationObservation::UnconfirmedUntilExpiry {
                    provider_expires_at: credential.expires_at,
                }
            };
            observations.push(observation);
        }
        state.issuances.clear();
        state.current_issuance = None;
        state.active = None;
        let local_state_destroyed =
            worker_stopped && config_removed && socket_removed && helper_removed && alias_removed;
        let report =
            WorkflowGitTeardownReport::from_observations(observations, local_state_destroyed);
        record_teardown(self.inner.recorder.as_ref(), &report);
        state.teardown_report = Some(report.clone());
        state.lifecycle = AuthorityLifecycle::Destroyed;
        self.inner.teardown_changed.notify_all();
        report
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AuthorityState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn serve_helper(listener: UnixListener, inner: Arc<WorkflowGitAuthorityInner>) {
    while !inner.stop.load(Ordering::Acquire) {
        let Ok((mut stream, _)) = listener.accept() else {
            break;
        };
        if inner.stop.load(Ordering::Acquire) {
            break;
        }
        let _ = stream
            .set_read_timeout(Some(HELPER_IO_TIMEOUT))
            .and_then(|()| stream.set_write_timeout(Some(HELPER_IO_TIMEOUT)));
        let request = read_frame(&mut stream, MAXIMUM_CREDENTIAL_REQUEST_BYTES);
        let matched = request
            .as_deref()
            .is_ok_and(|request| credential_request_matches(&inner.origin, request));
        if !matched || write_current_credential(&inner, &mut stream).is_err() {
            let _ = stream.write_all(&[0]);
            let _ = stream.flush();
        }
    }
}

fn write_current_credential(
    inner: &Arc<WorkflowGitAuthorityInner>,
    stream: &mut UnixStream,
) -> anyhow::Result<()> {
    let cancellation = {
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = inner.clock.utc_now();
        retire_expired_issuances(&mut state, now);
        let active = state
            .active
            .as_ref()
            .ok_or_else(|| anyhow!("workflow Git authority is inactive"))?;
        if state.lifecycle != AuthorityLifecycle::Active
            || !lease_is_current(&active.lease_clock, &active.authority)
        {
            return Err(anyhow!("workflow Git execution lease is fenced"));
        }
        let refresh_threshold = now
            .checked_add(TOKEN_REFRESH_MARGIN)
            .ok_or_else(|| anyhow!("workflow Git refresh threshold overflowed"))?;
        let current = state
            .current_issuance
            .and_then(|index| state.issuances.get(index));
        let reusable = current.is_some_and(|credential| credential.expires_at > refresh_threshold);
        if reusable {
            let credential =
                current.ok_or_else(|| anyhow!("workflow Git current issuance is missing"))?;
            write_token(stream, &credential.token.0)
                .context("return cached workflow Git credential")?;
            return Ok(());
        }
        active.issuance_cancellation.clone()
    };
    let issued = inner
        .broker
        .issue_workflow_git(&inner.assignment_id, &cancellation);
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let credential =
        issued.map_err(|failure| anyhow!("workflow Git credential broker failed: {failure:?}"))?;
    let origin_matches = credential.repository_url.as_ref() == inner.origin_text.as_ref();
    state.issuances.push(credential);
    let index = state
        .issuances
        .len()
        .checked_sub(1)
        .ok_or_else(|| anyhow!("workflow Git issuance was not retained"))?;
    let active = state
        .active
        .as_ref()
        .ok_or_else(|| anyhow!("workflow Git authority became inactive"))?;
    if state.lifecycle != AuthorityLifecycle::Active
        || !origin_matches
        || !lease_is_current(&active.lease_clock, &active.authority)
    {
        return Err(anyhow!("workflow Git authority changed during issuance"));
    }
    state.current_issuance = Some(index);
    let credential = state
        .issuances
        .get(index)
        .ok_or_else(|| anyhow!("workflow Git issuance disappeared"))?;
    write_token(stream, &credential.token.0).context("return newly issued workflow Git credential")
}

fn retire_expired_issuances(state: &mut AuthorityState, now: OffsetDateTime) {
    let current_issuance = state.current_issuance;
    let issuances = std::mem::take(&mut state.issuances);
    state.current_issuance = None;
    for (index, credential) in issuances.into_iter().enumerate() {
        if credential.expires_at <= now {
            state
                .expired_observations
                .push(RevocationObservation::LocallyExpired {
                    provider_expires_at: credential.expires_at,
                });
        } else {
            if current_issuance == Some(index) {
                state.current_issuance = Some(state.issuances.len());
            }
            state.issuances.push(credential);
        }
    }
}

fn write_token(stream: &mut UnixStream, token: &[u8]) -> io::Result<()> {
    if token.is_empty() || token.len() > MAXIMUM_TOKEN_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "workflow Git token is invalid",
        ));
    }
    stream.write_all(&[1])?;
    write_frame(stream, token)
}

fn lease_is_current(
    lease_clock: &LeaseClock,
    authority: &tokio::sync::watch::Receiver<LeaseAuthority>,
) -> bool {
    let authority = authority.borrow().clone();
    !authority.revoked
        && lease_clock.now().is_ok_and(|now| {
            matches!(
                now.checked_cmp(authority.force_stop_start),
                Ok(std::cmp::Ordering::Less)
            )
        })
}

fn credential_request_matches(origin: &Url, request: &[u8]) -> bool {
    let Ok(request) = std::str::from_utf8(request) else {
        return false;
    };
    let mut protocol = None;
    let mut host = None;
    let mut path = None;
    for line in request.lines() {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once('=') else {
            return false;
        };
        let target = match name {
            "protocol" => &mut protocol,
            "host" => &mut host,
            "path" => &mut path,
            "username" | "wwwauth[]" => continue,
            _ => return false,
        };
        if target.replace(value).is_some() {
            return false;
        }
    }
    let expected_host = match (origin.host_str(), origin.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_owned(),
        (None, None) => String::new(),
        (None, Some(_)) => return false,
    };
    protocol == Some(origin.scheme())
        && host.is_some_and(|host| host.eq_ignore_ascii_case(&expected_host))
        && path == Some(origin.path().trim_start_matches('/'))
}

fn write_frame(stream: &mut UnixStream, bytes: &[u8]) -> io::Result<()> {
    let length = u32::try_from(bytes.len()).map_err(io::Error::other)?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()
}

fn read_frame(stream: &mut UnixStream, maximum: usize) -> io::Result<Vec<u8>> {
    let mut encoded_length = [0_u8; 4];
    stream.read_exact(&mut encoded_length)?;
    let length = usize::try_from(u32::from_be_bytes(encoded_length)).map_err(io::Error::other)?;
    if length > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "workflow Git helper frame is oversized",
        ));
    }
    let mut bytes = vec![0_u8; length];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn create_socket_alias(private_root: &Path) -> io::Result<(PathBuf, PathBuf, PathBuf)> {
    for _ in 0..16 {
        let identity = ulid::Ulid::generate().to_string().to_ascii_lowercase();
        let directory = Path::new(SOCKET_ALIAS_ROOT).join(format!(".szg-{identity}"));
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(&directory) {
            Ok(()) => {
                let link = directory.join(SOCKET_ALIAS_LINK);
                if let Err(error) = symlink(private_root, &link) {
                    let _ = fs::remove_dir(&directory);
                    return Err(error);
                }
                let address = link.join(SOCKET_FILE);
                return Ok((directory, link, address));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "workflow Git socket alias identities were exhausted",
    ))
}

fn remove_socket_alias(directory: &Path, link: &Path) -> bool {
    remove_file_if_present(link)
        & match fs::remove_dir(directory) {
            Ok(()) => true,
            Err(error) => error.kind() == io::ErrorKind::NotFound,
        }
}

fn remove_file_if_present(path: &Path) -> bool {
    match fs::remove_file(path) {
        Ok(()) => true,
        Err(error) => error.kind() == io::ErrorKind::NotFound,
    }
}

fn helper_script(executable: &Path, socket: &Path) -> io::Result<String> {
    Ok(format!(
        "#!/bin/sh\nset -eu\nexport {INTERNAL_HELPER_ENVIRONMENT}={INTERNAL_HELPER_VERSION}\nexport {INTERNAL_HELPER_SOCKET_ENVIRONMENT}={}\nexec {} \"$@\"\n",
        shell_quote(socket)?,
        shell_quote(executable)?,
    ))
}

fn shell_quote(path: &Path) -> io::Result<String> {
    let raw = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "workflow Git helper path is not UTF-8",
        )
    })?;
    Ok(format!("'{}'", raw.replace('\'', "'\\''")))
}

fn write_executable(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o700)
        .open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.set_permissions(Permissions::from_mode(0o700))
}

fn set_local_config(
    workspace: &Path,
    environment: &EnvironmentSnapshot,
    key: &str,
    value: &str,
    cancellation: &CaptureCancellation,
) -> anyhow::Result<()> {
    if cancellation.is_cancelled() {
        return Err(anyhow!("workflow Git helper configuration was fenced"));
    }
    let status = isolated_git(workspace, environment)
        .args(["config", "--local", "--replace-all", key, value])
        .status()
        .context("run workflow Git config")?;
    if cancellation.is_cancelled() || !status.success() {
        Err(anyhow!("workflow Git config did not succeed"))
    } else {
        Ok(())
    }
}

fn unset_workflow_git_config(
    workspace: &Path,
    environment: &EnvironmentSnapshot,
    scoped_helper_key: &str,
) -> bool {
    let scoped_helper_removed = unset_local_config(workspace, environment, scoped_helper_key);
    let helper_reset_removed = unset_local_config(workspace, environment, "credential.helper");
    let use_http_path_removed =
        unset_local_config(workspace, environment, "credential.useHttpPath");
    scoped_helper_removed && helper_reset_removed && use_http_path_removed
}

fn unset_local_config(workspace: &Path, environment: &EnvironmentSnapshot, key: &str) -> bool {
    isolated_git(workspace, environment)
        .args(["config", "--local", "--unset-all", key])
        .status()
        .is_ok_and(|status| status.success() || status.code() == Some(5))
}

fn isolated_git(workspace: &Path, environment: &EnvironmentSnapshot) -> Command {
    let mut command = super::source::isolated_git_command(workspace, environment);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

fn wake_helper(socket: &Path) {
    let _ = UnixStream::connect(socket);
}

fn record_teardown(
    recorder: Option<&Arc<crate::runner::telemetry::Recorder>>,
    report: &WorkflowGitTeardownReport,
) {
    let Some(recorder) = recorder else {
        return;
    };
    for (index, observation) in report.observations.iter().enumerate() {
        let (outcome, issuance_id, provider_expires_at) = match observation {
            RevocationObservation::Settled(revocation) => (
                match revocation.outcome {
                    WorkflowGitRevocationOutcome::Revoked => "revoked",
                    WorkflowGitRevocationOutcome::AlreadyRevoked => "already_revoked",
                    WorkflowGitRevocationOutcome::Expired => "expired",
                    WorkflowGitRevocationOutcome::ResidualUntilExpiry => "residual_until_expiry",
                },
                revocation.issuance_id.clone(),
                revocation.provider_expires_at,
            ),
            RevocationObservation::LocallyExpired {
                provider_expires_at,
            } => (
                "expired",
                format!("local-issuance-{}", index + 1),
                *provider_expires_at,
            ),
            RevocationObservation::UnconfirmedUntilExpiry {
                provider_expires_at,
            } => (
                "unconfirmed_until_expiry",
                format!("local-issuance-{}", index + 1),
                *provider_expires_at,
            ),
        };
        recorder.record(
            "runner.workflow_git_revocation",
            [
                KeyValue::new("workflow_git.revocation_outcome", outcome),
                KeyValue::new("workflow_git.issuance_id", issuance_id),
                KeyValue::new(
                    "workflow_git.provider_expires_at_unix",
                    provider_expires_at.unix_timestamp(),
                ),
            ],
        );
    }
    recorder.record(
        "runner.workflow_git_teardown",
        [
            KeyValue::new(
                "workflow_git.revocation_summary",
                match report.summary {
                    RevocationSummary::NoIssuance => "no_issuance",
                    RevocationSummary::FullyRevoked => "fully_revoked",
                    RevocationSummary::FullyRevokedWithReplay => "fully_revoked_with_replay",
                    RevocationSummary::Expired => "expired",
                    RevocationSummary::SettledByRevocationAndExpiry => {
                        "settled_by_revocation_and_expiry"
                    }
                    RevocationSummary::PartialResidualUntilExpiry => {
                        "partial_residual_until_expiry"
                    }
                    RevocationSummary::ResidualUntilExpiry => "residual_until_expiry",
                },
            ),
            KeyValue::new(
                "workflow_git.local_state_destroyed",
                report.local_state_destroyed,
            ),
        ],
    );
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Mutex;

    use super::*;
    use crate::runner::service::source::{
        CommitAvailability, CredentialBrokerFailure, ProviderSecret, WorkflowGitRevocationOutcome,
    };

    const FIXTURE_ORIGIN: &str = "https://github.example/acme/private.git";

    struct FixtureBroker {
        origin: Arc<str>,
        credentials: Mutex<VecDeque<(Vec<u8>, OffsetDateTime)>>,
        revocations: Mutex<VecDeque<WorkflowGitRevocationOutcome>>,
        expiries: BTreeMap<Vec<u8>, OffsetDateTime>,
        revoked_tokens: Mutex<Vec<Vec<u8>>>,
    }

    impl FixtureBroker {
        fn new(
            origin: &str,
            credentials: impl IntoIterator<Item = (&'static [u8], OffsetDateTime)>,
            revocations: impl IntoIterator<Item = WorkflowGitRevocationOutcome>,
        ) -> Self {
            let credentials = credentials
                .into_iter()
                .map(|(token, expires)| (token.to_vec(), expires))
                .collect::<Vec<_>>();
            Self {
                origin: Arc::from(origin),
                credentials: Mutex::new(credentials.iter().cloned().collect()),
                revocations: Mutex::new(revocations.into_iter().collect()),
                expiries: credentials.into_iter().collect(),
                revoked_tokens: Mutex::new(Vec::new()),
            }
        }
    }

    impl SourceCredentialBroker for FixtureBroker {
        // Runtime-authority tests deliberately make preparation operations unavailable.
        // jscpd:ignore-start
        fn issue(
            &self,
            _assignment_id: &str,
            _cancellation: &CaptureCancellation,
        ) -> Result<ProviderCredential, CredentialBrokerFailure> {
            Err(CredentialBrokerFailure::Unavailable)
        }

        fn commit_availability(
            &self,
            _assignment_id: &str,
            _cancellation: &CaptureCancellation,
        ) -> Result<CommitAvailability, CredentialBrokerFailure> {
            Err(CredentialBrokerFailure::Unavailable)
        }

        // jscpd:ignore-end
        fn issue_workflow_git(
            &self,
            _assignment_id: &str,
            cancellation: &CaptureCancellation,
        ) -> Result<ProviderCredential, CredentialBrokerFailure> {
            super::super::source::ensure_broker_current(cancellation)?;
            let (token, expires_at) = self
                .credentials
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(CredentialBrokerFailure::Unavailable)?;
            Ok(ProviderCredential {
                repository_url: Arc::clone(&self.origin),
                token: ProviderSecret(token),
                expires_at,
            })
        }

        fn revoke_workflow_git(
            &self,
            _assignment_id: &str,
            token: &[u8],
        ) -> Result<WorkflowGitRevocation, CredentialBrokerFailure> {
            self.revoked_tokens.lock().unwrap().push(token.to_vec());
            let outcome = self
                .revocations
                .lock()
                .unwrap()
                .pop_front()
                .ok_or(CredentialBrokerFailure::Unavailable)?;
            let provider_expires_at = *self
                .expiries
                .get(token)
                .ok_or(CredentialBrokerFailure::InvalidResponse)?;
            Ok(WorkflowGitRevocation {
                issuance_id: format!("gti_{}", self.revoked_tokens.lock().unwrap().len()),
                outcome,
                provider_expires_at,
            })
        }
    }

    fn fixture_broker(
        credentials: impl IntoIterator<Item = (&'static [u8], OffsetDateTime)>,
        revocations: impl IntoIterator<Item = WorkflowGitRevocationOutcome>,
    ) -> Arc<FixtureBroker> {
        Arc::new(FixtureBroker::new(FIXTURE_ORIGIN, credentials, revocations))
    }

    struct Fixture {
        _temporary: tempfile::TempDir,
        workspace: PathBuf,
        private: PathBuf,
    }

    fn fixture() -> Fixture {
        let temporary = tempfile::tempdir().unwrap();
        let workspace = temporary.path().join("workspace");
        let private = temporary.path().join("private");
        fs::create_dir(&private).unwrap();
        let status = Command::new("git")
            .args(["init", "--quiet"])
            .arg(&workspace)
            .status()
            .unwrap();
        assert!(status.success());
        let status = Command::new("git")
            .current_dir(&workspace)
            .args(["config", "user.name", "Fixture"])
            .status()
            .unwrap();
        assert!(status.success());
        let status = Command::new("git")
            .current_dir(&workspace)
            .args(["config", "user.email", "fixture@example.test"])
            .status()
            .unwrap();
        assert!(status.success());
        fs::write(workspace.join("tracked"), b"fixture\n").unwrap();
        let status = Command::new("git")
            .current_dir(&workspace)
            .args(["add", "tracked"])
            .status()
            .unwrap();
        assert!(status.success());
        let status = Command::new("git")
            .current_dir(&workspace)
            .args(["commit", "--quiet", "-m", "fixture"])
            .status()
            .unwrap();
        assert!(status.success());
        let status = Command::new("git")
            .current_dir(&workspace)
            .args(["checkout", "--quiet", "--detach", "HEAD"])
            .status()
            .unwrap();
        assert!(status.success());
        Fixture {
            _temporary: temporary,
            workspace,
            private,
        }
    }

    fn lease() -> (LeaseClock, tokio::sync::watch::Sender<LeaseAuthority>) {
        let clock = LeaseClock::system().unwrap();
        let now = clock.now().unwrap();
        let authority = LeaseAuthority {
            sequence: 1,
            basis: now,
            renewal_request: now.checked_add(Duration::from_secs(30)).unwrap(),
            cancellation_start: now.checked_add(Duration::from_secs(40)).unwrap(),
            force_stop_start: now.checked_add(Duration::from_secs(50)).unwrap(),
            force_stop_end: now.checked_add(Duration::from_secs(55)).unwrap(),
            local_expiry: now.checked_add(Duration::from_secs(60)).unwrap(),
            terminal_report_delivery_budget: Duration::from_secs(5),
            revoked: false,
        };
        let (sender, _) = tokio::sync::watch::channel(authority);
        (clock, sender)
    }

    fn activate(authority: &WorkflowGitAuthority) -> tokio::sync::watch::Sender<LeaseAuthority> {
        let (clock, lease) = lease();
        authority.activate(clock, lease.subscribe()).unwrap();
        lease
    }

    fn teardown_and_assert_revocations(
        authority: &WorkflowGitAuthority,
        broker: &FixtureBroker,
        expected_summary: RevocationSummary,
        expected_tokens: &[&[u8]],
    ) -> WorkflowGitTeardownReport {
        let report = authority.teardown(ProcessQuiescence::Proven);
        assert!(report.local_state_destroyed);
        assert_eq!(report.summary, expected_summary);
        assert_eq!(
            broker.revoked_tokens.lock().unwrap().as_slice(),
            expected_tokens
        );
        report
    }

    fn request(authority: &WorkflowGitAuthority, repository: &str) -> Option<Vec<u8>> {
        let mut stream = UnixStream::connect(&authority.inner.socket_address).ok()?;
        let request = format!("protocol=https\nhost=github.example\npath={repository}\n\n");
        write_frame(&mut stream, request.as_bytes()).ok()?;
        let mut status = [0_u8; 1];
        stream.read_exact(&mut status).ok()?;
        (status == [1])
            .then(|| read_frame(&mut stream, MAXIMUM_TOKEN_BYTES).ok())
            .flatten()
    }

    fn install(fixture: &Fixture, broker: Arc<FixtureBroker>) -> WorkflowGitAuthority {
        install_with_clock(
            fixture,
            broker,
            Arc::new(crate::runner::service::TokioSleeper),
        )
    }

    fn install_with_clock(
        fixture: &Fixture,
        broker: Arc<FixtureBroker>,
        clock: Arc<dyn Sleeper>,
    ) -> WorkflowGitAuthority {
        WorkflowGitAuthority::install(WorkflowGitInstall {
            broker,
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
            origin: Arc::from(FIXTURE_ORIGIN),
            workspace: &fixture.workspace,
            private_root: &fixture.private,
            environment: &EnvironmentSnapshot::new([("PATH", std::env::var_os("PATH").unwrap())]),
            helper_executable: &test_helper_executable(),
            clock,
            recorder: None,
            cancellation: &CaptureCancellation::default(),
        })
        .unwrap()
    }

    fn test_helper_executable() -> PathBuf {
        std::env::current_exe()
            .unwrap()
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .join(format!("scherzo-cloud{}", std::env::consts::EXE_SUFFIX))
    }

    #[test]
    fn exact_origin_helper_is_inactive_before_start_and_refuses_other_repositories_after_start() {
        let fixture = fixture();
        let expires = crate::timing::utc_now() + time::Duration::hours(1);
        let broker = fixture_broker(
            [(b"current-runtime-token".as_slice(), expires)],
            [WorkflowGitRevocationOutcome::Revoked],
        );
        let authority = install(&fixture, Arc::clone(&broker));
        let initial_config = fs::read(fixture.workspace.join(".git/config")).unwrap();
        let initial_helper = fs::read(&authority.inner.helper_path).unwrap();
        assert!(
            !initial_config
                .windows(b"current-runtime-token".len())
                .any(|window| window == b"current-runtime-token")
        );
        assert!(
            !initial_helper
                .windows(b"current-runtime-token".len())
                .any(|window| window == b"current-runtime-token")
        );
        assert!(UnixStream::connect(&authority.inner.socket_address).is_err());
        assert!(
            !git_credential(
                &fixture.workspace,
                "https://github.example/acme/private.git"
            )
            .status
            .success()
        );
        assert_eq!(
            run_git(&fixture.workspace, ["branch", "--show-current"]).stdout,
            b""
        );
        assert!(
            !run_git(&fixture.workspace, ["symbolic-ref", "-q", "HEAD"])
                .status
                .success()
        );

        let lease = activate(&authority);
        let credential = git_credential(
            &fixture.workspace,
            "https://github.example/acme/private.git",
        );
        assert!(credential.status.success());
        assert!(
            credential
                .stdout
                .windows(b"password=current-runtime-token".len())
                .any(|window| window == b"password=current-runtime-token")
        );
        let active_config = fs::read(fixture.workspace.join(".git/config")).unwrap();
        let active_helper = fs::read(&authority.inner.helper_path).unwrap();
        assert!(
            !active_config
                .windows(b"current-runtime-token".len())
                .any(|window| window == b"current-runtime-token")
        );
        assert!(
            !active_helper
                .windows(b"current-runtime-token".len())
                .any(|window| window == b"current-runtime-token")
        );
        assert_eq!(request(&authority, "acme/second.git"), None);
        assert!(!run_git(&fixture.workspace, ["pull"]).status.success());
        lease.send_modify(|authority| authority.revoked = true);
        assert_eq!(request(&authority, "acme/private.git"), None);

        authority.disable();
        assert_eq!(request(&authority, "acme/private.git"), None);
        assert!(
            !git_credential(
                &fixture.workspace,
                "https://github.example/acme/private.git"
            )
            .status
            .success()
        );
        let report = authority.teardown(ProcessQuiescence::Proven);
        assert!(report.local_state_destroyed);
        assert_eq!(report.summary, RevocationSummary::FullyRevoked);
        assert!(!authority.inner.helper_path.exists());
        assert!(!authority.inner.socket_path.exists());
        assert!(!authority.inner.alias_directory.exists());
    }

    #[test]
    fn managed_helper_resets_inherited_global_credential_helpers() {
        let fixture = fixture();
        let global_home = fixture._temporary.path().join("global-home");
        fs::create_dir(&global_home).unwrap();
        let global_helper = fixture._temporary.path().join("global-credential-helper");
        write_executable(
            &global_helper,
            b"#!/bin/sh\nprintf 'username=global\\npassword=unrelated-global-token\\n\\n'\n",
        )
        .unwrap();
        fs::write(
            global_home.join(".gitconfig"),
            format!(
                "[credential]\n\thelper = {}\n",
                global_helper.to_string_lossy()
            ),
        )
        .unwrap();
        let expires = crate::timing::utc_now() + time::Duration::hours(1);
        let broker = fixture_broker(
            [(b"current-runtime-token".as_slice(), expires)],
            [WorkflowGitRevocationOutcome::Revoked],
        );
        let authority = install(&fixture, Arc::clone(&broker));
        activate(&authority);

        let credential = git_credential_with_home(
            &fixture.workspace,
            "https://github.example/acme/second.git",
            &global_home,
        );

        let inherited_credential_was_used = credential.status.success();
        let report = authority.teardown(ProcessQuiescence::Proven);
        assert!(report.local_state_destroyed);
        assert!(
            !inherited_credential_was_used,
            "managed source access must not fall back to a runner-global credential helper"
        );
    }

    #[test]
    fn replacement_issuances_are_revoked_independently_and_report_partial_residual() {
        let fixture = fixture();
        let first_expiry = crate::timing::utc_now() + time::Duration::seconds(5);
        let second_expiry = crate::timing::utc_now() + time::Duration::hours(1);
        let broker = fixture_broker(
            [
                (b"first-runtime-token".as_slice(), first_expiry),
                (b"second-runtime-token".as_slice(), second_expiry),
            ],
            [
                WorkflowGitRevocationOutcome::Revoked,
                WorkflowGitRevocationOutcome::ResidualUntilExpiry,
            ],
        );
        let authority = install(&fixture, Arc::clone(&broker));
        activate(&authority);
        assert_eq!(
            request(&authority, "acme/private.git"),
            Some(b"first-runtime-token".to_vec())
        );
        assert_eq!(
            request(&authority, "acme/private.git"),
            Some(b"second-runtime-token".to_vec())
        );

        let report = teardown_and_assert_revocations(
            &authority,
            &broker,
            RevocationSummary::PartialResidualUntilExpiry,
            &[
                b"first-runtime-token".as_slice(),
                b"second-runtime-token".as_slice(),
            ],
        );
        assert_eq!(report.observations.len(), 2);
    }

    #[tokio::test]
    async fn expired_replacement_is_destroyed_without_a_gateway_revocation_call() {
        let fixture = fixture();
        let (clock, mut waits) = crate::runner::service::test_support::controlled_sleeper();
        let now = clock.utc_now();
        let first_expiry = now + time::Duration::seconds(5);
        let second_expiry = now + time::Duration::hours(1);
        let broker = fixture_broker(
            [
                (b"expired-runtime-token".as_slice(), first_expiry),
                (b"live-runtime-token".as_slice(), second_expiry),
            ],
            [WorkflowGitRevocationOutcome::Revoked],
        );
        let authority = install_with_clock(&fixture, Arc::clone(&broker), Arc::clone(&clock));
        activate(&authority);
        assert_eq!(
            request(&authority, "acme/private.git"),
            Some(b"expired-runtime-token".to_vec())
        );

        let advance = tokio::spawn(async move {
            clock.sleep(Duration::from_secs(10)).await;
        });
        let (duration, release) = waits
            .recv()
            .await
            .expect("controlled clock advance was not requested");
        assert_eq!(duration, Duration::from_secs(10));
        release.release();
        advance.await.unwrap();
        assert_eq!(
            request(&authority, "acme/private.git"),
            Some(b"live-runtime-token".to_vec())
        );

        let report = teardown_and_assert_revocations(
            &authority,
            &broker,
            RevocationSummary::SettledByRevocationAndExpiry,
            &[b"live-runtime-token".as_slice()],
        );
        assert_eq!(report.observations.len(), 2);
    }

    #[test]
    fn provider_revocation_failure_still_destroys_local_state_and_reports_residual() {
        let fixture = fixture();
        let expires = crate::timing::utc_now() + time::Duration::hours(1);
        let broker = fixture_broker([(b"revoke-failure-token".as_slice(), expires)], []);
        let authority = install(&fixture, Arc::clone(&broker));
        activate(&authority);
        assert_eq!(
            request(&authority, "acme/private.git"),
            Some(b"revoke-failure-token".to_vec())
        );

        let report = authority.teardown(ProcessQuiescence::Proven);

        assert!(report.local_state_destroyed);
        assert_eq!(report.summary, RevocationSummary::ResidualUntilExpiry);
        assert_eq!(
            broker.revoked_tokens.lock().unwrap().as_slice(),
            [b"revoke-failure-token".as_slice()]
        );
        assert!(!authority.inner.helper_path.exists());
        assert!(!authority.inner.socket_path.exists());
    }

    #[test]
    fn failed_process_quiescence_destroys_local_state_without_claiming_provider_revocation() {
        let fixture = fixture();
        let expires = crate::timing::utc_now() + time::Duration::hours(1);
        let broker = fixture_broker(
            [(b"process-loss-token".as_slice(), expires)],
            [WorkflowGitRevocationOutcome::Revoked],
        );
        let authority = install(&fixture, Arc::clone(&broker));
        activate(&authority);
        assert_eq!(
            request(&authority, "acme/private.git"),
            Some(b"process-loss-token".to_vec())
        );

        let report = authority.teardown(ProcessQuiescence::Failed);

        assert!(report.local_state_destroyed);
        assert_eq!(report.summary, RevocationSummary::ResidualUntilExpiry);
        assert!(broker.revoked_tokens.lock().unwrap().is_empty());
        assert!(!authority.inner.helper_path.exists());
        assert!(!authority.inner.socket_path.exists());
    }

    #[test]
    fn revocation_summary_distinguishes_replay_expiry_and_unconfirmed_residual() {
        let expiry = crate::timing::utc_now() + time::Duration::hours(1);
        let replay = WorkflowGitTeardownReport::from_observations(
            vec![RevocationObservation::Settled(WorkflowGitRevocation {
                issuance_id: "gti_replay".to_owned(),
                outcome: WorkflowGitRevocationOutcome::AlreadyRevoked,
                provider_expires_at: expiry,
            })],
            true,
        );
        assert_eq!(replay.summary, RevocationSummary::FullyRevokedWithReplay);
        let expired = WorkflowGitTeardownReport::from_observations(
            vec![RevocationObservation::Settled(WorkflowGitRevocation {
                issuance_id: "gti_expired".to_owned(),
                outcome: WorkflowGitRevocationOutcome::Expired,
                provider_expires_at: expiry,
            })],
            true,
        );
        assert_eq!(expired.summary, RevocationSummary::Expired);
        let residual = WorkflowGitTeardownReport::from_observations(
            vec![RevocationObservation::UnconfirmedUntilExpiry {
                provider_expires_at: expiry,
            }],
            true,
        );
        assert_eq!(residual.summary, RevocationSummary::ResidualUntilExpiry);
    }

    struct GitOutput {
        status: std::process::ExitStatus,
        stdout: Vec<u8>,
    }

    fn git_credential(workspace: &Path, url: &str) -> GitOutput {
        run_git_credential(workspace, url, None)
    }

    fn git_credential_with_home(workspace: &Path, url: &str, home: &Path) -> GitOutput {
        run_git_credential(workspace, url, Some(home))
    }

    fn run_git_credential(workspace: &Path, url: &str, home: Option<&Path>) -> GitOutput {
        let mut command = Command::new("git");
        command
            .current_dir(workspace)
            .args(["credential", "fill"])
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(home) = home {
            command.env("HOME", home).env_remove("GIT_CONFIG_GLOBAL");
        }
        let mut child = command.spawn().unwrap();
        let mut input = child.stdin.take().unwrap();
        writeln!(input, "url={url}\n").unwrap();
        drop(input);
        let output = child.wait_with_output().unwrap();
        GitOutput {
            status: output.status,
            stdout: output.stdout,
        }
    }

    fn run_git<const N: usize>(workspace: &Path, arguments: [&str; N]) -> GitOutput {
        let output = Command::new("git")
            .current_dir(workspace)
            .args(arguments)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap();
        GitOutput {
            status: output.status,
            stdout: output.stdout,
        }
    }
}
