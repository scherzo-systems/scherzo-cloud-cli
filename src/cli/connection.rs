// CLI families keep their domain imports local rather than introducing a cross-family module.
// jscpd:ignore-start
use std::io::{self, Write};
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use scherzo_cloud_api::{
    HttpClient, LinearApi, LinearConnection, LinearFailure, LinearSession, LinearSessionStatus,
};
use scherzo_cloud_human_auth::Deployment;
// jscpd:ignore-end

use super::{OrganizationArg, PrincipalAuthenticationArgs};

pub(super) const ABOUT: &str = "Manage source connections";
const NAME: &str = "connection";
type Options = super::CommonArgs<super::StreamingJson, PrincipalAuthenticationArgs>;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<Provider>,
}
#[derive(Debug, Subcommand)]
enum Provider {
    #[command(about = "Manage Linear workspace connections")]
    Linear(LinearCommand),
}
#[derive(Debug, Args)]
struct LinearCommand {
    #[command(subcommand)]
    command: Option<Leaf>,
}
#[derive(Debug, Subcommand)]
enum Leaf {
    #[command(about = "Manage Linear authorization sessions")]
    Authorization(AuthorizationCommand),
    #[command(about = "Delete a Linear connection and release its workspace")]
    Delete(ConfirmationTarget),
    #[command(about = "List Linear connections")]
    List(List),
    #[command(about = "Remove a Linear connection credential and reserve its workspace")]
    Remove(ConfirmationTarget),
    #[command(about = "Show a Linear connection and its authorization state")]
    Show(Target),
}
#[derive(Debug, Args)]
struct AuthorizationCommand {
    #[command(subcommand)]
    command: Option<AuthorizationLeaf>,
}
#[derive(Debug, Subcommand)]
enum AuthorizationLeaf {
    #[command(about = "Create a Linear authorization session")]
    Create(Start),
    #[command(about = "Show an authorization session")]
    Show(SessionTarget),
    #[command(about = "Wait for an authorization session")]
    Wait(Wait),
}
#[derive(Debug, Args)]
struct Start {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(
        long,
        value_name = "CONNECTION",
        help = "Existing Linear connection ID to reauthorize; omit to connect a new workspace"
    )]
    connection_id: Option<String>,
    #[command(flatten)]
    wait: super::WaitTimeoutArgs,
    #[arg(
        long,
        conflicts_with = "timeout",
        help = "Return after starting consent instead of waiting for the callback"
    )]
    no_wait: bool,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
struct SessionTarget {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = "SESSION", help = "Linear authorization session ID")]
    session: String,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
struct Wait {
    #[command(flatten)]
    target: SessionTarget,
    #[command(flatten)]
    wait: super::WaitTimeoutArgs,
}
#[derive(Debug, Args)]
struct Target {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = "CONNECTION", help = "Linear connection ID")]
    connection: String,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
struct ConfirmationTarget {
    #[command(flatten)]
    target: Target,
    #[command(flatten)]
    confirmation: super::ConfirmationArgs,
}
// Linear connection pagination has a separate output and authority contract from publication lists.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct List {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[command(flatten)]
    page: super::PaginationArgs<100>,
    #[command(flatten)]
    options: Options,
}
// jscpd:ignore-end

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(Provider::Linear(command)) => command.execute(),
        }
    }
}
impl LinearCommand {
    fn execute(self) -> super::CommandResult {
        let path = &[NAME, "linear"];
        match self.command {
            None => super::print_help(path),
            Some(Leaf::Authorization(cmd)) => match cmd.command {
                None => super::print_help(&[NAME, "linear", "authorization"]),
                Some(AuthorizationLeaf::Create(cmd)) => deployment(cmd, |cmd, d| cmd.start(d)),
                Some(AuthorizationLeaf::Show(cmd)) => deployment(cmd, |cmd, d| cmd.show(d)),
                Some(AuthorizationLeaf::Wait(cmd)) => deployment(cmd, |cmd, d| cmd.wait(d)),
            },
            Some(Leaf::List(cmd)) => deployment(cmd, |cmd, d| cmd.execute(d)),
            Some(Leaf::Show(cmd)) => deployment(cmd, |cmd, d| cmd.execute(d, Action::Show)),
            Some(Leaf::Remove(cmd)) => {
                deployment(cmd, |cmd, d| cmd.target.execute(d, Action::Disconnect))
            }
            Some(Leaf::Delete(cmd)) => {
                deployment(cmd, |cmd, d| cmd.target.execute(d, Action::Delete))
            }
        }
    }
}
fn deployment<T>(
    cmd: T,
    f: impl FnOnce(T, Deployment) -> super::CommandResult,
) -> super::CommandResult {
    super::execute_deployment_command(
        Some(cmd),
        &[NAME],
        "configure Linear connection access",
        |cmd, d| f(cmd, d.clone()),
    )
}

fn with_api<T>(
    deployment: &Deployment,
    options: &Options,
    mut f: impl FnMut(&LinearApi) -> Result<T, LinearFailure>,
) -> anyhow::Result<Result<T, LinearFailure>> {
    let policy = options.http.transport_policy();
    let client = HttpClient::new(policy)
        .map_err(|error| anyhow!(error))
        .context("prepare human session networking")?;
    super::execute_selected_api_operation(
        super::principal_api_context(
            &client,
            deployment,
            &options.authentication,
            "acquire human session",
        ),
        |token| {
            let api = LinearApi::new(deployment.fingerprint().api_url(), token, policy)
                .map_err(|error| anyhow!(error))
                .context("prepare Linear connection networking")?;
            Ok(f(&api))
        },
        LinearFailure::credential_rejected,
        || LinearFailure::Unauthenticated,
        |category| LinearFailure::Unreachable { category },
    )
}

#[derive(Clone)]
struct Recovery {
    session: Option<String>,
}

impl Start {
    fn start(self, deployment: Deployment) -> super::CommandResult {
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate Linear authorization request key")?;
        let timeout = if self.no_wait {
            None
        } else {
            Some(self.wait.timeout.unwrap_or(Duration::from_secs(900)))
        };
        let recovery = Recovery { session: None };
        let signal_deployment = deployment.clone();
        let signal_organization = self.organization.to_string();
        let json = self.options.json;
        let timeout_deployment = signal_deployment.clone();
        let timeout_organization = signal_organization.clone();
        super::execute_mutation_with_signals_and_deferred_timeout(
            "Linear authorization",
            recovery,
            timeout,
            move |control, timeout_start| {
                if !control.begin_dispatch() {
                    return Ok(ExitCode::Interrupted);
                }
                let result = with_api(&deployment, &self.options, |api| {
                    // A lost response may have committed the session. Replay only this key.
                    match api.start(&self.organization, self.connection_id.as_deref(), &key) {
                        Err(LinearFailure::Unreachable { .. }) => {
                            api.start(&self.organization, self.connection_id.as_deref(), &key)
                        }
                        result => result,
                    }
                })?;
                let session = match result {
                    Ok(session) => session,
                    Err(failure) => {
                        return finish(control, || {
                            failure_output(
                                &deployment,
                                &self.organization,
                                None,
                                None,
                                &failure,
                                self.options.authentication.kind(),
                                json,
                            )
                        });
                    }
                };
                if !control.update_recovery(Recovery {
                    session: Some(session.id.clone()),
                }) {
                    return Ok(ExitCode::Interrupted);
                }
                if self.no_wait || session.status != LinearSessionStatus::LinearSessionPending {
                    return finish(control, || {
                        session_output(&deployment, &self.organization, &session, json)
                    });
                }
                if control.is_cancelled() {
                    return Ok(ExitCode::Interrupted);
                }
                session_output(&deployment, &self.organization, &session, json)?;
                timeout_start.start();
                observe(
                    &deployment,
                    &self.organization,
                    &session.id,
                    &self.options,
                    control,
                    POLL_INTERVAL,
                )
            },
            move |signal, snapshot| {
                if !snapshot.dispatched {
                    return Ok(signal);
                }
                stopped(
                    &signal_deployment,
                    &signal_organization,
                    &snapshot.recovery,
                    signal,
                    json,
                )
            },
            {
                let d = timeout_deployment;
                let org = timeout_organization;
                move |snapshot| {
                    stopped(&d, &org, &snapshot.recovery, ExitCode::GeneralFailure, json)
                }
            },
        )
    }
}

fn finish<R>(
    control: &super::OperationControl<R>,
    output: impl FnOnce() -> anyhow::Result<ExitCode>,
) -> super::CommandResult {
    super::complete_operation(control, || output().map_err(Into::into))
}
fn stopped(
    d: &Deployment,
    org: &str,
    recovery: &Recovery,
    signal: ExitCode,
    json: bool,
) -> super::CommandResult {
    status_output(
        d,
        org,
        stop_outcome(signal),
        recovery.session.as_deref(),
        json,
    )?;
    Ok(signal)
}

fn stop_outcome(signal: ExitCode) -> &'static str {
    if signal == ExitCode::GeneralFailure {
        "timed_out"
    } else {
        "observation_stopped"
    }
}

fn observe(
    d: &Deployment,
    org: &str,
    id: &str,
    options: &Options,
    control: &super::OperationControl<Recovery>,
    mut next: Duration,
) -> super::CommandResult {
    loop {
        if !wait_for_poll(control, next, &super::SystemObservationClock) {
            return Ok(ExitCode::Interrupted);
        }
        let result = with_api(d, options, |api| api.session(org, id))?;
        match result {
            Ok(session) if session.status == LinearSessionStatus::LinearSessionPending => {
                next = POLL_INTERVAL
            }
            Ok(session) => {
                return finish(control, || session_output(d, org, &session, options.json));
            }
            Err(LinearFailure::RateLimited { retry_after }) => next = retry_delay(retry_after),
            Err(failure) => {
                return finish(control, || {
                    failure_output(
                        d,
                        org,
                        Some(id),
                        None,
                        &failure,
                        options.authentication.kind(),
                        options.json,
                    )
                });
            }
        }
    }
}

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const POLL_WAIT_SLICE: Duration = Duration::from_millis(100);

fn wait_for_poll(
    control: &super::OperationControl<Recovery>,
    delay: Duration,
    clock: &impl super::ObservationClock,
) -> bool {
    let started = clock.now();
    loop {
        if control.is_cancelled() {
            return false;
        }
        let remaining = delay.saturating_sub(clock.now().saturating_duration_since(started));
        if remaining.is_zero() {
            return true;
        }
        clock.sleep(remaining.min(POLL_WAIT_SLICE));
    }
}

fn retry_delay(retry_after: Option<u64>) -> Duration {
    Duration::from_secs(retry_after.unwrap_or(5).max(5))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polling_rate_limit_never_precedes_retry_after() {
        assert_eq!(retry_delay(Some(9)), Duration::from_secs(9));
        assert_eq!(retry_delay(None), POLL_INTERVAL);
        assert_eq!(retry_delay(Some(0)), POLL_INTERVAL);
        assert_eq!(retry_delay(Some(3)), POLL_INTERVAL);
        assert_eq!(retry_delay(Some(600)), Duration::from_secs(600));
        assert_eq!(stop_outcome(ExitCode::GeneralFailure), "timed_out");
        assert_eq!(stop_outcome(ExitCode::Interrupted), "observation_stopped");
    }

    #[test]
    fn poll_wait_honors_cadence_and_server_backoff_without_real_time() {
        let clock = super::super::observation_test_support::ControlledObservationClock::new(
            scherzo_cloud_support::monotonic_now(),
        );
        let control = super::super::OperationControl::new(Recovery { session: None });
        assert!(wait_for_poll(&control, POLL_INTERVAL, &clock));
        assert!(wait_for_poll(&control, retry_delay(Some(9)), &clock));
        let sleeps = clock.into_sleeps();
        assert!(sleeps.iter().all(|slice| *slice <= POLL_WAIT_SLICE));
        assert_eq!(sleeps.iter().sum::<Duration>(), Duration::from_secs(14));
    }

    #[test]
    fn long_retry_after_stops_when_observation_is_cancelled() {
        use std::cell::Cell;
        use std::time::Instant;

        struct CancellingClock<'a> {
            now: Cell<Instant>,
            slices: Cell<usize>,
            control: &'a super::super::OperationControl<Recovery>,
        }
        impl super::super::ObservationClock for CancellingClock<'_> {
            fn now(&self) -> Instant {
                self.now.get()
            }
            fn sleep(&self, duration: Duration) {
                self.now.set(self.now.get() + duration);
                self.slices.set(self.slices.get() + 1);
                let _ = self.control.claim_signal();
            }
        }
        let control = super::super::OperationControl::new(Recovery {
            session: Some("known-session".into()),
        });
        let clock = CancellingClock {
            now: Cell::new(scherzo_cloud_support::monotonic_now()),
            slices: Cell::new(0),
            control: &control,
        };
        assert!(!wait_for_poll(&control, retry_delay(Some(3600)), &clock));
        assert_eq!(clock.slices.get(), 1);
        assert_eq!(control.recovery().session.as_deref(), Some("known-session"));
    }
}

impl SessionTarget {
    fn show(self, d: Deployment) -> super::CommandResult {
        let result = with_api(&d, &self.options, |api| {
            api.session(&self.organization, &self.session)
        })?;
        match result {
            Ok(session) => session_output(&d, &self.organization, &session, self.options.json)
                .map_err(Into::into),
            Err(failure) => failure_output(
                &d,
                &self.organization,
                Some(&self.session),
                None,
                &failure,
                self.options.authentication.kind(),
                self.options.json,
            )
            .map_err(Into::into),
        }
    }
}
impl Wait {
    fn wait(self, d: Deployment) -> super::CommandResult {
        let org = self.target.organization.to_string();
        let id = self.target.session.clone();
        let json = self.target.options.json;
        let recovery = Recovery {
            session: Some(id.clone()),
        };
        let signal_d = d.clone();
        let signal_org = org.clone();
        let timeout_d = signal_d.clone();
        let timeout_org = signal_org.clone();
        super::execute_mutation_with_signals_and_deferred_timeout(
            "Linear authorization observation",
            recovery,
            Some(self.wait.timeout.unwrap_or(Duration::from_secs(900))),
            move |control, timer| {
                timer.start();
                observe(&d, &org, &id, &self.target.options, control, Duration::ZERO)
            },
            move |signal, snapshot| {
                stopped(&signal_d, &signal_org, &snapshot.recovery, signal, json)
            },
            {
                let d = timeout_d;
                let org = timeout_org;
                move |snapshot| {
                    stopped(&d, &org, &snapshot.recovery, ExitCode::GeneralFailure, json)
                }
            },
        )
    }
}
#[derive(Clone, Copy)]
enum Action {
    Show,
    Disconnect,
    Delete,
}
impl Target {
    fn execute(self, d: Deployment, action: Action) -> super::CommandResult {
        if matches!(action, Action::Show) {
            let result = with_api(&d, &self.options, |api| {
                api.connection(&self.organization, &self.connection)
            })?;
            return match result {
                Ok(connection) => connection_output(
                    &d,
                    &self.organization,
                    &self.connection,
                    Some(&connection),
                    action,
                    self.options.json,
                )
                .map_err(Into::into),
                Err(failure) => failure_output(
                    &d,
                    &self.organization,
                    None,
                    None,
                    &failure,
                    self.options.authentication.kind(),
                    self.options.json,
                )
                .map_err(Into::into),
            };
        }
        let key = scherzo_cloud_support::generate_idempotency_key()
            .context("generate Linear lifecycle request key")?;
        let signal_d = d.clone();
        let signal_org = self.organization.to_string();
        let signal_id = self.connection.clone();
        let signal_key = key.clone();
        let json = self.options.json;
        super::execute_mutation_with_signals(
            "Linear connection lifecycle",
            (),
            move |control| {
                if !control.begin_dispatch() {
                    return Ok(ExitCode::Interrupted);
                }
                let result = with_api(&d, &self.options, |api| {
                    let call = || match action {
                        Action::Disconnect => api
                            .disconnect(&self.organization, &self.connection, &key)
                            .map(Some),
                        Action::Delete => api
                            .delete(&self.organization, &self.connection, &key)
                            .map(|()| None),
                        Action::Show => api
                            .connection(&self.organization, &self.connection)
                            .map(Some),
                    };
                    match call() {
                        Err(LinearFailure::Unreachable { .. }) => call(),
                        result => result,
                    }
                })?;
                finish(control, || match result {
                    Ok(connection) => connection_output(
                        &d,
                        &self.organization,
                        &self.connection,
                        connection.as_ref(),
                        action,
                        json,
                    ),
                    Err(failure) => failure_output(
                        &d,
                        &self.organization,
                        None,
                        Some(&key),
                        &failure,
                        self.options.authentication.kind(),
                        json,
                    ),
                })
            },
            move |signal, snapshot| {
                if !snapshot.dispatched {
                    return Ok(signal);
                }
                lifecycle_stopped(
                    &signal_d,
                    &signal_org,
                    &signal_id,
                    &signal_key,
                    signal,
                    json,
                )
            },
        )
    }
}
impl List {
    fn execute(self, d: Deployment) -> super::CommandResult {
        let result = with_api(&d, &self.options, |api| {
            api.list(
                &self.organization,
                self.page.limit.map(i32::from),
                self.page.cursor.as_deref(),
            )
        })?;
        match result {
            Ok(page) => {
                let write = || -> anyhow::Result<()> {
                    if self.options.json {
                        emit(
                            &serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"organizationRef":self.organization.to_string(),"outcome":"listed","items":page.items.iter().map(connection_view).collect::<Vec<_>>(),"nextCursor":page.next_cursor}),
                        )?;
                    } else {
                        let mut out = io::stdout().lock();
                        for item in &page.items {
                            writeln!(
                                out,
                                "{}  {}  {}",
                                item.id,
                                status(&item.status),
                                item.workspace_name.as_deref().unwrap_or("(no workspace)")
                            )?;
                        }
                        writeln!(out, "deployment: {}", d.fingerprint().api_url())?;
                        if let Some(cursor) = page.next_cursor {
                            writeln!(out, "next cursor: {cursor}")?;
                        }
                    }
                    Ok(())
                };
                write()?;
                Ok(ExitCode::Success)
            }
            Err(failure) => failure_output(
                &d,
                &self.organization,
                None,
                None,
                &failure,
                self.options.authentication.kind(),
                self.options.json,
            )
            .map_err(Into::into),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionView<'a> {
    id: &'a str,
    operation: &'a scherzo_cloud_api::linear_authorization_session::Operation,
    status: &'a LinearSessionStatus,
    connection_id: Option<&'a str>,
    reason_code: Option<&'a scherzo_cloud_api::linear_authorization_session::ReasonCode>,
    authorization_url: Option<&'a str>,
    expires_at: &'a str,
    result_connection: Option<ConnectionView<'a>>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionView<'a> {
    id: &'a str,
    status: &'a scherzo_cloud_api::linear_connection::Status,
    workspace_id: Option<&'a str>,
    workspace_name: Option<&'a str>,
    app_user_id: Option<&'a str>,
    required_scopes: Vec<&'a scherzo_cloud_api::linear_connection::RequiredScopes>,
    granted_scopes: &'a [String],
    lifecycle_generation: i64,
    activation_cutoff: Option<&'a str>,
    last_error: Option<&'a scherzo_cloud_api::linear_connection_error::LinearConnectionError>,
}
fn connection_view(c: &LinearConnection) -> ConnectionView<'_> {
    ConnectionView {
        id: &c.id,
        status: &c.status,
        workspace_id: c.workspace_id.as_deref(),
        workspace_name: c.workspace_name.as_deref(),
        app_user_id: c.app_user_id.as_deref(),
        required_scopes: c.required_scopes.iter().collect(),
        granted_scopes: &c.granted_scopes,
        lifecycle_generation: c.lifecycle_generation,
        activation_cutoff: c.activation_cutoff.as_deref(),
        last_error: c.last_error.as_deref(),
    }
}
fn session_view(s: &LinearSession) -> SessionView<'_> {
    SessionView {
        id: &s.id,
        operation: &s.operation,
        status: &s.status,
        connection_id: s.connection_id.as_deref(),
        reason_code: s.reason_code.as_ref(),
        authorization_url: s.authorization_url.as_deref(),
        expires_at: &s.expires_at,
        result_connection: s.result_connection.as_deref().map(connection_view),
    }
}
fn status(s: &scherzo_cloud_api::linear_connection::Status) -> &'static str {
    use scherzo_cloud_api::linear_connection::Status::*;
    match s {
        Active => "active",
        Disconnected => "disconnected",
        Revoked => "revoked",
        RecoveryRequired => "recovery_required",
        Deleted => "deleted",
    }
}
fn emit(value: &impl Serialize) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    io::stdout().lock().write_all(&bytes)?;
    Ok(())
}
fn session_output(
    d: &Deployment,
    org: &str,
    s: &LinearSession,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let outcome = match s.status {
        LinearSessionStatus::LinearSessionPending => "pending",
        LinearSessionStatus::LinearSessionCompleted => "completed",
        LinearSessionStatus::LinearSessionFailed => "failed",
        LinearSessionStatus::LinearSessionExpired => "expired",
        LinearSessionStatus::LinearSessionRecoveryRequired => "recovery_required",
    };
    if json {
        emit(
            &serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"organizationRef":org,"outcome":outcome,"session":session_view(s)}),
        )?;
    } else {
        let mut out = io::stdout().lock();
        writeln!(
            out,
            "Linear authorization: {}\nsession: {}\norganization: {org}\nexpires: {}",
            outcome.replace('_', " "),
            s.id,
            s.expires_at
        )?;
        if let Some(connection) = &s.result_connection {
            writeln!(
                out,
                "connection: {} ({})",
                connection.id,
                status(&connection.status)
            )?;
        }
        if let Some(reason) = &s.reason_code {
            writeln!(
                out,
                "reason: {}",
                serde_json::to_value(reason)?.as_str().unwrap_or("unknown")
            )?;
        }
        writeln!(out, "deployment: {}", d.fingerprint().api_url())?;
        if let Some(url) = &s.authorization_url {
            writeln!(out, "\nOpen this URL to approve the workspace:\n  {url}")?;
        } else if s.status == LinearSessionStatus::LinearSessionRecoveryRequired {
            writeln!(
                out,
                "\nShow the connection and deliberately reauthorize it if needed."
            )?;
        } else if matches!(
            s.status,
            LinearSessionStatus::LinearSessionFailed | LinearSessionStatus::LinearSessionExpired
        ) {
            writeln!(out, "\nCreate a new authorization session to try again.")?;
        }
    }
    Ok(
        if matches!(
            s.status,
            LinearSessionStatus::LinearSessionFailed
                | LinearSessionStatus::LinearSessionExpired
                | LinearSessionStatus::LinearSessionRecoveryRequired
        ) {
            ExitCode::GeneralFailure
        } else {
            ExitCode::Success
        },
    )
}
fn connection_output(
    d: &Deployment,
    org: &str,
    id: &str,
    connection: Option<&LinearConnection>,
    action: Action,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let outcome = match action {
        Action::Show => "shown",
        Action::Disconnect => "disconnected",
        Action::Delete => "deleted",
    };
    if json {
        emit(
            &serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"organizationRef":org,"outcome":outcome,"connectionId":id,"connection":connection.map(connection_view)}),
        )?;
    } else {
        let mut out = io::stdout().lock();
        writeln!(out, "Linear connection: {outcome}\nconnection: {id}")?;
        if let Some(c) = connection {
            writeln!(
                out,
                "status: {}\nworkspace: {} ({})\nrequired scopes: read\ngranted scopes: {}\ngeneration: {}",
                status(&c.status),
                c.workspace_name.as_deref().unwrap_or("(none)"),
                c.workspace_id.as_deref().unwrap_or("(none)"),
                c.granted_scopes.join(", "),
                c.lifecycle_generation
            )?;
            if let Some(app_user) = &c.app_user_id {
                writeln!(out, "app user: {app_user}")?;
            }
            if let Some(cutoff) = &c.activation_cutoff {
                writeln!(out, "activation cutoff: {cutoff}")?;
            }
            if let Some(error) = &c.last_error {
                writeln!(
                    out,
                    "last error: {} ({})",
                    error.reason_code, error.observed_at
                )?;
            }
        }
        writeln!(out, "deployment: {}", d.fingerprint().api_url())?;
    }
    Ok(ExitCode::Success)
}
fn lifecycle_stopped(
    d: &Deployment,
    org: &str,
    id: &str,
    key: &str,
    signal: ExitCode,
    json: bool,
) -> super::CommandResult {
    let output = || -> anyhow::Result<()> {
        if json {
            emit(
                &serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),
                "organizationRef":org,"connectionId":id,"idempotencyKey":key,"outcome":"commitment_unknown"}),
            )?;
        } else {
            writeln!(
                io::stderr().lock(),
                "error: Linear connection operation interrupted\nconnection: {id}\nrequest key: {key}\n\nShow the connection before repeating this operation."
            )?;
        }
        Ok(())
    };
    output().map_err(super::CommandFailure::from)?;
    Ok(signal)
}

fn status_output(
    d: &Deployment,
    org: &str,
    outcome: &str,
    id: Option<&str>,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        emit(
            &serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"organizationRef":org,"outcome":outcome,"sessionId":id}),
        )
    } else {
        let mut err = io::stderr().lock();
        writeln!(
            err,
            "error: Linear authorization {}",
            outcome.replace('_', " ")
        )?;
        if let Some(id) = id {
            writeln!(err, "session: {id}")?;
        }
        writeln!(
            err,
            "\nIf a session is known, show it before starting another authorization. Otherwise, inspect existing connections with the initiating owner before retrying."
        )?;
        Ok(())
    }
}
fn failure_output(
    d: &Deployment,
    org: &str,
    id: Option<&str>,
    key: Option<&str>,
    failure: &LinearFailure,
    auth: super::PrincipalAuthenticationKind,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, class, remedy) = match failure {
        LinearFailure::Unauthenticated => (
            "unauthenticated",
            OutcomeClass::Unauthenticated,
            auth.rejected_remedy("Sign in again with scherzo-cloud auth login."),
        ),
        LinearFailure::Forbidden => (
            "forbidden",
            OutcomeClass::Forbidden,
            "Ask an active organization owner to inspect this connection or session.",
        ),
        LinearFailure::InvalidInput => (
            "invalid_input",
            OutcomeClass::GeneralFailure,
            "Check the organization and connection identifiers.",
        ),
        LinearFailure::NotFound => (
            "not_found",
            OutcomeClass::GeneralFailure,
            "Check the organization and identifier.",
        ),
        LinearFailure::Conflict => (
            "conflict",
            OutcomeClass::GeneralFailure,
            "Inspect existing connections and sessions before starting another authorization.",
        ),
        LinearFailure::Unreachable { category } => (
            "unreachable",
            super::unreachable_outcome_class(*category),
            "Check access to the deployment; inspect the session before retrying.",
        ),
        LinearFailure::RateLimited { .. } => (
            "rate_limited",
            OutcomeClass::RateLimited,
            "Wait before inspecting this session again.",
        ),
        LinearFailure::InvalidResponse { .. } => (
            "invalid_response",
            OutcomeClass::Protocol,
            "Use a CLI version supported by this deployment.",
        ),
    };
    let uncertain_key = key.filter(|_| {
        matches!(
            failure,
            LinearFailure::Unreachable { .. } | LinearFailure::InvalidResponse { .. }
        )
    });
    if json {
        emit(
            &serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"organizationRef":org,"outcome":outcome,"sessionId":id,"idempotencyKey":uncertain_key.filter(|_| id.is_none()),"category":match failure { LinearFailure::Unreachable { category } => Some(category.as_str()), _ => None }, "retryAfter":match failure { LinearFailure::RateLimited { retry_after } => *retry_after, _ => None }}),
        )?;
    } else {
        let mut err = io::stderr().lock();
        writeln!(
            err,
            "error: Linear connection {}",
            outcome.replace('_', " ")
        )?;
        if let Some(id) = id {
            writeln!(err, "session: {id}")?;
        } else if let Some(key) = uncertain_key {
            writeln!(err, "request key: {key}")?;
        }
        writeln!(err, "\n{remedy}")?;
    }
    Ok(class.exit_code())
}
