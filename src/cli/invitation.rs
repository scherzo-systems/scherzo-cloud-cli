mod output;

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{ArgGroup, Args, Subcommand, builder::NonEmptyStringValueParser};
use zeroize::Zeroizing;

use crate::api::{
    AcceptInvitationOutcome, HttpClient, InvitationTarget, InvitationTerminationOutcome,
    IssueInvitationOutcome, ListInvitationInboxOutcome, ListOrganizationInvitationsOutcome,
    OrganizationError, PreviewInvitationOutcome, accept_invitation, decline_invitation,
    issue_invitation, list_invitation_inbox, list_organization_invitations, preview_invitation,
    revoke_invitation,
};
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;
use crate::human_auth::token::SecretToken;

use super::{OrganizationRef, PaginationArgs};

pub(super) const ABOUT: &str = "Manage Scherzo Cloud invitations";
const NAME: &str = "invitation";
const ERROR_CONTEXT: &str = "configure Scherzo Cloud invitation access";
const MAX_CAPABILITY_BYTES: u64 = 128;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<InvitationCommand>,
}

#[derive(Debug, Subcommand)]
enum InvitationCommand {
    #[command(about = "List your invitation inbox")]
    List(InboxCommand),
    #[command(about = "Preview an invitation")]
    Preview(AccessCommand),
    #[command(about = "Accept an invitation")]
    Accept(AccessCommand),
    #[command(about = "Decline an invitation")]
    Decline(DeclineCommand),
}

#[derive(Debug, Args)]
pub(super) struct OrganizationCommand {
    #[command(subcommand)]
    command: Option<OrganizationInvitationCommand>,
}

#[derive(Debug, Subcommand)]
enum OrganizationInvitationCommand {
    #[command(about = "Issue an organization invitation")]
    Issue(IssueCommand),
    #[command(
        about = "List organization invitation history",
        after_help = "Pagination:\n  This command returns one page. Pass --cursor <CURSOR> to continue."
    )]
    List(OrganizationListCommand),
    #[command(about = "Revoke an organization invitation")]
    Revoke(RevokeCommand),
}

// Invitation leaves keep operation-local option wording and error context rather than exposing
// organization-profile terminology through a shared options abstraction.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct InvitationOptions {
    #[arg(long, help = "Print the invitation result as JSON")]
    json: bool,

    #[command(flatten)]
    http: super::HttpOptions,
}

impl InvitationOptions {
    fn execute<O>(
        self,
        deployment: &Deployment,
        operation: impl FnMut(&HttpClient, &str, &str) -> Result<O, OrganizationError>,
        write: impl FnOnce(&str, &O, bool) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: super::HumanCredentialOutcome<Error = OrganizationError>,
    {
        let outcome = super::execute_with_human_credential(
            deployment,
            self.http.transport_policy(),
            "prepare invitation networking",
            "contact invitation API at",
            operation,
        )?;
        write(deployment.fingerprint().api_url(), &outcome, self.json)
            .context("write invitation result")
    }

    fn execute_mutation<O>(
        self,
        deployment: &Deployment,
        mut operation: impl FnMut(&HttpClient, &str, &str, &str) -> Result<O, OrganizationError>,
        write: impl FnOnce(&str, &O, bool) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: super::HumanCredentialOutcome<Error = OrganizationError>,
    {
        let idempotency_key = crate::idempotency::generate_idempotency_key()
            .context("generate invitation mutation request identity")?;
        self.execute(
            deployment,
            |client, api_url, access_token| {
                operation(client, api_url, access_token, &idempotency_key)
            },
            write,
        )
    }
}
// jscpd:ignore-end

#[derive(Debug, Args)]
struct InboxCommand {
    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    options: InvitationOptions,
}

#[derive(Debug, Args)]
struct InvitationAccess {
    #[arg(
        value_name = "INVITATION",
        value_parser = parse_invitation_id,
        help = "Exact invitation ID"
    )]
    invitation_id: String,

    #[arg(
        long,
        value_name = "PATH|-",
        help = "Read an email invitation capability from a protected file or explicit stdin"
    )]
    capability_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct AccessCommand {
    #[command(flatten)]
    access: InvitationAccess,

    #[command(flatten)]
    options: InvitationOptions,
}

#[derive(Debug, Args)]
struct DeclineCommand {
    #[command(flatten)]
    access: InvitationAccess,

    #[arg(
        long,
        required = true,
        action = clap::ArgAction::SetTrue,
        help = "Confirm invitation decline"
    )]
    yes: bool,

    #[command(flatten)]
    options: InvitationOptions,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("target")
        .required(true)
        .multiple(false)
        .args(["principal_id", "email"])
))]
struct IssueCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: OrganizationRef,

    #[arg(
        long = "principal",
        value_name = "PRINCIPAL",
        value_parser = parse_principal_id,
        help = "Invite an exact active principal ID"
    )]
    principal_id: Option<String>,

    #[arg(
        long,
        value_name = "EMAIL",
        value_parser = NonEmptyStringValueParser::new(),
        help = "Invite one email address"
    )]
    email: Option<String>,

    #[command(flatten)]
    options: InvitationOptions,
}

// This page belongs to owner-visible invitation history; keeping its arguments local prevents
// runner-list semantics from becoming a shared abstraction merely because both are paginated.
// jscpd:ignore-start
#[derive(Debug, Args)]
struct OrganizationListCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: OrganizationRef,

    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    options: InvitationOptions,
}
// jscpd:ignore-end

#[derive(Debug, Args)]
struct RevokeCommand {
    #[arg(value_name = "ORGANIZATION", help = "Organization ID or exact slug")]
    organization: OrganizationRef,

    #[arg(
        value_name = "INVITATION",
        value_parser = parse_invitation_id,
        help = "Exact invitation ID"
    )]
    invitation_id: String,

    #[arg(
        long,
        required = true,
        action = clap::ArgAction::SetTrue,
        help = "Confirm invitation revocation"
    )]
    yes: bool,

    #[command(flatten)]
    options: InvitationOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        super::execute_deployment_command(
            self.command,
            &[NAME],
            ERROR_CONTEXT,
            |command, deployment| match command {
                InvitationCommand::List(command) => command.execute(deployment).map_err(Into::into),
                InvitationCommand::Preview(command) => {
                    command.preview(deployment).map_err(Into::into)
                }
                InvitationCommand::Accept(command) => {
                    command.accept(deployment).map_err(Into::into)
                }
                InvitationCommand::Decline(command) => {
                    command.execute(deployment).map_err(Into::into)
                }
            },
        )
    }
}

impl OrganizationCommand {
    pub(super) fn execute(self) -> super::CommandResult {
        super::execute_deployment_command(
            self.command,
            &["organization", "invitations"],
            ERROR_CONTEXT,
            |command, deployment| match command {
                OrganizationInvitationCommand::Issue(command) => {
                    command.execute(deployment).map_err(Into::into)
                }
                OrganizationInvitationCommand::List(command) => {
                    command.execute(deployment).map_err(Into::into)
                }
                OrganizationInvitationCommand::Revoke(command) => {
                    command.execute(deployment).map_err(Into::into)
                }
            },
        )
    }
}

impl InboxCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        self.options.execute(
            deployment,
            |client, api_url, access_token| {
                list_invitation_inbox(
                    client,
                    api_url,
                    access_token,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
            output::write_inbox,
        )
    }
}

impl AccessCommand {
    fn preview(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        self.execute(
            deployment,
            preview_invitation,
            |deployment, _, outcome, json| output::write_preview(deployment, outcome, json),
        )
    }

    fn accept(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        self.execute_mutation(
            deployment,
            accept_invitation,
            |deployment, _, outcome, json| output::write_accept(deployment, outcome, json),
        )
    }

    fn execute<O>(
        self,
        deployment: &Deployment,
        mut operation: impl FnMut(
            &HttpClient,
            &str,
            &str,
            &str,
            Option<&str>,
        ) -> Result<O, OrganizationError>,
        write: impl FnOnce(&str, &str, &O, bool) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: super::HumanCredentialOutcome<Error = OrganizationError>,
    {
        self.with_access(deployment, |options, invitation_id, capability| {
            options.execute(
                deployment,
                |client, api_url, access_token| {
                    operation(
                        client,
                        api_url,
                        access_token,
                        &invitation_id,
                        capability.as_ref().map(SecretToken::expose),
                    )
                },
                |deployment, outcome, json| write(deployment, &invitation_id, outcome, json),
            )
        })
    }

    // The mutation variant stays separate because its operation receives the generated
    // idempotency key; merging it with reads would make that security boundary optional.
    // jscpd:ignore-start
    fn execute_mutation<O>(
        self,
        deployment: &Deployment,
        mut operation: impl FnMut(
            &HttpClient,
            &str,
            &str,
            &str,
            Option<&str>,
            &str,
        ) -> Result<O, OrganizationError>,
        write: impl FnOnce(&str, &str, &O, bool) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode>
    where
        O: super::HumanCredentialOutcome<Error = OrganizationError>,
    {
        // jscpd:ignore-end
        self.with_access(deployment, |options, invitation_id, capability| {
            options.execute_mutation(
                deployment,
                |client, api_url, access_token, idempotency_key| {
                    operation(
                        client,
                        api_url,
                        access_token,
                        &invitation_id,
                        capability.as_ref().map(SecretToken::expose),
                        idempotency_key,
                    )
                },
                |deployment, outcome, json| write(deployment, &invitation_id, outcome, json),
            )
        })
    }

    fn with_access(
        self,
        deployment: &Deployment,
        execute: impl FnOnce(InvitationOptions, String, Option<SecretToken>) -> anyhow::Result<ExitCode>,
    ) -> anyhow::Result<ExitCode> {
        match self.access.read_capability() {
            Ok(capability) => execute(self.options, self.access.invitation_id, capability),
            Err(error) => output::write_capability_error(
                deployment.fingerprint().api_url(),
                &error,
                self.options.json,
            ),
        }
    }
}

impl DeclineCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        AccessCommand {
            access: self.access,
            options: self.options,
        }
        .execute_mutation(
            deployment,
            decline_invitation,
            |deployment, invitation_id, outcome, json| {
                output::write_termination(
                    deployment,
                    invitation_id,
                    outcome,
                    output::TerminationAction::Decline,
                    json,
                )
            },
        )
    }
}

impl IssueCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let target = match (self.principal_id.as_deref(), self.email.as_deref()) {
            (Some(principal_id), None) => InvitationTarget::Principal(principal_id),
            (None, Some(email)) => InvitationTarget::Email(email),
            (None, None) | (Some(_), Some(_)) => {
                return Err(anyhow::anyhow!(
                    "invitation target is unavailable after command parsing"
                ));
            }
        };
        self.options.execute_mutation(
            deployment,
            |client, api_url, access_token, idempotency_key| {
                issue_invitation(
                    client,
                    api_url,
                    access_token,
                    &self.organization,
                    idempotency_key,
                    target,
                )
            },
            output::write_issue,
        )
    }
}

impl OrganizationListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        self.options.execute(
            deployment,
            |client, api_url, access_token| {
                list_organization_invitations(
                    client,
                    api_url,
                    access_token,
                    &self.organization,
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
            output::write_organization_list,
        )
    }
}

impl RevokeCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let organization = self.organization;
        let invitation_id = self.invitation_id;
        self.options.execute_mutation(
            deployment,
            |client, api_url, access_token, idempotency_key| {
                revoke_invitation(
                    client,
                    api_url,
                    access_token,
                    &organization,
                    &invitation_id,
                    idempotency_key,
                )
            },
            |deployment, outcome, json| {
                output::write_termination(
                    deployment,
                    &invitation_id,
                    outcome,
                    output::TerminationAction::Revoke {
                        organization: &organization,
                    },
                    json,
                )
            },
        )
    }
}

impl InvitationAccess {
    fn read_capability(&self) -> Result<Option<SecretToken>, CapabilityError> {
        self.capability_file
            .as_deref()
            .map(|path| read_capability(path, &self.invitation_id))
            .transpose()
    }
}

fn parse_invitation_id(value: &str) -> Result<String, String> {
    parse_typed_id(value, "inv_", "invitation")
}

fn parse_principal_id(value: &str) -> Result<String, String> {
    parse_typed_id(value, "prn_", "principal")
}

fn parse_typed_id(value: &str, prefix: &str, name: &str) -> Result<String, String> {
    if crate::public_id::valid_typed_id(value, prefix) {
        Ok(value.to_owned())
    } else {
        Err(format!("{name} must be an exact {prefix} identifier"))
    }
}

fn read_capability(path: &Path, invitation_id: &str) -> Result<SecretToken, CapabilityError> {
    let source = if path == Path::new("-") {
        CapabilitySource::Stdin
    } else {
        CapabilitySource::File(path.to_owned())
    };
    let mut bytes = Zeroizing::new(Vec::new());
    if path == Path::new("-") {
        io::stdin()
            .lock()
            .take(MAX_CAPABILITY_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| CapabilityError::io(source.clone(), "read", error))?;
    } else {
        read_private_capability_file(path, &source, &mut bytes)?;
    }
    if bytes.len() as u64 > MAX_CAPABILITY_BYTES {
        return Err(CapabilityError::invalid(
            source,
            "capability input exceeds 128 bytes",
        ));
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    let capability = std::str::from_utf8(&bytes)
        .map_err(|_| CapabilityError::invalid(source.clone(), "capability input is not UTF-8"))?;
    if !valid_capability(capability, invitation_id) {
        return Err(CapabilityError::invalid(
            source,
            "capability does not match the invitation ID or required syntax",
        ));
    }
    Ok(SecretToken::new(capability.to_owned()))
}

fn read_private_capability_file(
    path: &Path,
    source: &CapabilitySource,
    bytes: &mut Vec<u8>,
) -> Result<(), CapabilityError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| CapabilityError::io(source.clone(), "inspect", error))?;
    validate_private_capability_file(source, &metadata)?;
    if metadata.len() > MAX_CAPABILITY_BYTES {
        return Err(CapabilityError::invalid(
            source.clone(),
            "capability file exceeds 128 bytes",
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| CapabilityError::io(source.clone(), "open", error))?;
    let opened = file
        .metadata()
        .map_err(|error| CapabilityError::io(source.clone(), "inspect opened", error))?;
    validate_private_capability_file(source, &opened)?;
    file.take(MAX_CAPABILITY_BYTES + 1)
        .read_to_end(bytes)
        .map_err(|error| CapabilityError::io(source.clone(), "read", error))?;
    Ok(())
}

fn validate_private_capability_file(
    source: &CapabilitySource,
    metadata: &fs::Metadata,
) -> Result<(), CapabilityError> {
    if !metadata.file_type().is_file() {
        return Err(CapabilityError::unsafe_source(
            source.clone(),
            "expected a regular non-symbolic-link file",
        ));
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(CapabilityError::unsafe_source(
            source.clone(),
            "file must be owned by the current user",
        ));
    }
    if metadata.mode() & 0o7777 != 0o600 {
        return Err(CapabilityError::unsafe_source(
            source.clone(),
            "file mode must be 0600",
        ));
    }
    Ok(())
}

fn valid_capability(value: &str, invitation_id: &str) -> bool {
    let Some((embedded_id, secret)) = value.split_once('.') else {
        return false;
    };
    embedded_id == invitation_id
        && crate::public_id::valid_typed_id(embedded_id, "inv_")
        && secret.len() == 43
        && secret
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[derive(Clone, Debug)]
enum CapabilitySource {
    Stdin,
    File(PathBuf),
}

impl fmt::Display for CapabilitySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdin => formatter.write_str("stdin"),
            Self::File(path) => write!(formatter, "{}", path.display()),
        }
    }
}

#[derive(Debug)]
pub(super) struct CapabilityError {
    source: CapabilitySource,
    kind: CapabilityErrorKind,
}

impl CapabilityError {
    fn io(source: CapabilitySource, operation: &'static str, error: io::Error) -> Self {
        Self {
            source,
            kind: CapabilityErrorKind::Io { operation, error },
        }
    }

    fn unsafe_source(source: CapabilitySource, requirement: &'static str) -> Self {
        Self {
            source,
            kind: CapabilityErrorKind::Unsafe { requirement },
        }
    }

    fn invalid(source: CapabilitySource, reason: &'static str) -> Self {
        Self {
            source,
            kind: CapabilityErrorKind::Invalid { reason },
        }
    }

    pub(super) const fn outcome(&self) -> &'static str {
        match self.kind {
            CapabilityErrorKind::Invalid { .. } => "invalid_capability",
            CapabilityErrorKind::Io { .. } | CapabilityErrorKind::Unsafe { .. } => {
                "capability_source_unavailable"
            }
        }
    }
}

#[derive(Debug)]
enum CapabilityErrorKind {
    Io {
        operation: &'static str,
        error: io::Error,
    },
    Unsafe {
        requirement: &'static str,
    },
    Invalid {
        reason: &'static str,
    },
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            CapabilityErrorKind::Io { operation, error } => write!(
                formatter,
                "{operation} invitation capability source {}: {error}",
                self.source
            ),
            CapabilityErrorKind::Unsafe { requirement } => write!(
                formatter,
                "invitation capability source {} is unsafe: {requirement}",
                self.source
            ),
            CapabilityErrorKind::Invalid { reason } => write!(
                formatter,
                "invalid invitation capability from {}: {reason}",
                self.source
            ),
        }
    }
}

impl std::error::Error for CapabilityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            CapabilityErrorKind::Io { error, .. } => Some(error),
            CapabilityErrorKind::Unsafe { .. } | CapabilityErrorKind::Invalid { .. } => None,
        }
    }
}

super::impl_organization_human_credential_outcome!(
    IssueInvitationOutcome,
    ListOrganizationInvitationsOutcome,
    ListInvitationInboxOutcome,
    PreviewInvitationOutcome,
    AcceptInvitationOutcome,
    InvitationTerminationOutcome,
);
