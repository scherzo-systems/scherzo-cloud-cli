use std::io::{self, Write};

use anyhow::Context;
use clap::Args;
use serde::Serialize;

use crate::api::{
    ArtifactApi, ArtifactApiError, ArtifactInventoryPage, ArtifactMember, ArtifactSource,
    HttpEndpointError,
};
use crate::exit_code::{ExitCode, OutcomeClass};

pub(super) const ABOUT: &str = "List a run's remote Artifact Set members";
const DEFAULT_PAGE_LIMIT: u16 = 50;

pub(super) type Command = super::RemoteArtifactCommand<Operation>;

#[derive(Debug, Args)]
pub(super) struct Operation {
    #[command(flatten)]
    pagination: super::super::PaginationArgs,

    #[arg(long, help = "Print the Artifact Set result as JSON")]
    json: bool,
}

impl super::RemoteArtifactOperation for Operation {
    type Output = ArtifactInventoryPage;
    type Error = ArtifactApiError;

    const SESSION_CONTEXT: &'static str = "acquire human session for Artifact Set listing";

    fn request(
        &self,
        api: &mut ArtifactApi,
        run: &super::RunArtifactReference,
    ) -> Result<Self::Output, Self::Error> {
        api.inventory_page(
            &run.organization,
            &run.run_id,
            self.pagination.limit.unwrap_or(DEFAULT_PAGE_LIMIT),
            self.pagination.cursor.as_deref(),
        )
    }

    fn write_result(
        &self,
        output: super::RemoteArtifactResult<'_, Self::Output, Self::Error>,
    ) -> anyhow::Result<ExitCode> {
        write_list_result(
            output.deployment,
            &output.run.organization,
            &output.run.run_id,
            output.result,
            self.json,
        )
    }
}

fn write_list_result(
    deployment: &str,
    organization: &str,
    run_id: &str,
    result: Result<ArtifactInventoryPage, ArtifactApiError>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    match result {
        Ok(page) => {
            if json {
                write_json(deployment, organization, run_id, &page)?;
            } else {
                write_human(deployment, organization, run_id, &page)?;
            }
            Ok(ExitCode::Success)
        }
        Err(error) => write_failure(deployment, organization, run_id, &error, json),
    }
}

fn write_human(
    deployment: &str,
    organization: &str,
    run_id: &str,
    page: &ArtifactInventoryPage,
) -> anyhow::Result<()> {
    let mut output = io::stdout().lock();
    writeln!(output, "✓ Artifact Set members listed.\n")?;
    writeln!(output, "artifact set: {}", page.artifact_set_id)?;
    writeln!(output, "state: sealed")?;
    writeln!(output, "sealed: {}", page.sealed_at)?;
    writeln!(output, "expires: {}", page.expires_at)?;
    writeln!(output, "members: {}", page.member_count)?;
    writeln!(output, "bytes: {}", page.total_size_bytes)?;
    writeln!(output, "members this page: {}", page.members.len())?;
    for member in &page.members {
        writeln!(output, "\nmember: {}", member.path)?;
        writeln!(output, "media type: {}", member.media_type.escape_default())?;
        writeln!(output, "bytes: {}", member.size_bytes)?;
        writeln!(output, "sha256: {}", hex_sha256(&member.sha256))?;
    }
    if let Some(cursor) = &page.next_cursor {
        writeln!(output, "\nnext cursor: {}", cursor.escape_default())?;
    }
    writeln!(output, "organization: {organization}")?;
    writeln!(output, "run: {run_id}")?;
    writeln!(output, "deployment: {deployment}")?;
    Ok(())
}

fn write_json(
    deployment: &str,
    organization: &str,
    run_id: &str,
    page: &ArtifactInventoryPage,
) -> anyhow::Result<()> {
    let result = ListResult {
        schema_version: 1,
        deployment,
        outcome: "listed",
        organization_ref: organization,
        run_id,
        artifact_set: ArtifactSetResult {
            artifact_set_id: &page.artifact_set_id,
            state: "sealed",
            sealed_at: &page.sealed_at,
            expires_at: &page.expires_at,
            member_count: page.member_count,
            total_size_bytes: page.total_size_bytes,
            members: page
                .members
                .iter()
                .map(ArtifactMemberResult::from)
                .collect(),
            next_cursor: page.next_cursor.as_deref(),
        },
    };
    let mut output = io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, &result)
        .context("serialize JSON Artifact Set list result")?;
    writeln!(output).context("write JSON Artifact Set list result")
}

fn write_failure(
    deployment: &str,
    organization: &str,
    run_id: &str,
    error: &ArtifactApiError,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (outcome, category, human, class) = match error {
        ArtifactApiError::Endpoint(HttpEndpointError::Invalid) => (
            "invalid_configuration",
            None,
            format!(
                "error: deployment API URL cannot form an Artifact Set endpoint: {deployment}\n\nCheck the deployment configuration, then try again."
            ),
            OutcomeClass::GeneralFailure,
        ),
        ArtifactApiError::Endpoint(HttpEndpointError::InsecureHttp) => (
            "invalid_configuration",
            None,
            "error: deployment API URL uses insecure HTTP\n\nRerun with --allow-insecure-http to permit it."
                .to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ArtifactApiError::InvalidAuthorizationHeader => (
            "invalid_configuration",
            None,
            "error: stored access token cannot be used for Artifact Set access\n\nSign in again:\n  scherzo-cloud auth login"
                .to_owned(),
            OutcomeClass::GeneralFailure,
        ),
        ArtifactApiError::InvalidInput => (
            "invalid_input",
            None,
            format!(
                "error: Artifact Set request rejected by {deployment}\n\nCheck the organization, run identifier, and --cursor value, then try again."
            ),
            OutcomeClass::GeneralFailure,
        ),
        ArtifactApiError::Unauthenticated => (
            "unauthenticated",
            None,
            "error: Artifact Set access requires sign-in\n\nSign in first:\n  scherzo-cloud auth login"
                .to_owned(),
            OutcomeClass::Unauthenticated,
        ),
        ArtifactApiError::Forbidden => (
            "forbidden",
            None,
            "error: Artifact Set access is not permitted for this account\n\nAsk an organization owner for access."
                .to_owned(),
            OutcomeClass::Forbidden,
        ),
        ArtifactApiError::NotFound => (
            "unavailable",
            None,
            format!(
                "error: Artifact Set unavailable for run {run_id}\n\nThe run may not exist, or its Artifact Set may not be sealed. Check the organization and run identifier, or wait for the run to finish."
            ),
            OutcomeClass::GeneralFailure,
        ),
        ArtifactApiError::Gone => (
            "expired",
            None,
            format!(
                "error: Artifact Set expired for run {run_id}\n\nThe retention window has ended; no Artifact Set content is available."
            ),
            OutcomeClass::GeneralFailure,
        ),
        ArtifactApiError::Unreachable(unreachable) => (
            "unreachable",
            Some(unreachable.as_str()),
            format!(
                "error: contact Artifact Set API at {deployment}: {}\n\nCheck network access to the deployment and try again.",
                unreachable.as_str()
            ),
            super::super::unreachable_outcome_class(*unreachable),
        ),
        ArtifactApiError::CapabilityUnavailable
        | ArtifactApiError::LocalOutput
        | ArtifactApiError::Protocol { .. } => (
            "invalid_response",
            None,
            "error: Artifact Set API response does not match the metadata listing contract\n\nTry again later."
                .to_owned(),
            OutcomeClass::Protocol,
        ),
    };
    if json {
        let mut output = io::stdout().lock();
        serde_json::to_writer_pretty(
            &mut output,
            &FailureResult {
                schema_version: 1,
                deployment,
                outcome,
                organization_ref: organization,
                run_id,
                category,
            },
        )
        .context("serialize JSON Artifact Set failure result")?;
        writeln!(output).context("write JSON Artifact Set failure result")?;
    } else {
        writeln!(io::stderr().lock(), "{human}")?;
    }
    Ok(class.exit_code())
}

fn hex_sha256(digest: &[u8; 32]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
    artifact_set: ArtifactSetResult<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactSetResult<'a> {
    artifact_set_id: &'a str,
    state: &'static str,
    sealed_at: &'a str,
    expires_at: &'a str,
    member_count: usize,
    total_size_bytes: u64,
    members: Vec<ArtifactMemberResult<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactMemberResult<'a> {
    path: &'a str,
    media_type: &'a str,
    size_bytes: u64,
    digest: DigestResult,
}

impl<'a> From<&'a ArtifactMember> for ArtifactMemberResult<'a> {
    fn from(member: &'a ArtifactMember) -> Self {
        Self {
            path: &member.path,
            media_type: &member.media_type,
            size_bytes: member.size_bytes,
            digest: DigestResult {
                algorithm: "sha256",
                value: hex_sha256(&member.sha256),
            },
        }
    }
}

#[derive(Serialize)]
struct DigestResult {
    algorithm: &'static str,
    value: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    organization_ref: &'a str,
    run_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'a str>,
}
