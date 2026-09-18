use anyhow::anyhow;
use clap::Args;

use crate::api::{create_organization, create_organization_with_delegator};
use crate::exit_code::ExitCode;
use crate::human_auth::deployment::Deployment;

use super::{LeafOptions, output};

pub(super) const ABOUT: &str = "Create a Scherzo Cloud organization";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "Set the organization display name")]
    display_name: String,

    #[arg(long, help = "Request an exact organization slug")]
    slug: Option<String>,

    #[arg(
        long,
        value_name = "PRINCIPAL",
        value_parser = parse_principal_id,
        requires = "service_api_key_file",
        help = "Attribute delegated service creation to an exact human principal"
    )]
    delegator_principal_id: Option<String>,

    // Clap input ownership remains operation-local; shared execution policy lives in LeafOptions.
    // jscpd:ignore-start
    #[command(flatten)]
    options: LeafOptions,
}

impl Command {
    pub(super) fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let Self {
            display_name,
            slug,
            delegator_principal_id,
            options,
        } = self;
        // jscpd:ignore-end
        if options.authentication.service_api_key_file.is_some() && delegator_principal_id.is_none()
        {
            return Err(anyhow!(
                "--delegator-principal-id is required when --service-api-key-file is used for organization creation"
            ));
        }
        options.execute_mutation(
            deployment,
            |client, api_url, access_token, idempotency_key| {
                if let Some(delegator_principal_id) = delegator_principal_id.as_deref() {
                    create_organization_with_delegator(
                        client,
                        api_url,
                        access_token,
                        idempotency_key,
                        &display_name,
                        slug.as_deref(),
                        Some(delegator_principal_id),
                    )
                } else {
                    create_organization(
                        client,
                        api_url,
                        access_token,
                        idempotency_key,
                        &display_name,
                        slug.as_deref(),
                    )
                }
            },
            output::write_create,
        )
    }
}

fn parse_principal_id(value: &str) -> Result<String, String> {
    if crate::public_id::valid_typed_id(value, "prn_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact human principal ID".to_owned())
    }
}
