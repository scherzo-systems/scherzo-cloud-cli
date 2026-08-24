mod artifacts;
#[cfg(test)]
mod artifacts_tests;
mod current_principal;
mod http_client;
pub(crate) mod http_util;
mod human_principal;
mod organizations;
mod problem;
mod runners;
mod signup;

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use artifacts::{
    ArtifactApi, ArtifactApiError, ArtifactCapabilityMember, ArtifactInventoryPage, ArtifactMember,
    ArtifactSource,
};
#[cfg(test)]
pub(crate) use artifacts::{ArtifactCapabilities, DownloadedMember};
pub(crate) use current_principal::{
    AuthenticatedPrincipal, CurrentPrincipalError, CurrentPrincipalOutcome, UnreachableCategory,
    classify_reqwest_error, get_current_principal,
};
pub(crate) use http_client::{HttpClient, HttpEndpointError, HttpTransportPolicy};
pub(crate) use human_principal::HumanPrincipal;
pub(crate) use organizations::{
    CommonOrganizationFailure, CreateOrganizationOutcome, GetOrganizationOutcome,
    ListOrganizationMembershipsOutcome, MembershipRole, Organization, OrganizationError,
    OrganizationMembershipDirectoryEntry, OrganizationState, PrincipalType,
    UpdateOrganizationOutcome, create_organization, get_organization,
    list_organization_memberships, update_organization,
};
pub(crate) use runners::{
    RunnerActivationIssuance, RunnerActivationState, RunnerApi, RunnerCredentialEffectiveState,
    RunnerCredentialStoredState, RunnerFailure, RunnerPool, RunnerPoolList, RunnerRegistration,
    RunnerRegistrationList, RunnerRegistrationMode,
};
pub(crate) use signup::{SignupError, SignupOutcome, signup_human};

// OpenAPI Generator emits a library-shaped client; keep its public declarations
// intact and contain the binary crate's visibility exception to this generated tree.
#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    clippy::derivable_impls,
    clippy::enum_variant_names,
    clippy::needless_return,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::unimplemented,
    clippy::uninlined_format_args,
    reason = "OpenAPI Generator emits a library-shaped client outside handwritten policy"
)]
mod generated;

#[cfg(test)]
mod tests {
    use super::{generated, test_support::ScriptedHttpServer};

    #[test]
    fn generated_problem_preserves_opaque_actions() {
        let input = serde_json::json!({
            "type": "https://api.scherzo.dev/problems/principal-not-provisioned",
            "title": "Principal not provisioned",
            "status": 403,
            "actions": [{
                "id": "future.action",
                "kind": "future-representation",
                "guide": "https://example.invalid/future-action",
                "additionalField": { "preserved": true }
            }]
        });

        let problem: generated::models::Problem =
            serde_json::from_value(input.clone()).expect("problem should decode");
        let actions = problem.actions.expect("actions should be present");

        assert_eq!(actions, input["actions"].as_array().unwrap().to_owned());
    }

    #[test]
    fn generated_audit_page_decodes_closed_variants() {
        let input = serde_json::json!({
            "items": [
                {
                    "id": "aud_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "occurredAt": "2026-08-04T12:00:00Z",
                    "retention": {
                        "identifier": "identity-tenancy-production-730d-v1",
                        "retainUntil": "2028-08-03T12:00:00Z"
                    },
                    "detailsStatus": "details_available",
                    "actor": {
                        "kind": "principal",
                        "principalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abc"
                    },
                    "action": "organization.created",
                    "subject": {
                        "kind": "organization",
                        "id": "org_01k0z6r1w8f4jy2m7q9v3x5abc"
                    },
                    "changes": [{ "field": "state", "after": "active" }]
                },
                {
                    "id": "aud_01k0z6r1w8f4jy2m7q9v3x5abd",
                    "occurredAt": "2026-08-04T12:01:00Z",
                    "retention": {
                        "identifier": "identity-tenancy-production-730d-v1",
                        "retainUntil": "2028-08-03T12:01:00Z"
                    },
                    "detailsStatus": "details_unavailable"
                }
            ]
        });

        let page: generated::models::OrganizationAuditRecordList =
            serde_json::from_value(input).expect("contracted audit page should decode");
        assert_eq!(page.items.len(), 2);
    }

    #[test]
    fn membership_patch_client_uses_contract_media_type() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let body = r#"{"id":"mem_01k0z6r1w8f4jy2m7q9v3x5abc","organizationId":"org_01k0z6r1w8f4jy2m7q9v3x5abc","principalId":"prn_01k0z6r1w8f4jy2m7q9v3x5abc","principalType":"human","role":"owner","state":"active","createdAt":"2026-07-29T00:00:00Z","updatedAt":"2026-07-29T00:00:00Z"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes();
        let server = ScriptedHttpServer::respond(response);
        let mut configuration = generated::apis::configuration::Configuration::new();
        configuration.base_path = server.api_url.trim_end_matches('/').to_owned();
        configuration.bearer_access_token = Some("fixture-token".to_owned());
        let patch = generated::models::UpdateOrganizationMembershipPatch {
            role: Some(
                generated::models::update_organization_membership_patch::Role::MembershipPatchRoleOwner,
            ),
            state: None,
        };

        let result = generated::apis::organizations_api::update_organization_membership(
            &configuration,
            "acme",
            "mem_01k0z6r1w8f4jy2m7q9v3x5abc",
            "fixture-key",
            patch,
        );

        assert!(result.is_ok());
        let request = server.finish_one();
        assert!(
            request
                .lines()
                .any(|line| line == "content-type: application/merge-patch+json")
        );
    }

    #[test]
    fn generated_current_principal_response_preserves_opaque_actions() {
        let input = serde_json::json!({
            "principal": {
                "id": "prn_fixture",
                "type": "human",
                "state": "active"
            },
            "actions": [
                {
                    "id": "future.action",
                    "kind": "future-representation",
                    "guide": "https://guarded.invalid/future-action",
                    "additionalField": { "preserved": true }
                },
                "unknown-action-shape"
            ]
        });

        let response: generated::models::CurrentPrincipalResponse =
            serde_json::from_value(input.clone()).expect("current principal should decode");
        let actions = response.actions.expect("actions should be present");

        assert_eq!(actions, input["actions"].as_array().unwrap().to_owned());
    }
}
