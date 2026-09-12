mod artifacts;
#[cfg(test)]
mod artifacts_tests;
mod current_principal;
mod github;
mod http_client;
pub(crate) mod http_util;
mod human_principal;
mod identities;
mod lifecycle;
mod organizations;
mod problem;
mod profile;
mod projects;
mod runners;
mod runs;
mod signup;

use reqwest::header::{HeaderValue, InvalidHeaderValue};
use zeroize::{Zeroize as _, Zeroizing};

fn clear_generated_access_token(configuration: &mut generated::apis::configuration::Configuration) {
    if let Some(access_token) = &mut configuration.bearer_access_token {
        access_token.zeroize();
    }
}

fn bearer_authorization(access_token: &str) -> Result<HeaderValue, InvalidHeaderValue> {
    let mut value = Zeroizing::new(String::with_capacity("Bearer ".len() + access_token.len()));
    value.push_str("Bearer ");
    value.push_str(access_token);
    HeaderValue::from_str(&value)
}

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
pub(crate) use github::{
    GitHubAccountType, GitHubApi, GitHubFailure, GitHubInstallation, GitHubInstallationState,
    GitHubRepository, GitHubRepositoryList, GitHubSetupSession,
};
pub(crate) use http_client::{HttpClient, HttpEndpointError, HttpTransportPolicy};
pub(crate) use human_principal::HumanPrincipal;
pub(crate) use identities::{
    CommonIdentityFailure, IdentityApiError, LinkIdentityOutcome, ListIdentitiesOutcome,
    OidcIdentity, RemoveIdentityOutcome, link_identity, list_identities, remove_identity,
};
pub(crate) use lifecycle::{
    CancelDeletionOutcome, CommonLifecycleFailure, DeletionSchedule, LifecycleApiError,
    LifecycleResourceKind, LifecycleState, LifecycleTransition, RequestDeletionOutcome,
    cancel_current_principal_deletion, cancel_organization_deletion,
    request_current_principal_deletion, request_organization_deletion,
};
pub(crate) use organizations::{
    AcceptInvitationOutcome, AcceptedInvitationMembership, AuditActor, AuditProjectionWarning,
    AuditProjectionWarningReason, CommonOrganizationFailure, CreateOrganizationOutcome,
    CurrentPrincipalMembership, GetOrganizationOutcome, Invitation, InvitationDeliveryState,
    InvitationInboxEntry, InvitationPreview, InvitationState, InvitationTarget,
    InvitationTargetKind, InvitationTerminationOutcome, IssueInvitationOutcome,
    ListCurrentPrincipalMembershipsOutcome, ListInvitationInboxOutcome,
    ListOrganizationAuditRecordsOutcome, ListOrganizationInvitationsOutcome,
    ListOrganizationMembershipHistoryOutcome, ListOrganizationMembershipsOutcome, MembershipRole,
    MembershipState, MembershipTerminationOutcome, Organization, OrganizationAuditRecord,
    OrganizationAuditSubjectKind, OrganizationError, OrganizationMembershipDirectoryEntry,
    OrganizationMembershipHistoryEntry, OrganizationState, PreviewInvitationOutcome, PrincipalType,
    UpdateOrganizationMembershipOutcome, UpdateOrganizationOutcome, accept_invitation,
    create_organization, decline_invitation, end_organization_membership, get_organization,
    issue_invitation, leave_organization, list_current_principal_memberships,
    list_invitation_inbox, list_organization_audit_records, list_organization_invitations,
    list_organization_membership_history, list_organization_memberships, preview_invitation,
    revoke_invitation, update_organization, update_organization_membership_role,
};
pub(crate) use profile::{UpdateProfileError, UpdateProfileOutcome, update_current_principal};
pub(crate) use projects::{
    CreateProjectInput, GitHubInstallation as ProjectGitHubInstallation,
    GitHubInstallationList as ProjectGitHubInstallationList,
    GitHubRepository as ProjectGitHubRepository,
    GitHubRepositoryList as ProjectGitHubRepositoryList, Project, ProjectApi, ProjectFailure,
    ProjectList, ProjectReadinessBlocker, ProjectRepository,
};
pub(crate) use runners::{
    RunnerActivationIssuance, RunnerActivationState, RunnerApi, RunnerCredentialEffectiveState,
    RunnerCredentialStoredState, RunnerDeletionBlocker, RunnerFailure, RunnerPool, RunnerPoolList,
    RunnerRegistration, RunnerRegistrationList, RunnerRegistrationMode,
};
pub(crate) use runs::{
    CreateRunInput, NamedTextInputMetadata, Run, RunApi, RunCreationAcceptance, RunFailure,
    RunState, TextInputSet,
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
    reason = "api::signup_human and runner enrollment call the generated client while OpenAPI Generator retains the full contract surface"
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
    fn generated_create_run_accepts_nullable_input_set_identity() {
        let omitted: generated::models::CreateRunRequest = serde_json::from_value(
            serde_json::json!({"projectId":"prj_fixture","workflowPath":"workflow.yaml"}),
        )
        .expect("omitted input set should decode");
        let null: generated::models::CreateRunRequest = serde_json::from_value(
            serde_json::json!({"projectId":"prj_fixture","workflowPath":"workflow.yaml","inputSetId":null}),
        )
        .expect("null input set should decode");
        let present: generated::models::CreateRunRequest = serde_json::from_value(
            serde_json::json!({"projectId":"prj_fixture","workflowPath":"workflow.yaml","inputSetId":"ris_fixture"}),
        )
        .expect("present input set should decode");

        assert_eq!(omitted.input_set_id, None);
        assert_eq!(null.input_set_id, None);
        assert_eq!(present.input_set_id, Some(Some("ris_fixture".to_owned())));

        let mut explicit_null = generated::models::CreateRunRequest::new(
            "prj_fixture".to_owned(),
            "workflow.yaml".to_owned(),
        );
        explicit_null.input_set_id = Some(None);
        let encoded = serde_json::to_value(explicit_null).expect("null input set should encode");
        assert_eq!(encoded["inputSetId"], serde_json::Value::Null);
    }

    #[test]
    fn generated_run_input_union_consumes_its_discriminator_once() {
        let entry = generated::models::RunInputManifestEntry::Text(Box::new(
            generated::models::RunInputTextEntry::new(
                generated::models::run_input_text_entry::Kind::Text,
                0,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
            ),
        ));

        let encoded = serde_json::to_string(&entry).expect("Run Input entry should encode");
        assert_eq!(encoded.matches("\"kind\"").count(), 1);
        assert!(matches!(
            serde_json::from_str::<generated::models::RunInputManifestEntry>(&encoded)
                .expect("Run Input entry should decode"),
            generated::models::RunInputManifestEntry::Text(_)
        ));
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
