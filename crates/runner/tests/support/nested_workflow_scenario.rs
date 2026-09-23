use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, anyhow, ensure};
use ring::digest::{SHA256, digest};
use scherzo_cloud_execution::{CaptureCancellation, resolve};
use scherzo_cloud_runner_protocol::{
    ArtifactRegistrationOutcome, ArtifactRegistrationResponse, ArtifactResultRegistrationOutcome,
    ArtifactResultRegistrationResponse, ExecutionCapacityV1RunnerProjection, ExecutionLeaseGrant,
    ExecutionLeasePolicy, ExecutionLimitsV1RunnerProjection,
    PrimaryWorkspaceSourceV1RunnerProjection, WorkflowDefinitionSourceV1RunnerProjection,
    WorkflowSourceClosureDigestV1RunnerProjection,
};
use time::OffsetDateTime;
use url::Url;

use super::*;
use crate::credential::Credential;
use crate::service::config::{AssignmentConfig, RepositoryUrlPolicy};
use crate::service::source::{
    CommitAvailability, CredentialBrokerFailure, ProviderCredential, ProviderSecret,
    SourceCredentialBroker, WorkflowGitRevocation,
};
use crate::service::workspace::{WorkRootLease, WorkspaceFilesystem};

const BOOT_ID: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
const EXPECTED_RESULT: &[u8] = b"nested portable result";

pub(crate) async fn run_nested_workflow_delivery_failure_scenario(
    fixture_arguments: &[String],
) -> anyhow::Result<()> {
    ensure!(
        !fixture_arguments.is_empty(),
        "nested workflow fixture command is empty"
    );
    let nested_arguments = serde_json::to_string(fixture_arguments)
        .context("encode nested workflow fixture command")?;
    let workflow = format!(
        "schemaVersion: 1\nsteps:\n  nested:\n    kind: cmd\n    command:\n      argv: {nested_arguments}\n    outputs:\n      result:\n        kind: file\n        from: path\n        path: delivery-rounds/0001/nested-result.txt\n        mediaType: text/plain\nexports:\n  nestedResult:\n    ref: outputs.nested.result\n"
    );
    let nested_workflow = "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"/bin/sh\", \"-c\", \"printf 'nested portable result' > nested.txt\"]\n    outputs:\n      result:\n        kind: file\n        from: path\n        path: nested.txt\n        mediaType: text/plain\n  consume:\n    kind: cmd\n    inputs:\n      payload:\n        ref: outputs.produce.result\n    command:\n      argv: [\"/bin/sh\", \"-c\", \"mkdir -p ../run/.private/workflow-retained/.inputs-retained; cp -a \\\"$SCHERZO_STEP_INPUTS\\\" ../run/.private/workflow-retained/.inputs-retained/view-retained\"]\nexports:\n  portable:\n    ref: outputs.produce.result\n";

    let temporary = tempfile::tempdir().context("create nested workflow scenario root")?;
    let source = temporary.path().join("source");
    let work = temporary.path().join("work");
    fs::create_dir(&source).context("create nested workflow source")?;
    fs::create_dir(&work).context("create nested workflow runner work root")?;
    fs::set_permissions(&work, fs::Permissions::from_mode(0o700))
        .context("protect nested workflow runner work root")?;
    fs::write(source.join("workflow.yaml"), workflow)
        .context("write assignment workflow fixture")?;
    fs::write(source.join("nested.yaml"), nested_workflow)
        .context("write nested workflow fixture")?;
    initialize_source_repository(&source)?;

    let assignment = AssignmentConfig::new(&work)?;
    let credential = Credential::from_enrolled_state(
        "rnr_01k0z6r1w8f4jy2m7q9v3x5abd",
        "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    )
    .map_err(|error| anyhow!(error))?;
    let config = Config::new(
        "wss://gateway.example.test/v1/runner/connect",
        credential,
        false,
        assignment,
        RepositoryUrlPolicy::with_file_repositories(true),
    )?;
    let work_root = WorkRootLease::acquire_with(
        config.assignment().work_root(),
        BOOT_ID,
        WorkspaceFilesystem::testing(),
    )?;
    let source_broker: Arc<dyn SourceCredentialBroker> =
        Arc::new(FixtureSourceBroker::new(&source)?);
    let dependencies = AssignmentDependencies::new(
        Arc::clone(&work_root),
        Arc::new(crate::service::TokioSleeper),
        Some(source_broker),
        None,
        Arc::from("nested-workflow-test"),
        None,
        false,
    );
    let mut manager = AssignmentManager::new(&config, LeaseClock::system()?, dependencies);
    manager
        .retain_lease_policy(&lease_policy())
        .map_err(|error| anyhow!("retain nested workflow lease policy: {error:?}"))?;

    let mut offered = offer();
    align_offer_with_source(&source, &mut offered)?;
    let assignment_path = config
        .assignment()
        .work_root()
        .join(BOOT_ID)
        .join(&offered.assignment_id);
    manager
        .handle_offer(offered.clone())
        .map_err(|error| anyhow!("offer nested workflow assignment: {error:?}"))?;
    prepare_current(&mut manager, &offered).await?;
    spawn_execution(&mut manager, &offered)?;

    let pending = wait_for_carrier_registration(&mut manager).await;
    let expected_sha256 = digest(&SHA256, EXPECTED_RESULT)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    ensure!(
        pending.iter().any(|entry| matches!(
            &entry.observation,
            AssignmentObservation::Artifact {
                request: ArtifactRequest::RegisterCarrier {
                    portable_owner_path,
                    media_type,
                    size_bytes,
                    sha256,
                    ..
                },
                ..
            } if portable_owner_path == "exports/0001"
                && media_type == "text/plain"
                && *size_bytes == u64::try_from(EXPECTED_RESULT.len()).unwrap_or(u64::MAX)
                && sha256 == &expected_sha256
        )),
        "nested workflow export did not reach carrier registration"
    );
    ensure!(
        assignment_path.join("workspace").exists(),
        "assignment workspace disappeared before delivery failed"
    );
    ensure!(
        fail_pending_artifact_registrations(&mut manager, &pending)?,
        "nested workflow carrier registration was not failed"
    );

    let reports = wait_for_terminal(&mut manager).await?;
    ensure!(
        matches!(
            reports.last(),
            Some(ExecutionReport::Finished { outcome, .. })
                if outcome == &serde_json::json!({
                    "outcome": "succeeded",
                    "forceAbort": null,
                })
        ),
        "nested workflow assignment did not finish successfully: {reports:#?}"
    );
    acknowledge_terminal_and_settle(&mut manager).await?;
    ensure!(
        !manager.cleanup_failed,
        "nested workflow assignment cleanup failed"
    );
    ensure!(
        manager.slot.is_none(),
        "nested workflow slot remained occupied"
    );
    ensure!(
        assignment_path.exists(),
        "delivery failure did not retain the assignment root"
    );
    ensure!(
        assignment_path
            .join("private")
            .read_dir()
            .context("read retained assignment private directory")?
            .next()
            .is_some(),
        "delivery failure did not retain private assignment staging"
    );
    Ok(())
}

struct FixtureSourceBroker {
    repository_url: Arc<str>,
}

impl FixtureSourceBroker {
    fn new(repository: &Path) -> anyhow::Result<Self> {
        let repository_url = Url::from_file_path(repository)
            .map_err(|()| anyhow!("nested workflow source path cannot be represented as a URL"))?;
        Ok(Self {
            repository_url: Arc::from(repository_url.as_str()),
        })
    }

    fn credential(&self) -> ProviderCredential {
        ProviderCredential {
            repository_url: Arc::clone(&self.repository_url),
            token: ProviderSecret(b"fixture-provider-token".to_vec()),
            expires_at: OffsetDateTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800),
        }
    }
}

impl SourceCredentialBroker for FixtureSourceBroker {
    fn issue(
        &self,
        _assignment_id: &str,
        cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        if cancellation.is_cancelled() {
            Err(CredentialBrokerFailure::Fenced)
        } else {
            Ok(self.credential())
        }
    }

    fn commit_availability(
        &self,
        _assignment_id: &str,
        cancellation: &CaptureCancellation,
    ) -> Result<CommitAvailability, CredentialBrokerFailure> {
        if cancellation.is_cancelled() {
            Err(CredentialBrokerFailure::Fenced)
        } else {
            Ok(CommitAvailability::CommitAvailable)
        }
    }

    fn issue_workflow_git(
        &self,
        _assignment_id: &str,
        _cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        Err(CredentialBrokerFailure::Unavailable)
    }

    fn revoke_workflow_git(
        &self,
        _assignment_id: &str,
        _token: &[u8],
    ) -> Result<WorkflowGitRevocation, CredentialBrokerFailure> {
        Err(CredentialBrokerFailure::Unavailable)
    }
}

fn initialize_source_repository(source: &Path) -> anyhow::Result<()> {
    run_git(source, &["init", "--quiet", "--object-format=sha1"])?;
    run_git(source, &["config", "user.name", "Scherzo Fixture"])?;
    run_git(source, &["config", "user.email", "fixture@scherzo.invalid"])?;
    run_git(source, &["add", "."])?;
    run_git(source, &["commit", "--quiet", "-m", "nested fixture"])?;
    Ok(())
}

fn run_git(repository: &Path, arguments: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("git")
        .current_dir(repository)
        .args(arguments)
        .output()
        .with_context(|| format!("run Git fixture command {arguments:?}"))?;
    ensure!(
        output.status.success(),
        "Git fixture command {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .context("Git fixture command emitted non-UTF-8 standard output")
        .map(|output| output.trim().to_owned())
}

fn lease_policy() -> ExecutionLeasePolicy {
    ExecutionLeasePolicy {
        schema_version: 2,
        force_stop_and_reap_budget_milliseconds: 5000,
        terminal_report_delivery_budget_milliseconds: 5000,
        renewal_delivery_budget_milliseconds: 5000,
        lease_duration_milliseconds: 371_000,
        fencing_margin_milliseconds: 11_000,
    }
}

fn offer() -> AssignmentOffer {
    AssignmentOffer {
        effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
        assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
        run_id: "run_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
        project_id: "prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
        attempt_number: 1,
        execution_spec: scherzo_cloud_runner_protocol::ExecutionSpecV1RunnerProjection {
            execution_spec_id: "xsp_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
            schema_version: 1,
            execution_limits: ExecutionLimitsV1RunnerProjection {
                maximum_parallel_steps: 1,
                cancellation_grace_seconds: 1,
            },
            source_branch: "main".to_owned(),
            source_display_snapshot: None,
            workflow_definition_source: WorkflowDefinitionSourceV1RunnerProjection {
                repository_connection_id: "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                object_format: "sha1".to_owned(),
                commit_oid: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                workflow_path: "workflow.yaml".to_owned(),
                workflow_source_closure_digest: source_digest_fixture(),
            },
            primary_workspace_source: PrimaryWorkspaceSourceV1RunnerProjection {
                kind: "connected_repository".to_owned(),
                provider_kind: "github".to_owned(),
                repository_connection_id: "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                object_format: "sha1".to_owned(),
                commit_oid: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                materialization_contract: "git_full_clone_v1".to_owned(),
            },
            capacity: ExecutionCapacityV1RunnerProjection {
                execution_contract: "workflow_v1_cloud_inputs_artifacts@1".to_owned(),
                source_closure_digest: source_digest_fixture(),
                general_maximum_transitions: 8,
                selected_maximum_transitions: 7,
                maximum_invocations: 1,
                maximum_retained_bytes_per_invocation: 4_194_304,
                diagnostic_retention_bytes: 8_388_608,
                native_session_retention_bytes: 4_194_304,
                aggregate_retention_bytes: 12_582_912,
                condition_transition_count: 0,
                aggregate_condition_transition_bytes: 0,
                terminal_result_structure_bytes: 67_108_864,
                portable_result_bytes: 202_027_692,
                encoded_outbox_bytes: 85_458_944,
            },
            run_inputs: None,
        },
    }
}

fn source_digest_fixture() -> WorkflowSourceClosureDigestV1RunnerProjection {
    WorkflowSourceClosureDigestV1RunnerProjection {
        algorithm: "sha256".to_owned(),
        value: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
    }
}

fn align_offer_with_source(source: &Path, offered: &mut AssignmentOffer) -> anyhow::Result<()> {
    let commit_oid = run_git(source, &["rev-parse", "HEAD"])?;
    offered.execution_spec.workflow_definition_source.commit_oid = commit_oid.clone();
    offered.execution_spec.primary_workspace_source.commit_oid = commit_oid;
    let workflow = resolve(source, Path::new("workflow.yaml"))
        .map_err(|error| anyhow!("resolve assignment workflow fixture: {error:?}"))?;
    let digest = &workflow.capacity.source_closure_digest;
    let requirements = workflow.capacity.requirements;
    let projected_digest = WorkflowSourceClosureDigestV1RunnerProjection {
        algorithm: digest.algorithm.as_str().to_owned(),
        value: digest.value.clone(),
    };
    offered
        .execution_spec
        .workflow_definition_source
        .workflow_source_closure_digest = projected_digest.clone();
    offered.execution_spec.capacity = ExecutionCapacityV1RunnerProjection {
        execution_contract: "workflow_v1_cloud_inputs_artifacts@1".to_owned(),
        source_closure_digest: projected_digest,
        general_maximum_transitions: requirements.general_maximum_transitions,
        selected_maximum_transitions: requirements.cloud_maximum_transitions,
        maximum_invocations: requirements.maximum_invocations,
        maximum_retained_bytes_per_invocation: requirements.maximum_retained_bytes_per_invocation,
        diagnostic_retention_bytes: requirements.diagnostic_retention_bytes,
        native_session_retention_bytes: requirements.native_session_retention_bytes,
        aggregate_retention_bytes: requirements.aggregate_retention_bytes,
        condition_transition_count: requirements.condition_transition_count,
        aggregate_condition_transition_bytes: requirements.aggregate_condition_transition_bytes,
        terminal_result_structure_bytes: requirements.terminal_result_structure_bytes,
        portable_result_bytes: requirements.portable_result_bytes,
        encoded_outbox_bytes: requirements.encoded_outbox_bytes,
    };
    Ok(())
}

async fn prepare_current(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
) -> anyhow::Result<()> {
    let preparation_id = wait_for_observation(manager, |observation| {
        matches!(observation, AssignmentObservation::Preparing { .. })
    })
    .await
    .id;
    manager.acknowledge_observation(preparation_id);
    let preparation_expires_at = (manager.sleeper.utc_now() + time::Duration::minutes(15))
        .format(&time::format_description::well_known::Rfc3339)
        .context("format assignment preparation deadline")?;
    manager
        .handle_prepare(AssignmentPrepare {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5acz".to_owned(),
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            execution_spec_id: offered.execution_spec.execution_spec_id.clone(),
            preparation_expires_at,
        })
        .map_err(|error| anyhow!("prepare nested workflow assignment: {error:?}"))?;
    wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Preparing(_)))
    })
    .await;
    let pending = manager.pending_observations(&BTreeSet::new(), 10);
    ensure!(
        matches!(manager.slot, Some(LocalSlot::Accepted(_))),
        "nested workflow assignment preparation was not accepted: {pending:#?}"
    );
    for id in pending.into_iter().filter_map(|pending| {
        matches!(
            pending.observation,
            AssignmentObservation::PreparationProgress { .. }
        )
        .then_some(pending.id)
    }) {
        manager.acknowledge_observation(id);
    }
    Ok(())
}

fn spawn_execution(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
) -> anyhow::Result<()> {
    let job = manager
        .handle_start(AssignmentStart {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned(),
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            execution_spec_id: offered.execution_spec.execution_spec_id.clone(),
            lease: ExecutionLeaseGrant { sequence: 1 },
        })
        .map_err(|error| anyhow!("start nested workflow assignment: {error:?}"))?
        .ok_or_else(|| anyhow!("nested workflow assignment start did not dispatch execution"))?;
    manager
        .handle_start_authorized(AssignmentStartAuthorization {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned(),
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
        })
        .map_err(|error| anyhow!("authorize nested workflow assignment: {error:?}"))?;
    job.spawn();
    Ok(())
}

async fn wait_for_observation(
    manager: &mut AssignmentManager,
    matches: impl Fn(&AssignmentObservation) -> bool,
) -> PendingAssignmentObservation {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        if let Some(observation) = manager
            .pending_observations(&BTreeSet::new(), 100)
            .into_iter()
            .find(|pending| matches(&pending.observation))
        {
            return observation;
        }
        notified.await;
    }
}

async fn wait_for_manager_state(
    manager: &mut AssignmentManager,
    mut reached: impl FnMut(&mut AssignmentManager) -> bool,
) {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        if reached(manager) {
            return;
        }
        notified.await;
    }
}

async fn wait_for_carrier_registration(
    manager: &mut AssignmentManager,
) -> Vec<PendingAssignmentObservation> {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        if pending.iter().any(|entry| {
            matches!(
                entry.observation,
                AssignmentObservation::Artifact {
                    request: ArtifactRequest::RegisterCarrier { .. },
                    ..
                }
            )
        }) {
            return pending;
        }
        notified.await;
    }
}

fn fail_pending_artifact_registrations(
    manager: &mut AssignmentManager,
    pending: &[PendingAssignmentObservation],
) -> anyhow::Result<bool> {
    let registrations = pending
        .iter()
        .filter_map(|entry| match &entry.observation {
            AssignmentObservation::Artifact {
                delivery_id,
                request: ArtifactRequest::RegisterCarrier { .. },
            } => Some((entry.id, *delivery_id, false)),
            AssignmentObservation::Artifact {
                delivery_id,
                request: ArtifactRequest::RegisterResult { .. },
            } => Some((entry.id, *delivery_id, true)),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (observation_id, delivery_id, is_result) in &registrations {
        let response = if *is_result {
            ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                outcome: ArtifactResultRegistrationOutcome::Failed {
                    code: "storage_quota_exceeded".to_owned(),
                },
            })
        } else {
            ArtifactCloudResponse::CarrierRegistration(ArtifactRegistrationResponse {
                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                outcome: ArtifactRegistrationOutcome::Failed {
                    code: "storage_quota_exceeded".to_owned(),
                },
            })
        };
        manager
            .handle_artifact_response(*observation_id, *delivery_id, response)
            .map_err(|error| anyhow!("fail nested workflow artifact registration: {error:?}"))?;
    }
    Ok(!registrations.is_empty())
}

async fn wait_for_terminal(
    manager: &mut AssignmentManager,
) -> anyhow::Result<Vec<ExecutionReport>> {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        if fail_pending_artifact_registrations(manager, &pending)? {
            continue;
        }
        if pending
            .iter()
            .any(|pending| pending.observation.is_terminal())
        {
            return Ok(pending
                .into_iter()
                .filter_map(|pending| match pending.observation {
                    AssignmentObservation::Execution { report, .. } => Some(report),
                    AssignmentObservation::Preparing { .. }
                    | AssignmentObservation::PreparationProgress { .. }
                    | AssignmentObservation::Decision(_)
                    | AssignmentObservation::CancellationApplied(_)
                    | AssignmentObservation::LeaseRenewalRequested { .. }
                    | AssignmentObservation::Artifact { .. } => None,
                })
                .collect());
        }
        notified.await;
    }
}

async fn acknowledge_terminal_and_settle(manager: &mut AssignmentManager) -> anyhow::Result<()> {
    wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Running(_)))
    })
    .await;
    let terminal_id = manager
        .pending_observations(&BTreeSet::new(), 100)
        .into_iter()
        .find(|entry| entry.observation.is_terminal())
        .ok_or_else(|| anyhow!("nested workflow terminal observation is missing"))?
        .id;
    manager.acknowledge_observation(terminal_id);
    wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Releasing(_)))
    })
    .await;
    Ok(())
}
