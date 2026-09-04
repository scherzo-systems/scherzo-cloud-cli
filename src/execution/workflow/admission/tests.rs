use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use super::*;
use crate::execution::claude_code::{
    ClaudeCodeCompatibilityProfile, ValidatedClaudeCodeInstallation,
};
use crate::execution::codex::{
    CODEX_APP_SERVER_V1_QUALIFICATION_VERSION, CodexCompatibilityProfile,
    ValidatedCodexInstallation,
};
use crate::execution::pi::{
    PI_JSON_V1_QUALIFICATION_VERSION, PiCapability, PiCompatibilityProfile, ValidatedPiInstallation,
};
use crate::execution::workflow::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use crate::execution::workflow::codex::CodexConfig;
use crate::execution::workflow::pi::Thinking;
use crate::execution::workflow::resolution::{self, ResolvedWorkflow};

const COMMAND_WORKFLOW: &str = r#"schemaVersion: 1
steps:
  check:
    kind: cmd
    inputs:
      prompt:
        ref: imports.prompt
      attachments:
        ref: imports.attachments
    command:
      argv: ["./must-not-run"]
"#;

const COMMAND_WORKFLOW_WITHOUT_IMPORTS: &str = r#"schemaVersion: 1
steps:
  check:
    kind: cmd
    command:
      argv: ["true"]
"#;

const AGENT_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text:
          - file: message.md
          - ref: imports.prompt
"#;

const RECOVERY_AGENT_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  repair:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  check:
    kind: cmd
    recovery:
      retries: 2
      handler:
        kind: agent
        profile: repair
        prompt: recovery.md
        cwd: repairs
    command:
      argv: ["true"]
"#;

const MIXED_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  prepare:
    kind: cmd
    command:
      argv: ["true"]
  agent:
    kind: agent
    dependsOn: [prepare]
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text:
          - file: message.md
          - ref: imports.prompt
"#;

struct WorkflowFixture {
    _temporary: TempDir,
    source_root: PathBuf,
    execution_root: PathBuf,
}

impl WorkflowFixture {
    fn new(source: &str) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&source_root).unwrap();
        fs::create_dir(&execution_root).unwrap();
        fs::write(source_root.join("workflow.yaml"), source).unwrap();
        if source.contains("kind: agent") {
            fs::write(source_root.join("system.md"), "System.\n").unwrap();
            fs::write(source_root.join("message.md"), "Message.\n").unwrap();
        }
        Self {
            _temporary: temporary,
            source_root,
            execution_root,
        }
    }

    fn resolve(&self) -> ResolvedWorkflow {
        resolution::resolve(&self.source_root, Path::new("workflow.yaml")).unwrap()
    }

    fn context(&self, maximum_parallel_steps: usize, grace: Duration) -> ExecutionContext {
        execution_context(self.execution_root.clone(), maximum_parallel_steps, grace)
    }
}

struct AdmissionOperationRecorder {
    directory: PathBuf,
    operation_log: PathBuf,
}

impl AdmissionOperationRecorder {
    fn new(root: &Path) -> Self {
        let directory = root.join("admission-operation-recorder");
        fs::create_dir(&directory).unwrap();
        let operation_log = directory.join("operations.log");
        let script = "#!/bin/sh\noperation_log=${0%/*}/operations.log\nprintf 'native operation: %s\\n' \"$*\" >> \"$operation_log\"\nexit 97\n";
        for executable in [directory.join("validated-pi"), directory.join("pi")] {
            fs::write(&executable, script).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        Self {
            directory,
            operation_log,
        }
    }

    fn installation(&self) -> ValidatedPiInstallation {
        ValidatedPiInstallation::fixture(self.directory.join("validated-pi"))
    }

    fn path(&self) -> &Path {
        &self.directory
    }

    fn assert_empty(&self) {
        assert!(
            !self.operation_log.exists(),
            "admission performed a native operation: {}",
            fs::read_to_string(&self.operation_log).unwrap_or_default()
        );
    }
}

const THINKING_LEVELS: [(&str, Thinking); 7] = [
    ("off", Thinking::Off),
    ("minimal", Thinking::Minimal),
    ("low", Thinking::Low),
    ("medium", Thinking::Medium),
    ("high", Thinking::High),
    ("xhigh", Thinking::XHigh),
    ("max", Thinking::Max),
];

fn all_thinking_levels_workflow() -> String {
    let mut source = String::from("schemaVersion: 1\nagentProfiles:\n");
    for (index, (thinking, _)) in THINKING_LEVELS.iter().enumerate() {
        source.push_str(&format!(
            "  profile{index}:\n    harness:\n      kind: pi\n      config:\n        model: future-provider/unknown-model-{index}\n        thinking: {thinking}\n"
        ));
    }
    source.push_str("steps:\n");
    for index in 0..THINKING_LEVELS.len() {
        source.push_str(&format!(
            "  agent{index}:\n    kind: agent\n    agent:\n      profile: profile{index}\n      systemPrompt: system.md\n      message:\n        text:\n          - file: message.md\n"
        ));
    }
    source
}

#[test]
fn cancellation_source_rearms_graceful_cancellation_and_authorizes_one_force_abort() {
    let source = CancellationSource::new();
    assert!(source.request_cancellation(CancellationReason::UserRequest));
    assert!(!source.request_cancellation(CancellationReason::RunnerShutdown));
    let mut operations = source.subscribe_operations();
    assert_eq!(
        operations.next_operation(),
        Some(CancellationOperation::Graceful {
            id: CancellationOperationId::fixture(1),
            reason: CancellationReason::UserRequest,
        })
    );

    assert!(source.begin_finalization_arm());
    assert!(!source.begin_finalization_arm());
    assert!(source.request_cancellation(CancellationReason::RunnerShutdown));
    assert_eq!(
        source.cancellation_reason(),
        Some(CancellationReason::UserRequest)
    );
    assert_eq!(operations.next_operation(), None);
    assert!(source.request_force_abort());
    assert!(!source.request_force_abort());
    assert_eq!(operations.next_operation(), None);
    assert!(source.complete_finalization_arm());
    assert!(!source.complete_finalization_arm());
    assert_eq!(
        source.cancellation_reason(),
        Some(CancellationReason::RunnerShutdown)
    );
    assert_eq!(
        operations.next_operation(),
        Some(CancellationOperation::Graceful {
            id: CancellationOperationId::fixture(2),
            reason: CancellationReason::RunnerShutdown,
        })
    );
    assert_eq!(
        source.cancellation_reason(),
        Some(CancellationReason::RunnerShutdown)
    );
    assert_eq!(
        operations.next_operation(),
        Some(CancellationOperation::ForceAbort {
            id: CancellationOperationId::fixture(3),
        })
    );
    assert_eq!(operations.next_operation(), None);

    let direct_force = CancellationSource::new();
    assert!(direct_force.begin_finalization_arm());
    assert!(direct_force.complete_finalization_arm());
    assert!(direct_force.request_force_abort());
    assert_eq!(
        direct_force.cancellation_reason(),
        Some(CancellationReason::FinalizationForceAbort)
    );

    let aborted = CancellationSource::new();
    assert!(aborted.request_cancellation(CancellationReason::UserRequest));
    let mut aborted_operations = aborted.subscribe_operations();
    assert!(aborted_operations.next_operation().is_some());
    assert!(aborted.begin_finalization_arm());
    assert!(aborted.request_cancellation(CancellationReason::RunnerShutdown));
    assert!(aborted.abort_finalization_arm());
    assert!(!aborted.complete_finalization_arm());
    assert_eq!(aborted_operations.next_operation(), None);
    assert_eq!(
        aborted.cancellation_reason(),
        Some(CancellationReason::UserRequest)
    );
}

#[test]
fn admission_partitions_durable_stream_bytes_before_capture() {
    let mut source = String::from("schemaVersion: 1\nsteps:\n");
    for index in 0..256 {
        source.push_str(&format!(
            "  step{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
        ));
    }
    let fixture = WorkflowFixture::new(&source);
    let resolved = fixture.resolve();
    let carried_limit = resolved
        .capacity
        .requirements
        .maximum_retained_bytes_per_invocation;
    let admitted = admit_workflow(
        resolved,
        ResolvedImports::default(),
        fixture.context(1, Duration::from_secs(1)),
    )
    .unwrap();

    assert_eq!(
        admitted.execution().limits().maximum_step_log_bytes().get(),
        carried_limit
    );
}

#[test]
fn admission_requires_pi_only_for_graphs_containing_agent_steps() {
    for (source, agent_step_count) in [
        (AGENT_WORKFLOW, 1),
        (MIXED_WORKFLOW, 1),
        (COMMAND_WORKFLOW_WITHOUT_IMPORTS, 0),
    ] {
        for supply_installation in [false, true] {
            let fixture = WorkflowFixture::new(source);
            let root_before = root_snapshot(&fixture.execution_root);
            let imports = if agent_step_count == 0 {
                ResolvedImports::default()
            } else {
                ResolvedImports::new(Some(Arc::from("Caller prompt.")), Arc::from([]))
            };
            let mut context = fixture.context(1, Duration::from_secs(1));
            if supply_installation {
                // A validated value is sufficient even when its pinned path no longer exists.
                context = context.with_pi_installation(ValidatedPiInstallation::fixture(
                    fixture.execution_root.join("removed-after-validation"),
                ));
            }

            let result = admit_workflow(fixture.resolve(), imports, context);
            if agent_step_count > 0 && !supply_installation {
                assert_failure(
                    result,
                    AdmissionFailureKind::AgentStepRuntimeUnsupported,
                    AdmissionLocation::Step {
                        step: "agent".to_owned(),
                    },
                );
            } else {
                let admitted = result.unwrap();
                assert_eq!(admitted.agent_steps().len(), agent_step_count);
            }
            assert_eq!(root_snapshot(&fixture.execution_root), root_before);
        }
    }
}

#[test]
fn local_and_runner_admission_preserve_source_bound_capacity_and_guards() {
    let fixture = WorkflowFixture::new(RECOVERY_AGENT_WORKFLOW);
    fs::write(
        fixture.source_root.join("recovery.md"),
        b"Repair the failed target.\n",
    )
    .unwrap();
    fs::create_dir(fixture.execution_root.join("repairs")).unwrap();
    let resolved = fixture.resolve();
    let exact = WorkflowCapacityBudget::exact(&resolved.capacity);
    let installation = ValidatedPiInstallation::fixture(fixture.execution_root.join("pi"));

    let local = admit_local_workflow(
        resolved.clone(),
        ResolvedImports::default(),
        fixture
            .context(1, Duration::from_secs(1))
            .with_capacity_budget(exact)
            .with_pi_installation(installation.clone()),
    )
    .unwrap();
    assert_eq!(
        local.capacity().maximum_transitions,
        resolved.capacity.requirements.general_maximum_transitions
    );
    assert_eq!(local.capacity().resolved, resolved.capacity);
    assert_eq!(local.recovery_handlers().len(), 1);

    let runner = admit_runner_workflow(
        resolved.clone(),
        ResolvedImports::default(),
        fixture
            .context(1, Duration::from_secs(1))
            .with_capacity_budget(exact)
            .with_pi_installation(installation),
    )
    .unwrap();
    assert_eq!(
        runner.capacity().maximum_transitions,
        resolved.capacity.requirements.cloud_maximum_transitions
    );
    assert_eq!(runner.capacity().resolved, resolved.capacity);
    assert_eq!(
        runner.capacity().execution_contract.as_str(),
        "workflow_v1_cloud_inputs_artifacts@1"
    );
}

#[test]
fn maximal_handler_workflow_admits_exact_cloud_capacity() {
    let mut source = String::from(
        "schemaVersion: 1\nagentProfiles:\n  repair:\n    harness:\n      kind: pi\n      config: {model: openai/gpt-5, thinking: high}\nsteps:\n",
    );
    for index in 0..20 {
        let recovery = match index {
            0..=17 => {
                "    recovery:\n      retries: 10\n      handler:\n        kind: agent\n        profile: repair\n        prompt: recovery.md\n"
            }
            18 => {
                "    recovery:\n      retries: 8\n      handler:\n        kind: agent\n        profile: repair\n        prompt: recovery.md\n"
            }
            19 => "    recovery:\n      retries: 1\n",
            _ => "",
        };
        source.push_str(&format!(
            "  step{index}:\n    kind: cmd\n{recovery}    command: {{argv: [\"true\"]}}\n"
        ));
    }
    let fixture = WorkflowFixture::new(&source);
    fs::write(
        fixture.source_root.join("recovery.md"),
        b"Repair the failed target.\n",
    )
    .unwrap();
    let resolved = fixture.resolve();
    let requirements = resolved.capacity.requirements;
    assert_eq!(requirements.general_maximum_transitions, 1_048);
    assert_eq!(requirements.cloud_maximum_transitions, 1_027);
    assert_eq!(requirements.maximum_invocations, 397);
    assert_eq!(requirements.maximum_retained_bytes_per_invocation, 169_039);
    let exact = WorkflowCapacityBudget::exact(&resolved.capacity);

    let admitted = admit_runner_workflow(
        resolved,
        ResolvedImports::default(),
        fixture
            .context(1, Duration::from_secs(1))
            .with_capacity_budget(exact)
            .with_pi_installation(ValidatedPiInstallation::fixture(
                fixture.execution_root.join("pi"),
            )),
    )
    .unwrap();
    assert_eq!(admitted.recovery_handlers().len(), 19);
    assert_eq!(admitted.capacity().resolved.requirements, requirements);
    assert_eq!(
        admitted.execution().limits().maximum_step_log_bytes().get(),
        requirements.maximum_retained_bytes_per_invocation
    );
}

#[test]
fn admission_rejects_capacity_reused_after_source_closure_changes() {
    let fixture = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    let mut resolved = fixture.resolve();
    resolved.source_closure.insert(
        "injected-after-resolution.txt".to_owned(),
        Arc::from(b"changed closure bytes".as_slice()),
    );
    let context = || fixture.context(1, Duration::from_secs(1));

    let local = admit_local_workflow(resolved.clone(), ResolvedImports::default(), context());
    let runner = admit_runner_workflow(resolved, ResolvedImports::default(), context());
    assert_eq!(
        (
            local.err().map(|failure| failure.kind()),
            runner.err().map(|failure| failure.kind()),
        ),
        (
            Some(AdmissionFailureKind::CapacitySourceBindingMismatch),
            Some(AdmissionFailureKind::CapacitySourceBindingMismatch),
        )
    );
}

#[test]
fn admission_rejects_recovery_placement_capacity_and_binding_before_execution() {
    let fixture = WorkflowFixture::new(RECOVERY_AGENT_WORKFLOW);
    fs::write(
        fixture.source_root.join("recovery.md"),
        b"Repair the failed target.\n",
    )
    .unwrap();
    let resolved = fixture.resolve();
    let exact = WorkflowCapacityBudget::exact(&resolved.capacity);
    let context = || fixture.context(1, Duration::from_secs(1));

    assert_failure(
        admit_local_workflow(
            resolved.clone(),
            ResolvedImports::default(),
            context().with_capacity_budget(exact),
        ),
        AdmissionFailureKind::AgentStepRuntimeUnsupported,
        AdmissionLocation::RecoveryHandler {
            step: "check".to_owned(),
        },
    );

    let installation = ValidatedPiInstallation::fixture(fixture.execution_root.join("pi"));
    let requirements = resolved.capacity.requirements;
    for (budget, kind, location) in [
        (
            WorkflowCapacityBudget {
                maximum_invocations: requirements.maximum_invocations - 1,
                ..exact
            },
            AdmissionFailureKind::InvocationCapacityUnavailable,
            AdmissionLocation::MaximumInvocations,
        ),
        (
            WorkflowCapacityBudget {
                diagnostic_retention_bytes: requirements.diagnostic_retention_bytes - 1,
                ..exact
            },
            AdmissionFailureKind::DiagnosticRetentionCapacityUnavailable,
            AdmissionLocation::DiagnosticRetention,
        ),
        (
            WorkflowCapacityBudget {
                native_session_retention_bytes: requirements.native_session_retention_bytes - 1,
                ..exact
            },
            AdmissionFailureKind::NativeSessionRetentionCapacityUnavailable,
            AdmissionLocation::NativeSessionRetention,
        ),
        (
            WorkflowCapacityBudget {
                aggregate_retention_bytes: requirements.aggregate_retention_bytes - 1,
                ..exact
            },
            AdmissionFailureKind::AggregateRetentionCapacityUnavailable,
            AdmissionLocation::AggregateRetention,
        ),
        (
            WorkflowCapacityBudget {
                encoded_outbox_bytes: requirements.encoded_outbox_bytes - 1,
                ..exact
            },
            AdmissionFailureKind::EncodedOutboxCapacityUnavailable,
            AdmissionLocation::EncodedOutbox,
        ),
    ] {
        assert_failure(
            admit_runner_workflow(
                resolved.clone(),
                ResolvedImports::default(),
                context()
                    .with_capacity_budget(budget)
                    .with_pi_installation(installation.clone()),
            ),
            kind,
            location,
        );
    }

    let mut mismatched = resolved;
    mismatched.content_digest.value = "0".repeat(64);
    assert_failure(
        admit_local_workflow(
            mismatched,
            ResolvedImports::default(),
            context()
                .with_capacity_budget(exact)
                .with_pi_installation(installation),
        ),
        AdmissionFailureKind::CapacitySourceBindingMismatch,
        AdmissionLocation::CapacitySourceBinding,
    );
}

#[test]
fn admission_pins_every_pi_configuration_and_bound_without_native_or_mutable_lookups() {
    let source = all_thinking_levels_workflow();
    let fixture = WorkflowFixture::new(&source);
    let resolved = fixture.resolve();
    let recorder = AdmissionOperationRecorder::new(fixture._temporary.path());
    let installation = recorder.installation();
    let root_before = root_snapshot(&fixture.execution_root);

    // Admission must consume only the resolved profile snapshots, not mutable source or registries.
    fs::remove_dir_all(&fixture.source_root).unwrap();
    let admitted = admit_workflow(
        resolved,
        ResolvedImports::default(),
        ExecutionContext::new(
            fixture.execution_root.clone(),
            ExecutionPolicyLimits::new(
                2,
                CaptureLimits::new(11, 3 * 1024 * 1024, 9 * 1024 * 1024).with_git_carrier_limits(
                    19,
                    4 * 1024 * 1024,
                    10 * 1024 * 1024,
                ),
                InputLimits::new(13, 2 * 1024 * 1024, 7 * 1024 * 1024, 5 * 1024 * 1024),
                96 * 1024,
            ),
            EnvironmentSnapshot::new([("PATH", recorder.path().to_string_lossy().into_owned())]),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(19)),
        )
        .with_pi_installation(installation.clone()),
    )
    .unwrap();

    assert_eq!(admitted.agent_steps().len(), THINKING_LEVELS.len());
    for (index, (_, thinking)) in THINKING_LEVELS.iter().enumerate() {
        let AdmittedHarness::Pi(step) = admitted.agent_step(&format!("agent{index}")).unwrap()
        else {
            panic!("the public Pi profile must retain Pi admission");
        };
        assert_eq!(step.installation(), &installation);
        assert_eq!(
            step.installation().executable(),
            recorder.path().join("validated-pi")
        );
        assert_eq!(
            step.installation().version().as_str(),
            PI_JSON_V1_QUALIFICATION_VERSION
        );
        assert_eq!(
            step.installation().profile(),
            PiCompatibilityProfile::PiJsonV1
        );
        assert!(
            step.installation()
                .capabilities()
                .required()
                .contains(&PiCapability::InvocationScopedProjectTrust)
        );
        assert_eq!(
            step.configuration().model,
            format!("future-provider/unknown-model-{index}")
        );
        assert_eq!(step.configuration().thinking, *thinking);
        assert_eq!(
            step.limits().maximum_system_prompt_bytes().get(),
            MAXIMUM_AGENT_PROMPT_BYTES
        );
        assert_eq!(
            step.limits().maximum_message_bytes().get(),
            MAXIMUM_AGENT_PROMPT_BYTES
        );
        assert_eq!(
            step.limits().maximum_response_bytes().get(),
            MAXIMUM_AGENT_RESPONSE_BYTES
        );
        assert_eq!(
            step.limits().maximum_result_bytes().get(),
            MAXIMUM_AGENT_RESULT_BYTES
        );
        assert_eq!(
            step.limits()
                .maximum_result_rejection_feedback_bytes()
                .get(),
            8 * 1024
        );
        assert_eq!(
            step.limits().result_validation_deadline().get(),
            Duration::from_secs(60)
        );
        assert_eq!(
            step.limits().result_settlement_grace().get(),
            Duration::from_secs(30)
        );
        let maximum_frame_bytes = step.limits().adapter_protocol().maximum_frame_bytes().get();
        assert_eq!(maximum_frame_bytes, 16 * 1024 * 1024);
        assert!(step.limits().maximum_response_bytes().get() < maximum_frame_bytes);
        assert!(step.limits().maximum_result_bytes().get() < maximum_frame_bytes);
    }
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_captured_file_bytes()
            .get(),
        3 * 1024 * 1024
    );
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_captured_git_carriers()
            .get(),
        19
    );
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_captured_git_carrier_bytes()
            .get(),
        4 * 1024 * 1024
    );
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_total_captured_git_carrier_bytes()
            .get(),
        10 * 1024 * 1024
    );
    recorder.assert_empty();
    assert_eq!(root_snapshot(&fixture.execution_root), root_before);
}

#[test]
fn internal_claude_admission_retains_native_effort_and_profile_limits() {
    let fixture = WorkflowFixture::new(AGENT_WORKFLOW);
    let mut resolved = fixture.resolve();
    let ValidatedStep::Agent(step) = resolved.definition.steps.get_mut("agent").unwrap() else {
        panic!("the fixture must contain an agent step");
    };
    step.agent.harness = ValidatedHarness::ClaudeCode(ClaudeCodeConfig {
        model: "claude-opus-4-1".to_owned(),
        effort: ClaudeCodeEffort::XHigh,
    });
    let installation =
        ValidatedClaudeCodeInstallation::fixture(fixture.execution_root.join("validated-claude"));
    let admitted = admit_workflow(
        resolved,
        ResolvedImports::new(Some(Arc::from("Caller prompt.")), Arc::from([])),
        fixture
            .context(1, Duration::from_secs(1))
            .with_claude_code_installation(installation.clone()),
    )
    .unwrap();

    let AdmittedHarness::ClaudeCode(agent) = admitted.agent_step("agent").unwrap() else {
        panic!("the internal Claude profile must retain Claude admission");
    };
    assert_eq!(agent.installation(), &installation);
    assert_eq!(
        agent.installation().profile(),
        ClaudeCodeCompatibilityProfile::ClaudeCodeStreamJsonV1
    );
    assert_eq!(agent.installation().version().as_str(), "2.1.259");
    assert_eq!(agent.configuration().model, "claude-opus-4-1");
    assert_eq!(agent.configuration().effort, ClaudeCodeEffort::XHigh);
    assert_eq!(
        agent.limits().adapter_protocol(),
        &ClaudeCodeStreamJsonV1ProtocolLimits::profile()
    );
    assert_eq!(
        agent.limits().maximum_response_bytes().get(),
        MAXIMUM_AGENT_RESPONSE_BYTES
    );
}

#[test]
fn codex_admission_preserves_the_exact_installation_configuration_and_limits() {
    let fixture = WorkflowFixture::new(AGENT_WORKFLOW);
    let mut resolved = fixture.resolve();
    let ValidatedStep::Agent(step) = resolved.definition.steps.get_mut("agent").unwrap() else {
        panic!("the fixture must contain an agent step");
    };
    step.agent.harness = ValidatedHarness::Codex(CodexConfig {
        model: "gpt-5.4".to_owned(),
        effort: "future-native-effort".to_owned(),
    });
    let installation =
        ValidatedCodexInstallation::fixture(fixture.execution_root.join("validated-codex"));
    let admitted = admit_workflow(
        resolved,
        ResolvedImports::new(Some(Arc::from("Caller prompt.")), Arc::from([])),
        fixture
            .context(1, Duration::from_secs(1))
            .with_codex_installation(installation.clone()),
    )
    .unwrap();

    let AdmittedHarness::Codex(agent) = admitted.agent_step("agent").unwrap() else {
        panic!("the Codex profile must retain Codex admission");
    };
    assert_eq!(agent.installation(), &installation);
    assert_eq!(
        agent.installation().profile(),
        CodexCompatibilityProfile::CodexAppServerV1
    );
    assert_eq!(
        agent.installation().version().as_str(),
        CODEX_APP_SERVER_V1_QUALIFICATION_VERSION
    );
    assert_eq!(agent.configuration().model, "gpt-5.4");
    assert_eq!(agent.configuration().effort, "future-native-effort");
    assert_eq!(
        agent.limits().adapter_protocol(),
        &CodexAppServerV1ProtocolLimits::profile()
    );
    assert_eq!(
        agent.limits().maximum_response_bytes().get(),
        MAXIMUM_AGENT_RESPONSE_BYTES
    );
}

#[test]
fn admission_uses_only_the_resolved_snapshot_and_leaves_the_execution_root_unchanged() {
    let fixture = WorkflowFixture::new(COMMAND_WORKFLOW);
    let resolved = fixture.resolve();
    let digest = resolved.content_digest.clone();
    let workflow_bytes = resolved.source_bytes("workflow.yaml").unwrap().to_vec();
    fs::write(
        fixture.execution_root.join("caller-owned.txt"),
        b"keep me\n",
    )
    .unwrap();
    let process_guard = fixture.execution_root.join("must-not-run");
    fs::write(
        &process_guard,
        b"#!/bin/sh\nprintf scheduled > admission-started\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&process_guard, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let root_before = root_snapshot(&fixture.execution_root);

    fs::remove_dir_all(&fixture.source_root).unwrap();
    let cancellation = CancellationSource::new();
    let caller_cancellation = cancellation.clone();
    let mut cancellation_notifications = cancellation.subscribe();
    let imports = ResolvedImports::new(
        Some(Arc::<str>::from("Run the checks.")),
        Arc::<[ResolvedAttachment]>::from(vec![ResolvedAttachment::new(
            Arc::<str>::from("application/octet-stream"),
            Arc::<[u8]>::from([0, 0xff, b'\n']),
        )]),
    );
    let admitted = admit_workflow(
        resolved,
        imports,
        ExecutionContext::new(
            fixture.execution_root.join("."),
            ExecutionPolicyLimits::new(
                3,
                CaptureLimits::new(17, 2 * 1024 * 1024, 8 * 1024 * 1024),
                InputLimits::new(31, 2 * 1024 * 1024, 8 * 1024 * 1024, 7 * 1024 * 1024),
                64 * 1024,
            ),
            EnvironmentSnapshot::new([
                ("PATH", "/admitted/bin"),
                ("SCHERZO_INHERITED", "must be removed"),
            ]),
            CancellationPolicy::new(cancellation, Duration::from_secs(15)),
        ),
    )
    .unwrap();

    assert_eq!(
        admitted.execution().root(),
        fs::canonicalize(&fixture.execution_root).unwrap()
    );
    assert_eq!(
        admitted.execution().limits().maximum_parallel_steps().get(),
        3
    );
    assert_eq!(
        admitted.execution().limits().maximum_captured_files().get(),
        17
    );
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_captured_file_bytes()
            .get(),
        2 * 1024 * 1024
    );
    assert_eq!(
        admitted.execution().limits().maximum_step_log_bytes().get(),
        64 * 1024
    );
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_total_captured_bytes()
            .get(),
        8 * 1024 * 1024
    );
    assert_eq!(
        admitted.execution().limits().maximum_input_values().get(),
        31
    );
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_input_value_bytes()
            .get(),
        2 * 1024 * 1024
    );
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_total_input_bytes()
            .get(),
        8 * 1024 * 1024
    );
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_live_input_bytes()
            .get(),
        7 * 1024 * 1024
    );
    assert_eq!(
        admitted
            .execution()
            .environment()
            .variable(std::ffi::OsStr::new("PATH")),
        Some(std::ffi::OsStr::new("/admitted/bin"))
    );
    assert!(
        admitted
            .execution()
            .environment()
            .variable(std::ffi::OsStr::new("SCHERZO_INHERITED"))
            .is_none()
    );
    assert_eq!(
        admitted.execution().cancellation().grace(),
        Duration::from_secs(15)
    );
    assert!(!admitted.execution().cancellation().source().is_cancelled());
    assert!(caller_cancellation.request_cancellation(CancellationReason::UserRequest));
    assert!(cancellation_notifications.has_changed().unwrap());
    assert_eq!(
        *cancellation_notifications.borrow_and_update(),
        Some(CancellationReason::UserRequest)
    );
    assert!(!caller_cancellation.request_cancellation(CancellationReason::RunnerShutdown));
    assert!(!cancellation_notifications.has_changed().unwrap());
    assert_eq!(
        admitted
            .execution()
            .cancellation()
            .source()
            .cancellation_reason(),
        Some(CancellationReason::UserRequest)
    );

    assert_eq!(admitted.imports().prompt(), Some("Run the checks."));
    assert_eq!(admitted.imports().attachments().len(), 1);
    assert_eq!(
        admitted.imports().attachments()[0].media_type(),
        "application/octet-stream"
    );
    assert_eq!(
        admitted.imports().attachments()[0].bytes(),
        [0, 0xff, b'\n']
    );
    assert_eq!(admitted.workflow().content_digest, digest);
    assert_eq!(
        admitted.workflow().source_bytes("workflow.yaml"),
        Some(workflow_bytes.as_slice())
    );
    assert!(!fixture.source_root.exists());
    assert!(!fixture.execution_root.join("admission-started").exists());
    assert_eq!(root_snapshot(&fixture.execution_root), root_before);
}

#[test]
fn admission_rejects_each_invalid_execution_root_kind() {
    let missing = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    let missing_root = missing.execution_root.join("missing");
    assert_failure(
        admit_workflow(
            missing.resolve(),
            ResolvedImports::default(),
            execution_context(missing_root, 1, Duration::from_secs(1)),
        ),
        AdmissionFailureKind::ExecutionRootUnavailable,
        AdmissionLocation::ExecutionRoot,
    );

    let file = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    let file_root = file.execution_root.join("not-a-directory");
    fs::write(&file_root, b"file\n").unwrap();
    assert_failure(
        admit_workflow(
            file.resolve(),
            ResolvedImports::default(),
            execution_context(file_root, 1, Duration::from_secs(1)),
        ),
        AdmissionFailureKind::ExecutionRootNotDirectory,
        AdmissionLocation::ExecutionRoot,
    );
}

#[test]
fn admission_rejects_invalid_execution_limits_and_out_of_bounds_cancellation_policy() {
    let zero_parallelism = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_workflow(
            zero_parallelism.resolve(),
            ResolvedImports::default(),
            zero_parallelism.context(0, Duration::from_secs(1)),
        ),
        AdmissionFailureKind::NonPositiveParallelism,
        AdmissionLocation::MaximumParallelSteps,
    );

    for (captured_files, file_bytes, total_bytes, kind, location) in [
        (
            0,
            1,
            1,
            AdmissionFailureKind::NonPositiveCapturedFiles,
            AdmissionLocation::MaximumCapturedFiles,
        ),
        (
            1,
            0,
            1,
            AdmissionFailureKind::NonPositiveCapturedFileBytes,
            AdmissionLocation::MaximumCapturedFileBytes,
        ),
        (
            1,
            1,
            0,
            AdmissionFailureKind::NonPositiveTotalCapturedBytes,
            AdmissionLocation::MaximumTotalCapturedBytes,
        ),
    ] {
        let fixture = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
        assert_failure(
            admit_workflow(
                fixture.resolve(),
                ResolvedImports::default(),
                ExecutionContext::new(
                    fixture.execution_root.clone(),
                    ExecutionPolicyLimits::new(
                        1,
                        CaptureLimits::new(captured_files, file_bytes, total_bytes),
                        InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                        1024 * 1024,
                    ),
                    EnvironmentSnapshot::default(),
                    CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
                ),
            ),
            kind,
            location,
        );
    }

    for (git_carriers, carrier_bytes, total_bytes, kind, location) in [
        (
            0,
            1,
            1,
            AdmissionFailureKind::NonPositiveCapturedGitCarriers,
            AdmissionLocation::MaximumCapturedGitCarriers,
        ),
        (
            1,
            0,
            1,
            AdmissionFailureKind::NonPositiveCapturedGitCarrierBytes,
            AdmissionLocation::MaximumCapturedGitCarrierBytes,
        ),
        (
            1,
            1,
            0,
            AdmissionFailureKind::NonPositiveTotalCapturedGitCarrierBytes,
            AdmissionLocation::MaximumTotalCapturedGitCarrierBytes,
        ),
    ] {
        let fixture = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
        assert_failure(
            admit_workflow(
                fixture.resolve(),
                ResolvedImports::default(),
                ExecutionContext::new(
                    fixture.execution_root.clone(),
                    ExecutionPolicyLimits::new(
                        1,
                        CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024)
                            .with_git_carrier_limits(git_carriers, carrier_bytes, total_bytes),
                        InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                        1024 * 1024,
                    ),
                    EnvironmentSnapshot::default(),
                    CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
                ),
            ),
            kind,
            location,
        );
    }

    for (values, value_bytes, total_bytes, live_bytes, kind, location) in [
        (
            0,
            1,
            1,
            1,
            AdmissionFailureKind::NonPositiveInputValues,
            AdmissionLocation::MaximumInputValues,
        ),
        (
            1,
            0,
            1,
            1,
            AdmissionFailureKind::NonPositiveInputValueBytes,
            AdmissionLocation::MaximumInputValueBytes,
        ),
        (
            1,
            1,
            0,
            1,
            AdmissionFailureKind::NonPositiveTotalInputBytes,
            AdmissionLocation::MaximumTotalInputBytes,
        ),
        (
            1,
            1,
            1,
            0,
            AdmissionFailureKind::NonPositiveLiveInputBytes,
            AdmissionLocation::MaximumLiveInputBytes,
        ),
    ] {
        let fixture = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
        assert_failure(
            admit_workflow(
                fixture.resolve(),
                ResolvedImports::default(),
                ExecutionContext::new(
                    fixture.execution_root.clone(),
                    ExecutionPolicyLimits::new(
                        1,
                        CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                        InputLimits::new(values, value_bytes, total_bytes, live_bytes),
                        1024 * 1024,
                    ),
                    EnvironmentSnapshot::default(),
                    CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
                ),
            ),
            kind,
            location,
        );
    }

    let zero_log_limit = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_workflow(
            zero_log_limit.resolve(),
            ResolvedImports::default(),
            ExecutionContext::new(
                zero_log_limit.execution_root.clone(),
                ExecutionPolicyLimits::new(
                    1,
                    CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                    InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                    0,
                ),
                EnvironmentSnapshot::default(),
                CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
            ),
        ),
        AdmissionFailureKind::NonPositiveStepLogBytes,
        AdmissionLocation::MaximumStepLogBytes,
    );

    let short_grace = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_workflow(
            short_grace.resolve(),
            ResolvedImports::default(),
            short_grace.context(1, MINIMUM_CANCELLATION_GRACE - Duration::from_nanos(1)),
        ),
        AdmissionFailureKind::CancellationGraceTooShort,
        AdmissionLocation::CancellationPolicy,
    );

    let excessive_grace = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_workflow(
            excessive_grace.resolve(),
            ResolvedImports::default(),
            excessive_grace.context(1, MAXIMUM_CANCELLATION_GRACE + Duration::from_nanos(1)),
        ),
        AdmissionFailureKind::CancellationGraceTooLong,
        AdmissionLocation::CancellationPolicy,
    );
}

#[test]
fn admission_rejects_missing_required_prompt_and_agent_steps_at_typed_locations() {
    let missing_prompt = WorkflowFixture::new(COMMAND_WORKFLOW);
    assert_failure(
        admit_workflow(
            missing_prompt.resolve(),
            ResolvedImports::default(),
            missing_prompt.context(1, Duration::from_secs(1)),
        ),
        AdmissionFailureKind::MissingRequiredPrompt,
        AdmissionLocation::PromptImport,
    );

    let agent = WorkflowFixture::new(AGENT_WORKFLOW);
    assert_failure(
        admit_workflow(
            agent.resolve(),
            ResolvedImports::default(),
            agent.context(1, Duration::from_secs(1)),
        ),
        AdmissionFailureKind::MissingRequiredPrompt,
        AdmissionLocation::PromptImport,
    );
    assert_failure(
        admit_workflow(
            agent.resolve(),
            ResolvedImports::new(Some(Arc::<str>::from("Prompt.")), Arc::from([])),
            agent.context(1, Duration::from_secs(1)),
        ),
        AdmissionFailureKind::AgentStepRuntimeUnsupported,
        AdmissionLocation::Step {
            step: "agent".to_owned(),
        },
    );
}

#[test]
fn admission_rejects_invalid_attachment_media_type() {
    let fixture = WorkflowFixture::new(COMMAND_WORKFLOW);
    let imports = ResolvedImports::new(
        Some(Arc::<str>::from("Run the checks.")),
        Arc::<[ResolvedAttachment]>::from(vec![
            ResolvedAttachment::new(
                Arc::<str>::from("application/octet-stream"),
                Arc::<[u8]>::from([]),
            ),
            ResolvedAttachment::new(Arc::<str>::from("not a media type"), Arc::<[u8]>::from([])),
        ]),
    );

    assert_failure(
        admit_workflow(
            fixture.resolve(),
            imports,
            fixture.context(1, Duration::from_secs(1)),
        ),
        AdmissionFailureKind::InvalidAttachmentMediaType,
        AdmissionLocation::AttachmentImport { index: 1 },
    );
}

#[test]
fn managed_runner_environment_removes_git_trace_redaction_overrides() {
    let environment = EnvironmentSnapshot::new([
        ("GIT_TRACE_CURL", "1"),
        ("GIT_TRACE_REDACT", "0"),
        ("GIT_CURL_VERBOSE", "1"),
        ("VISIBLE", "retained"),
    ])
    .without_managed_runner_credentials_and_helpers();

    for name in ["GIT_TRACE_CURL", "GIT_TRACE_REDACT", "GIT_CURL_VERBOSE"] {
        assert!(environment.variable(OsStr::new(name)).is_none());
    }
    assert_eq!(
        environment.variable(OsStr::new("VISIBLE")),
        Some(OsStr::new("retained"))
    );
}

#[test]
fn cloud_source_revision_replaces_inherited_reserved_values_while_local_has_none() {
    let fixture = WorkflowFixture::new(COMMAND_WORKFLOW);
    let environment = EnvironmentSnapshot::new([
        ("PATH", "/bin"),
        ("SCHERZO_SOURCE_BRANCH", "inherited-branch"),
        ("SCHERZO_SOURCE_COMMIT_OID", "inherited-commit"),
        ("SCHERZO_OTHER", "inherited-other"),
    ]);
    let context = ExecutionContext::new(
        fixture.execution_root.clone(),
        default_execution_policy_limits(1),
        environment.clone(),
        CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
    );
    let local = admit_workflow(
        fixture.resolve(),
        ResolvedImports::new(Some(Arc::from("prompt")), Arc::from([])),
        context.clone(),
    )
    .unwrap();
    for name in ["SCHERZO_SOURCE_BRANCH", "SCHERZO_SOURCE_COMMIT_OID"] {
        assert!(
            local
                .execution()
                .environment()
                .variable(OsStr::new(name))
                .is_none(),
            "local admission retained {name}"
        );
    }

    let cloud = admit_runner_workflow(
        fixture.resolve(),
        ResolvedImports::new(Some(Arc::from("prompt")), Arc::from([])),
        context.with_source_revision(SourceRevisionProvenance::new(
            "refs/heads/exact-source",
            "0123456789abcdef0123456789abcdef01234567",
        )),
    )
    .unwrap();
    assert_eq!(
        cloud
            .execution()
            .environment()
            .variable(OsStr::new("SCHERZO_SOURCE_BRANCH")),
        Some(OsStr::new("refs/heads/exact-source"))
    );
    assert_eq!(
        cloud
            .execution()
            .environment()
            .variable(OsStr::new("SCHERZO_SOURCE_COMMIT_OID")),
        Some(OsStr::new("0123456789abcdef0123456789abcdef01234567"))
    );
    assert!(
        cloud
            .execution()
            .environment()
            .variable(OsStr::new("SCHERZO_OTHER"))
            .is_none()
    );
}

#[test]
fn workflow_admission_reserves_only_engine_environment_variables() {
    let environment = EnvironmentSnapshot::new([
        ("PATH", "/bin"),
        ("GIT_ASKPASS", "/local/askpass"),
        ("GIT_CONFIG_COUNT", "1"),
        ("GIT_CONFIG_KEY_0", "credential.helper"),
        ("GIT_CONFIG_VALUE_0", "/local/helper"),
        ("GIT_SSH_COMMAND", "local-ssh"),
        ("GH_TOKEN", "local-gh-token"),
        ("GITHUB_TOKEN", "local-github-token"),
        ("SCHERZO_SOURCE_TOKEN_FD", "9"),
    ]);

    let filtered = environment.without_engine_reserved_variables();

    for (name, value) in [
        ("PATH", "/bin"),
        ("GIT_ASKPASS", "/local/askpass"),
        ("GIT_CONFIG_COUNT", "1"),
        ("GIT_CONFIG_KEY_0", "credential.helper"),
        ("GIT_CONFIG_VALUE_0", "/local/helper"),
        ("GIT_SSH_COMMAND", "local-ssh"),
        ("GH_TOKEN", "local-gh-token"),
        ("GITHUB_TOKEN", "local-github-token"),
    ] {
        assert_eq!(
            filtered.variable(OsStr::new(name)),
            Some(OsStr::new(value)),
            "{name}"
        );
    }
    assert!(
        filtered
            .variable(OsStr::new("SCHERZO_SOURCE_TOKEN_FD"))
            .is_none()
    );
}

fn execution_context(
    root: PathBuf,
    maximum_parallel_steps: usize,
    grace: Duration,
) -> ExecutionContext {
    ExecutionContext::new(
        root,
        ExecutionPolicyLimits::new(
            maximum_parallel_steps,
            CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
            InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
            1024 * 1024,
        ),
        EnvironmentSnapshot::default(),
        CancellationPolicy::new(CancellationSource::new(), grace),
    )
}

fn root_snapshot(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut entries = fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().to_string_lossy().into_owned(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
}

fn assert_failure(
    result: Result<AdmittedWorkflow, AdmissionFailure>,
    kind: AdmissionFailureKind,
    location: AdmissionLocation,
) {
    let failure = result.unwrap_err();
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.location(), &location);
}
