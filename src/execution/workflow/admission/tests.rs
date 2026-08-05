use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use super::*;
use crate::execution::pi::{PiCapability, PiCompatibilityProfile, ValidatedPiInstallation};
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

    fn context(
        &self,
        lifecycle: ExecutionRootLifecycle,
        maximum_parallel_steps: usize,
        grace: Duration,
    ) -> ExecutionContext {
        execution_context(
            self.execution_root.clone(),
            lifecycle,
            maximum_parallel_steps,
            grace,
        )
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
            let mut context = fixture.context(
                ExecutionRootLifecycle::EngineOwnedEphemeral,
                1,
                Duration::from_secs(1),
            );
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
            ExecutionRootLifecycle::EngineOwnedEphemeral,
            ExecutionPolicyLimits::new(
                2,
                CaptureLimits::new(11, 3 * 1024 * 1024, 9 * 1024 * 1024),
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
        let step = admitted.agent_step(&format!("agent{index}")).unwrap();
        assert_eq!(step.installation(), &installation);
        assert_eq!(
            step.installation().executable(),
            recorder.path().join("validated-pi")
        );
        assert_eq!(step.installation().version().as_str(), "0.83.0");
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
            step.project_trust(),
            ProjectTrustPolicy::InvocationScopedEnabled
        );
        assert_eq!(step.limits().maximum_system_prompt_bytes().get(), 64 * 1024);
        assert_eq!(step.limits().maximum_message_bytes().get(), 64 * 1024);
        assert_eq!(step.limits().maximum_response_bytes().get(), 1024 * 1024);
        assert_eq!(step.limits().maximum_result_bytes().get(), 1024 * 1024);
        assert_eq!(
            step.limits()
                .maximum_result_rejection_feedback_bytes()
                .get(),
            8 * 1024
        );
        assert_eq!(
            step.limits().result_validation_deadline().get(),
            Duration::from_secs(5)
        );
        assert_eq!(
            step.limits().result_settlement_grace().get(),
            Duration::from_secs(30)
        );
        assert_eq!(
            step.limits().adapter_protocol().maximum_frame_bytes().get(),
            16 * 1024 * 1024
        );
    }
    assert_eq!(
        admitted
            .execution()
            .limits()
            .maximum_captured_file_bytes()
            .get(),
        3 * 1024 * 1024
    );
    recorder.assert_empty();
    assert_eq!(root_snapshot(&fixture.execution_root), root_before);
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
            ExecutionRootLifecycle::CallerOwnedRetained,
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
        admitted.execution().root_lifecycle(),
        ExecutionRootLifecycle::CallerOwnedRetained
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
            execution_context(
                missing_root,
                ExecutionRootLifecycle::EngineOwnedEphemeral,
                1,
                Duration::from_secs(1),
            ),
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
            execution_context(
                file_root,
                ExecutionRootLifecycle::EngineOwnedRetained,
                1,
                Duration::from_secs(1),
            ),
        ),
        AdmissionFailureKind::ExecutionRootNotDirectory,
        AdmissionLocation::ExecutionRoot,
    );
}

#[test]
fn admission_rejects_nonpositive_execution_limits_and_unbounded_cancellation_policy() {
    let zero_parallelism = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_workflow(
            zero_parallelism.resolve(),
            ResolvedImports::default(),
            zero_parallelism.context(
                ExecutionRootLifecycle::EngineOwnedRetained,
                0,
                Duration::from_secs(1),
            ),
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
                    ExecutionRootLifecycle::EngineOwnedRetained,
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
                    ExecutionRootLifecycle::EngineOwnedRetained,
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
                ExecutionRootLifecycle::EngineOwnedRetained,
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

    let zero_grace = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_workflow(
            zero_grace.resolve(),
            ResolvedImports::default(),
            zero_grace.context(
                ExecutionRootLifecycle::EngineOwnedRetained,
                1,
                Duration::ZERO,
            ),
        ),
        AdmissionFailureKind::NonPositiveCancellationGrace,
        AdmissionLocation::CancellationPolicy,
    );

    let excessive_grace = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_workflow(
            excessive_grace.resolve(),
            ResolvedImports::default(),
            excessive_grace.context(
                ExecutionRootLifecycle::EngineOwnedRetained,
                1,
                MAX_CANCELLATION_GRACE + Duration::from_nanos(1),
            ),
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
            missing_prompt.context(
                ExecutionRootLifecycle::CallerOwnedRetained,
                1,
                Duration::from_secs(1),
            ),
        ),
        AdmissionFailureKind::MissingRequiredPrompt,
        AdmissionLocation::PromptImport,
    );

    let agent = WorkflowFixture::new(AGENT_WORKFLOW);
    assert_failure(
        admit_workflow(
            agent.resolve(),
            ResolvedImports::default(),
            agent.context(
                ExecutionRootLifecycle::EngineOwnedEphemeral,
                1,
                Duration::from_secs(1),
            ),
        ),
        AdmissionFailureKind::MissingRequiredPrompt,
        AdmissionLocation::PromptImport,
    );
    assert_failure(
        admit_workflow(
            agent.resolve(),
            ResolvedImports::new(Some(Arc::<str>::from("Prompt.")), Arc::from([])),
            agent.context(
                ExecutionRootLifecycle::EngineOwnedEphemeral,
                1,
                Duration::from_secs(1),
            ),
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
            fixture.context(
                ExecutionRootLifecycle::CallerOwnedRetained,
                1,
                Duration::from_secs(1),
            ),
        ),
        AdmissionFailureKind::InvalidAttachmentMediaType,
        AdmissionLocation::AttachmentImport { index: 1 },
    );
}

#[test]
fn admitted_root_lifecycle_preserves_each_closed_ownership_variant() {
    for lifecycle in [
        ExecutionRootLifecycle::CallerOwnedRetained,
        ExecutionRootLifecycle::EngineOwnedRetained,
        ExecutionRootLifecycle::EngineOwnedEphemeral,
    ] {
        let fixture = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
        let admitted = admit_workflow(
            fixture.resolve(),
            ResolvedImports::default(),
            fixture.context(lifecycle, 1, MAX_CANCELLATION_GRACE),
        )
        .unwrap();
        assert_eq!(admitted.execution().root_lifecycle(), lifecycle);
    }
}

fn execution_context(
    root: PathBuf,
    lifecycle: ExecutionRootLifecycle,
    maximum_parallel_steps: usize,
    grace: Duration,
) -> ExecutionContext {
    ExecutionContext::new(
        root,
        lifecycle,
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
