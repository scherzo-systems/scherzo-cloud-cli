use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use super::*;
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
        if source == AGENT_WORKFLOW {
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
    let admitted = admit_command_workflow(
        resolved,
        imports,
        ExecutionContext::new(
            fixture.execution_root.join("."),
            ExecutionRootLifecycle::CallerOwnedRetained,
            3,
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
        admit_command_workflow(
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
        admit_command_workflow(
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
fn admission_rejects_nonpositive_parallelism_and_unbounded_cancellation_policy() {
    let zero_parallelism = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_command_workflow(
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

    let zero_grace = WorkflowFixture::new(COMMAND_WORKFLOW_WITHOUT_IMPORTS);
    assert_failure(
        admit_command_workflow(
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
        admit_command_workflow(
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
        admit_command_workflow(
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
        admit_command_workflow(
            agent.resolve(),
            ResolvedImports::default(),
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
        admit_command_workflow(
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
        let admitted = admit_command_workflow(
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
        maximum_parallel_steps,
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
    result: Result<AdmittedCommandWorkflow, AdmissionFailure>,
    kind: AdmissionFailureKind,
    location: AdmissionLocation,
) {
    let failure = result.unwrap_err();
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.location(), &location);
}
