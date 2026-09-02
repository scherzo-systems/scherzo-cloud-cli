use std::collections::BTreeMap;
use std::fs;
use std::os::fd::OwnedFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::*;
use crate::execution::claude_code::ValidatedClaudeCodeInstallation;
use crate::execution::codex::{
    CODEX_APP_SERVER_V1_QUALIFICATION_VERSION, ValidatedCodexInstallation,
};
use crate::execution::pi::ValidatedPiInstallation;
use crate::execution::workflow::admission::{
    CancellationPolicy, CaptureLimits, EnvironmentSnapshot, ExecutionContext,
    ExecutionPolicyLimits, ExecutionRootLifecycle, InputLimits, ResolvedAttachment,
    ResolvedImports, admit_workflow,
};
use crate::execution::workflow::agent::{AgentValueKind, NoopAgentObservationSink, WorkflowRunId};
use crate::execution::workflow::agent_diagnostics::AgentDiagnosticSessionStore;
use crate::execution::workflow::artifact::{ArtifactStaging, CaptureDeclaration};
use crate::execution::workflow::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use crate::execution::workflow::codex::CodexConfig;
use crate::execution::workflow::codex_app_server_v1::CodexAppServerV1ProtocolLimits;
use crate::execution::workflow::input::{InputStaging, InputValue};
use crate::execution::workflow::pi::Thinking;
use crate::execution::workflow::resolution;
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};
use crate::execution::workflow::validated::{ValidatedHarness, WorkflowNode, WorkflowNodeRole};

const SYSTEM_PROMPT: &str = "System @ exact.\n";
const STATIC_MESSAGE: &str = "static - text\n";
const STATIC_ATTACHMENT: &[u8] = &[0, 0xff, b'@'];
const RESULT_SCHEMA: &str =
    "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"object\"}\n";

#[derive(Clone, Copy)]
enum ConsumerValueMode {
    None,
    Response,
    Result,
}

#[derive(Clone, Copy)]
enum ConsumerHarness {
    Pi,
    ClaudeCode,
    Codex,
}

struct Fixture {
    _temporary: tempfile::TempDir,
    source_root: PathBuf,
    execution_root: PathBuf,
    staging_parent: PathBuf,
    admitted: AdmittedWorkflow,
    artifacts: ArtifactStaging,
    staging: AgentInputStaging,
    diagnostic_sessions: AgentDiagnosticSessionStore,
    upstream: BTreeMap<ResolvedOutputSource, CapturedValue>,
}

impl Fixture {
    fn new(mode: ConsumerValueMode) -> Self {
        Self::with_configuration(mode, 1, ConsumerHarness::Pi)
    }

    fn with_attachment_splices(mode: ConsumerValueMode, attachment_splices: usize) -> Self {
        Self::with_configuration(mode, attachment_splices, ConsumerHarness::Pi)
    }

    fn with_claude_code_consumer(mode: ConsumerValueMode) -> Self {
        Self::with_configuration(mode, 1, ConsumerHarness::ClaudeCode)
    }

    fn with_codex_consumer(mode: ConsumerValueMode) -> Self {
        Self::with_configuration(mode, 1, ConsumerHarness::Codex)
    }

    fn with_configuration(
        mode: ConsumerValueMode,
        attachment_splices: usize,
        consumer_harness: ConsumerHarness,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let staging_parent = temporary.path().join("staging");
        for directory in [
            source_root.join("prompts"),
            source_root.join("attachments"),
            source_root.join("schemas"),
            execution_root.join("work"),
            staging_parent.clone(),
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::write(source_root.join("prompts/system.md"), SYSTEM_PROMPT).unwrap();
        fs::write(source_root.join("prompts/message.md"), STATIC_MESSAGE).unwrap();
        fs::write(
            source_root.join("attachments/static.bin"),
            STATIC_ATTACHMENT,
        )
        .unwrap();
        fs::write(source_root.join("schemas/result.json"), RESULT_SCHEMA).unwrap();
        let workflow = workflow_source(mode, attachment_splices);
        fs::write(source_root.join("workflow.yaml"), &workflow).unwrap();
        fs::write(
            execution_root.join("artifact-source.bin"),
            b"captured exact",
        )
        .unwrap();

        let mut resolved = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let ValidatedStep::Agent(consumer) = resolved.definition.steps.get_mut("consumer").unwrap()
        else {
            panic!("the fixture consumer must be an agent step");
        };
        match consumer_harness {
            ConsumerHarness::Pi => {}
            ConsumerHarness::ClaudeCode => {
                consumer.agent.harness = ValidatedHarness::ClaudeCode(ClaudeCodeConfig {
                    model: "claude-opus-4-1".to_owned(),
                    effort: ClaudeCodeEffort::High,
                });
            }
            ConsumerHarness::Codex => {
                consumer.agent.harness = ValidatedHarness::Codex(CodexConfig {
                    model: "gpt-5.4".to_owned(),
                    effort: "xhigh".to_owned(),
                });
            }
        }

        // This ordered mutation occurs after resolution and before materialization. The
        // invocation must continue to use only the retained closure and captured artifact.
        fs::write(
            source_root.join("prompts/system.md"),
            b"mutated system prompt",
        )
        .unwrap();
        fs::write(source_root.join("prompts/message.md"), b"mutated message").unwrap();
        fs::write(
            source_root.join("attachments/static.bin"),
            b"mutated attachment",
        )
        .unwrap();
        fs::write(
            source_root.join("schemas/result.json"),
            b"{\"mutated\":true}",
        )
        .unwrap();

        let imports = ResolvedImports::new(
            Some(Arc::from("- imported prompt\n@ remains instruction text")),
            Arc::from([
                ResolvedAttachment::new(Arc::from("image/png"), Arc::from(*b"import-one"))
                    .with_diagnostic_source_name(Arc::from("../../caller.png")),
                ResolvedAttachment::new(
                    Arc::from("text/plain; charset=utf-8"),
                    Arc::from(*b"import-two"),
                )
                .with_diagnostic_source_name(Arc::from("@notes.txt")),
            ]),
        );
        let mut context = execution_context(&execution_root);
        match consumer_harness {
            ConsumerHarness::Pi => {}
            ConsumerHarness::ClaudeCode => {
                context = context.with_claude_code_installation(
                    ValidatedClaudeCodeInstallation::fixture("/validated/claude".into()),
                );
            }
            ConsumerHarness::Codex => {
                context = context.with_codex_installation(ValidatedCodexInstallation::fixture(
                    "/validated/codex".into(),
                ));
            }
        }
        let admitted = admit_workflow(resolved, imports, context).unwrap();
        let artifacts = ArtifactStaging::create(admitted.execution(), &staging_parent).unwrap();
        let captured = artifacts
            .capture_files(&[CaptureDeclaration::file(
                "file",
                Path::new("artifact-source.bin"),
                "application/x-captured",
            )])
            .unwrap()
            .remove("file")
            .unwrap();
        fs::write(
            execution_root.join("artifact-source.bin"),
            b"mutated captured source",
        )
        .unwrap();

        let upstream = BTreeMap::from([
            (
                output_source("responseProducer", "response", WorkflowValueType::Text),
                CapturedValue::text(Arc::from("@ upstream response")),
            ),
            (
                output_source("resultProducer", "result", WorkflowValueType::Json),
                CapturedValue::json_fixture(Arc::new(json!({"z": 2, "a": 1}))),
            ),
            (
                output_source("fileProducer", "file", WorkflowValueType::File),
                CapturedValue::file(captured),
            ),
        ]);
        let staging = AgentInputStaging::create(admitted.execution(), &staging_parent).unwrap();
        let attempt_directory = temporary.path().join("run/attempts/000001");
        fs::create_dir_all(&attempt_directory).unwrap();
        let attempt_handle: OwnedFd = fs::File::open(&attempt_directory).unwrap().into();
        let diagnostic_sessions = AgentDiagnosticSessionStore::create(
            &attempt_handle,
            &attempt_directory,
            Arc::from("00000000-0000-4000-8000-000000000001"),
            1,
        )
        .unwrap();
        Self {
            _temporary: temporary,
            source_root,
            execution_root,
            staging_parent,
            admitted,
            artifacts,
            staging,
            diagnostic_sessions,
            upstream,
        }
    }

    fn materialize(
        &self,
        cancellation: CancellationSource,
    ) -> Result<MaterializedAgentInvocation<NoopAgentObservationSink>, AgentInputMaterializationError>
    {
        materialize_agent_invocation(
            &self.admitted,
            &self.artifacts,
            &self.staging,
            &self.diagnostic_sessions,
            identity(),
            &self.upstream,
            None,
            cancellation,
            crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
            NoopAgentObservationSink,
        )
    }
}

fn workflow_source(mode: ConsumerValueMode, attachment_splices: usize) -> String {
    let consumer_output = match mode {
        ConsumerValueMode::None => String::new(),
        ConsumerValueMode::Response => {
            "    outputs:\n      response:\n        kind: text\n        from: agent_response\n".to_owned()
        }
        ConsumerValueMode::Result => {
            "    outputs:\n      result:\n        kind: json\n        from: agent_result\n        schema: schemas/result.json\n"
                .to_owned()
        }
    };
    let imported_attachments = "          - ref: imports.attachments\n".repeat(attachment_splices);
    format!(
        r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
steps:
  responseProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      response:
        kind: text
        from: agent_response
  resultProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: schemas/result.json
  fileProducer:
    kind: cmd
    command:
      argv: ["true"]
    outputs:
      file:
        kind: file
        from: path
        path: artifact-source.bin
        mediaType: application/x-captured
  consumer:
    kind: agent
    cwd: work
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
          - ref: imports.prompt
          - ref: outputs.responseProducer.response
        attachments:
          - file: attachments/static.bin
{imported_attachments}          - ref: outputs.resultProducer.result
          - ref: outputs.fileProducer.file
{consumer_output}"#
    )
}

fn execution_context(execution_root: &Path) -> ExecutionContext {
    ExecutionContext::new(
        execution_root.to_owned(),
        ExecutionRootLifecycle::CallerOwnedRetained,
        ExecutionPolicyLimits::new(
            4,
            CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
            InputLimits::new(32, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
            64 * 1024,
        ),
        EnvironmentSnapshot::new([
            ("PATH", "/runner/bin"),
            ("VISIBLE", "exact"),
            ("SCHERZO_CALLER_VALUE", "must-be-removed"),
        ]),
        CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
    )
    .with_pi_installation(ValidatedPiInstallation::fixture("/validated/pi".into()))
}

fn output_source(step: &str, output: &str, value_type: WorkflowValueType) -> ResolvedOutputSource {
    ResolvedOutputSource {
        node: WorkflowNode {
            id: step.to_owned(),
            role: WorkflowNodeRole::Step,
        },
        output: output.to_owned(),
        value_type,
    }
}

fn identity() -> AgentInvocationIdentity {
    AgentInvocationIdentity::new(
        WorkflowRunId::from(Arc::from("run-fixed")),
        Arc::from("consumer"),
        ActionId {
            transition_sequence: TransitionSequence::default(),
        },
    )
}

#[test]
fn retained_imported_and_upstream_values_materialize_exactly_in_declared_order() {
    let fixture = Fixture::new(ConsumerValueMode::Result);
    let materialized = fixture.materialize(CancellationSource::new()).unwrap();
    let invocation = materialized.invocation();

    assert_eq!(invocation.prompt().system_prompt(), SYSTEM_PROMPT);
    assert_eq!(
        invocation.prompt().message(),
        "static - text\n\n\n- imported prompt\n@ remains instruction text\n\n@ upstream response"
    );
    assert_eq!(
        invocation.process().cwd(),
        fs::canonicalize(fixture.execution_root.join("work")).unwrap()
    );
    assert_eq!(
        invocation
            .process()
            .environment()
            .variables()
            .keys()
            .map(|name| name.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["PATH", "VISIBLE"]
    );
    assert_eq!(invocation.value_mode().kind(), AgentValueKind::Result);
    assert_eq!(invocation.value_mode().output(), Some("result"));
    let AgentValueMode::Result { schema, .. } = invocation.value_mode() else {
        panic!("consumer must select result mode");
    };
    assert_eq!(schema.bytes(), RESULT_SCHEMA.as_bytes());
    assert_eq!(schema.document()["type"], "object");

    let expected: [(&[u8], &str, Option<&str>); 5] = [
        (
            STATIC_ATTACHMENT,
            STATIC_ATTACHMENT_MEDIA_TYPE,
            Some("attachments/static.bin"),
        ),
        (
            b"import-one".as_slice(),
            "image/png",
            Some("../../caller.png"),
        ),
        (
            b"import-two".as_slice(),
            "text/plain; charset=utf-8",
            Some("@notes.txt"),
        ),
        (
            br#"{"a":1,"z":2}"#.as_slice(),
            "application/json",
            Some("outputs.resultProducer.result"),
        ),
        (
            b"captured exact".as_slice(),
            "application/x-captured",
            Some("outputs.fileProducer.file"),
        ),
    ];
    assert_eq!(invocation.attachments().len(), expected.len());
    let attachment_directory = materialized.staging_path().join(ATTACHMENT_DIRECTORY);
    for (index, (attachment, (bytes, media_type, diagnostic))) in
        invocation.attachments().iter().zip(expected).enumerate()
    {
        assert_eq!(fs::read(attachment.path()).unwrap(), bytes);
        assert_eq!(attachment.media_type(), media_type);
        assert_eq!(attachment.diagnostic_source_name(), diagnostic);
        let expected_name = format!("{index:06}");
        assert_eq!(
            attachment.path().file_name().and_then(|name| name.to_str()),
            Some(expected_name.as_str())
        );
        assert_eq!(
            attachment.path().parent(),
            Some(attachment_directory.as_path())
        );
        assert!(!attachment.path().to_string_lossy().contains("caller.png"));
    }
    assert_ne!(
        fs::read(fixture.source_root.join("attachments/static.bin")).unwrap(),
        fs::read(invocation.attachments()[0].path()).unwrap()
    );
}

#[test]
fn result_mode_keeps_a_private_writable_endpoint_beside_sealed_attachments() {
    let fixture = Fixture::new(ConsumerValueMode::Result);
    let materialized = fixture.materialize(CancellationSource::new()).unwrap();
    let invocation = materialized.invocation();
    let endpoint = invocation.staging().result_endpoint_directory();
    let attachment_directory = materialized.staging_path().join(ATTACHMENT_DIRECTORY);

    assert_eq!(endpoint.parent(), Some(materialized.staging_path()));
    assert_eq!(
        fs::metadata(endpoint).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&attachment_directory)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o500
    );
    let endpoint_probe = endpoint.join("write-probe");
    fs::write(&endpoint_probe, b"").unwrap();
    fs::remove_file(endpoint_probe).unwrap();
}

#[test]
fn each_declared_agent_value_mode_is_selected_without_file_outputs_affecting_it() {
    for (mode, expected) in [
        (ConsumerValueMode::None, AgentValueKind::None),
        (ConsumerValueMode::Response, AgentValueKind::Response),
        (ConsumerValueMode::Result, AgentValueKind::Result),
    ] {
        let fixture = Fixture::new(mode);
        let materialized = fixture.materialize(CancellationSource::new()).unwrap();
        assert_eq!(materialized.invocation().value_mode().kind(), expected);
    }
}

#[test]
fn materialization_preserves_each_profiles_native_configuration_and_limits() {
    let pi_fixture = Fixture::new(ConsumerValueMode::None);
    let pi_materialized = pi_fixture.materialize(CancellationSource::new()).unwrap();
    let ClosedAgentInvocation::Pi(pi) = pi_materialized.invocation() else {
        panic!("the Pi fixture must materialize a Pi invocation");
    };
    assert_eq!(pi.adapter().native_configuration().model, "openai/gpt-5");
    assert_eq!(
        pi.adapter().native_configuration().thinking,
        Thinking::XHigh
    );
    assert_eq!(
        pi.limits().adapter_protocol(),
        &PiJsonV1ProtocolLimits::profile()
    );

    let claude_fixture = Fixture::with_claude_code_consumer(ConsumerValueMode::None);
    let claude_materialized = claude_fixture
        .materialize(CancellationSource::new())
        .unwrap();
    let ClosedAgentInvocation::ClaudeCode(claude) = claude_materialized.invocation() else {
        panic!("the Claude fixture must materialize a Claude invocation");
    };
    assert_eq!(
        claude.adapter().native_configuration().model,
        "claude-opus-4-1"
    );
    assert_eq!(
        claude.adapter().native_configuration().effort,
        ClaudeCodeEffort::High
    );
    assert_eq!(
        claude.limits().adapter_protocol(),
        &ClaudeCodeStreamJsonV1ProtocolLimits::profile()
    );
    assert_eq!(
        claude
            .diagnostic_session()
            .directory()
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str()),
        Some("claude-code-stream-json-v1")
    );

    let codex_fixture = Fixture::with_codex_consumer(ConsumerValueMode::None);
    let codex_materialized = codex_fixture
        .materialize(CancellationSource::new())
        .unwrap();
    let ClosedAgentInvocation::Codex(codex) = codex_materialized.invocation() else {
        panic!("the Codex fixture must materialize a Codex invocation");
    };
    assert_eq!(codex.adapter().executable(), Path::new("/validated/codex"));
    assert_eq!(
        codex.adapter().version(),
        CODEX_APP_SERVER_V1_QUALIFICATION_VERSION
    );
    assert_eq!(codex.adapter().native_configuration().model, "gpt-5.4");
    assert_eq!(codex.adapter().native_configuration().effort, "xhigh");
    assert_eq!(
        codex.limits().adapter_protocol(),
        &CodexAppServerV1ProtocolLimits::profile()
    );
    assert_eq!(
        codex
            .diagnostic_session()
            .directory()
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str()),
        Some("codex-app-server-v1")
    );
}

#[test]
fn expanded_attachment_splices_are_bounded_before_staging() {
    let fixture = Fixture::with_attachment_splices(ConsumerValueMode::None, 127);

    assert_eq!(
        materialization_error(fixture.materialize(CancellationSource::new())),
        AgentInputMaterializationError::Start(
            AgentInputStartFailure::AttachmentCountLimitExceeded { maximum: 256 }
        )
    );
    assert_eq!(fixture.staging.active_view_count(), 0);
}

#[test]
fn attachment_budget_counts_exact_canonical_json_bytes() {
    let canonical = br#"{"a":1,"z":2}"#;
    let mut attachments = Vec::new();
    let mut budget = AttachmentBudget {
        count: 0,
        bytes: 0,
        maximum_count: 3,
        maximum_bytes: 14,
    };
    budget
        .push(
            PlannedAgentAttachment {
                payload: PlannedAttachment::Bytes(b"x"),
                media_type: Arc::from("text/plain"),
                diagnostic_source_name: None,
            },
            &mut attachments,
        )
        .unwrap();
    budget
        .push(
            PlannedAgentAttachment {
                payload: PlannedAttachment::CanonicalJson(canonical),
                media_type: Arc::from("application/json"),
                diagnostic_source_name: None,
            },
            &mut attachments,
        )
        .unwrap();

    assert_eq!(budget.bytes, 14);
    assert_eq!(attachments.len(), 2);
    assert_eq!(
        budget.push(
            PlannedAgentAttachment {
                payload: PlannedAttachment::Bytes(b"y"),
                media_type: Arc::from("text/plain"),
                diagnostic_source_name: None,
            },
            &mut attachments,
        ),
        Err(AgentInputMaterializationError::Start(
            AgentInputStartFailure::AttachmentBytesLimitExceeded { maximum: 14 }
        ))
    );
    assert_eq!(attachments.len(), 2);
}

#[test]
fn missing_values_invalid_cwd_and_unavailable_staging_are_typed_before_launch() {
    let mut missing = Fixture::new(ConsumerValueMode::None);
    let missing_source = output_source("resultProducer", "result", WorkflowValueType::Json);
    missing.upstream.remove(&missing_source);
    assert_eq!(
        materialization_error(missing.materialize(CancellationSource::new())),
        AgentInputMaterializationError::Start(AgentInputStartFailure::MissingUpstreamValue {
            source: missing_source,
        })
    );
    assert_eq!(missing.staging.active_view_count(), 0);

    let invalid_cwd = Fixture::new(ConsumerValueMode::None);
    fs::remove_dir(invalid_cwd.execution_root.join("work")).unwrap();
    assert_eq!(
        materialization_error(invalid_cwd.materialize(CancellationSource::new())),
        AgentInputMaterializationError::Start(AgentInputStartFailure::WorkingDirectory(
            WorkingDirectoryFailure::Unavailable
        ))
    );
    assert_eq!(invalid_cwd.staging.active_view_count(), 0);

    let unavailable = Fixture::new(ConsumerValueMode::None);
    unavailable.staging.release().unwrap();
    let launch_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = unavailable.materialize(CancellationSource::new());
    if result.is_ok() {
        launch_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    assert_eq!(
        materialization_error(result),
        AgentInputMaterializationError::Start(AgentInputStartFailure::StagingUnavailable)
    );
    assert_eq!(launch_count.load(std::sync::atomic::Ordering::SeqCst), 0);
}

struct BoundaryGate {
    reached: std::sync::mpsc::Sender<AgentMaterializationBoundary>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl AgentMaterializationBoundaryObserver for BoundaryGate {
    fn reached(&self, boundary: AgentMaterializationBoundary) {
        self.reached.send(boundary).unwrap();
        self.release.lock().unwrap().recv().unwrap();
    }
}

#[test]
fn cancellation_at_the_ready_barrier_removes_partial_staging_without_launch() {
    let fixture = Fixture::new(ConsumerValueMode::None);
    let (reached_sender, reached) = std::sync::mpsc::channel();
    let (release, release_receiver) = std::sync::mpsc::channel();
    let staging = AgentInputStaging::with_observer(
        fixture.admitted.execution(),
        &fixture.staging_parent,
        Arc::new(BoundaryGate {
            reached: reached_sender,
            release: Mutex::new(release_receiver),
        }),
    )
    .unwrap();
    let admitted = fixture.admitted.clone();
    let artifacts = fixture.artifacts.clone();
    let upstream = fixture.upstream.clone();
    let cancellation = CancellationSource::new();
    let request_cancellation = cancellation.clone();
    let materializing_staging = staging.clone();
    let diagnostic_sessions = fixture.diagnostic_sessions.clone();
    let (result_sender, result) = std::sync::mpsc::channel();
    let task = std::thread::spawn(move || {
        let result = materialize_agent_invocation(
            &admitted,
            &artifacts,
            &materializing_staging,
            &diagnostic_sessions,
            identity(),
            &upstream,
            None,
            cancellation,
            crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
            NoopAgentObservationSink,
        );
        result_sender.send(result.map(|_| ())).unwrap();
    });

    for index in 0..5 {
        assert_eq!(
            reached.recv().unwrap(),
            AgentMaterializationBoundary::BeforeAttachment { index }
        );
        release.send(()).unwrap();
    }
    assert_eq!(reached.recv().unwrap(), AgentMaterializationBoundary::Ready);
    assert!(request_cancellation.request_cancellation(CancellationReason::UserRequest));
    release.send(()).unwrap();
    assert_eq!(
        result.recv().unwrap(),
        Err(AgentInputMaterializationError::Cancelled {
            reason: CancellationReason::UserRequest,
        })
    );
    task.join().unwrap();
    assert_eq!(staging.active_view_count(), 0);
}

#[test]
fn staging_lease_survives_adapter_and_output_barriers_then_removes_the_view() {
    let fixture = Fixture::new(ConsumerValueMode::None);
    let materialized = fixture.materialize(CancellationSource::new()).unwrap();
    let view = materialized.staging_path().to_owned();
    let (invocation, lease) = materialized.into_parts();
    let (adapter_ready_sender, adapter_ready) = std::sync::mpsc::channel();
    let (adapter_release, adapter_release_receiver) = std::sync::mpsc::channel();
    let (adapter_done_sender, adapter_done) = std::sync::mpsc::channel();
    let adapter = std::thread::spawn(move || {
        assert!(
            invocation
                .attachments()
                .iter()
                .all(|attachment| attachment.path().is_file())
        );
        adapter_ready_sender.send(()).unwrap();
        adapter_release_receiver.recv().unwrap();
        drop(invocation);
        adapter_done_sender.send(()).unwrap();
    });

    adapter_ready.recv().unwrap();
    assert!(view.is_dir());
    adapter_release.send(()).unwrap();
    adapter_done.recv().unwrap();
    adapter.join().unwrap();
    assert!(
        view.is_dir(),
        "output settlement still owns the staging lease"
    );

    let (output_ready_sender, output_ready) = std::sync::mpsc::channel();
    let (output_release, output_release_receiver) = std::sync::mpsc::channel();
    let output = std::thread::spawn(move || {
        output_ready_sender.send(()).unwrap();
        output_release_receiver.recv().unwrap();
        lease.release()
    });
    output_ready.recv().unwrap();
    assert!(view.is_dir());
    output_release.send(()).unwrap();
    output.join().unwrap().unwrap();
    assert!(!view.exists());
    assert_eq!(fixture.staging.active_view_count(), 0);
}

#[test]
fn cleanup_failure_is_typed_and_the_run_owner_can_retry_after_quiescence() {
    let fixture = Fixture::new(ConsumerValueMode::None);
    let materialized = fixture.materialize(CancellationSource::new()).unwrap();
    let view = materialized.staging_path().to_owned();
    let (_invocation, lease) = materialized.into_parts();
    fixture.staging.cleanup_blocker().block();
    assert_eq!(
        lease.release(),
        Err(AgentInputStagingReleaseFailure::CleanupUnavailable)
    );
    assert!(view.exists());
    fixture.staging.cleanup_blocker().unblock();
    fixture.staging.release().unwrap();
    assert!(!view.exists());
    assert_eq!(fixture.staging.active_view_count(), 0);
}

struct ExecutionRootRebinder {
    root: PathBuf,
    moved_root: PathBuf,
    rebound: Mutex<bool>,
}

impl AgentMaterializationBoundaryObserver for ExecutionRootRebinder {
    fn reached(&self, boundary: AgentMaterializationBoundary) {
        if boundary != (AgentMaterializationBoundary::BeforeAttachment { index: 0 }) {
            return;
        }
        let mut rebound = self.rebound.lock().unwrap();
        if *rebound {
            return;
        }
        fs::rename(&self.root, &self.moved_root).unwrap();
        fs::create_dir_all(self.root.join("work")).unwrap();
        *rebound = true;
    }
}

#[test]
fn execution_root_rebinding_during_materialization_is_rejected_before_launch() {
    let fixture = Fixture::new(ConsumerValueMode::None);
    let moved_root = fixture.execution_root.with_extension("moved");
    let staging = AgentInputStaging::with_observer(
        fixture.admitted.execution(),
        &fixture.staging_parent,
        Arc::new(ExecutionRootRebinder {
            root: fixture.execution_root.clone(),
            moved_root,
            rebound: Mutex::new(false),
        }),
    )
    .unwrap();

    let result = materialize_agent_invocation(
        &fixture.admitted,
        &fixture.artifacts,
        &staging,
        &fixture.diagnostic_sessions,
        identity(),
        &fixture.upstream,
        None,
        CancellationSource::new(),
        crate::execution::workflow::process_group::ProcessGuardRegistry::default(),
        NoopAgentObservationSink,
    );

    assert_eq!(
        result.map(|_| ()),
        Err(AgentInputMaterializationError::Start(
            AgentInputStartFailure::WorkingDirectory(WorkingDirectoryFailure::ExecutionRootRebound)
        ))
    );
    assert_eq!(staging.active_view_count(), 0);
}

#[test]
fn agent_staging_rejects_an_execution_root_location() {
    let fixture = Fixture::new(ConsumerValueMode::None);
    assert_eq!(
        staging_creation_error(AgentInputStaging::create(
            fixture.admitted.execution(),
            &fixture.execution_root,
        )),
        AgentInputStagingFailure::StagingParentExposed
    );
}

#[test]
fn agent_staging_rejects_a_replaced_execution_root_identity() {
    let fixture = Fixture::new(ConsumerValueMode::None);
    let moved_root = fixture.execution_root.with_extension("replaced");
    fs::rename(&fixture.execution_root, moved_root).unwrap();
    fs::create_dir(&fixture.execution_root).unwrap();

    assert!(!fixture.staging.is_bound_to(fixture.admitted.execution()));
}

fn materialization_error<Sink>(
    result: Result<MaterializedAgentInvocation<Sink>, AgentInputMaterializationError>,
) -> AgentInputMaterializationError
where
    Sink: AgentObservationSink,
{
    match result {
        Ok(_) => panic!("materialization unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn staging_creation_error(
    result: Result<AgentInputStaging, AgentInputStagingFailure>,
) -> AgentInputStagingFailure {
    match result {
        Ok(_) => panic!("staging creation unexpectedly succeeded"),
        Err(error) => error,
    }
}
#[test]
fn finalization_context_bytes_are_shared_by_command_and_agent() {
    let fixture = Fixture::new(ConsumerValueMode::None);
    let exact = br#"{"schemaVersion":1,"trigger":"succeeded","primaryIssueStepId":null,"cancellationReason":null,"ordinaryIssues":[]}"#;
    let ValidatedStep::Agent(consumer) = fixture
        .admitted
        .workflow()
        .definition
        .steps
        .get("consumer")
        .unwrap()
    else {
        panic!("fixture consumer must be an agent");
    };
    let mut consumer = consumer.clone();
    consumer.agent.message.attachments = vec![ValidatedMessageSource::Reference {
        source: ResolvedValueSource::FinalizationContext,
        value_type: WorkflowValueType::Json,
    }];
    // Build the plan directly because the retained admitted workflow is immutable by design.
    let harness = fixture.admitted.agent_step("consumer").unwrap();
    let plan = build_plan(
        &fixture.admitted,
        "consumer",
        &consumer,
        &fixture.upstream,
        Some(exact),
        harness,
    )
    .unwrap();

    assert!(!plan.prompt.message().contains("schemaVersion"));
    assert_eq!(plan.attachments.len(), 1);
    let PlannedAttachment::CanonicalJson(bytes) = plan.attachments[0].payload else {
        panic!("context was not retained as exact canonical bytes");
    };
    assert_eq!(bytes, exact);
    assert_eq!(plan.attachments[0].media_type.as_ref(), "application/json");

    let command_inputs =
        InputStaging::create(fixture.admitted.execution(), &fixture.staging_parent).unwrap();
    let command_view = command_inputs
        .materialize(
            &BTreeMap::from([(
                "context".to_owned(),
                InputValue::CanonicalJson(exact.as_slice()),
            )]),
            &fixture.artifacts,
        )
        .unwrap();
    assert_eq!(
        fs::read(command_view.path().join("values/context")).unwrap(),
        bytes
    );
}
