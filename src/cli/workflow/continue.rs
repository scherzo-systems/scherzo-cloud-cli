use std::io::{self, Write};
use std::path::PathBuf;

use clap::Args;
use scherzo_cloud_execution::{
    CancellationSource, LocalContinuationOpen, PendingLocalContinuation, WorkflowRunOutput,
    WorkflowRunPresentationResult, acquire_local_continuation, admit_local_continuation_workflow,
    reconcile_current_result_publication, resolve_workflow_file,
};

pub(super) const ABOUT: &str = "Continue selected steps of a local workflow run";
pub(super) const AFTER_HELP: &str = "Select one or more ordinary steps with --from. Their downstream ordinary dependents run again; other eligible ordinary steps are inherited. Every finalizer runs again. The original run inputs and Git baseline remain authoritative.";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,
    #[arg(
        long = "from",
        required = true,
        value_name = "NODE",
        help = "Ordinary step to reexecute (repeatable)"
    )]
    from_steps: Vec<String>,
    #[arg(
        long,
        value_name = "FILE",
        help = "Replacement workflow file within the run's original source root"
    )]
    workflow: Option<PathBuf>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Existing execution root; defaults to the previous attempt's root"
    )]
    execution_root: Option<PathBuf>,
    #[command(flatten)]
    presentation: super::PresentationOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::run::execute_with_runtime(
            "start local workflow continuation runtime",
            self.execute_async(),
        )
    }

    async fn execute_async(self) -> super::super::CommandResult {
        let config = super::run::presentation_config(&self.presentation);
        let cancellation = CancellationSource::new();
        let signal_task = super::run::start_signal_observation(cancellation.clone())?;
        let run_directory = self.run.run_dir.clone();
        let opened = tokio::task::spawn_blocking(move || {
            reconcile_current_result_publication(&run_directory);
            acquire_local_continuation(&run_directory)
        })
        .await
        .map_err(anyhow::Error::new)?
        .map_err(anyhow::Error::new)?;
        let pending = match opened {
            LocalContinuationOpen::Acquired(pending) => *pending,
            LocalContinuationOpen::Rejected(rejection) => {
                signal_task.abort();
                return super::run::rejection_exit(
                    WorkflowRunOutput::new(config, io::stdout(), io::stderr())
                        .for_continue(rejection.run_directory())
                        .render_retry_rejection(&rejection),
                );
            }
        };
        let original_root = pending.previous_definition().source.source_root.clone();
        let workflow = if let Some(path) = &self.workflow {
            let root = original_root.clone();
            let selected = path.clone();
            match super::run::blocking_operation(move || resolve_workflow_file(&root, &selected))
                .await
            {
                Ok(workflow) => workflow,
                Err(super::run::BlockingOperationError::Operation(failure)) => {
                    let result = WorkflowRunOutput::new(config, io::stdout(), io::stderr())
                        .for_continue(pending.run_directory())
                        .render_continuation_resolution_rejection(
                            pending.run_directory(),
                            pending.prior_attempt_number(),
                            &failure,
                        );
                    return reject_after_settlement(pending, &signal_task, result).await;
                }
                Err(super::run::BlockingOperationError::WorkerUnavailable) => {
                    return fail_after_settlement(
                        pending,
                        &signal_task,
                        "resolve continuation definition",
                    )
                    .await;
                }
            }
        } else {
            pending.previous_definition().clone()
        };
        let from = self.from_steps;
        let selection = pending.partition(&workflow, &from);
        let candidate_inputs = pending.candidate_inputs(&workflow);
        let input_admission_failures = scherzo_cloud_execution::local_continuation_input_failures(
            &workflow,
            &candidate_inputs,
        );
        let inputs: Result<_, Vec<String>> = Ok(candidate_inputs);
        // Graph selection is independent of predecessor dispositions. Continue collecting
        // applicable profile and Git-admission failures even if inheritance was rejected.
        let candidate_reexecuted = pending.candidate_reexecuted_steps(&workflow, &from);
        let execution_root = self
            .execution_root
            .unwrap_or_else(|| pending.prior_execution_root().to_owned());
        let maximum_parallel_steps = pending.maximum_parallel_steps();
        let baseline = pending.git_baseline();
        let workflow_for_context = workflow.clone();
        let context_cancellation = cancellation.clone();
        // Discovery is shared with retry, while rejection here must settle abandonment.
        // jscpd:ignore-start
        let (context, installation_failures) = match tokio::task::spawn_blocking(move || {
            super::run::continuation_execution_context(
                &workflow_for_context,
                execution_root,
                maximum_parallel_steps,
                context_cancellation,
            )
        })
        .await
        {
            Ok(context) => context,
            Err(_) => {
                return fail_after_settlement(
                    pending,
                    &signal_task,
                    "prepare continuation execution context",
                )
                .await;
            }
        };
        // jscpd:ignore-end
        let context = if workflow.requires_git_capture() {
            context.with_local_git_baseline(baseline)
        } else {
            context
        };
        let admitted = if let (Some(reexecuted), Some(projected_inputs)) =
            (candidate_reexecuted, inputs.as_ref().ok().cloned())
        {
            let context = context.with_continuation_reexecuted_steps(&reexecuted);
            let candidate = workflow.clone();
            match super::run::blocking_operation(move || {
                admit_local_continuation_workflow(candidate, projected_inputs, context)
            })
            .await
            {
                Ok(admitted) => Some(admitted),
                Err(super::run::BlockingOperationError::Operation(failures)) => {
                    let admission_failures = failures
                        .iter()
                        .filter(|failure| {
                            installation_failures.is_empty()
                                || failure.kind()
                                    != scherzo_cloud_execution::AdmissionFailureKind::AgentStepRuntimeUnsupported
                        })
                        .collect::<Vec<_>>();
                    let result = continuation_rejection(
                        config,
                        &pending,
                        &selection,
                        &inputs,
                        &installation_failures,
                        &admission_failures,
                    );
                    return reject_after_settlement(pending, &signal_task, result).await;
                }
                Err(super::run::BlockingOperationError::WorkerUnavailable) => {
                    return fail_after_settlement(
                        pending,
                        &signal_task,
                        "admit continuation execution",
                    )
                    .await;
                }
            }
        } else {
            None
        };
        if selection.is_err() || inputs.is_err() || !installation_failures.is_empty() {
            let input_admission_failures = input_admission_failures.iter().collect::<Vec<_>>();
            let result = continuation_rejection(
                config,
                &pending,
                &selection,
                &inputs,
                &installation_failures,
                &input_admission_failures,
            );
            return reject_after_settlement(pending, &signal_task, result).await;
        }
        let Some(admitted) = admitted else {
            return fail_after_settlement(pending, &signal_task, "complete continuation admission")
                .await;
        };
        if let Err(error) = pending.validate_execution_root(&admitted) {
            let settlement = tokio::task::spawn_blocking(move || pending.settle_abandoned()).await;
            signal_task.abort();
            settlement
                .map_err(anyhow::Error::new)?
                .map_err(anyhow::Error::new)?;
            return super::run::diagnose(error);
        }
        let replacement_requested = self.workflow.is_some();
        let admitted_for_begin = admitted.clone();
        let owned = tokio::task::spawn_blocking(move || {
            pending.begin(&admitted_for_begin, from, replacement_requested)
        })
        .await
        .map_err(anyhow::Error::new)?
        .map_err(anyhow::Error::new)?;
        let record = owned.continuation_record().map_err(anyhow::Error::new)?;
        let Some(record) = record else {
            signal_task.abort();
            return Err(anyhow::anyhow!("claimed continuation has no continuation record").into());
        };
        let attempt_number = owned.attempt_number();
        {
            let mut stderr = io::stderr().lock();
            if config.mode() == scherzo_cloud_execution::PresentationMode::Json {
                let event = serde_json::json!({
                    "event": "continuation_partition", "attemptNumber": attempt_number,
                    "continuation": record,
                });
                serde_json::to_writer(&mut stderr, &event).map_err(anyhow::Error::new)?;
                writeln!(stderr).map_err(anyhow::Error::new)?;
            } else {
                writeln!(
                    stderr,
                    "Continuation attempt {attempt_number}: reexecute [{}], inherit [{}]",
                    record.reexecuted_steps().join(", "),
                    record.inherited_step_ids().collect::<Vec<_>>().join(", ")
                )
                .map_err(anyhow::Error::new)?;
            }
            stderr.flush().map_err(anyhow::Error::new)?;
        }
        let (owned, admitted) = tokio::task::spawn_blocking(move || {
            let admitted = owned.bind_continuation_context(admitted)?;
            Ok::<_, scherzo_cloud_execution::LocalRunDirectoryError>((owned, admitted))
        })
        .await
        .map_err(anyhow::Error::new)?
        .map_err(anyhow::Error::new)?;
        super::run::execute_owned_attempt(
            workflow,
            admitted,
            owned,
            cancellation,
            signal_task,
            config,
            super::run::ExecutionLeaf::Continue,
        )
        .await
    }
}

fn continuation_rejection(
    config: scherzo_cloud_execution::PresentationConfig,
    pending: &PendingLocalContinuation,
    selection: &Result<
        (Vec<String>, Vec<String>),
        Vec<scherzo_cloud_execution::ContinuationAdmissionViolation>,
    >,
    inputs: &Result<scherzo_cloud_execution::ResolvedInputs, Vec<String>>,
    installation: &[scherzo_cloud_execution::AgentHarnessInstallationFailure],
    admission: &[&scherzo_cloud_execution::AdmissionFailure],
) -> WorkflowRunPresentationResult {
    WorkflowRunOutput::new(config, io::stdout(), io::stderr())
        .for_continue(pending.run_directory())
        .render_continuation_admission_rejection(
            pending.run_directory(),
            pending.prior_attempt_number(),
            selection
                .as_ref()
                .err()
                .map(Vec::as_slice)
                .unwrap_or_default(),
            inputs.as_ref().err().map(Vec::as_slice).unwrap_or_default(),
            installation,
            admission,
        )
}

async fn fail_after_settlement(
    pending: PendingLocalContinuation,
    signal_task: &tokio::task::JoinHandle<()>,
    message: &'static str,
) -> super::super::CommandResult {
    let settlement = tokio::task::spawn_blocking(move || pending.settle_abandoned()).await;
    signal_task.abort();
    settlement
        .map_err(anyhow::Error::new)?
        .map_err(anyhow::Error::new)?;
    super::run::diagnose(message)
}

async fn reject_after_settlement(
    pending: PendingLocalContinuation,
    signal_task: &tokio::task::JoinHandle<()>,
    result: WorkflowRunPresentationResult,
) -> super::super::CommandResult {
    let settlement = tokio::task::spawn_blocking(move || pending.settle_abandoned()).await;
    signal_task.abort();
    settlement
        .map_err(anyhow::Error::new)?
        .map_err(anyhow::Error::new)?;
    super::run::rejection_exit(result)
}
