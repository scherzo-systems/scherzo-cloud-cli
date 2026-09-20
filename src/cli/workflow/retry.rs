use std::io::{self, Write};

use clap::Args;

use crate::execution::{
    CancellationSource, LocalRetryBeginError, LocalRetryOpen, WorkflowRunOutput,
    acquire_local_retry, admit_local_workflow, reconcile_current_result_publication,
};

pub(super) const ABOUT: &str = "Retry a local workflow run";
pub(super) const AFTER_HELP: &str = "Retry eligibility:
  Retry is available when the latest attempt did not succeed or end in rejection, no
  execution owner holds the run, and prior process ownership can be proven safe. A retry
  executes every workflow step as a new attempt.";

// Run, retry, and status intentionally compose different subsets of shared workflow options.
// jscpd:ignore-start
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[command(flatten)]
    execution: super::LocalExecutionRoot,

    #[command(flatten)]
    presentation: super::PresentationOptions,
}
// jscpd:ignore-end

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::run::execute_with_runtime("start local workflow retry runtime", self.execute_async())
    }

    async fn execute_async(self) -> super::super::CommandResult {
        let presentation_config = super::run::presentation_config(&self.presentation);
        let cancellation = CancellationSource::new();
        let signal_task = match super::run::start_signal_observation(cancellation.clone()) {
            Ok(task) => task,
            Err(error) => return Err(error.into()),
        };
        let run_directory = self.run.run_dir.clone();
        let opened = match tokio::task::spawn_blocking(move || {
            reconcile_current_result_publication(&run_directory);
            acquire_local_retry(&run_directory)
        })
        .await
        {
            Ok(opened) => opened,
            Err(_) => {
                signal_task.abort();
                return super::run::diagnose("inspect local workflow retry state");
            }
        };
        let pending = match opened {
            Ok(LocalRetryOpen::Acquired(pending)) => *pending,
            Ok(LocalRetryOpen::Rejected(rejection)) => {
                signal_task.abort();
                return render_retry_rejection(presentation_config, &rejection);
            }
            Err(error) => {
                signal_task.abort();
                return super::run::diagnose(error);
            }
        };
        let (workflow, inputs, maximum_parallel_steps) = {
            let (workflow, inputs, maximum_parallel_steps) = pending.execution_specification();
            (workflow.clone(), inputs.clone(), maximum_parallel_steps)
        };
        let workflow_for_context = workflow.clone();
        let context_cancellation = cancellation.clone();
        let execution_root = self.execution.execution_root;
        let mut context = match super::run::blocking_operation(move || {
            super::run::execution_context_for_workflow(
                &workflow_for_context,
                execution_root,
                maximum_parallel_steps,
                context_cancellation,
            )
        })
        .await
        {
            Ok(context) => context,
            Err(super::run::BlockingOperationError::Operation(failure)) => {
                signal_task.abort();
                let output =
                    WorkflowRunOutput::new(presentation_config, io::stdout(), io::stderr())
                        .for_retry(pending.run_directory());
                return super::run::rejection_exit(
                    output.render_agent_harness_installation_rejection(&workflow, &failure),
                );
            }
            Err(super::run::BlockingOperationError::WorkerUnavailable) => {
                signal_task.abort();
                return super::run::diagnose("prepare local workflow retry context");
            }
        };
        if workflow.requires_git_capture() {
            context = context.with_local_git_baseline(pending.git_baseline().cloned());
        }
        let workflow_for_admission = workflow.clone();
        let admitted = match super::run::blocking_operation(move || {
            admit_local_workflow(workflow_for_admission, inputs, context)
        })
        .await
        {
            Ok(admitted) => admitted,
            Err(super::run::BlockingOperationError::Operation(failure)) => {
                signal_task.abort();
                let output =
                    WorkflowRunOutput::new(presentation_config, io::stdout(), io::stderr())
                        .for_retry(pending.run_directory());
                return super::run::rejection_exit(
                    output.render_admission_rejection(&workflow, &failure),
                );
            }
            Err(super::run::BlockingOperationError::WorkerUnavailable) => {
                signal_task.abort();
                return super::run::diagnose("admit local workflow retry");
            }
        };
        if workflow.source.source_root.to_str().is_none()
            || admitted.execution().root().to_str().is_none()
        {
            signal_task.abort();
            return super::run::diagnose("prepare local workflow retry paths");
        }

        let reused_attempts = match pending.reused_execution_root_attempts(&admitted) {
            Ok(attempts) => attempts,
            Err(error) => {
                signal_task.abort();
                return super::run::diagnose(error);
            }
        };
        if !reused_attempts.is_empty()
            && let Err(error) = write_reuse_warning(admitted.execution().root(), &reused_attempts)
        {
            signal_task.abort();
            return super::run::diagnose(format_args!("write workflow retry warning: {error}"));
        }

        let admitted_for_begin = admitted.clone();
        let owned_run =
            match tokio::task::spawn_blocking(move || pending.begin(&admitted_for_begin)).await {
                Ok(Ok(owned_run)) => owned_run,
                Ok(Err(LocalRetryBeginError::Rejected(rejection))) => {
                    signal_task.abort();
                    return render_retry_rejection(presentation_config, &rejection);
                }
                Ok(Err(LocalRetryBeginError::Operational(error))) => {
                    signal_task.abort();
                    return super::run::diagnose(error);
                }
                Err(_) => {
                    signal_task.abort();
                    return super::run::diagnose("commit local workflow retry attempt");
                }
            };
        super::run::execute_owned_attempt(
            workflow,
            admitted,
            owned_run,
            cancellation,
            signal_task,
            presentation_config,
            super::run::ExecutionLeaf::Retry,
        )
        .await
    }
}

fn render_retry_rejection(
    config: crate::execution::PresentationConfig,
    rejection: &crate::execution::LocalRetryRejection,
) -> super::super::CommandResult {
    let output = WorkflowRunOutput::new(config, io::stdout(), io::stderr())
        .for_retry(rejection.run_directory());
    super::run::rejection_exit(output.render_retry_rejection(rejection))
}

fn write_reuse_warning(execution_root: &std::path::Path, attempts: &[u64]) -> io::Result<()> {
    let standard_error = io::stderr();
    let mut standard_error = standard_error.lock();
    let attempts = attempts
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    writeln!(
        standard_error,
        "! Execution root {execution_root:?} was used by earlier attempt(s) {attempts} and may contain prior changes; Scherzo does not check cleanliness or mutations."
    )?;
    standard_error.flush()
}
