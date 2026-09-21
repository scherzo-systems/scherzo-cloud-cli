use std::process::ExitCode;

use scherzo_cloud_execution::ExecutionOutcome;

fn main() -> ExitCode {
    let outcome = if scherzo_cloud_execution::child_guard_worker_requested() {
        scherzo_cloud_execution::run_child_guard_worker()
    } else if scherzo_cloud_execution::result_validation_worker_requested() {
        scherzo_cloud_execution::run_result_validation_worker()
    } else {
        ExecutionOutcome::Failed
    };
    ExitCode::from(match outcome {
        ExecutionOutcome::Succeeded => 0,
        ExecutionOutcome::Failed => 1,
        ExecutionOutcome::Interrupted => 130,
        ExecutionOutcome::Terminated => 143,
    })
}
