## Breaking changes

- Replace the inputless Runner Serve Workflow V1 contract with verified Cloud run inputs.
- Require the receipt-bound Cloud Git source contract and exact managed Runner release.
- Replace Workflow V1 node evidence with canonical detail and primary issues.

## Added

- Add generated Cloud run input admission and retention API support.
- Add runnable examples for workflow recovery, command dataflow, Codex agents, advisory failures, and advanced finalizers.
- Materialize verified Cloud run inputs before Runner Serve accepts assignments.

## Fixed

- Prevent adapter launch preparation from blocking workflow execution.
- Reject Git versions that cannot materialize assigned source.
- Fix workflow output-capture recovery failing before the target reruns.
- Report workflow capacity and step task failures instead of terminating or stalling runs.
- Report when Claude Code does not write its native diagnostic transcript.
- Prevent Runner Serve shutdown from missing workspace-cleanup cancellation.
- Show terminal target-failure and output-owner evidence in workflow status output.
