## Added

- Retain private native Pi sessions in durable local workflow runs for post-run diagnostics.
- Add runner pool administration and runner list, show, and rename commands.

## Changed

- Local workflow results publish captured file and Git carriers without allocating a second full byte copy.
- Improve CLI help text and human-readable report formatting.
- The wide workflow view gives more space to step details and logs.
- Retain more live TUI log history for typical workflows without increasing the run-wide memory budget, and report discarded bytes when history is evicted.
- Give sign-in-required and service-unavailable failures dedicated exit codes.

## Fixed

- Allow artifact validation on filesystems that record read access.
- Allow runners to accept workflows with up to 256 parallel steps.
- Give Git branch capture more time and identify commands that time out.
- Allow runner telemetry more time to finish exporting during shutdown.
- Retain up to 4 MiB from each workflow command output stream.
- Allow runner cancellations to use the full five-minute shutdown grace.
- Allow Pi workflows to recover when a result tool call reaches the model output limit.
- Wrapped TUI log rows no longer repeat source labels on every continuation.
- Allow Pi installation checks to accommodate slower startups and larger probe output.
- Allow larger prompts and structured results in agent workflows.
- CLI error remedies now explain account creation, identify paths, and offer retry and terminal alternatives.
- CLI operational failures now consistently appear on standard error with contextual causes.
- Workflow DAGs no longer draw redundant transitive connectors.
