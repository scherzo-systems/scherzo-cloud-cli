## Breaking changes

- Runner Serve now requires pinned assignment source and no longer accepts development workflow mappings.
- Remove runner capacity fields from protocol and administration output.

## Added

- Add bounded assignment decline details to runner protocol telemetry.
- Add Codex App Server installation diagnostics.
- Keep CLI sign-ins active with automatic session refresh and revoking logout.
- Enable Git branch outputs in Cloud workflow runs.
- Upload exported workflow artifacts directly from Runner Serve to Cloud storage.
- Download complete verified Cloud Artifact Sets.
- Show durable finalizer results in local status, view, artifacts, and retry.
- Add workflow finalizer support to Runner Serve.
- Deliver complete workflow artifact sets from Runner Serve.
- Accept Cloud workflow results in portable artifact validation.
- Add plain and JSON output for completed workflow viewing.
- Runner Serve now verifies pinned Cloud repository source before accepting assignments.
- Retain owner-private Claude Code sessions for local workflow diagnostics.

## Changed

- Update the admitted Claude Code release to `2.1.234`.

## Fixed

- Accept provider-finalized Pi thinking rewrites during workflow execution.
- Recognize expired runner activations in organization audit history.
- Retain structured Claude Code protocol rejection diagnostics.
- Retain structured Pi protocol rejection diagnostics.
- Fix authenticated private repository checkout on macOS runners.
- Preserve Pi native retries after interrupted tool-call streams.
- Preserve caller-provided GitHub authentication during local workflow runs.
- Prevent temporary source-service failures from blocking later workflow runs on the same runner boot.
- Fix Claude Code steps failing with a harness protocol error on macOS after an accepted result.
