## Breaking changes

- Remove the unused registered workflow identifier from runs and runner assignments.

## Added

- Run Codex workflow assignments on Runner Serve.
- Add ready-to-run command and agent workflow examples.
- Add `workflow schema` for offline access to the installed workflow structural schema.

## Fixed

- Fix Git branch capture rejecting bundles that reuse baseline objects.
- Fix Codex workflow steps failing when thread-scoped warnings or thread and turn start notifications arrive before their responses.
