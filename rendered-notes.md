## Breaking changes

- Require immutable Git source provenance in Cloud runner assignment offers.
- Require runner serve to load enrolled connectivity and workflow settings from one operator configuration.

## Added

- Add workflow finalizers for cleanup and reporting after ordinary execution.
- Add local live Runner Serve status and credential reload.
- Add GitHub App installation and repository connections for project source access.
- Add Claude Code support to local workflows.
- Allow Cloud runner assignments to use installed Claude Code.
- Add runner enable, drain, disable, and move commands.
- Add zero-downtime runner credential rotation and recovery.
- Add commands to list, retire, and revoke runner credentials.

## Fixed

- Preserve structured workflow results in Pi session transcripts.
- Ensure runner startup diagnostics are written before later startup failures exit.
- Run local and assigned agent steps through the same harness dispatcher.
- Prevent accepted workflow results from failing during automatic context compaction.
