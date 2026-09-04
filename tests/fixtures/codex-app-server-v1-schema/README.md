# Codex App Server schema fixtures

The nine schema files at this directory root were generated from the authenticated native
Codex CLI 0.153.0 package in fresh isolated `HOME` and `CODEX_HOME` directories. They
exercise the common compiled `CodexAppServerV1` capability probe; candidate-only exact
assertions remain in `scripts/test-codex-app-server-v1-capabilities` so additions in this
bundle do not become requirements for every admitted older release.

`older-0.149.0/` retains the minimized representative fixture from the prior qualification
anchor. The compiled capability test runs the same common probe against it to preserve
evidence that later additive fields and methods did not narrow the reviewed range.
