# Development guidance

## Public source boundary

Everything in this repository is public. Do not add credentials, private URLs, internal
incident details, customer data, or other material that cannot be published.

The repository must remain self-contained. Source, scripts, tests, and build files must
not import or read a parent directory, depend on a sibling checkout, inherit a parent
workspace, or use a local path override outside this repository. Do not add symbolic
links.

## Canonical check

Run the complete local validation from the repository root with:

```sh
./scripts/check
```

Keep this command aligned with public CI. Do not place substantive test logic only in a
publication workflow or mirror script.

The implementation language is Rust. Keep `scripts/check` as the complete isolated
validation entrypoint and enter `scripts/strict-devenv` before its test logic runs. It
must continue to run deterministic formatting checks, Clippy, unit and integration tests,
dependency and import boundary validation, and a complete release build.

## Release policy

Keep `release.toml` at schema 2 with only the static initial version, development
version, and pre-1.0 breaking-impact policy. Keep the Cargo package fallback permanently
at `0.0.0-dev`; Nix development builds and allocated release builds inject their
revision-bearing or exact authoritative versions. Source validation must not fetch,
synchronize, or inspect public tags. Run the release fixture suites when changing this
policy or its pure impact arithmetic.

Public release execution must consume only the exact managed allocation and, for
recovery, its current allowlisted recovery chain. Never derive a version from public tags
or the frozen archive. Preserve read-only permissions for checks, verification, and native
builds; grant write only to the final reconcile job; pin every action by commit; and keep
pull requests, ordinary checks, allocation or recovery refs, and recovery-mirror pushes
incapable of publication. Exact manual dispatch is recovery-only and must build the
original allocated mirror rather than current repaired source.

## Release-note archive

`changes/` is a frozen archive of legacy notes. Do not add, modify, move, or delete a
fragment. In the canonical private repository, new release intent is an append-only
record under `cli-release/` and becomes immutable on first admission to canonical
`main`; follow that journal's README for notes, replacements, withdrawals, corrections,
and the bounded legacy migration. The exported CLI tree intentionally contains neither
new private intent nor a dependency on it.

Describe user behavior, not implementation. Never publish credentials, private URLs,
customer or incident details, or unsafe vulnerability details, and never use `internal`
to disguise a user-visible security correction.

## CLI output style

Follow [`docs/output-style.md`](docs/output-style.md) for every human-facing string
the CLI prints: help text, errors, warnings, progress, reports, and prompts. It
defines the voice registers, the error diagnostic-plus-remedy format, stream
discipline, report layout, the terminology glossary and banned vocabulary, and
help-text conventions. Machine surfaces (JSON documents, schemas, protocol
messages) follow their contracts instead.

## Generated source

Generated API clients and protocol codecs needed for a normal build must be committed.
Their generator version and source-contract digest must be recorded in the generated
files. A normal build must not require contract files or generators that are absent from
this repository.

## Architecture

Keep human API commands, runner machine behavior, and workflow execution separate
internally even while they share the `scherzo-cloud` executable. In particular, never
allow the runner to discover or read a human OAuth credential store. Workflow scheduling
and execution belong to the embedded execution component, not runner connectivity code.
Implement that execution component as new code within this repository. The prior
single-user Scherzo daemon may inform behavior, but it is not a dependency, subprocess,
protocol peer, source boundary, or compatibility target.

## Mirror workflow

This repository is a read-only mirror. Public Discussions are welcome, but pull
requests cannot be merged into the mirror directly. Do not add publication credentials
or mirror infrastructure here.
