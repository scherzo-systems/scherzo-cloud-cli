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
validation entrypoint. It must continue to run deterministic formatting checks, Clippy,
unit and integration tests, dependency and import boundary validation, and a complete
release build.

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

## Mirror workflow

This repository is a read-only mirror. Public Discussions are welcome, but pull
requests cannot be merged into the mirror directly. Do not add publication credentials
or mirror infrastructure here.
