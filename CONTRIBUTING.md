# Contributing

Thank you for your interest in Scherzo Cloud CLI.

## Read-only mirror

This repository is a read-only mirror. Its public Git history contains mirror commits,
so maintainers cannot merge pull requests directly into this repository.

Public Discussions are welcome for bug reports, design feedback, documentation
problems, and feature requests. Maintainers may incorporate a proposal and publish it
through the normal mirror process, but starting a Discussion does not guarantee that a
patch will be adopted.

## Development checks

If you inspect or modify a local copy, run the canonical check from the repository root:

```sh
./scripts/check
```

The project uses its standalone devenv environment to provide the pinned Rust toolchain.
Run `devenv test` for the same formatting, linting, testing, source-boundary, and release
build checks used by CI.

## Release intent

`release.toml` is the visible source of truth for the CLI's `MAJOR.MINOR` release series.
Compatible work remains in the configured series and will eventually receive automatic
patch versions. Before `1.0`, a breaking command or output change must advance the minor
series by exactly one. A major bump must advance by exactly one and reset minor to zero.

When changing the series, update the package fallback in `Cargo.toml` and `Cargo.lock` to
`MAJOR.MINOR.0` in the same change. `./scripts/check-release` rejects inconsistent,
regressing, skipped, or malformed release intent.

## Security reports

Do not report vulnerabilities, credentials, or other sensitive details in a public
Discussion. Follow `SECURITY.md` instead.
