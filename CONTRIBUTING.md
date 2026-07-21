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

## Security reports

Do not report vulnerabilities, credentials, or other sensitive details in a public
Discussion. Follow `SECURITY.md` instead.
