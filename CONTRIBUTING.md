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

The project uses its standalone devenv environment to provide the minimum Rust toolchain
declared in `Cargo.toml` and the pinned Node 24 line used by the private PiJsonV1
extension project. `./scripts/check` enters that environment through the clean boundary
and runs the same formatting, linting, testing, source-boundary, and release build checks
used by CI. The canonical check performs a
locked npm install before checking the extension; run
`./scripts/check-pi-json-v1-extension` for that focused path.

Rust unit and integration tests run through cargo-nextest with the checked-in
`.config/nextest.toml` policy. Both `./scripts/check` and the production Nix package select the
locked workspace with all targets and all features; the local suite additionally rebuilds the
version tests with a fixed development version, while Nix injects the package version and build
identity into its complete run. The current workspace is binary-only and has no documentation-test
target; formatting, Clippy, structural checks, and the release build remain explicit parts of the
broader local suite rather than Nix package checks.

Nextest gives each selected test its own process, preventing a child forked by one test from
inheriting another test's open descriptors. This isolation does not prevent races among processes
created within one test, and it does not change production process behavior.

## Rust baseline upgrades

`package.rust-version` in `Cargo.toml` is the single Rust baseline declaration. Express
it as the selected stable `MAJOR.MINOR` release line. The development and MSRV
toolchains, production Nix package, and GitHub release builds derive their compiler from
that value; do not add a `rust-toolchain` file, a second version constant, or an older
compatibility build.

Advance the baseline directly when the project intentionally adopts a newer compiler or
dependency requirement. Before a bump, confirm that the release line is available from
the pinned Rust overlay and from rustup on every native release runner. Update
`Cargo.lock` only when the new baseline changes dependency resolution or when the same
change deliberately refreshes dependencies. Because the CLI is pre-release, replace the
old baseline rather than retaining parallel support.

Treat a higher source-build requirement as breaking release intent in the canonical
private journal. Validate a bump with `./scripts/check`, the monorepo dependency-state
check, and `nix flake check`; release CI builds every supported native target with the
candidate version.

## Release policy and archived notes

`release.toml` schema 2 is static policy, not mutable next-version intent. Keep its
initial version at `0.1.0`, development version at `0.0.0-dev`, and pre-1.0 breaking
impact at `minor`. Keep the Cargo fallback in `Cargo.toml` and `Cargo.lock` permanently
at `0.0.0-dev`. `./scripts/check-release` validates those values, and
`./scripts/release-impact` derives a version only when given explicit impact and an
optional latest stable version.

`changes/` is a frozen legacy archive. Do not add, modify, move, or delete fragments.
New release intent is admitted through the canonical private repository's append-only
journal and is intentionally absent from this exported source tree. Public notes still
describe present-tense user behavior and must not include credentials, private URLs,
customer or incident details, premature vulnerability details, or an `internal` marker
that conceals a user-visible security fix. Run `./scripts/check-change-fragments` to
validate the complete frozen archive offline.

Managed Buildkite advances public `main` to an untagged candidate after validating the
canonical source-evidence artifact. Public Actions verifies the candidate contract with
`scripts/verify-release-candidate`, checks Linux and macOS, and builds all three native
archives before the protected `cli-release` environment asks a human to approve the
proposed version and notes. The write-scoped `scripts/reconcile-release` publishes only a
candidate that is still public `main`, accepts matching absent, direct-tag, or draft state,
and leaves an exact published release unchanged. Run
`python3 scripts/test-release-candidate` for the local Git and mocked GitHub fixture. A
pre-tag failure is repaired by advancing `main` with a corrected candidate; do not reset
the branch, move a stable tag, edit a published release, or restore allocation and recovery
state.

## Security reports

Do not report vulnerabilities, credentials, or other sensitive details in a public
Discussion. Follow `SECURITY.md` instead.
