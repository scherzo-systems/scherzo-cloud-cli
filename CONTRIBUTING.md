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
extension project. Run `./scripts/strict-devenv test` for the same lock-strict
formatting, linting, testing, source-boundary, and release build checks used by CI. The
canonical check performs a
locked npm install before checking the extension; run
`./scripts/check-pi-json-v1-extension` for that focused path.

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

Treat a higher source-build requirement as a breaking change fragment. Validate a bump
with `./scripts/check`, the monorepo dependency-state check, and `nix flake check`; CI
then builds every supported native release target with the declared release line.

## Release intent

`release.toml` is the visible source of truth for the CLI's `MAJOR.MINOR` release series.
Choose its value together with the fragment category: `internal`, `fixed`, and compatible
`changed` work stays in the stable series for a patch; `added` selects the adjacent minor
series; and `breaking` selects the adjacent minor before `1.0` or the adjacent major,
with minor reset to zero, from `1.0` onward. The highest impact across every fragment
since the latest stable tag controls one cumulative plan, so a second addition behind an
unreleased minor does not advance the series again.

When changing the series, update the package fallback in `Cargo.toml` and `Cargo.lock` to
`MAJOR.MINOR.0` in the same change. `./scripts/check-release` validates the declaration
and fallback, and `./scripts/release-impact` rejects missing, regressing, skipped, or
impact-inconsistent intent. `./scripts/classify-release-path` is the sole releaseable-path
policy, while `./scripts/plan-release` inspects synthetic public Git history and remains
the source of exact tag discovery and deterministic semantic planning for workflow
automation.

### Change fragments

Record release intent under `changes/` (`cli/changes/` in the canonical monorepo).
Choose the category by primary user impact: `added` for a new capability and an adjacent
minor series, `changed` for an observable compatible patch, `fixed` for a corrected
user-visible patch, and `breaking` when users or integrations must adapt. A breaking note
includes the migration in the same sentence and selects an adjacent minor before `1.0`
or an adjacent major afterward. Use `internal` only when the change has no user-visible
effect; it selects a patch and its file must contain exactly
`No user-visible changes.` and one newline. Make the fragment, `release.toml`, Cargo
fallback, and lockfile update one coherent candidate change.

Generate the filename from the canonical monorepo root with the exact command in
[`changes/README.md`](changes/README.md):

```bash
set -euo pipefail
category=fixed
fragment_id=$(LC_ALL=C od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]')
[[ $fragment_id =~ ^[0-9a-f]{32}$ ]]
mkdir -p "cli/changes/$category"
printf '%s\n' 'Fix run monitoring occasionally stopping before completion.' \
  >"cli/changes/$category/$fragment_id.md"
```

Public notes describe present-tense user behavior rather than implementation details.
Do not include credentials, private URLs, customer or incident details, or premature
vulnerability details. Do not use `internal` to conceal a user-visible security fix.
Run `./scripts/check-change-fragments` in the standalone checkout to validate the full
byte grammar and tree.

Human review may require editing, replacing, or removing a fragment before release.
Once a stable public tag contains that fragment, it is immutable; correct released text
with a new fragment instead.

## Security reports

Do not report vulnerabilities, credentials, or other sensitive details in a public
Discussion. Follow `SECURITY.md` instead.
