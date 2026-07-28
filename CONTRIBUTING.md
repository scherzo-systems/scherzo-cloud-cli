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
declared in `Cargo.toml`. Run `devenv test` for the same formatting, linting, testing,
source-boundary, and release build checks used by CI.

## Release intent

`release.toml` is the visible source of truth for the CLI's `MAJOR.MINOR` release series.
Compatible releaseable work remains in the configured series and receives automatic
patch versions after public checks pass. Before `1.0`, a breaking command or output
change must advance the minor series by exactly one. A major bump must advance by exactly
one and reset minor to zero.

When changing the series, update the package fallback in `Cargo.toml` and `Cargo.lock` to
`MAJOR.MINOR.0` in the same change. `./scripts/check-release` rejects inconsistent,
regressing, skipped, or malformed release intent. `./scripts/classify-release-path` is
the sole releaseable-path policy, while `./scripts/plan-release` inspects synthetic
public Git history and remains the sole source of tag discovery and next-version
planning for workflow automation.

### Change fragments

Record release intent under `changes/` (`cli/changes/` in the canonical monorepo).
Choose the category by primary user impact: `added` for a new capability, `changed` for
an observable compatible change, `fixed` for a corrected user-visible symptom, and
`breaking` when users or integrations must adapt. A breaking note includes the migration
in the same sentence. Use `internal` only when the change has no user-visible effect; its
file must contain exactly `No user-visible changes.` and one newline.

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
