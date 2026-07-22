# Scherzo Cloud CLI

> [!IMPORTANT]
> This repository is a read-only mirror. Public Discussions are welcome, but pull
> requests cannot be merged into the mirror directly.

This repository contains the open-source source for the early `scherzo-cloud`
executable.

## Current capabilities

The current release supports help, version inspection, OAuth Device Authorization,
server-confirmed human authentication status, explicit human-principal signup, local
human-credential logout, and a narrow local runner prerequisite diagnostic. The
`runner serve` command remains an explicit stub and exits with an error.

Apart from creating the signed-in human's account, the CLI cannot currently create cloud
resources, configure a repository, submit workflows, or serve runner assignments. The
rest of Cloud onboarding is not implemented yet.

## Version inspection

Use `scherzo-cloud --version` or `scherzo-cloud version` for conventional one-line
output. Use `scherzo-cloud version --json` for the schema-version-1 structured contract:

```json
{
  "schemaVersion": 1,
  "command": "scherzo-cloud",
  "version": "0.2.0",
  "executablePath": "/resolved/path/to/scherzo-cloud",
  "buildIdentity": "unknown"
}
```

Packaged builds replace the local `unknown` build identity with their source revision.
The schema does not define a release channel.

## Human authentication

Use `scherzo-cloud auth login` to authenticate through a browser on the same machine or
another machine. The CLI prints an activation URL and user code, never opens a browser,
and never listens for an inbound callback. Add `--json` to receive newline-delimited
schema-version-1 events. Use `--force` to start a new device authorization transaction
without checking an existing credential with the API.

Use `scherzo-cloud auth status` to ask the selected deployment whether the current
identity is authenticated, requires signup, is unauthenticated, or is unreachable. Add
`--json` for the schema-version-1 structured result. Status always contacts the public
API, including when no local credential exists.

Use `scherzo-cloud auth logout` to remove the human credential for the active deployment
without making a network request. Normal operation stores short-lived human access
tokens in `~/.scherzo-cloud/credentials.json`; this store is separate from all future
runner credentials.

Development deployments that use HTTP require `--allow-insecure-http` on the networked
leaf command: `auth login`, `auth status`, or `account signup`. The option is not global
and does not apply to local commands such as `auth logout`.

## Account signup

OAuth login does not implicitly create a Scherzo Cloud account. When authentication
status is `signup_required` and the deployment advertises signup, use
`scherzo-cloud account signup` after the customer explicitly approves account creation.
Add `--json` for a schema-version-1 structured result. The CLI authenticates the request
with the existing human credential and retries an ambiguous transport failure once with
the same opaque idempotency key.

## Runner doctor

Use `scherzo-cloud runner doctor` to inspect the local prerequisites currently known to
the runner. Today the default set contains only `environment.command.git`. It executes
the `git` resolved from the runner process's `PATH`, requires a parseable version at
least `0.0.1`, and reports a pass or failure for that check. A successful result does
not mean the runner is ready to serve assignments: runner configuration, machine
identity, connectivity, and execution requirements are not implemented or checked yet.

```sh
# Run the default checks.
scherzo-cloud runner doctor

# Run a named check. Repeat --check to select more than one registered check.
scherzo-cloud runner doctor --check environment.command.git

# List IDs without running any checks.
scherzo-cloud runner doctor --list-checks

# Emit the schema-version-1 JSON report.
scherzo-cloud runner doctor --json
```

Checks are registered statically by components compiled into this executable. The
command does not load plugins, read human credentials, contact Scherzo Cloud, or change
runner configuration. It executes `git --version` with a five-second deadline, bounds
captured standard output, drains standard error without reporting it, and exposes only
a normalized numeric version in its report. The JSON report has no `ready` field.

## Release series

`release.toml` declares the manually selected `MAJOR.MINOR` release series. The current
series is `0.2`. Automatic release planning derives patches from immutable public tags,
so the first release in this series is `0.2.0` and later compatible releases increment
the patch.

Run `./scripts/check-release` to validate the declaration and its Cargo fallback. To
preview version planning from an explicit latest tag, run:

```sh
./scripts/check-release --next-version v0.2.7
```

This prints `0.2.8`.

After every releaseable mirror update passes the public check, GitHub Actions builds and
runs native archives for x86-64 and ARM64 Linux and for Intel and Apple Silicon macOS.
It then publishes the archives, `SHA256SUMS`, and GitHub build-provenance attestations on
the [Releases](https://github.com/scherzo-systems/scherzo-cloud-cli/releases) page.
Markdown, test, workflow, and development-environment-only changes do not increment the
patch after the initial release.

Release binaries are not currently signed or notarized. Verify a downloaded archive
with the attached checksums and GitHub attestation before running it:

```sh
archive='scherzo-cloud-<version>-<target>.tar.gz'

# Linux
sha256sum --ignore-missing --check SHA256SUMS

# macOS
shasum -a 256 --ignore-missing --check SHA256SUMS

gh attestation verify "$archive" \
  --repo scherzo-systems/scherzo-cloud-cli
```

## Source boundary

Everything in this repository builds and tests using only its checked-in source and
declared external dependencies. The canonical check verifies that the public source is
self-contained.

## Development

The repository contains a standalone devenv environment with the pinned Rust toolchain.
Enter it and run the canonical check from the repository root:

```sh
devenv shell
./scripts/check
```

For the same entrypoint used by CI, run:

```sh
devenv test
```

The check verifies public-source isolation, formatting, Clippy, unit and integration
tests, and a release build.

## License

Scherzo Cloud CLI is licensed under the Apache License 2.0. See `LICENSE`.
