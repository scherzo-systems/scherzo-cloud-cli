# Scherzo Cloud CLI

> [!IMPORTANT]
> This repository is a read-only mirror. Public Discussions are welcome, but pull
> requests cannot be merged into the mirror directly.

This repository contains the open-source source for the early `scherzo-cloud`
executable.

## Current capabilities

The current release supports help, version inspection, OAuth Device Authorization,
server-confirmed human authentication status, and local human-credential logout. The
`runner serve` command remains an explicit stub and exits with an error.

The CLI cannot currently create cloud resources, configure a repository, submit
workflows, or serve runner assignments. Explicit principal signup and the rest of Cloud
onboarding are not implemented yet.

## Version inspection

Use `scherzo-cloud --version` or `scherzo-cloud version` for conventional one-line
output. Use `scherzo-cloud version --json` for the schema-version-1 structured contract:

```json
{
  "schemaVersion": 1,
  "command": "scherzo-cloud",
  "version": "0.1.0",
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

## Release series

`release.toml` declares the manually selected `MAJOR.MINOR` release series. The current
series is `0.1`. Automatic release planning derives patches from immutable public tags,
so the first release in this series is `0.1.0` and later compatible releases increment
the patch.

Run `./scripts/check-release` to validate the declaration and its Cargo fallback. To
preview version planning from an explicit latest tag, run:

```sh
./scripts/check-release --next-version v0.1.7
```

This prints `0.1.8`.

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
