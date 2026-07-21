# Scherzo Cloud CLI

> [!IMPORTANT]
> This repository is a read-only mirror. Public Discussions are welcome, but pull
> requests cannot be merged into the mirror directly.

This repository contains the open-source source for the early `scherzo-cloud`
executable.

## Current capabilities

The current release is a command-surface preview, not a functional Scherzo Cloud
client. It supports help and version inspection. The `auth login`, `auth status`,
`auth logout`, and `runner serve` commands are present only as explicit stubs and exit
with an error.

The CLI cannot currently authenticate a customer, create or inspect cloud resources,
configure a repository, submit workflows, or serve runner assignments. Current releases
are useful for verifying distribution and the structured version contract; Cloud
onboarding is not implemented yet.

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
