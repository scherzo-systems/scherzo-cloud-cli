# Scherzo Cloud CLI

> [!IMPORTANT]
> This repository is a read-only mirror. Public Discussions are welcome, but pull
> requests cannot be merged into the mirror directly.

This repository contains the open-source source for `scherzo-cloud`, the command-line
interface to Scherzo Cloud.

## Status

The initial Rust executable is a command-surface stub. It supports help and version
output, and reserves `scherzo-cloud runner serve` for the future long-running runner.
The runner, authentication, and Cloud API commands are not implemented yet.

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

## Intended responsibilities

The `scherzo-cloud` executable is expected to support two kinds of work:

- short-lived commands that call the public Scherzo Cloud API; and
- the long-running customer-hosted or local runner started explicitly with
  `scherzo-cloud runner serve`.

The runner will connect outbound to Scherzo Cloud and invoke the embedded Scherzo
execution component. Runner connectivity and assignment handling will remain separate
from workflow scheduling, step execution, and recovery policy.

## Source boundary

Everything in this repository must build and test using only its checked-in source and
declared external dependencies. Generated API clients and protocol codecs will be
committed here when they are introduced, so normal builds will not require their
contract inputs or generators.

The CLI and runner will share one executable initially, but their command handling,
credentials, and runtime responsibilities will remain separate internally.

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
