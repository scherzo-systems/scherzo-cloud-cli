# PiJsonV1 extension toolchain

This private npm project checks the engine-owned TypeScript extension that Scherzo will
materialize for a `PiJsonV1` result invocation. It is development tooling, not a
published package and not a workflow runtime. Rust remains responsible for invocation
identity, retained-schema validation, and terminal workflow state. The result
extension leaves provider tool arguments unchanged while Pi streams, validates,
executes, and persists them; it does not maintain a redacted or restorable copy.

The checked-in fixture is a complete materialization for the fixed inputs in
`fixtures/materialization-input.json`. The extension loaded by Pi is a standalone file;
it imports only Pi-provided extension types, Pi-provided `typebox`, and the `node:net`
built-in. A workflow invocation does not run npm, read this package's `node_modules`, or
resolve a package from a registry.

## Checks

From the exported CLI root, run the canonical package check:

```sh
./scripts/check-pi-json-v1-extension
```

The script copies the package into an invocation-local temporary tree, installs exactly
`package-lock.json` with lifecycle scripts disabled, then runs formatting, linting,
TypeScript checking against Pi 0.84.4, generated-fixture checking, and Node's built-in
test runner. Formatting installs its tools in a separate temporary tree, so concurrent
repository checks never share or mutate a source-tree `node_modules` directory.

The pinned Devenv environment supplies Node 24. After the locked install, the script
sets npm's offline mode for all package checks so lint, formatting, type-checking,
generation checking, and tests cannot resolve a missing package from a registry.
The source tree also type-checks the qualification-only fake provider against the
exact Pi 0.84.4 extension and provider APIs. That provider communicates only through
framed local Unix sockets and is copied into isolated test projects by the Rust
conformance suite; it is never available to workflow configuration.

For a direct clean install and package-only check:

```sh
cd src/execution/workflow/pi-json-v1-extension
npm ci --ignore-scripts --no-audit --no-fund
npm run check
```

Regenerate the representative fixture after an intentional template or fixed-input
change, then rerun the check:

```sh
npm run fixture:generate
npm run fixture:check
```
