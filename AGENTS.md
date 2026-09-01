# Development guidance

## Public source boundary

Everything in this repository is public. Do not add credentials, private URLs, internal
incident details, customer data, or other material that cannot be published.

The repository must remain self-contained. Source, scripts, tests, and build files must
not import or read a parent directory, depend on a sibling checkout, inherit a parent
workspace, or use a local path override outside this repository. Do not add symbolic
links.

## Canonical check

Run the complete local validation from the repository root with:

```sh
./scripts/check
```

Keep this command aligned with public CI. Do not place substantive test logic only in a
publication workflow or mirror script.

The implementation language is Rust. Keep `scripts/check` as the complete isolated
validation entrypoint and enter `scripts/strict-devenv` before its test logic runs. It
must continue to run deterministic formatting checks, Clippy, unit and integration tests,
dependency and import boundary validation, and a complete release build.

## Rust CLI rules

These rules apply the repository [pre-release compatibility policy](../AGENTS.md) to
Rust; tracked legacy violations are cleanup work, not precedent for new code.

- **Denied-lint gate ([LIV-2139]).** Treat an `#[allow]` or `#[expect]` on a denied lint
  as a design defect to remove, not justify; repeated boilerplate reasons are not
  accepted. Only a documented production clock-adapter or unsafe-FFI boundary may carry
  a suppression, with a site-specific reason and named reviewer approval in the change
  review.
- **Test-seam gate ([LIV-2140], [LIV-2141], [LIV-2142]).** Do not put a `#[cfg(test)]`
  field, parameter, constant value, or alternate body in a production type or function.
  Inject a trait or closure that exists in both test and production builds.
- **Async-executor gate ([LIV-2099], [LIV-2100], [LIV-2102], [LIV-2104]).** The runner
  executes workflows on a current-thread Tokio runtime, while the human CLI workflow
  runtime has two worker threads. Any `fsync`, directory removal, poll-sleep loop, or
  synchronous socket write reachable from an `async fn` must run in `spawn_blocking` or
  behind an event-driven boundary.
- **Panic-policy gate ([LIV-2138]).** `unreachable!`, `assert!`, `todo!`, and
  unwrap/expect equivalents all count as panics under the crate panic policy and must not
  appear in production paths. Narrow the accepted type or state, or return a typed error.
- **Boundary-error gate ([LIV-2119], [LIV-2123]).** Errors at OS and protocol boundaries
  must carry the underlying error and the failed stage. `Result<_, ()>`, unit-struct
  errors, and single-variant reason enums are not accepted.
- **Test-value gate ([LIV-2125], [LIV-2135]).** A test that returns early when a fixture
  is missing, asserts only `.is_err()`, or compares prose does not prove the claimed code
  ran. Mark environment-dependent suites `#[ignore]` and run them with
  `--include-ignored` where their dependency exists.
- **Pre-release design gate ([LIV-2124]).** Do not add `_vN` module names, single-variant
  profile enums, unread version fields, or callback-shaped return values without a
  current second case. Remove unused shape instead of reserving it for hypothetical
  compatibility.

[LIV-2099]: https://linear.app/living-systems/issue/LIV-2099/move-the-child-guard-launch-handshake-and-durable-guard-writes-off-the
[LIV-2100]: https://linear.app/living-systems/issue/LIV-2100/take-blocking-workspace-preparation-and-cleanup-off-the-runner
[LIV-2102]: https://linear.app/living-systems/issue/LIV-2102/move-presentation-output-and-tui-drawing-off-the-workflow-executor
[LIV-2104]: https://linear.app/living-systems/issue/LIV-2104/run-adapter-launch-preparation-off-the-tokio-runtime
[LIV-2119]: https://linear.app/living-systems/issue/LIV-2119/carry-cause-context-in-local-run-directory-errors-and-split-attempt
[LIV-2123]: https://linear.app/living-systems/issue/LIV-2123/give-adapter-launch-and-bridge-failures-typed-causes-and-a-tracing
[LIV-2124]: https://linear.app/living-systems/issue/LIV-2124/simplify-the-agent-invocation-and-dispatch-abstractions-to-the-closed
[LIV-2125]: https://linear.app/living-systems/issue/LIV-2125/make-harness-conformance-suites-fail-when-the-pinned-binary-is-absent
[LIV-2135]: https://linear.app/living-systems/issue/LIV-2135/remove-prose-assertions-tooling-tests-and-soak-loops-from-the-cli-test
[LIV-2138]: https://linear.app/living-systems/issue/LIV-2138/close-the-panic-lint-gap-for-unreachable-and-assert-in-production
[LIV-2139]: https://linear.app/living-systems/issue/LIV-2139/remove-the-module-wide-dead-code-allow-on-execution-and-delete-what-it
[LIV-2140]: https://linear.app/living-systems/issue/LIV-2140/replace-cfgtest-forks-in-the-runner-service-with-injected-seams
[LIV-2141]: https://linear.app/living-systems/issue/LIV-2141/replace-cfgtest-forks-in-the-workflow-engine-with-injected-seams
[LIV-2142]: https://linear.app/living-systems/issue/LIV-2142/replace-cfgtest-forks-in-artifact-staging-publication-and-the-tui-host

## Release policy

Keep `release.toml` at schema 2 with only the static initial version, development
version, and pre-1.0 breaking-impact policy. Keep the Cargo package fallback permanently
at `0.0.0-dev`; Nix development builds and allocated release builds inject their
revision-bearing or exact authoritative versions. Source validation must not fetch,
synchronize, or inspect public tags. Run the release fixture suites when changing this
policy or its pure impact arithmetic.

Public release execution must consume only the exact managed allocation and, for
recovery, its current allowlisted recovery chain. Never derive a version from public tags
or the frozen archive. Preserve read-only permissions for checks, verification, and native
builds; grant write only to the final reconcile job; pin every action by commit; and keep
pull requests, ordinary checks, allocation or recovery refs, and recovery-mirror pushes
incapable of publication. Exact manual dispatch is recovery-only and must build the
original allocated mirror rather than current repaired source.

## Release-note archive

`changes/` is a frozen archive of legacy notes. Do not add, modify, move, or delete a
fragment. In the canonical private repository, new release intent is an append-only
record under `cli-release/` and becomes immutable on first admission to canonical
`main`; follow that journal's README for notes, replacements, withdrawals, corrections,
and the bounded legacy migration. The exported CLI tree intentionally contains neither
new private intent nor a dependency on it.

Describe user behavior, not implementation. Never publish credentials, private URLs,
customer or incident details, or unsafe vulnerability details, and never use `internal`
to disguise a user-visible security correction.

## CLI output style

Follow [`docs/output-style.md`](docs/output-style.md) for every human-facing string
the CLI prints: help text, errors, warnings, progress, reports, and prompts. It
defines the voice registers, the error diagnostic-plus-remedy format, stream
discipline, report layout, the terminology glossary and banned vocabulary, and
help-text conventions. Machine surfaces (JSON documents, schemas, protocol
messages) follow their contracts instead.

## Generated source

Generated API clients and protocol codecs needed for a normal build must be committed.
Their generator version and source-contract digest must be recorded in the generated
files. A normal build must not require contract files or generators that are absent from
this repository.

## Architecture

Keep human API commands, runner machine behavior, and workflow execution separate
internally even while they share the `scherzo-cloud` executable. In particular, never
allow the runner to discover or read a human OAuth credential store. Workflow scheduling
and execution belong to the embedded execution component, not runner connectivity code.
Implement that execution component as new code within this repository. The prior
single-user Scherzo daemon may inform behavior, but it is not a dependency, subprocess,
protocol peer, source boundary, or compatibility target.

## Mirror workflow

This repository is a read-only mirror. Public Discussions are welcome, but pull
requests cannot be merged into the mirror directly. Do not add publication credentials
or mirror infrastructure here.
