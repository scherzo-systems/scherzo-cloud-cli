# Architecture

## Current state

This repository defines the public source boundary for the Rust `scherzo-cloud`
executable. The current binary is a command-surface stub: it provides help and version
output and reserves `scherzo-cloud runner serve`, but it does not yet call the Cloud API
or run assignments.

## One executable with separate roles

`scherzo-cloud` will initially provide one installation and command tree. A thin command
entrypoint will dispatch to components with distinct responsibilities:

- human-facing commands will perform short-lived public API operations;
- the public API client will encode requests and decode responses from versioned
  generated assets;
- the runner service will maintain an outbound connection, advertise capacity, accept
  assignments, and report observations;
- the runner protocol component will encode, order, acknowledge, and validate runner
  messages; and
- the Scherzo execution adapter will translate an assigned cloud run into Scherzo
  Core's one-run execution contract.

The long-running runner starts only through an explicit command such as
`scherzo-cloud runner serve`. Bare `scherzo-cloud runner` will not implicitly start a
service.

These are component boundaries before they are separate packages or executables. A
second runner binary should be introduced only if platform dependencies, privilege
isolation, artifact size, or independent release cadence creates a demonstrated need.

## Credential separation

Human commands and the runner use different security identities.

Human commands will use an interactive OAuth credential store. Human login will use
OAuth Device Authorization so the browser may run on a different machine from the CLI;
the CLI will not require an inbound connection or loopback callback. It will display the
short-lived activation URL and user code, keep the private device code and OAuth tokens
out of command output and logs, and poll the authorization server until the transaction
finishes.

After OAuth login, the CLI will ask the public API whether the identity is linked to a
principal. Login alone will never create one. An onboarding agent may invoke the separate
signup command only after reporting that signup is required and obtaining explicit human
approval.

The runner will use a machine identity issued for that runner installation or managed
runtime. Runner startup must use explicit machine configuration and must never discover
or read the human token store. Human commands likewise must not use runner credentials
to call the public API.

Sharing an executable does not permit sharing credential files, environment variables,
refresh logic, or authorization scopes accidentally.

## Execution boundary

The runner coordinates cloud assignments but does not own workflow scheduling, step
retries, workspaces, checkpoints, or agent execution. Those responsibilities belong to
a separate Scherzo execution component embedded in the Rust executable. The runner will
invoke a versioned one-run boundary and translate its structured events and outcome into
the cloud runner protocol.

The execution component will be organized as an internal source boundary before there
is evidence that a separately published crate or process is necessary. The existing
Gleam Scherzo implementation is a behavioral reference, not a module layout to copy.

## Public source isolation

The complete normal development loop must operate from this repository root without
access to a parent checkout. Formatting, linting, tests, dependency inspection, code
generation checks, and builds may use only files committed here and declared external
dependencies.

The source tree may not contain symbolic links, parent-relative path dependencies,
workspace inheritance from outside this repository, or imports of implementation
packages that are not declared public dependencies. `scripts/check` is the canonical
local and CI entrypoint for this invariant.

## Generated contracts

Versioned OpenAPI and runner protocol contracts define the interface with the Scherzo
Cloud control plane. Generated clients, types, and codecs needed to build this
executable will be committed here.

A normal public build will consume the committed generated assets and will not require
the contract source files or generator. Generated files must identify their generator
version and source-contract digest and must be checked for drift before they are
mirrored.

## Rust source shape

The implementation begins as one Cargo package and one executable. Internal Rust modules
will separate human CLI commands, runner connectivity and assignment ownership, protocol
DTOs, and one-run execution. Additional workspace crates are not introduced until a
real compile-time dependency boundary requires them.

The CLI uses a typed `clap` command tree. Each command module owns its arguments, help
metadata, and execution dispatch; parent modules compose those commands so parsing and
rendered help come from the same structure. Bare command groups may print their composed
help, but only an explicit leaf command may start long-running behavior.

`release.toml` is the public release-intent contract. It selects only the manually
managed `MAJOR.MINOR` series; immutable public release tags will provide patch history.
The Cargo package version remains the matching `MAJOR.MINOR.0` fallback so source builds
are coherent without pretending to know an automatically assigned patch.

Local builds report the package version from `Cargo.toml`. Reproducible release builds
inject `SCHERZO_CLOUD_VERSION` and `SCHERZO_CLOUD_BUILD_IDENTITY` at compile time, and
both `scherzo-cloud version` and `scherzo-cloud --version` read the same version.
Structured version output also reports the resolved executable path and separately
injected build identity. Packaging must verify the installed executable reports these
exact values. `scripts/check-release` validates release-series syntax, Cargo fallback
consistency, and candidate transitions before packaging. The version schema does not
infer or advertise a release channel.

The runner and execution components should use owned state and explicit message passing
rather than shared mutable global state. Protocol DTOs must be translated into domain
types at their boundary instead of becoming the workflow model.

## Deferred decisions

The following decisions remain open because the runner implementation is the main
technical unknown:

- runner transport and reconnect behavior;
- supported operating systems and service managers;
- installation, update, and release packaging; and
- whether the runner eventually warrants a dedicated executable.

Selecting any of these must preserve the public source and credential boundaries above.
