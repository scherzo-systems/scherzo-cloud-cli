# Architecture

## Current state

This repository defines the public source boundary for the Rust `scherzo-cloud`
executable. The current binary provides help, version output, deployment selection, a
secure local human credential store, caller-managed service API-key input, OAuth Device
Authorization, server-confirmed authentication status, explicit human-principal signup,
service-principal and credential lifecycle management, revoking logout, organization
profile and membership management, one-page active member-directory and owner-only membership-history reads, inputless Cloud run creation
and current projection reads, local Workflow V1 definition validation, and an outbound,
enrolled runner transport.
`scherzo-cloud runner serve` opens a versioned WebSocket connection, durably
acknowledges received assignment effects, and uses the shared workflow resolver,
admission boundary, and execution engine for one configured inputless command, Pi,
Claude Code, Codex, or mixed workflow. Semantic acceptance reserves the runner's single
local assignment slot; a matching later start effect authorizes execution.
`scherzo-cloud runner doctor` performs one
default local Git check and can explicitly check the `pi`, `claude`, and `codex`
installations selected independently from inherited `PATH`; passing those checks
establishes only the selected closed harness installation, not complete runner readiness.

## One executable with separate roles

`scherzo-cloud` will initially provide one installation and command tree. A thin command
entrypoint will dispatch to components with distinct responsibilities:

- human-facing commands will perform short-lived public API operations;
- the public API client will encode requests and decode responses from versioned
  generated assets;
- the runner service will maintain an outbound connection, enforce its single-assignment
  limit, accept assignments, and report observations;
- the runner protocol component will encode, order, acknowledge, and validate runner
  messages; and
- the new Scherzo Cloud execution engine will implement the one-run contract,
  workflow scheduler, execution roots, checkpoints, and agent execution in Rust.

The long-running runner starts only through an explicit command such as
`scherzo-cloud runner serve`. Bare `scherzo-cloud runner` will not implicitly start a
service.

## Local workflow validation

`src/cli/workflow/validate.rs` is an offline typed Clap adapter around the shared
resolver in `crates/execution/src/workflow/resolution.rs`. The execution component embeds the
public `schemas/workflow-v1.schema.json` artifact for structural validation; no
implementation-local schema copy exists. `src/cli/workflow/schema.rs` writes that same
embedded asset unchanged to standard output. `src/cli/workflow/reference.rs` likewise
writes the reviewed `docs/workflow-v1.md` authoring asset unchanged. The private mirror
workflow verifies that checked-in asset against its canonical public-documentation
source before export, while the exported build remains self-contained. The validation
adapter requires both an explicit source root and a selected workflow path, then renders only normalized
provenance, digest, step-count, required-import, and closed diagnostic fields. It does
not parse or validate workflow definitions independently.

Validation stops at definition resolution. The adapter does not construct run
admission or runtime state, execute command or agent steps, inspect harness
availability, load either credential type, make a network request, or enter Runner
Serve connectivity. Bare `scherzo-cloud workflow` prints composed help rather than
selecting a workflow or inferring a source boundary.

The pure lexical and JSON decoding rules shared by Workflow V1 inputs and Cloud Run
Input manifests live in the private `scherzo-cloud-support` package beneath
`crates/support/`. The public API, command, runner, and execution components may depend
on its explicit facade for duplicate-aware strict JSON decoding, input identifiers,
diagnostic display names, media types, and hexadecimal digests; the leaf owns no
transport, credentials, acquisition bytes, or execution state. The same package owns
the shared clock, TLS-provider, public-ID, and opaque idempotency-key leaves and has no
internal package edge.

## Runner diagnostics

`src/cli/runner/doctor.rs` is a typed Clap adapter: it parses runner-doctor arguments
and renders human or JSON output. Its machine behavior lives separately in
`crates/runner/src/doctor/`. That runner-package module owns the pass/fail report model,
ordered registry, selection rules, bounded process probe, and built-in Git check. The
unpublished runner facade exposes only the report and registry inventory consumed by the
command adapter; it is not a dynamic plugin API, does not discover libraries or scripts,
and is not a third-party extension contract.

The first registry entry is `environment.command.git` and is the sole default. The
operator-selected entries `execution.harness.pi-json-v1`,
`execution.harness.claude-code-stream-json-v1`, and
`execution.harness.codex-app-server-v1` resolve `pi`, `claude`, and `codex` independently
from the doctor's inherited `PATH`. Doctor does not construct human deployment state,
read the human credential store, or make a network request. Future runner bootstrap code
can add compiled-in checks through the same registry without adding a central check-name
enum, but it must keep those boundaries intact.

The three harness validators live at the execution boundary and are shared by doctor,
local Workflow Run, and agent-capable Runner Serve initialization. Each selects the first executable
with its fixed name in inherited `PATH` and never accepts an executable path from a
workflow, assignment, import, remote value, dedicated environment variable, or CLI
option. Validation canonicalizes the selected path and invokes only that absolute
executable's native version and help probes. The isolated probes retain inherited `PATH`
only so an environment-based launcher can resolve its interpreter; they never use it to
select another harness candidate.

Pi maps canonical stable versions in `>=0.84.2 <0.88.0` into
`ValidatedPiInstallation`; the repository separately qualifies exact release `0.87.1`.
Claude Code maps canonical stable versions in
`>=2.1.234 <2.2.0` into `ValidatedClaudeCodeInstallation`; the repository separately
qualifies exact release `2.1.263`. Codex maps stable `>=0.147.0 <0.155.0`
installations with the maintained App Server schema capabilities into
`ValidatedCodexInstallation`; the repository separately qualifies exact release
`0.154.0`. Each immutable value carries the absolute path, exact
observed version, closed profile, and closed capability set. Local and runner admission
inspect resolved workflows and require only each selected installation. Admission and
later execution use those values without another `PATH` lookup or native probe, so later
`PATH` changes cannot switch an active operation's executable. Claude execution also
requires every native initialization frame to equal the retained observed version rather
than a compile-time qualification release. Command-only work requires no harness, and
each single-harness workflow requires no unrelated installation. Runner Serve retains
independent optional Pi, Claude Code, and Codex snapshots for its process lifetime and
exposes none through the runner protocol.

## Runner service observability

Long-running runner machine behavior owns a recorder beneath `crates/runner/src/`. One recorder
projects each completed unit of work to a newline-delimited JSON object on standard
error and to an OpenTelemetry span through a process-local SDK provider. JSON records
enter a bounded queue without waiting; a dedicated local thread owns standard error,
preserves record framing after partial writes, and counts records dropped by queue
saturation or output failure. Recorder initialization is scoped to `runner serve`;
interactive and offline commands retain their existing stdout and stderr contracts.

The provider has no network exporter by default. A runner-owned OTLP/HTTP protobuf span
processor is added only when `runner serve` finds an explicit, valid standard
OpenTelemetry endpoint after runner configuration and credential validation.
`OTEL_SDK_DISABLED=true` is a hard remote-export veto; malformed privacy or exporter
configuration disables export without disabling local JSON or propagation. The endpoint
and any standard OTLP header credentials are user-owned. Transport policy accepts remote
HTTPS and exact loopback HTTP, performs no application retry, and bounds its non-blocking
queue, request, batching, and shutdown work. Export failures report only bounded closed
diagnostics and cannot feed back into runner state.

The SDK resource is built from an empty resource and adds only the fixed runner service
name, root-resolved version injected at Runner Serve startup, and generated boot ID, so
default and environment resource
detectors cannot expand the reviewed contract. Connection spans inject only W3C Trace
Context into their WebSocket upgrade. Gateway sessions can use the runner connection as
a remote parent; baggage is excluded, and effect acknowledgement spans remain
independent roots correlated by their existing domain IDs.

`runner.gateway_connection` bounds one outbound connection attempt, including live
handshake progress, attempt-local frame and effect counts, a closed connection cause,
and retry backoff selected by the service. `runner.effect_acknowledgement` bounds one
offer from receipt through gateway confirmation of the transport acknowledgement. It
includes safe effect, assignment, run, runner, boot, sequence, and lease context, but it
does not represent assignment acceptance or execution. The name `runner.run` remains
reserved until the execution component exists.

Telemetry call sites accept only reviewed scalar attributes and closed classifications.
They do not copy credentials, complete endpoints, protocol frames, peer close reasons,
or arbitrary errors into either projection. Local or export queue saturation, JSON write
failures, malformed export configuration, receiver failures, and export shutdown timeout
do not change connection, acknowledgement, retry, terminal result, or shutdown behavior.

API, runner protocol, support, execution, and runner are unpublished workspace packages;
human authentication and command composition remain root modules. Runner depends only
on support, runner protocol, and execution in production, and on test-support for tests;
it has no API, human-authentication, command, or root-package edge. A second runner binary
should be introduced only if platform dependencies, privilege isolation, artifact size,
or independent release cadence creates a demonstrated need.

## Credential separation

Human sessions, service-principal credentials, and runners use distinct security
identities and storage rules.

Human commands use the credential implementation beneath `src/human_auth/`. The store
binds each renewable access-and-refresh credential to the exact API URL, issuer,
audience, and public client ID that issued it. It rejects symbolic links, unexpected
ownership or modes, malformed schemas, duplicate fingerprints, and oversized tokens;
serializes file access with a bounded inter-process lock; and atomically replaces files
using user-private modes. Refresh, replacement login, rejection cleanup, and logout use
an additional lock derived from the exact deployment fingerprint.

Human login uses OAuth Device Authorization so the browser may run on a different
machine from the CLI; the CLI does not require an inbound connection or loopback
callback. It displays the short-lived activation URL and user code, keeps the private
device code and OAuth tokens out of command output and logs, requests `offline_access`,
and polls the authorization server until the transaction finishes. Polling honors the
server interval, slows down when directed, stops at the transaction deadline, and remains
interruptible while waiting. A successful exchange must include a refresh token.

After OAuth login, the CLI asks the public API whether the identity is linked to a
principal. The successful response is an envelope containing the base principal and
optional actions. Authentication status preserves complete action values as opaque JSON
without validating their IDs, kinds, origins, fields, or command-shaped content. It
never retrieves a guide or executes an action. The CLI persists the short-lived access
token, expiration, and refresh token atomically before confirmation so a temporary
API failure does not require another browser flow. Login alone never creates a principal.
An onboarding agent may invoke the separate signup command only after reporting that
signup is required and obtaining explicit human
approval. `scherzo-cloud account signup` uses the existing human credential, creates
one opaque idempotency key per invocation, and retries an ambiguous transport failure
once with that same key. It reports an authenticated principal only from the signup
response and never begins another device authorization transaction.

Human-only operations, including login, logout, signup, service-principal creation,
account-deletion cancellation, and organization-deletion request or cancellation, use
that same human-session acquisition path and no other identity source. Service-capable
operations select either that path or an explicit per-invocation service API-key file;
they never infer service credentials from the environment or fall back from a rejected
service credential to the human store. The human path silently refreshes expired or
near-expiry access tokens, refreshes once after HTTP 401, and retries the API operation
once. A refresher holds deployment-specific authority, re-reads current state,
and conditionally commits only a rotation of the token it exchanged. One ambiguous
refresh response may be retried once inside Auth0's bounded overlap. Terminal OAuth
rejection removes the matching session; transient failures preserve it. Organization creation, profile updates, membership role updates, membership removal,
and self-leave serialize their request once when applicable and retry at most one
ambiguous transport failure under one in-memory idempotency key. Reads make one attempt,
and active-member and membership-history listing return one server page without
following its opaque continuation cursor. Private not-found responses remain one
indistinguishable CLI outcome. Membership commands rely on API authorization and
last-human-owner enforcement rather than making stale client-side preflight decisions. Logout removes the local selected session and asks Auth0 to revoke its refresh
token, reporting when server revocation cannot be confirmed. These commands do not
interpret status actions; action selection and approval remain responsibilities of the
governing agent guide.

Caller-managed service credentials live beneath `src/service_auth.rs`. That boundary
accepts only canonical service keys from standard input or regular, non-symlink,
current-user-owned mode-`0600` files, bounds every read, and retains secret values in
zeroizing, redacted types. It writes show-once issued keys only to an explicitly selected
new mode-`0600` file or directly to standard output. It has no credential store,
environment discovery, refresh path, or runner-state access. Service workload identity
linking accepts a separate protected workload-token input; it never treats that token as
organization authority. The API boundary may carry redacted service-key values only for
show-once delivery, while the command boundary owns secret destination policy.

The runner uses the current `rrc_` machine credential and Cloud-issued connection URL
from protected enrollment state. `runner enroll` and `runner serve` consume one closed
operator configuration; startup validates and locks the owner-only state directory,
reads the bounded non-symlink state file, and accepts no endpoint or credential override.
Initial and replacement enrollment journals, pending staging, and promotion writes use
one kernel-held state lock. Replacement enrollment verifies the activation and Cloud
response against the protected runner ID, keeps current and pending material across
interruption, and invokes only the configured secret-free local reload operation. Runner
Serve welcomes a pending credential on a second same-boot connection before atomically
promoting it and retains its service-scoped assignment manager throughout. Startup
preserves a usable current connection, retries pending before terminal authentication,
and never discards current material merely because pending authentication or protocol
handling did not complete. Runner startup must never discover or read the human token
store. Human commands likewise must not use runner credentials to call the public API.

Sharing an executable does not permit sharing credential files, environment variables,
refresh logic, or authorization scopes accidentally.

## Execution boundary

The runner service coordinates cloud assignments and supplies each run's execution
context, including its filesystem root and lifecycle. The connection adapter owns frame
transport and effect receipt only; a service-scoped assignment manager retains the
welcomed lease policy, single local assignment slot, admitted workflow, execution root,
and stable semantic decisions across reconnects. Development configuration maps exactly one
Cloud workflow ID to a contained local workflow path, while assignment payloads supply
no host path or diagnostic text.

The runner connectivity layer does not schedule workflow steps, implement retries,
manage checkpoints, or execute agents directly. The embedded execution component owns
those responsibilities. After start authorization, the service-scoped assignment manager
invokes that component's one-run boundary and translates structured events and outcomes
into the Cloud runner protocol.

The execution component is an unpublished workspace crate with a private implementation
tree and explicit facade; there is no evidence that a separately published crate or
process is necessary. All of its production code is developed within this public source
boundary.

## Workflow execution model

A run invocation resolves one workflow and supplies its required named inputs. Every
invocation produces the same internal resolved workflow, including its immutable static
source closure and digest, before execution begins. A local invocation begins from an
explicit file path and carries no Cloud source provenance. Runner Serve instead receives
an immutable workflow-definition source, primary-workspace source, and recorded branch,
then prepares one full detached clone at the pinned commit before shared admission.

A workflow contains a schema version, one dependency graph of command and agent steps,
explicit data references, and output declarations. It has no mandatory checkout,
preparation, or execution phases. Cloning a repository, using an existing working tree,
creating a Git worktree or Jujutsu workspace, installing dependencies, and other setup
are ordinary workflow-authorized steps.

The caller supplies the execution root and its lifecycle. A local invocation can lend
its current directory or request a retained engine-owned root. Runner Serve always lends
the verified checkout as a caller-owned retained root, supplies exact branch and commit
provenance through engine-owned environment variables, and installs a token-free helper
whose one-origin read authority exists only under the current execution lease. After
finalizers and capture quiesce, Runner Serve destroys that authority, verifies staged
carriers, and releases the checkout before artifact delivery or terminal acknowledgement.

Local execution and Runner Serve call the same execution component. Runner Serve adds
source preparation, assignment, lease, durable observation, and cleanup behavior around
it; the connection adapter still does not schedule steps or interpret workflow outputs.

The private npm project under
`crates/execution/src/workflow/pi-json-v1-extension/` checks the single-file PiJsonV1 result
extension and one deterministic materialization. It is not another execution component:
Rust owns invocation identity, the retained schema and authoritative validation, and
terminal workflow state. Workflow execution never invokes npm or reads this project's
`node_modules`; a materialized extension uses only the Pi-provided extension API and
TypeBox plus Node's `node:net` built-in.

## Public source isolation

The complete normal development loop must operate from this repository root without
access to a parent checkout. Formatting, linting, tests, dependency inspection, code
generation checks, and builds may use only files committed here and declared external
dependencies.

The source tree may not contain symbolic links, path dependencies outside the exported
`cli/` workspace, workspace inheritance from outside this repository, or imports of
implementation packages that are not declared workspace dependencies. Every path
dependency resolves to a declared member beneath `cli/`. `scripts/check` is the canonical
local and CI entrypoint for this invariant.

## Generated contracts

Versioned OpenAPI and runner protocol contracts define the interface with the Scherzo
Cloud control plane. Generated clients, types, and codecs needed to build this
executable will be committed here.

A normal public build consumes the committed client beneath
`crates/api/src/generated` and does not require the contract source files or generator. The client is generated only
from the customer API contract; private operator routes, operation bindings, and models
are not part of the public source. Each generated Rust file identifies OpenAPI Generator
7.22.0, the canonical customer contract path, and its digest. Monorepo tooling regenerates
the client, rejects operator bindings, and checks it for drift before the public source
is mirrored. The hosted contract at
<https://docs.scherzo.dev/openapi/public-api.yaml> describes the deployed API; the digest
in a CLI build's generated source identifies the exact contract used to generate that
build.

The generated module remains private to the handwritten API boundary so generated DTOs
do not become command or workflow domain types. Generation overlays the public contract's
typed playbook action with raw `serde_json::Value` objects in problem and successful
current-principal responses; this preserves opaque server actions without teaching the
CLI their vocabulary. Handwritten transport construction remains responsible for
redirect, timeout, retry, response-size, and secret-handling policy. The
authentication-status path translates the generated current-principal envelope and
problem DTOs into handwritten domain states before the CLI renders human or structured
output.

Organization request and response DTOs follow the same boundary. The handwritten
`crates/api/src/organizations/` module uses generated DTOs only to serialize merge patches and
decode successful API representations, then converts successes into validated
handwritten organization and membership models. It owns route-specific outcomes,
problem classification, opaque path and query construction, bounded responses, and the
mutation retry contract. Owner-only membership history, role updates, bodyless terminal
removal, and self-leave use the same boundary; generated blocking transport is never a
substitute for those command implementations. Generated blocking organization transport is not called by the
command layer. The Run API adapter similarly owns status, receipt-header, response-size,
and projection validation around generated request and response DTOs. API contract
validation reuses the crate's shared public-ID syntax validator rather than defining a
second ID policy inside the API boundary.

## Rust source shape

`cli/Cargo.toml` is both the workspace root and the sole binary package. Six unpublished
library members now occupy their final roots:

- `scherzo-cloud-support` owns shared public-ID, timing, TLS-provider, and Workflow
  contract leaves and has no internal dependency;
- `scherzo-cloud-test-support` owns the HTTP and hermetic Git fixtures and is reachable
  only through dev dependencies;
- `scherzo-cloud-runner-protocol` owns runner wire DTOs, its generated codec, and its
  embedded schema and has no internal dependency;
- `scherzo-cloud-api` owns the handwritten API and private generated client, depends on
  support in production, and uses test-support only for tests; and
- `scherzo-cloud-execution` owns execution, harness adapters, process containment,
  workflow assets, and their tests, depends on support in production, and uses
  test-support only for tests. Its test-only internal-worker example keeps package
  suites independent of a previously built root executable; and
- `scherzo-cloud-runner` owns Runner Serve, enrollment, doctor, local control,
  assignment execution, artifact delivery, and telemetry. It depends only on support,
  runner-protocol, and execution in production and uses test-support only for tests.

The binary depends on support, API, execution, and runner in production and on
test-support for tests. Its dev dependencies enable only the `test-fixtures` seams used
by root tests; production dependencies do not expose fixture constructors. Commands,
human authentication, service credential files, build identity, and exit policy remain
rooted in `src/`. There is one `scherzo-cloud` executable, Cargo continues to provide
`CARGO_BIN_EXE_scherzo-cloud`, and the archive shape is unchanged.

The remaining seams have this closed ownership matrix:

| Concern | Owner | Permitted consumption |
| --- | --- | --- |
| Execution implementation | Private modules beneath `crates/execution/src/` | Non-execution code uses only the explicit flat facade in `crates/execution/src/lib.rs`; implementation modules are not public. |
| Process implementation | Private `crates/execution/src/process.rs` module | Execution uses it internally and its facade exports exactly `ManagedProcessGroup`, `CommandRunner`, `CommandRequest`, `CommandOutput`, `CommandProbeError`, and `SystemCommandRunner` for runner consumers. |
| Runner implementation | Private modules beneath `crates/runner/src/` | Root commands and the binary helper dispatch use only the explicit flat facade; the runner package receives the root-resolved version when Runner Serve starts. |
| Build identity | `src/build_info.rs` and the crate root | Root CLI dispatch injects the resolved version into local agent dispatch and Runner Serve. Execution and runner code do not read build environment or root build policy. |
| Exit policy | `src/exit_code.rs` and the crate root | Execution returns the closed `ExecutionOutcome` domain value. Root command dispatch maps that value to the unchanged process exit statuses. |

`tests/architecture.rs` enforces the exact member and internal-edge inventories, each
member's inherited lint policy, the residual root-module graph, private generated API,
the final execution, process, runner, and idempotency ownership, and confinement of
command parsing, HTTP, WebSocket, telemetry, and terminal dependencies to their owning
packages. The dev-only
`scherzo-cloud-test-support` facade supplies the Git fixture to execution and runner tests
without entering the production graph. The `src/service_auth.rs` boundary depends only
on support's public-ID syntax and remains a root-owned policy for caller-managed service
secrets. The API returns issued secrets in
a redacted zeroizing value; the root validates the canonical service-key syntax before
delivery. Changing a package or module boundary requires updating that test and this
document in the same change.

The CLI uses a typed `clap` command tree. Each command module owns its arguments, help
metadata, and execution dispatch; parent modules compose those commands so parsing and
rendered help come from the same structure. Bare command groups may print their composed
help, but only an explicit leaf command may start long-running behavior.

Organization parsing and credential policy live in `src/cli/organization.rs`; its
`audit.rs`, `create.rs`, `show.rs`, `update.rs`, `leave.rs`, and `members.rs` children own
leaf arguments and API calls. `output.rs` exhaustively maps route-specific outcomes to
human text, schema-version-1 JSON, and process status. The command modules never expose
generated DTOs or map raw HTTP statuses independently.

`release.toml` schema 2 is a static public policy contract: initial version `0.1.0`,
development version `0.0.0-dev`, and minor impact for breaking changes before `1.0`.
It contains no next-series declaration. `scripts/release-impact` is pure over that
policy, explicit impact, and an optional latest stable version. Source validation uses
only checked-in bytes and cannot become stale when public releases move. `changes/` is a
frozen legacy archive; new append-only intent is reviewed outside this exported tree.

Native Cargo builds report the permanent `0.0.0-dev` fallback. Reproducible Nix and
release builds inject `SCHERZO_CLOUD_VERSION` and `SCHERZO_CLOUD_BUILD_IDENTITY` at
compile time, and both `scherzo-cloud version` and `scherzo-cloud --version` read the same
version. Structured version output also reports the resolved executable path and
separately injected build identity. Packaging must verify the installed executable
reports these exact values. `scripts/check-release` validates static policy and fallback
consistency. Release-only planning observes stable state after source validation and binds that
snapshot into an untagged candidate commit. Public `main` is a rolling candidate rather
than a release reservation. Managed Buildkite reads canonical source and private journal
objects as data, verifies the canonical source-evidence artifact, renders notes, and
advances only public `main` with a force-with-lease. Its ordered candidate contract binds
the exact source revision and CLI tree, journal selection, public parent, stable refs,
latest complete release, proposed version, and release notes. It creates no tag, release,
or metadata ref.

Public GitHub Actions verifies the candidate contract and compiles every target, including
macOS, before any release approval or write. The three release builds check out that exact
candidate and build x86-64 and ARM64 Linux plus Apple Silicon macOS archives. Archives
have a canonical inventory and metadata so a transient retry reproduces the same asset
bytes. Only the final job names the protected `cli-release` environment and receives
`contents: write` after a human approves its rendered notes.

Before creating an absent tag, reconciliation requires the candidate to remain current
public `main`. A pre-tag failure is repaired by advancing `main` to a corrected candidate;
no version or recovery state blocks it. Once an exact direct tag exists, reconciliation
accepts only tag-only state or a matching draft containing an unchanged subset of the four
expected assets. It creates or completes the draft, verifies archive digests and
`SHA256SUMS`, attests the four assets, and publishes. A tag at another commit, unrelated
stable-tag movement, changed notes or identity, unexpected or changed assets, a
conflicting draft, or a mismatched published release fails before a contents write. An
exact published release is a successful no-op; stable tags and published releases are
never moved or edited. Signing, notarization, installers, package-manager metadata, and
update channels remain separate decisions.

The runner and execution components should use owned state and explicit message passing
rather than shared mutable global state. Protocol DTOs must be translated into domain
types at their boundary instead of becoming the workflow model.

## Deferred decisions

The following decisions remain open:

- repository checkout and execution behavior;
- supported operating systems and service managers;
- installation, update, and release packaging; and
- whether the runner eventually warrants a dedicated executable.

Selecting any of these must preserve the public source and credential boundaries above.
