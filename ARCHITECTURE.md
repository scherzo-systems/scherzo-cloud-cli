# Architecture

## Current state

This repository defines the public source boundary for the Rust `scherzo-cloud`
executable. The current binary provides help, version output, deployment selection, a
secure local human credential store, OAuth Device Authorization, server-confirmed
authentication status, explicit human-principal signup, revoking logout, organization
profile management, one-page active member-directory reads, local Workflow V1
definition validation, and an outbound, enrolled runner transport.
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
resolver in `src/execution/workflow/resolution.rs`. The execution component embeds the
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

## Runner diagnostics

`src/cli/runner/doctor.rs` is a typed Clap adapter: it parses runner-doctor arguments
and renders human or JSON output. Its machine behavior lives separately in
`src/runner/doctor/`. That internal module owns the pass/fail report model, ordered
registry, selection rules, bounded process probe, and built-in Git check. The registry
is crate-private and accepts boxed checks from components assembled into this executable;
it is not a dynamic plugin API, does not discover libraries or scripts, and is not a
third-party extension contract.

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

Pi maps canonical stable versions in `>=0.84.2 <0.85.0` into
`ValidatedPiInstallation`. Claude Code maps canonical stable versions in
`>=2.1.234 <2.2.0` into `ValidatedClaudeCodeInstallation`; the repository separately
qualifies exact release `2.1.241`. Codex maps stable `>=0.147.0 <0.150.0`
installations with the maintained App Server schema capabilities into
`ValidatedCodexInstallation`. Each immutable value carries the absolute path, exact
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

Long-running runner machine behavior owns a recorder beneath `src/runner/`. One recorder
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
name, package version, and generated boot ID, so default and environment resource
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

These are component boundaries before they are separate packages or executables. A
second runner binary should be introduced only if platform dependencies, privilege
isolation, artifact size, or independent release cadence creates a demonstrated need.

## Credential separation

Human commands and the runner use different security identities.

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

The status, signup, organization, and runner cloud-administration commands use that same
human-session acquisition path and no other identity source. It silently refreshes
expired or near-expiry access tokens, refreshes once after HTTP 401, and retries the API
operation once. A refresher holds deployment-specific authority, re-reads current state,
and conditionally commits only a rotation of the token it exchanged. One ambiguous
refresh response may be retried once inside Auth0's bounded overlap. Terminal OAuth
rejection removes the matching session; transient failures preserve it. Create and
update serialize their request once and retry at most one ambiguous transport failure under one in-memory idempotency key. Reads make one
attempt, and member listing returns one server page without following its opaque
continuation cursor. Private not-found responses remain one indistinguishable CLI
outcome. Logout removes the local selected session and asks Auth0 to revoke its refresh
token, reporting when server revocation cannot be confirmed. These commands do not
interpret status actions; action selection and approval remain responsibilities of the
governing agent guide.

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

The execution component is organized as an internal source boundary; there is no
evidence that a separately published crate or process is necessary. All of its
production code is developed within this public source boundary. The prior single-user
Scherzo daemon is behavioral and design inspiration only: the Cloud runner does not
import, embed, invoke, or communicate with it, and its APIs, storage, messages,
configuration, and runtime layout are not compatibility targets.

## Workflow execution model

A run invocation resolves one workflow and supplies its resolved imports. Every
invocation produces the same internal resolved workflow, including its immutable static
source closure and digest, before execution begins. A local invocation currently begins
from an explicit file path. The Cloud source-selection and transfer contract remains
undefined; the current Runner Serve development slice instead maps an opaque workflow
selector to operator-configured local source. That selector is neither an architectural
workflow identity nor authoritative source provenance.

A workflow contains a schema version, one dependency graph of command and agent steps,
explicit data references, and output declarations. It has no mandatory checkout,
preparation, or execution phases. Cloning a repository, using an existing working tree,
creating a Git worktree or Jujutsu workspace, installing dependencies, and other setup
are ordinary workflow-authorized steps.

The caller supplies the execution root and its lifecycle. An on-demand runner normally
uses an empty engine-owned ephemeral root, an active local invocation can explicitly
lend its current directory, and a standalone local invocation can request a new retained
root. Workflows select the applicable behavior through ordinary inputs, environment, and
commands; the language may add engine-visible step conditions without introducing a
separate phase model.

Local execution and Runner Serve call the same execution component. Runner Serve adds
assignment, lease, durable observation, and cleanup behavior around it, but connectivity
code does not acquire source, schedule steps, or interpret workflow outputs.

The private npm project under
`src/execution/workflow/pi-json-v1-extension/` checks the single-file PiJsonV1 result
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

The source tree may not contain symbolic links, parent-relative path dependencies,
workspace inheritance from outside this repository, or imports of implementation
packages that are not declared public dependencies. `scripts/check` is the canonical
local and CI entrypoint for this invariant.

## Generated contracts

Versioned OpenAPI and runner protocol contracts define the interface with the Scherzo
Cloud control plane. Generated clients, types, and codecs needed to build this
executable will be committed here.

A normal public build consumes the committed client beneath `src/api/generated` and
does not require the contract source files or generator. Each generated Rust file
identifies OpenAPI Generator 7.22.0 and the canonical contract digest. Monorepo tooling
regenerates the client and checks it for drift before the public source is mirrored.

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
`src/api/organizations/` module uses generated DTOs only to serialize merge patches and
decode successful API representations, then converts successes into validated
handwritten organization and membership models. It owns route-specific outcomes,
problem classification, opaque path and query construction, bounded responses, and the
mutation retry contract. Generated blocking organization transport is not called by the
command layer.

## Rust source shape

The implementation begins as one Cargo package and one executable. Internal Rust modules
will separate human CLI commands, runner connectivity and assignment ownership, protocol
DTOs, and one-run execution. Additional workspace crates are not introduced until a
real compile-time dependency boundary requires them.

`tests/architecture.rs` enforces the top-level module dependency graph, the containment
of `api::generated` within the API boundary, and the confinement of command parsing,
HTTP, WebSocket, telemetry, and terminal dependencies to their owning modules. The
crate-root `src/test_support.rs` module is a `cfg(test)`-only leaf shared exclusively by
execution and runner tests for hermetic Git command construction; it is not a production
component or a general cross-component utility. Changing a module boundary requires
updating that test and this document in the same change.

The CLI uses a typed `clap` command tree. Each command module owns its arguments, help
metadata, and execution dispatch; parent modules compose those commands so parsing and
rendered help come from the same structure. Bare command groups may print their composed
help, but only an explicit leaf command may start long-running behavior.

Organization parsing and credential policy live in `src/cli/organization.rs`; its
`create.rs`, `show.rs`, `update.rs`, and `members.rs` children own leaf arguments and API
calls. `output.rs` exhaustively maps the four route-specific outcomes to human text,
schema-version-1 JSON, and process status. The command modules never expose generated
DTOs or map raw HTTP statuses independently.

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
consistency. Release-only planning may observe stable state after source validation, but
a stale observation requires fresh approval rather than a source edit.

Managed Buildkite is the sole allocator. An allocation mirror names an append-only
`refs/heads/release-allocations/<plan-digest>` orphan record whose ordered contract 2 and
`rendered-notes.md` bind the approved source revision and tree, parent public state,
stable-ref and prior-release snapshots, policy, exact version and tag, and release body.
Public GitHub Actions verifies those bytes and arithmetic without selecting a version.
Push triggers ignore the `release-allocations/**` and `release-recoveries/**` metadata
branches; only the accompanying public `main` push can start normal release execution.
Pull requests, ordinary checks, metadata-ref creation, and a recovery-mirror push remain
read-only. Provider rules permit each content-addressed metadata ref to be created once
but forbid updates and deletion. Only the final reconcile job receives `contents: write`
after exact verification and native builds pass.

The three release builds check out the original allocated mirror, including during
recovery, and build x86-64 and ARM64 Linux plus Apple Silicon macOS archives. Archives
have a canonical inventory and metadata so a transient retry reproduces the same asset
bytes. The reconcile job accepts only an absent release, an exact tag-only partial state,
or a matching draft containing an unchanged subset of the four expected assets. It
creates or completes the draft, verifies archive digests and `SHA256SUMS`, attests the
four assets, and publishes. A tag at another commit, unrelated stable-tag
movement, changed notes or identity, unexpected or changed assets, a conflicting draft,
or a mismatched published release fails before a contents write. An exact published
release is a successful no-op; stable tags and published releases are never moved or
edited.

A permanent verifier or reconciler defect is repaired without reallocating. Managed
Buildkite may mirror only the workflow, verifier, reconciler, focused allocation test,
and data fixtures named by recovery contract 1, after a separate approval. Each
content-addressed recovery record names its exact predecessor and the original allocation.
Exact `workflow_dispatch` recovery must run from current public `main`, verify the whole
chain, accept the original allocated mirror as a lowercase commit input, and build and tag
that original mirror rather than the repaired commit. Signing, notarization, installers,
package-manager metadata, and update channels remain separate decisions.

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
