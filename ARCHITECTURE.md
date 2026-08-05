# Architecture

## Current state

This repository defines the public source boundary for the Rust `scherzo-cloud`
executable. The current binary provides help, version output, deployment selection, a
secure local human credential store, OAuth Device Authorization, server-confirmed
authentication status, explicit human-principal signup, local logout, organization
profile management, one-page active member-directory reads, local Workflow V1
definition validation, and an outbound, development-only runner transport.
`scherzo-cloud runner serve` opens a versioned
WebSocket connection, durably acknowledges received assignment offers, and never claims
to execute them. `scherzo-cloud runner doctor` performs one default local Git check and
can validate an explicitly configured Pi installation for the closed `PiJsonV1` profile;
it does not claim that the runner is ready to execute assignments.

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
- the new Scherzo Cloud execution engine will implement the one-run contract,
  workflow scheduler, execution roots, checkpoints, and agent execution in Rust.

The long-running runner starts only through an explicit command such as
`scherzo-cloud runner serve`. Bare `scherzo-cloud runner` will not implicitly start a
service.

## Local workflow validation

`src/cli/workflow/validate.rs` is an offline typed Clap adapter around the shared
resolver in `src/execution/workflow/resolution.rs`. Structural validation embeds the
public `schemas/workflow-v1.schema.json` artifact; no implementation-local schema copy
exists. The adapter requires both an explicit source root and a selected workflow path,
then renders only normalized provenance, digest, step-count, required-import, and closed
diagnostic fields. It does not parse or validate workflow definitions independently.

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

The first registry entry is `environment.command.git`. It remains the sole default when
no Pi executable is configured. The second entry,
`execution.harness.pi-json-v1`, becomes a default check only when doctor receives
`--pi-executable`; operators can also select it explicitly. Doctor does not construct
human deployment state, read the human credential store, or make a network request.
Future runner bootstrap code can add compiled-in checks through the same registry
without adding a central check-name enum, but it must keep those boundaries intact.

The Pi validator lives at the execution boundary and is shared by doctor and optional
agent-capable Runner Serve initialization. It canonicalizes the configured path, invokes
only that absolute executable's native version and help probes, and maps canonical
stable versions in `>=0.83.0 <0.84.0` plus their required non-model flags into an
immutable `ValidatedPiInstallation`. The value carries the absolute path, ordered
numeric and exact observed version, closed profile enum, and closed capability set.
Neither run admission nor later execution
needs executable discovery or native probing. Runner Serve keeps command-only startup
available when no Pi path is configured; when one is configured, startup must validate
and retain it before entering the service runtime.


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
binds each short-lived access token to the exact API URL, issuer, audience, and public
client ID that issued it. It rejects symbolic links, unexpected ownership or modes,
malformed schemas, duplicate fingerprints, and oversized tokens; serializes access with
a bounded inter-process lock; and atomically replaces files using user-private modes.
Local logout removes only the active deployment's entry and never contacts the network.

Human login uses OAuth Device Authorization so the browser may run on a different
machine from the CLI; the CLI does not require an inbound connection or loopback
callback. It displays the short-lived activation URL and user code, keeps the private
device code and OAuth tokens out of command output and logs, and polls the authorization
server until the transaction finishes. Polling honors the server interval, slows down
when directed, stops at the transaction deadline, and remains interruptible while
waiting.

After OAuth login, the CLI asks the public API whether the identity is linked to a
principal. The successful response is an envelope containing the base principal and
optional actions. Authentication status preserves complete action values as opaque JSON
without validating their IDs, kinds, origins, fields, or command-shaped content. It
never retrieves a guide or executes an action. The CLI persists the short-lived access
token before confirmation so a temporary API failure does not require another browser
flow. Login alone never creates a principal. An onboarding agent may invoke the separate
signup command only after reporting that signup is required and obtaining explicit human
approval. `scherzo-cloud account signup` uses the existing human credential, creates
one opaque idempotency key per invocation, and retries an ambiguous transport failure
once with that same key. It reports an authenticated principal only from the signup
response and never begins another device authorization transaction.

The `organization create`, `show`, `update`, and `members list` commands use that same
selected human OAuth credential boundary and no other identity source. A shared command
adapter selects the exact deployment credential and conditionally removes only the token
rejected by HTTP 401. Create and update serialize their request once and retry at most
one ambiguous transport failure under one in-memory idempotency key. Reads make one
attempt, and member listing returns one server page without following its opaque
continuation cursor. Private not-found responses remain one indistinguishable CLI
outcome. These commands do not interpret status actions; action selection and approval
remain responsibilities of the governing agent guide.

The runner uses a machine credential file supplied explicitly to `runner serve`. The
current development-only format embeds a runner ID and a 43-character base64url secret;
the loader rejects symlinks, files not owned by the current user, group/other-readable
files, malformed values, and values larger than 256 bytes. Runner startup must never
discover or read the human token store. Human commands likewise must not use runner
credentials to call the public API.

Sharing an executable does not permit sharing credential files, environment variables,
refresh logic, or authorization scopes accidentally.

## Execution boundary

The runner service coordinates cloud assignments and supplies each run's execution
context, including its filesystem root and lifecycle. It does not schedule workflow
steps, perform retries, manage checkpoints, or execute agents. Those responsibilities
belong to a new execution component implemented in this repository and initially
embedded in the Rust executable. The runner invokes its versioned one-run boundary and
translates structured events and outcomes into the cloud runner protocol.

The execution component will be organized as an internal source boundary before there
is evidence that a separately published crate or process is necessary. All of its
production code is developed within this public source boundary. The prior single-user
Scherzo daemon is behavioral and design inspiration only: the Cloud runner does not
import, embed, invoke, or communicate with it, and its APIs, storage, messages,
configuration, and runtime layout are not compatibility targets.

## Workflow execution model

A run invocation resolves one workflow and supplies its resolved imports. Workflow
resolution is a closed union: a local invocation may select an explicit file path,
while a Cloud invocation selects a registered workflow identity that the control plane
resolves to an immutable revision and digest. Both paths produce the same internal
resolved workflow before execution begins.

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

The CLI uses a typed `clap` command tree. Each command module owns its arguments, help
metadata, and execution dispatch; parent modules compose those commands so parsing and
rendered help come from the same structure. Bare command groups may print their composed
help, but only an explicit leaf command may start long-running behavior.

Organization parsing and credential policy live in `src/cli/organization.rs`; its
`create.rs`, `show.rs`, `update.rs`, and `members.rs` children own leaf arguments and API
calls. `output.rs` exhaustively maps the four route-specific outcomes to human text,
schema-version-1 JSON, and process status. The command modules never expose generated
DTOs or map raw HTTP statuses independently.

`release.toml` is the public release-intent contract. It selects the reviewed
`MAJOR.MINOR` series, immutable public tags provide stable history, and fragment
categories supply semantic impact. Patch categories retain the stable series, additions
select the adjacent minor, and breaking changes select an adjacent minor before `1.0` or
an adjacent major afterward. The highest unreleased impact is evaluated from the stable
tag, so accumulated work shares one target series. The Cargo package version remains the
matching `MAJOR.MINOR.0` fallback so source builds are coherent without pretending to
know an automatically assigned patch.

Local builds report the package version from `Cargo.toml`. Reproducible release builds
inject `SCHERZO_CLOUD_VERSION` and `SCHERZO_CLOUD_BUILD_IDENTITY` at compile time, and
both `scherzo-cloud version` and `scherzo-cloud --version` read the same version.
Structured version output also reports the resolved executable path and separately
injected build identity. Packaging must verify the installed executable reports these
exact values. `scripts/check-release` validates release-series syntax and Cargo fallback
consistency, while `scripts/release-impact` is the shared semantic policy used by
candidate validation and deterministic evidence. `scripts/plan-release` validates
synthetic public history, selects the latest tag numerically, derives the cumulative
impact, suppresses stale or non-releaseable work, and emits the digest-bound version plan
consumed and independently rechecked by GitHub Actions. The version schema does not infer
or advertise a release channel.

Public GitHub Actions builds each supported target on its native architecture and grants
write permission only to the final job after checks and builds pass. Managed mirror
commits carry the approved deterministic plan digest alongside `Source-Revision`; the
public planner independently reproduces it, while legacy commits without that trailer
remain valid until mirror-authority cutover. Release tags point directly to exact
synthetic mirror commits; automation never writes generated version commits to `main`.
Archives contain the executable, public README, and license, and ship
with aggregate checksums and GitHub provenance attestations. Signing, notarization,
installers, package-manager metadata, and update channels remain separate decisions.

The runner and execution components should use owned state and explicit message passing
rather than shared mutable global state. Protocol DTOs must be translated into domain
types at their boundary instead of becoming the workflow model.

## Deferred decisions

The following decisions remain open:

- production runner enrollment, credential rotation, and revocation;
- repository checkout and execution behavior;
- supported operating systems and service managers;
- installation, update, and release packaging; and
- whether the runner eventually warrants a dedicated executable.

Selecting any of these must preserve the public source and credential boundaries above.
