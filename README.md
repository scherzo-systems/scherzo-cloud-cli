# Scherzo Cloud CLI

> [!IMPORTANT]
> This repository is a read-only mirror. Public Discussions are welcome, but pull
> requests cannot be merged into the mirror directly.

This repository contains the open-source source for the early `scherzo-cloud`
executable.

## Current capabilities

The current release supports help, version inspection, OAuth Device Authorization,
server-confirmed human authentication status, explicit human-principal signup, local
human-credential logout, organization profile management and one-page member-directory
reads, local Workflow V1 definition validation, portable artifact-set validation, mixed
command and agent execution, durable run status inspection, runner diagnostics, and a
development-only outbound runner transport. `runner serve` connects
to an explicitly configured runner gateway, transport-acknowledges assignment effects,
and resolves and admits one configured local command workflow without claiming that
execution occurred.

The CLI can create an organization, read or update its initial profile, and list one
page of active members. It cannot configure repositories, invite or change members,
submit workflows, or execute runner assignments. Agent-guided organization creation
and the rest of Cloud onboarding are not implemented yet.

## Local workflow validation

Use `scherzo-cloud workflow validate` to resolve a checked-out Workflow V1 bundle
without running it:

```sh
scherzo-cloud workflow validate \
  --source-root ./my-repository \
  ./my-repository/.scherzo/workflows/check.yaml
```

`--source-root` is required and defines the complete directory boundary for the
selected workflow YAML and all static prompts, message files, attachments, and result
schemas. The workflow file is an ordinary host path resolved from the process's initial
working directory, then canonicalized and required to remain within that explicit root.
The CLI does not infer the boundary from an enclosing repository or the YAML file's
directory.

The source distribution publishes the self-contained Workflow V1 JSON Schema at
[`schemas/workflow-v1.schema.json`](schemas/workflow-v1.schema.json). Configure a JSON
Schema-aware YAML editor or validation tool to use that checked-out file for Workflow
V1 documents; every schema reference resolves within the file, so normal use does not
need the private monorepo contract tree. Do not add a `$schema` property to the workflow
document.

The schema checks document structure. `workflow validate` additionally checks the step
graph, references, path containment, static files, types, and policy. Passing a schema
check alone therefore does not establish that the CLI will accept the complete workflow
bundle.

A successful human result reports the normalized source-root-relative workflow path,
the SHA-256 digest of the resolved source closure, step count, and required optional
imports. It never prints static file contents. Add `--json` for one schema-version-1
result with `valid` or `invalid` as its closed `outcome`; invalid results contain one
bounded CLI-owned diagnostic rather than parser or schema-library error text.

This command performs definition resolution only. It does not admit or start a run,
execute command or agent steps, check harness or model availability, read human or
runner credentials, or contact Scherzo Cloud. A zero exit status means only that the
local definition resolved successfully.

## Portable artifact validation

Validate one copied or downloaded Artifact Set V1 directory without its original run,
workflow source, or execution checkout:

```sh
scherzo-cloud artifact validate ./downloaded-attempt-result
```

The command validates the complete closed `result.json` contract and every declared
export. It checks the exact root and `exports/` inventory, aliases, portable carrier
paths, confined regular-file identity, sizes, SHA-256 digests, media types, UTF-8 text,
compact ordered JSON, and changed or zero-delta `git_branch` artifacts. Git validation
checks the closed semantic metadata, carrier-presence rule, bundle-v2 header and profile,
pack framing and checksum, and reconstructible object facts without a destination
repository. Unknown kinds are rejected. There is no single-export selector or repair
mode.

Validation opens the selected directory read-only, follows no symbolic link beneath its
opened root, and leaves the set unchanged. It does not read workflow-run state,
credentials, or configuration, and it does not contact Scherzo Cloud, a provider, a Git
remote, or any other network service.

Add `--json` for one Artifact Validate Result Schema 1 document. A valid result has
`outcome: "valid"`, `exitStatus: 0`, the canonical artifact-directory path, and bounded
summary counts. An invalid result has `outcome: "invalid"`, `exitStatus: 1`, and the
complete contractually bounded deterministic diagnostic sequence instead of a summary.
Human mode reports the same diagnostic codes and ordering. Command-line usage errors
return 2 and do not inspect the artifact directory.

## Local workflow execution

Use `scherzo-cloud workflow run` to execute a mixed command and PiJsonV1 agent
Workflow V1 DAG in an existing caller-owned directory and create one durable local run
directory:

```sh
scherzo-cloud workflow run \
  --source-root ./my-repository \
  --execution-root ./my-checkout \
  --run-dir ./runs/check-001 \
  --max-parallel 2 \
  ./my-repository/.scherzo/workflows/check.yaml
```

The run directory must not exist and must be disjoint from the execution root. The CLI
normalizes it from its nearest existing parent and creates any missing parent suffix.
First-workflow onboarding uses the owner-private `~/.scherzo/runs/` durable state root
by default; retained runs are application state and do not belong under `~/.config`.
The CLI retains immutable workflow and import bytes, durable closed run and attempt
state, and attempt 1's atomic result beneath
`attempts/000001/result`. The run-directory path is the local run handle. Add `--json`
for one terminal schema-version-1 object on stdout while the live presentation remains
on stderr, or `--plain` to force the line presentation on stdout.

Use `--prompt-file <PATH>` or `--prompt-file -` for an optional UTF-8 prompt and repeat
`--attachment <MEDIA_TYPE> <PATH>` for ordered immutable attachments. Imports are read
completely before execution. Workflow commands always receive closed standard input;
the CLI never forwards prompt input or terminal input to them.

Without `--plain` or `--json`, run uses the interactive terminal interface when stdin and
stdout are terminals, `TERM` is present and neither empty nor `dumb`, and stdin is not
reserved by `--prompt-file -`. Every other combination keeps the plain line stream.
Stderr terminal status does not select the interface. `NO_COLOR` disables semantic color
under `--color=auto` but does not disable interactive mode; `--color=always` overrides it.

The interface keeps a stable step selection, an inspector, and bounded per-step logs.
Use `Up`/`Down` or `j`/`k` to select a step, `Enter` and `Escape` to enter and leave its
full log, and `?` for all log navigation controls. Wide and narrow terminals use
side-by-side or stacked layouts. Resizing recomputes the layout without leaving
interactive mode; below 64 columns or 20 rows a resize notice replaces the panes while
execution, `Ctrl-C`, and eventual `q` handling continue.

`Ctrl-C` requests the existing orderly `user_request` cancellation and does not abandon
owned child work. `q` is ignored while execution, publication, cleanup, or run ownership
is active. Once the retained attempt is complete, the interface remains open for
post-run inspection until `q`; Scherzo then restores raw mode, the alternate screen, and
the cursor before invoking the shared standard plain-summary renderer. The summary and
process status are therefore the same contracts used by forced plain output. Inspect the
durable run later with `workflow status <RUN_DIR>`; retry is always explicit.

Local execution snapshots the inherited environment after resolution and removes
`SCHERZO_` variables before launching commands or agents. When the resolved workflow has
an agent step, the adapter selects the first executable `pi` in the launching process's
inherited `PATH`, validates it once for PiJsonV1, and pins its canonical absolute path for
the run. Later `PATH` changes cannot switch that executable. Command-only workflows do
not resolve or probe Pi. Workflow definitions, imports, and remote values cannot supply
an executable or alter selection. The adapter does not read Scherzo human or runner
credentials and does not contact Scherzo Cloud; an admitted agent harness may use the
provider and other host authority selected by its closed profile and inherited
environment.

Command and agent steps may declare `kind: git_branch`. Such an output is export-only in
Workflow V1 and cannot bind a downstream command input or agent message. Before any step
starts, local admission requires the execution root to be exactly one SHA-1 Git worktree
root and pins its current commit as the baseline. Successful capture requires a clean
committed head descended from that baseline. A changed head publishes one Git bundle
carrier; an unchanged head publishes the semantic zero-delta result without a carrier.
Repository remotes, destination branches, providers, credentials, and publication policy
are not workflow fields.

## Local workflow status

Inspect the durable state and retry eligibility of an existing local run without changing
it:

```sh
scherzo-cloud workflow status ./runs/check-001
```

The run-directory positional is an ordinary host path resolved from the process's
initial working directory. Retry uses the same durable handle explicitly:

```sh
scherzo-cloud workflow retry ./runs/check-001 --execution-root ./my-checkout
```

Status reads only the closed `run.json` and `state.json` contracts and an existing
regular `run.lock`; it never scans `.private`, creates the lock, acquires execution
ownership, waits for an owner, signals child work, or reads standard input. Its
non-acquiring lock query combines with two matching state revisions to distinguish an
active owner, a settled run, an abandoned nonterminal attempt, and ownership that cannot
be proven safely. An exact live process-group identity remains `ownership_unproven`; an
unlocked outstanding start action without its required process-guard registration makes
the run directory invalid rather than retry-eligible. The retry projection is eligible
or uses the first applicable closed reason in this order: `run_locked`,
`ownership_unproven`, `latest_attempt_succeeded`, then `latest_attempt_rejected`.

Without a mode option, and with `--plain`, one complete human snapshot is written to
stdout and operational diagnostics use stderr. `--json` writes exactly one ANSI-free
object conforming to
[`schemas/workflow-status-result-v1.schema.json`](schemas/workflow-status-result-v1.schema.json)
to stdout; run-directory, schema, lock-query, and unstable-snapshot failures are encoded
in that object rather than duplicated on stderr. Status has no TUI or live presentation
stream.

`--color` accepts `auto`, `always`, or `never`. It affects only semantic tokens in plain
output; `auto` requires terminal stdout, a usable `TERM`, and an unset or empty
`NO_COLOR`. JSON never contains ANSI. A complete snapshot returns 0 regardless of run
or retry disposition, an operational or output failure returns 1, and a command-line
usage error returns 2. SIGINT or SIGTERM before completed output returns 130 or 143,
respectively.

## Local workflow archived view

Inspect a successfully published terminal attempt in the read-only terminal viewer:

```sh
scherzo-cloud workflow view ./runs/check-001 [--attempt 1]
```

Omitting `--attempt` selects the current attempt from one stable durable snapshot. The
viewer requires terminal stdin, terminal stdout, and a usable `TERM`; it has no plain or
JSON fallback. It loads the immutable retained workflow and selected published result,
then displays a frozen DAG, inspector, and separate retained stdout and stderr prefixes.
It does not acquire run ownership, resume execution, retry work, or follow later state.

Use `q` to restore the terminal and return 0 without a workflow-run summary. `Ctrl-C`
and SIGTERM restore the terminal and return 130 or 143, respectively. Terminal and
durable-read failures return 1.

## Version inspection

Use `scherzo-cloud --version` or `scherzo-cloud version` for conventional one-line
output. Use `scherzo-cloud version --json` for the schema-version-1 structured contract:

```json
{
  "schemaVersion": 1,
  "command": "scherzo-cloud",
  "version": "0.10.0",
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
`--json` for the schema-version-1 structured result. Authenticated and signup-required
results preserve any server actions as complete opaque JSON values. The CLI does not
validate action IDs or guide origins, fetch guides, infer commands, or execute actions.
Status always contacts the public API, including when no local credential exists.

Use `scherzo-cloud auth logout` to remove the human credential for the active deployment
without making a network request. Normal operation stores short-lived human access
tokens in `~/.scherzo/credentials.json`; this store is separate from workflow-run state
and all runner credentials.

Development deployments that use HTTP require `--allow-insecure-http` on the networked
leaf command: `auth login`, `auth status`, `account signup`, `organization create`,
`organization show`, `organization update`, or `organization members list`. The option
is not global and does not apply to local commands such as `auth logout`.

## Account signup

OAuth login does not implicitly create a Scherzo Cloud account. When authentication
status is `signup_required` and the deployment advertises signup, use
`scherzo-cloud account signup` after the customer explicitly approves account creation.
Add `--json` for a schema-version-1 structured result. The CLI authenticates the request
with the existing human credential and retries an ambiguous transport failure once with
the same opaque idempotency key.

## Organization management

Organization commands use only the selected human OAuth credential. They do not start a
login or signup flow and never read runner credentials. Organization references may be
an organization ID or exact slug and are passed to the deployment without local
normalization.

```sh
# Create an organization. The deployment may assign the slug when it is omitted.
scherzo-cloud organization create \
  --display-name "Acme Research" \
  --slug acme-research

# Read an accessible active organization by ID or exact slug.
scherzo-cloud organization show acme-research

# Update the display name, slug, or both. At least one option is required.
scherzo-cloud organization update acme-research \
  --display-name "Acme Labs" \
  --slug acme-labs

# Read one member-directory page. Both pagination options are optional.
scherzo-cloud organization members list acme-labs \
  --limit 50 \
  --cursor opaque-continuation
```

Add `--json` to any of these leaves for its schema-version-1 result. Member listing
returns exactly one page, preserves `nextCursor`, and does not follow it automatically;
`--limit` accepts 1 through 200.

Create and update generate a fresh opaque idempotency key per process invocation. After
an ambiguous transport failure, the CLI retries once with the same key and serialized
request. If both attempts are ambiguous, it reports `unreachable` because the mutation
result cannot be confirmed. It does not persist the key or retry a contracted HTTP
response. Do not issue a new mutation merely because an earlier result was unconfirmed.

These commands are a direct human management surface. Authentication status may carry a
server-advertised `organization.create` action, but the CLI only transports that value;
a trusted external guide owns action selection, explanation, and approval.

## Runner doctor

Use `scherzo-cloud runner doctor` to inspect the local prerequisites currently known to
the runner. The default set contains only `environment.command.git`. It executes the
`git` resolved from the runner process's `PATH`, requires a parseable version at least
`0.0.1`, and reports a pass or failure for that check. Select
`execution.harness.pi-json-v1` explicitly to check the `pi` inherited through the same
operator-controlled `PATH`. A successful result does not mean the runner is ready to
serve assignments: runner configuration, machine identity, connectivity, and other
execution requirements are not all checked yet.

```sh
# Run the default checks.
scherzo-cloud runner doctor

# Run a named check. Repeat --check to select more than one registered check.
scherzo-cloud runner doctor --check environment.command.git

# Validate the Pi installation selected from inherited PATH only.
scherzo-cloud runner doctor \
  --check execution.harness.pi-json-v1

# List IDs without running any checks.
scherzo-cloud runner doctor --list-checks

# Emit the schema-version-1 JSON report.
scherzo-cloud runner doctor --json
```

Checks are registered statically by components compiled into this executable. The
command does not load plugins, read human credentials, contact Scherzo Cloud, or change
runner configuration. It executes `git --version` with a five-second deadline, bounds
captured standard output, drains standard error without reporting it, and exposes only
a normalized numeric version in its report.

The Pi check searches inherited `PATH` in order, canonicalizes the first candidate named
`pi` that the current process can execute, and invokes that absolute path with one version
probe and one capability-help probe. The probes clear the child environment except for
the captured inherited `PATH`, fresh temporary Pi state and working-directory paths, and
the required isolation controls. Retaining `PATH` lets an environment-based launcher
resolve its interpreter without allowing another `pi` selection. It admits
canonical stable versions in the range `>=0.83.0 <0.84.0` with the JSON event,
ephemeral-session, extension, system-prompt append, and invocation-scoped `--approve`
capabilities required by `PiJsonV1`. It retains the exact observed release, never falls
through to another candidate after selection, and does not inspect model metadata or
credentials, execute the caller's project, or read or write saved Pi project-trust
decisions. Missing, unexecutable, malformed, unsupported-version, and missing-capability
outcomes have distinct report codes. The JSON report has no `ready` field.

## Runner serve

`runner serve` uses a machine credential that is completely separate from human OAuth
credentials. The credential file contains exactly one private line in the form
`rnr_<ulid>.<43-character-base64url-secret>`, must be owned by the current user, and
must not grant group or other permissions. The command does not search for, read, or
reuse `~/.scherzo/credentials.json`.

```sh
scherzo-cloud runner serve \
  --gateway-url wss://runners.example.test/v1/connect \
  --credential-file ~/.scherzo/runner.credential \
  --workflow-id wfl_01k0z6r1w8f4jy2m7q9v3x5abr \
  --workflow-source-root ./repository \
  --workflow-path .scherzo/workflows/check.yaml \
  --work-root ./runner-work
```

Runner startup selects `pi` from its inherited operator-controlled `PATH`. When present,
initialization validates it once and retains the resulting absolute path, exact observed
version in `>=0.83.0 <0.84.0`, `PiJsonV1` profile, and required non-model capabilities
for the process lifetime; admission and invocation never repeat the lookup or probes.
When `pi` is absent, Runner Serve remains available for command-only assignments. A
selected incompatible installation fails initialization rather than falling through to
another executable. Assignments and workflow data cannot influence selection.


For local development only, use a loopback `ws://` URL with the explicit opt-in:

```sh
scherzo-cloud runner serve \
  --gateway-url ws://127.0.0.1:8081/v1/connect \
  --credential-file ./runner.credential \
  --allow-insecure-http \
  --workflow-id wfl_01k0z6r1w8f4jy2m7q9v3x5abr \
  --workflow-source-root ./repository \
  --workflow-path .scherzo/workflows/check.yaml \
  --work-root ./runner-work
```

The runner reconnects after transient failures with jittered backoff, replies to
WebSocket Ping controls, and uses at-least-once transport acknowledgement. Terminal
failures — a gateway transport-integrity rejection (WebSocket close status 1008) or a
rejected credential or configuration at upgrade — exit nonzero instead of retrying;
restarting the process begins a fresh boot and re-reads the credential file. The source
and work roots must be existing, nonoverlapping directories, and the workflow path must
remain within its source root. The service maps only the configured workflow ID, validates
the welcomed execution-lease policy and local clock health, and reserves its one local
slot before reporting semantic acceptance. Transport receipt and semantic acceptance are
separate; execution still requires a later start effect and is not implemented by this
increment. Repository checkout and production runner enrollment also remain unimplemented.

While the service runs, standard error contains newline-delimited JSON. Each outbound
attempt completes one `runner.gateway_connection` event with safe runner and boot IDs,
server host and port, protocol progress and counts, retry classification, selected
backoff when applicable, and a closed outcome and error type. Each offered effect
completes one `runner.effect_acknowledgement` event after transport confirmation or an
earlier safe ending. Its `success` outcome means only that the gateway confirmed the
runner's transport acknowledgement; the runner does not emit `runner.run` yet. Completed
JSON records enter a bounded non-blocking queue, so a stalled standard-error consumer
cannot delay runner protocol work; saturation or output failure drops and counts a
record.

`runner serve` can also send those same reviewed spans to a user-owned OTLP/HTTP
protobuf receiver. There is no default destination: export is enabled only when
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is non-empty. The
trace-specific endpoint is the complete request URL and takes precedence; `/v1/traces`
is appended to the generic endpoint. Remote endpoints must use HTTPS. HTTP is accepted
only for the exact `localhost` host or a loopback IP address. User information, queries,
fragments, remote cleartext, and non-HTTP protocols disable export without stopping the
runner.

The following standard OpenTelemetry environment variables are supported, with the
trace-specific value taking precedence over its generic equivalent:

- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT`;
- `OTEL_EXPORTER_OTLP_TRACES_HEADERS` / `OTEL_EXPORTER_OTLP_HEADERS`, using the standard
  comma-separated `name=percent-encoded-value` form;
- `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` / `OTEL_EXPORTER_OTLP_TIMEOUT`, as an integer from
  1 through 30000 milliseconds; and
- `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` / `OTEL_EXPORTER_OTLP_PROTOCOL`, when set to
  `http/protobuf`.

For example, the receiver and its credential both remain under the operator's control:

```sh
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://telemetry.example.test/v1/traces \
OTEL_EXPORTER_OTLP_TRACES_HEADERS='authorization=Bearer%20operator-owned-token' \
  scherzo-cloud runner serve \
    --gateway-url wss://runners.example.test/v1/connect \
    --credential-file ~/.scherzo/runner.credential
```

`OTEL_SDK_DISABLED` is the sole remote-export privacy switch. Case-insensitive `true`
vetoes export even when every exporter variable is set; unset, empty, or
case-insensitive `false` permits an explicitly configured endpoint. Any other non-empty
value disables export and emits only a closed local diagnostic. The switch never
disables local JSON or W3C Trace Context propagation.

OTLP work uses a bounded, non-blocking queue, bounded requests and shutdown, and no
application retry. Receiver failures, rejection, stalls, saturation, malformed
configuration, and shutdown timeout cannot change WebSocket traffic, acknowledgements,
backoff, results, or exit status. Diagnostics are emitted at most once per closed
classification and contain no endpoint, header, response body, or raw exporter error.
Local JSON continues alongside export.

The WebSocket upgrade carries only the connection span's W3C `traceparent` (and a
non-empty `tracestate`, if one exists), never baggage. The gateway can extract it as the
session's remote parent. Effect acknowledgement spans remain independent roots and use
the existing runner, boot, run, assignment, and effect IDs for correlation. Exported
resource metadata contains only `service.name=scherzo-runner`, the package version, and
the generated boot ID as `service.instance.id`; arbitrary resource environment
attributes are not imported.

Service events never contain the machine credential, endpoint path or query, raw
protocol frames, WebSocket close reasons, or arbitrary network errors. OTLP credentials
are supplied only by the user through the standard header variables; the runner ships
no Scherzo-owned endpoint, ingestion credential, Honeycomb behavior, or collector
requirement. Human and JSON output contracts for help, version, authentication, account,
and `runner doctor` remain unchanged and do not initialize runner telemetry.

## Release series

`release.toml` declares the reviewed `MAJOR.MINOR` release series. The current series is
`0.11`. Planning takes the highest fragment impact since the latest stable tag:
`internal`, `fixed`, and compatible `changed` produce a patch; `added` produces a minor;
and `breaking` produces a minor before `1.0` or a major afterward. The Cargo package
fallback remains the selected `MAJOR.MINOR.0`, while release builds inject the exact
planned version.

Run `./scripts/check-release` to validate the declaration and Cargo fallback. Running
`./scripts/plan-release` in a synthetic public mirror checkout binds the stable release,
impact, exact proposed version and tag, source revision, and rendered-changelog digest
into one deterministic plan. A series already advanced for an accumulated unreleased
minor or major remains on that adjacent series.

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
./scripts/strict-devenv test
```

The check verifies public-source isolation, formatting, every target and feature on the
`rust-version` declared in `Cargo.toml`, checked-in Clippy policy, unit and integration
tests, and a release build.

## License

Scherzo Cloud CLI is licensed under the Apache License 2.0. See `LICENSE`.
