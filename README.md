# Scherzo Cloud CLI

> [!IMPORTANT]
> This repository is a read-only mirror. Public Discussions are welcome, but pull
> requests cannot be merged into the mirror directly.

This repository contains the open-source source for the early `scherzo-cloud`
executable.

## Current capabilities

The current release supports help, version inspection, OAuth Device Authorization,
server-confirmed human authentication status, explicit human-principal signup, renewable
human sessions and revoking logout, organization profile management and one-page member-directory
reads, local Workflow V1 definition validation, portable artifact-set validation, mixed
command and agent execution, durable run status inspection, runner diagnostics, and an
enrolled outbound runner transport. `runner serve` connects only to the Cloud-issued
endpoint retained in protected state and resolves, admits, and executes one configured
inputless Workflow V1 command, Pi, Claude Code, or mixed assignment after explicit start
authorization.

The CLI can create an organization, read or update its initial profile, and list one
page of active members. It cannot configure repositories, invite or change members, or
submit workflows. Agent-guided organization creation
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

Use `scherzo-cloud workflow run` to execute a mixed command, PiJsonV1,
ClaudeCodeStreamJsonV1, and CodexAppServerV1 agent Workflow V1 DAG in an existing
caller-owned directory and create one durable local run
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
state, and attempt 1's atomic result beneath `attempts/000001/result`. Every PiJsonV1
and ClaudeCodeStreamJsonV1 invocation receives fresh native session storage beneath its
profile directory under `attempts/<attempt>/diagnostics/`. It remains after normal
private-staging cleanup together with metadata identifying the attempt, step,
invocation, immutable profile, exact harness version, and native format. Claude sessions
are never resumed and are removed from ambient Claude history when the invocation
quiesces. Codex instead starts one fresh ephemeral thread and uses one transient
owner-private SQLite directory beneath invocation staging. App Server events remain
authoritative, the transient directory is removed after settlement, and no Codex-native
thread or file is retained. The run-directory path is the local run handle. Add `--json` for one
terminal schema-version-1 object on stdout while the live presentation remains on
stderr, or `--plain` to force the line presentation on stdout.

Retained Pi and Claude Code harness diagnostics can contain sensitive prompts, model
output, tool activity, paths, and extension or provider state. Native sessions may be
incomplete or malformed after a failure or forced stop and are diagnostic only: workflow
status, results, retry, and recovery never read them as authority. They are owner-private
on Unix, are not included in published attempt results, have no stable viewer or download
interface, and disappear when the owning run directory is removed.

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

A Claude Code profile is configured independently from Pi:

```yaml
agentProfiles:
  claude:
    harness:
      kind: claude_code
      config:
        model: claude-opus-4-1
        effort: high
```

`model` is a nonempty native Claude model string. `effort` is one of `low`, `medium`,
`high`, `xhigh`, or `max`; both fields are required and additional configuration is
rejected. Scherzo does not query a model catalog, install Claude Code, or supply a
fallback model or harness.

A Codex profile is also independent:

```yaml
agentProfiles:
  codex:
    harness:
      kind: codex
      config:
        model: gpt-5.4
        effort: xhigh
```

Both Codex values are required nonempty native strings, and additional configuration is
rejected. Scherzo does not read provider credentials during definition resolution,
installation discovery, rejection presentation, or doctor checks.

Local execution snapshots the inherited environment after resolution and removes
`SCHERZO_` variables before launching commands or agents. Other caller-provided values are
retained unless the closed harness profile fixes them. ClaudeCodeStreamJsonV1 removes
`CLAUDE_CODE_PROJECT_DIR_NAME` to keep retained-session routing authoritative and applies
its documented native controls; values such as `GH_TOKEN`, `GITHUB_TOKEN`, and Git or SSH
credential-helper configuration remain available for local repository setup and cloning.
The local caller owns that authority; Runner Serve applies a separate managed
credential-isolation policy. The CLI inspects the resolved agent steps and validates
each required harness once: `pi` for PiJsonV1, `claude` for
ClaudeCodeStreamJsonV1, and `codex` for CodexAppServerV1. Each search selects the first
executable candidate in inherited `PATH` and pins its canonical absolute path, exact
observed version, profile, and capabilities for the run. Claude Code must be a canonical
stable release in `>=2.1.234 <2.2.0`, and every native initialization frame must report
the pinned observed version exactly. Later `PATH` changes cannot switch an admitted
executable. Command-only workflows probe no harness; each single-harness workflow
requires no unrelated installation. A mixed workflow requires exactly its selected
harnesses and never substitutes or falls back between them. Workflow definitions,
imports, and remote values cannot supply an executable or alter selection. The adapter
does not read Scherzo human or runner credentials and does not contact Scherzo Cloud; an
admitted agent harness may use the provider and other host authority selected by its
closed profile and inherited environment.

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

View a successfully published terminal attempt from one stable read-only snapshot:

```sh
scherzo-cloud workflow view ./runs/check-001 [--attempt 1] [--plain | --json]
```

Omitting `--attempt` selects the current attempt from the snapshot. Without an explicit
mode, terminal stdin plus terminal stdout plus a usable `TERM` opens the existing frozen
archived TUI; every other stream arrangement prints a plain completed-attempt summary.
`--plain` and `--json` force their modes regardless of terminal capability and conflict
with each other. The command never opens `/dev/tty` or falls back after selecting the
TUI.

The plain summary contains retained attempt and execution identity, lifecycle, duration,
node disposition, direct typed causes, terminal counts, outcome, cancellation, and
finalization facts. It does not replay events or print retained stdout or stderr,
commands, exports, artifact contents, or agent observations, and it claims no ordering
between retained streams.

`--json` writes exactly one ANSI-free document conforming to
[`schemas/workflow-view-result-v1.schema.json`](schemas/workflow-view-result-v1.schema.json).
A successful view returns 0 and embeds the complete validated immutable selected result,
including when the workflow outcome is failed or cancelled. Expected acquisition and
attempt-selection failures return 1 as the schema's closed error variant on stdout
without duplicate stderr prose. Usage errors return 2. Local serialization, output, or
terminal failures return 1 and do not append a replacement document after a partial
prefix.

The loader validates the retained workflow, published result, cross-document identity,
artifact set, and retained budget before exposing either the safe archived projection or
the complete wire result. It opens only the state-recorded publication once, never
acquires execution ownership, and does not retarget if a retry advances the run.

Use `q` to restore an active TUI and return 0 without a workflow-run summary. SIGINT and
SIGTERM interrupt any mode with status 130 or 143; noninteractive rendering, writing,
and flushing remain abandonable even when stdout is full. Viewer signals never cancel a
workflow. `--color` affects only plain and TUI styling; JSON is always ANSI-free.

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

One successful login establishes a renewable human session. The CLI stores the one-hour
access token, its expiration, and a rotating refresh token in
`~/.scherzo/credentials.json`, then silently renews access for every human-authenticated
command while the refresh session remains valid. Auth0 expires refresh sessions after 30
days idle or 90 days total.

Use `scherzo-cloud auth logout` to remove the human credential for the active deployment
and ask Auth0 to revoke its refresh token. The result distinguishes confirmed and
unconfirmed server revocation; local removal still completes when Auth0 is unreachable.
The human store remains separate from workflow-run state and all runner credentials.

Development deployments that use HTTP require `--allow-insecure-http` on the networked
leaf command: `auth login`, `auth status`, `auth logout`, `account signup`,
`organization create`, `organization show`, `organization update`, or
`organization members list`. The option
is not global.

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
operator-controlled `PATH`, select `execution.harness.claude-code-stream-json-v1` to 
check `claude`, or `execution.harness.codex-app-server-v1` to check `codex` 
independently. A successful result does not mean the runner is ready to serve
assignments: runner configuration, machine identity, connectivity, and other 
execution requirements are not all checked yet.

```sh
# Run the default checks.
scherzo-cloud runner doctor

# Run a named check. Repeat --check to select more than one registered check.
scherzo-cloud runner doctor --check environment.command.git

# Validate the Pi installation selected from inherited PATH only.
scherzo-cloud runner doctor \
  --check execution.harness.pi-json-v1

# Validate Claude Code independently, or repeat --check to inspect multiple harnesses.
scherzo-cloud runner doctor \
  --check execution.harness.claude-code-stream-json-v1

# Validate Codex independently.
scherzo-cloud runner doctor \
  --check execution.harness.codex-app-server-v1

# List IDs without running any checks.
scherzo-cloud runner doctor --list-checks

# Emit the schema-version-1 JSON report.
scherzo-cloud runner doctor --json
```

A selected harness check identifies its execution profile and compatibility policy in
both report formats, including when the harness is missing or incompatible. Human
reports label an available observed version separately from a supported range or exact
required version. When a range has a repository qualification release, the report shows
that qualification version separately rather than presenting it as the only admitted
release.

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
canonical stable versions in the range `>=0.84.2 <0.85.0` with the JSON event,
custom-session-directory, extension, system-prompt append, and invocation-scoped
`--approve` capabilities required by `PiJsonV1`. It retains the exact observed release, never falls
through to another candidate after selection, and does not inspect model metadata or
credentials, execute the caller's project, or read or write saved Pi project-trust
decisions. Missing, unexecutable, malformed, unsupported-version, and missing-capability
outcomes have distinct report codes.

The Claude Code check follows the same first-candidate and immutable-path rules for
`claude`, admitting canonical stable releases in `>=2.1.234 <2.2.0`. Scherzo does not
install or upgrade that executable. Its isolated `--version` and `--help` probes require the closed stream
input/output, partial-message, subagent-forwarding, explicit-session-identity,
permission-mode, setting-source, model, effort, append-system-prompt-file, and JSON-schema
capabilities used by `ClaudeCodeStreamJsonV1`. The report contains only status, profile,
observed version, supported range, exact repository qualification version, closed
capabilities, and the selected absolute path. Qualification remains pinned to `2.1.234`
and does not claim that every admitted release or unexecuted host received exact-binary
conformance. The report never exposes environment values, credentials, or loaded Claude
settings. The JSON report has no `ready` field.

The Codex check selects `codex` independently, accepts stable `>=0.147.0 <0.148.0`, and
requires the generated App Server schema capability used by CodexAppServerV1. Its
isolated version and schema probes do not read ambient `CODEX_HOME`, provider
credentials, or native configuration. The report contains the exact observed version,
supported range, exact repository qualification version, closed capabilities, and
canonical executable without starting a thread.

## Runner serve

`runner serve` uses the protected `rrc_` credential created by `runner enroll`; it
never reads human OAuth credentials. Enrollment and service startup consume the same
closed operator configuration:

```json
{
  "schemaVersion": 1,
  "deploymentMode": "production",
  "runnerStatePath": "/var/lib/scherzo-cloud/runner-state.json",
  "controlSocketPath": "/run/scherzo-cloud/runner.sock",
  "workRoot": "/var/lib/scherzo-cloud/work"
}
```

After enrollment has committed protected state, start the service with one value:

```sh
scherzo-cloud runner serve --config /etc/scherzo/runner.json
```

Production operators should follow the private monorepo's
`docs/operations/runner-v1-deployment.md` runbook for confidential activation transfer,
service-manager shutdown allowance, control-plane cutover, drain, and recovery. That
operator runbook is intentionally outside this public CLI source mirror.

The connection URL, runner ID, and current credential come only from enrolled state.
There are no endpoint environment overrides or command-line aliases for the removed
Gateway, credential, transport-mode, workflow-mapping, source, workflow-path, and work
root options. `production` requires the Cloud-issued `wss://` connection URL.
`development` additionally permits `ws://` only for exact `localhost`, IPv4 loopback,
or `[::1]` hosts; it never permits a caller-selected endpoint. Deployment mode changes
transport security only. Every assignment supplies a pinned Cloud repository source,
and every runner materializes and verifies that source before admission.

Rotate a running host only with a replacement activation issued for its existing runner:

```sh
scherzo-cloud runner enroll \
  --replace-credential \
  --activation-file /protected/path/replacement-activation.json \
  --config /etc/scherzo/runner.json
```

The command reports Cloud enrollment and live promotion separately. It stages the new
credential as pending without replacing the current credential, then asks Runner Serve
over the configured local socket to welcome and promote it in the existing boot. An
unreachable service or a pending authentication, protocol, network, or state-write
error leaves both credentials in protected state and returns nonzero; it never restarts
the service. Rerun the same command with the same protected activation file to retry a
staged credential without enrolling a third credential. On process startup, Runner
Serve prefers a usable current connection and then retries pending promotion. Keep the
old Cloud credential active until local status shows the replacement current and
`runner credential list` shows its `lastAuthenticatedAt`; then retire it with the fixed
grace or revoke it immediately if compromise is suspected. Never print an activation
artifact or runner state while transferring or verifying it.

Runner startup selects `pi` and `claude` independently from its inherited
operator-controlled `PATH`. Install one stable Claude Code release in
`>=2.1.234 <2.2.0` in that service environment before starting a runner that should
accept Claude work; Scherzo never installs it automatically. Each successful installation
is validated once and retained as an immutable snapshot for the process lifetime: Pi
requires a version in `>=0.84.2 <0.85.0`, while Claude Code requires a version in
`>=2.1.234 <2.2.0`. Admission and invocation
never repeat either lookup or probe. A missing or incompatible installation leaves only
that harness unavailable, so Runner Serve remains available for command-only and
unrelated-harness assignments. An assignment requiring the unavailable harness is rejected
before launch, and selection never falls through to another executable. Assignments and
workflow data cannot influence selection. Each Claude execution requires every native
initialization frame to report the exact version retained at startup; a contradiction
fails closed without committing workflow output. Claude runs in the profile's fixed unattended
`bypassPermissions` mode, which is not a sandbox. The runner owner remains responsible
for filesystem, process, network, resource, and secret isolation and for trusted user,
project, and local Claude settings, instructions, skills, hooks, MCP servers, and plugins.
An operator rollout installs and validates Claude in the existing service environment,
restarts that runner, canaries one deterministic Claude assignment, stops new Claude work
on failure, and rolls back by restoring the previous service environment or runner release.

The runner reconnects after retryable network, timeout, rate-limit, Gateway restart, and
server failures with jittered backoff while retaining boot-scoped assignment state.
Credential rejection is terminal authentication; unsupported subprotocol or malformed
Cloud protocol is terminal protocol. Terminal outcomes exit nonzero without exposing
bearer material. The work root must be an existing directory. Every offer supplies a
pinned source that the service materializes and verifies beneath that root. The service
validates the welcomed execution-lease policy and local clock health and reserves its
one local slot before reporting semantic acceptance. Transport
receipt and semantic acceptance remain separate; execution requires a later start
effect and remains bounded by the latest received execution lease across same-boot
reconnects.

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
  scherzo-cloud runner serve --config /etc/scherzo/runner.json
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

## Release policy

`release.toml` schema 2 contains only static policy: the initial release is `0.1.0`, the
source-development version is `0.0.0-dev`, and breaking impact before `1.0` is minor.
Native Cargo builds always report the development fallback. Nix development packages
inject a revision-bearing development version, while allocated release builds inject
their exact approved version.

Run `./scripts/check-release` to validate the policy and Cargo fallback. Impact and
version arithmetic in `./scripts/release-impact` is pure over this policy, explicit
impact, and an optional latest stable version. Source checks never fetch or inspect
public tags. The files in `changes/` are a frozen legacy archive; new intent is reviewed
in the canonical private journal before this standalone tree is mirrored.

Managed publication allocates one exact version after review and mirrors an immutable
allocation record with the source. Metadata-branch creation is ignored by workflow push
triggers, and provider rules forbid updating or deleting those content-addressed refs.
GitHub Actions verifies the allocation and any separately approved recovery chain from the
accompanying public `main` push; it never chooses a version from mutable tags. The
three native builds always check out the original allocated mirror for x86-64 and ARM64
Linux and for Apple Silicon macOS. The final write-scoped job creates or
reconciles only matching tag and draft state, then publishes the exact archives,
`SHA256SUMS`, and GitHub build-provenance attestations on the
[Releases](https://github.com/scherzo-systems/scherzo-cloud-cli/releases) page. Exact
published state and repeated valid recovery are no-ops.

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
Run the canonical check from the repository root:

```sh
./scripts/check
```

The command uses `scripts/strict-devenv` to remove the caller's environment before
Devenv constructs the declared test environment. This is the same entrypoint used by CI.
The check verifies public-source isolation, formatting, every target and feature on the
`rust-version` declared in `Cargo.toml`, checked-in Clippy policy, unit and integration
tests, and a release build.

## License

Scherzo Cloud CLI is licensed under the Apache License 2.0. See `LICENSE`.
