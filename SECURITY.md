# Security policy

## Reporting a vulnerability

Use GitHub's private vulnerability reporting flow for this repository:

1. Open the repository's **Security** tab.
2. Select **Report a vulnerability**.
3. Describe the affected behavior, reproduction steps, potential impact, and any known
   mitigations.

Do not open a public Discussion for a suspected vulnerability. Do not include live
credentials, customer data, or unrelated secrets in the report. Use minimal synthetic
examples whenever possible.

The project is pre-1.0 and publishes unsigned CLI archives through GitHub Releases.
Verify the attached SHA-256 checksums and GitHub build-provenance attestation before
running an archive. Security reports about release artifacts, provenance, the source
boundary, mirror contents, CI workflow, or implementation are welcome.

## Human credentials

The human credential store contains one-hour OAuth access tokens and rotating refresh
tokens without application-level encryption. Normal operation protects the
`~/.scherzo/` application home with mode `0700` and `credentials.json` with mode `0600`.
The CLI refuses unsafe ownership, permissions, symbolic links, malformed schemas, and
unbounded token values rather than silently repairing or replacing them. Human
credentials are never runner credentials.

Treat the credential file as a secret. Do not copy it into bug reports, command output,
logs, repositories, or runner configuration. Use `scherzo-cloud auth logout` to remove
the active deployment's local credential.

`auth login` requests `openid profile email offline_access`, requires both access and
refresh tokens, opens no browser, and listens on no inbound port. The private OAuth
device code and issued tokens must never be copied from memory into output or
diagnostics; only the activation URL and user code are displayed. Interrupting a pending
login stops polling and reports cancellation.

Every human-authenticated command sends a selected access token only to the exact API
deployment recorded beside it. Expired, near-expiry, or API-rejected access tokens are
renewed through one shared path. Refresh use is serialized across processes by exact
deployment fingerprint; a process re-reads after acquiring authority and atomically
replaces the access token, expiration, and rotated refresh token. OAuth and public API
requests reject redirects, use a 20-second deadline, and reject response bodies larger
than 1 MiB. Refresh retries one ambiguous response at most once within Auth0's bounded
overlap; an authenticated API request is retried at most once after renewal.

An explicit invalid, expired, or revoked refresh token removes only the matching local
session. Transport, rate-limit, and server failures preserve it. `auth logout` first
removes the selected local session and then submits its refresh token to Auth0's
revocation endpoint while retaining refresh authority. It distinguishes confirmed,
unconfirmed, and inapplicable revocation without exposing credential material.

## Organization commands

Organization commands send only the selected human OAuth access token to the exact
configured API deployment. They never discover or read runner credentials, initiate
interactive OAuth, or accept credentials or idempotency keys as command input. HTTP is rejected
unless the individual leaf explicitly opts into insecure development transport.

Create and update serialize one request and keep one random idempotency key only in
memory. They retry one ambiguous transport failure with the same request and key, but do
not retry explicit HTTP responses or claim that two ambiguous attempts failed to commit.
Show and member listing make one request; member listing follows no continuation cursor
automatically.

Organization output and diagnostics never copy bearer tokens, idempotency keys, complete
response bodies, or API problem title and detail. Private absent, inactive, and
inaccessible organizations share one `not_found` result. A contracted or malformed HTTP
401 first uses the shared bounded refresh path; a second rejection conditionally removes
only the matching renewable credential, while 403 and other failures retain it.
Successful response values are decoded through generated contract DTOs and projected
into the documented schema-version-1 output rather than printing generated debug data.

## Runner service telemetry

`runner serve` writes one newline-delimited JSON event for each gateway connection
attempt and effect transport acknowledgement. The same reviewed attributes are attached
to a local OpenTelemetry span. Remote export is absent by default and is constructed
only by `runner serve`, after its closed operator configuration and protected enrolled
state are valid.

Users may opt in to OTLP/HTTP protobuf export with a standard trace-specific or generic
OpenTelemetry endpoint environment variable. The trace-specific endpoint takes
precedence. Remote receivers require HTTPS; cleartext HTTP is limited to the exact
`localhost` host or a loopback IP. Endpoints containing user information, a query, or a
fragment are rejected. Standard OTLP header variables may carry credentials, but the
user owns both those credentials and the receiver. Scherzo supplies no telemetry
endpoint, ingestion credential, backend-specific behavior, or collector.

`OTEL_SDK_DISABLED` is the only export privacy switch. Case-insensitive `true` is a hard
veto. Unset, empty, or case-insensitive `false` allows an explicitly configured export;
any other non-empty value fails closed for remote telemetry. The veto does not disable
local JSON output, local spans, or W3C Trace Context propagation.

The allowlist contains opaque public runner, boot, effect, assignment, and run IDs;
closed outcomes and error types; the gateway host and port; protocol sequence, progress,
and bounded counters; and durations. It excludes the machine credential, authorization
header, credential path, endpoint path and query, raw protocol frames, peer close
reasons, and arbitrary network, TLS, HTTP, WebSocket, filesystem, or child-process error
text. Exported resources contain only the fixed runner service name, package version,
and generated boot ID. Environment resource attributes, host and user identity, process
arguments, filesystem paths, prompts, outputs, credentials, and raw errors are not
collected. A successful effect event means only that the gateway confirmed transport
receipt; it is not evidence of assignment acceptance or execution.

The WebSocket upgrade receives only the connection span's W3C Trace Context. Baggage and
arbitrary process context are never injected. Effect acknowledgements remain independent
root spans correlated through the reviewed domain IDs until producing trace context is
part of the runner protocol.

Telemetry is subordinate to runner work. Local records and export spans enter separate
bounded queues without waiting. OTLP requests and shutdown are bounded and there is no
application retry. Malformed configuration, queue saturation, receiver failure,
rejection, or stalling drops remote telemetry rather than changing protocol handling,
acknowledgements, backoff, terminal results, exit status, or bounded shutdown. Owned
telemetry diagnostics are emitted at most once for each closed classification and never
copy raw errors, endpoint values, header names or values, or response bodies. Help,
version, authentication, account, usage errors, invalid runner configuration, and
`runner doctor` do not initialize export or change their output contracts.

## Runner doctor

`runner doctor` is offline. It does not load human deployment configuration, read the
human credential store, contact a network service, or create persistent state. The Git
check executes the `git` resolved from the runner process's `PATH` with the fixed
`--version` argument.

When an operator explicitly selects the PiJsonV1 check, doctor searches its inherited
`PATH` in order for executable name `pi`, canonicalizes the first candidate that the
current process can execute, and invokes only the resulting absolute path for a
`--version` probe and a capability-help probe. It never falls through to a later
candidate after selecting an incompatible installation. The latter probe combines
`--help` with Pi's invocation-scoped project rejection and resource-discovery disable
flags. Both probes run from a fresh private temporary working directory with fresh home,
Pi agent, and XDG directories. Their child environment is cleared before Scherzo
restores the captured inherited `PATH` for environment-based launcher interpreter
resolution and supplies only those temporary paths, deterministic no-color controls,
and Pi's offline, no-update-check, and no-install-telemetry controls. The absolute Pi
path remains fixed throughout both probes. Temporary state is removed after validation.

The Pi validator does not request provider or model data, check authentication, execute a
workflow or caller project, install or update Pi, or substitute another executable. The
help probe verifies the one-run `--approve` flag without reading `defaultProjectTrust` or
reading or mutating the operator's `trust.json`.

The operator-selected ClaudeCodeStreamJsonV1 check applies the same first-executable,
canonical-path, no-fallback rule to executable name `claude`. Scherzo does not install,
upgrade, repair, or substitute that executable. Its probes run from a fresh private
project, home, Claude configuration, and XDG directories with a cleared child
environment, inherited `PATH` only for launcher interpreter resolution, fixed update and
nonessential-traffic disables, and deterministic no-color controls. It requires a
canonical stable version in `>=2.1.234 <2.2.0` and the closed non-model capabilities used
by the production adapter. The repository's exact qualification package remains
`2.1.234`; accepting the range does not claim exact-binary conformance for every release
or host. The validator does not read ambient `CLAUDE_CONFIG_DIR`, provider credentials, or native
settings; query a provider or model catalog; execute the caller project; install or update
Claude Code; or expose any of those values in doctor output.

The operator-selected CodexAppServerV1 check applies the same first-executable,
canonical-path, no-fallback rule to executable name `codex`. Its isolated version and
generated-schema probes use fresh private native and XDG directories, accept only stable
`>=0.147.0 <0.148.0`, and require the maintained App Server schema capability, including
the ephemeral-thread contract. The validator does not read ambient `CODEX_HOME`, provider
credentials, or
native configuration; start an App Server thread; query a provider; or expose any such
value in doctor output.

Each probe has a bounded deadline, runs in an owned process group, drains both child
output streams so a child cannot block on a full pipe, bounds retained standard output,
and terminates and reaps the group before joining those streams. Truncated output is
rejected. Reports never copy raw standard output, standard error, operating-system error
text, or process exit text. They expose only strictly parsed versions, closed capability
identifiers, and the normalized path of a compatible installation. Workflow data,
assignments, imports, and
remote values cannot supply or alter the search path. Once validation succeeds,
admission and execution retain the absolute installation identity and exact observed version and never search `PATH` again,
so a later `PATH` change cannot redirect an admitted invocation. Every native
initialization frame must report that retained version exactly; a mismatch fails through
the typed harness path before any workflow output can commit.

## Claude Code execution authority

ClaudeCodeStreamJsonV1 launches normal Claude Code with the fixed unattended
`bypassPermissions` mode and runner-owned user, project, and local setting sources. That
mode suppresses interactive permission prompts; it does not reduce native tool authority,
confine filesystem paths, filter network access, isolate processes, restrict resources,
or protect ambient secrets. Scherzo's update, nonessential-traffic, marketplace, memory,
Git-instruction, and fresh retained-session controls are deterministic profile behavior,
not a sandbox. The profile removes `CLAUDE_CODE_PROJECT_DIR_NAME` so an inherited native
path override cannot redirect the validated release away from Scherzo's fresh retained-session
links.

Local users and runner operators own the security boundary around Claude Code. They must
trust or isolate the execution root and every loaded project instruction, skill, hook,
MCP server, plugin, provider configuration, and credential. They also own filesystem,
process, network, resource, and secret policy. Scherzo does not claim that runner-owned
settings can contain a hostile same-user process, and the maintained conformance suite
runs only in fresh synthetic roots against a loopback provider without live credentials.

Each invocation drains Claude Code's bounded stderr as a generic process-diagnostic
stream, independently of stream-JSON stdout. Stderr bytes remain observable and retained;
the parser never consumes their release-specific prose, and their presence cannot alter
protocol or result authority.

Each durable local invocation retains its potentially sensitive native Claude transcript
inside the owner-private run diagnostics tree and removes its temporary ambient history
links after containment quiesces. The transcript can be incomplete or malformed and is
never workflow, result, failure, retry, or recovery authority. There is no automatic
installation or cross-harness fallback. A missing or incompatible Claude installation
fails Claude-required admission before launch while command-only and other selected work
remain independent. Removing Claude from a runner service environment and restarting
prevents future Claude admission; it is not an emergency sandbox or a way to revoke
authority from already-running native work.

## Codex execution authority

CodexAppServerV1 launches the admitted canonical executable as `codex app-server` with
unattended approval, runner-owned containment, native configuration, provider resources,
and project resources. The profile does not sandbox native tool authority, supply
credentials, translate model settings, or fall back to Pi or Claude Code. Local admission
validates and pins only a workflow-selected Codex installation; command-only, Pi-only,
and Claude-only workflows do not probe it.

Each invocation creates one fresh ephemeral thread and one owner-private transient SQLite
directory beneath private invocation staging. The directory remains outside the workflow
execution root and ambient `CODEX_HOME`, stays live through settlement and process-group
quiescence, and is then removed. Scherzo retains no native thread transcript or
Codex-native state. App Server events remain the only native workflow, result, failure,
retry, cancellation, settlement, or recovery authority.
