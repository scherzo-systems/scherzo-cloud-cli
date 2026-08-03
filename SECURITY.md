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

The initial human credential store contains short-lived OAuth access tokens without
application-level encryption. Normal operation protects `~/.scherzo-cloud` with mode
`0700` and `credentials.json` with mode `0600`. The CLI refuses unsafe ownership,
permissions, symbolic links, malformed schemas, and unbounded token values rather than
silently repairing or replacing them. Human credentials are never runner credentials.

Treat the credential file as a secret. Do not copy it into bug reports, command output,
logs, repositories, or runner configuration. Use `scherzo-cloud auth logout` to remove
the active deployment's local credential.

`auth login` requests only `openid profile email`, stores no refresh token, opens no
browser, and listens on no inbound port. The private OAuth device code and issued tokens
must never be copied from memory into output or diagnostics; only the activation URL and
user code are displayed. Interrupting a pending login stops polling and reports
cancellation.

`auth status` sends a selected access token only to the exact API deployment recorded
beside it. OAuth and public API requests reject redirects, use a 20-second deadline,
disable general retries, and reject response bodies larger than 1 MiB. A `401` response
removes the rejected credential without deleting a token that another process replaced
while the request was in flight.

## Organization commands

Organization commands send only the selected human OAuth access token to the exact
configured API deployment. They never discover or read runner credentials, initiate
OAuth, or accept credentials or idempotency keys as command input. HTTP is rejected
unless the individual leaf explicitly opts into insecure development transport.

Create and update serialize one request and keep one random idempotency key only in
memory. They retry one ambiguous transport failure with the same request and key, but do
not retry explicit HTTP responses or claim that two ambiguous attempts failed to commit.
Show and member listing make one request; member listing follows no continuation cursor
automatically.

Organization output and diagnostics never copy bearer tokens, idempotency keys, complete
response bodies, or API problem title and detail. Private absent, inactive, and
inaccessible organizations share one `not_found` result. A contracted or malformed HTTP
401 conditionally removes only the matching token, while 403 and other failures retain
it. Successful response values are decoded through generated contract DTOs and projected
into the documented schema-version-1 output rather than printing generated debug data.

## Runner service telemetry

`runner serve` writes one newline-delimited JSON event for each gateway connection
attempt and effect transport acknowledgement. The same reviewed attributes are attached
to a local OpenTelemetry span. Remote export is absent by default and is constructed
only by `runner serve`, after its gateway configuration and machine credential are
valid.

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

When an operator supplies `--pi-executable`, the PiJsonV1 check canonicalizes that
configured path without searching `PATH`, then invokes only the resulting absolute path
for a `--version` probe and a capability-help probe. The latter combines `--help` with
Pi's invocation-scoped project rejection and resource-discovery disable flags. Both
probes run from a fresh private temporary working directory with fresh home, Pi agent,
and XDG directories. Their child environment is cleared before Scherzo supplies only
those paths, deterministic no-color controls, and Pi's offline, no-update-check, and
no-install-telemetry controls. Temporary state is removed after validation.

The validator does not request provider or model data, check authentication, execute a
workflow or caller project, install or update Pi, or substitute another executable. The
help probe verifies the one-run `--approve` flag without reading `defaultProjectTrust` or
reading or mutating the operator's `trust.json`.

Each probe has a five-second deadline, runs in an owned process group, drains both child
output streams so a child cannot block on a full pipe, bounds retained standard output,
and terminates and reaps the group before joining those streams. Truncated output is
rejected. Reports never copy raw standard output, standard error, operating-system error
text, or process exit text. They expose only strictly parsed versions, closed capability
identifiers, and the normalized path of a compatible installation.
