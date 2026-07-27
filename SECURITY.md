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
to a local OpenTelemetry span, but the runner configures no exporter and sends no
telemetry over the network.

The allowlist contains opaque public runner, boot, effect, assignment, and run IDs;
closed outcomes and error types; the gateway host and port; protocol sequence, progress,
and bounded counters; and durations. It excludes the machine credential, authorization
header, credential path, endpoint path and query, raw protocol frames, peer close
reasons, and arbitrary network, TLS, HTTP, WebSocket, filesystem, or child-process error
text. A successful effect event means only that the gateway confirmed transport receipt;
it is not evidence of assignment acceptance or execution.

Telemetry is subordinate to runner work. Completed records enter a bounded queue without
waiting, and a dedicated local thread owns standard error and repairs the line boundary
after a partial write. Queue saturation or a failed local write drops and counts a
record rather than changing protocol handling, retry classification, or process
shutdown. Help, version, authentication, account, and `runner doctor` commands do not
initialize this recorder or change their output contracts.

## Runner doctor

`runner doctor` is offline. It does not load human deployment configuration, read the
human credential store, contact a network service, or create persistent state. Its only
built-in probe executes the `git` resolved from the runner process's `PATH` with the
fixed `--version` argument.

The probe has a five-second deadline, drains both child output streams so a child cannot
block on a full pipe, retains at most 8 KiB of standard output, kills and waits for a
child that exceeds the deadline, and rejects truncated output. It never copies raw
standard output, standard error, operating-system error text, or process exit text into
a human report, JSON report, or diagnostic. The only command-derived value reported is
the strictly parsed and normalized numeric Git version.
