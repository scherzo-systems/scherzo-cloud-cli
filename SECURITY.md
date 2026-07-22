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
