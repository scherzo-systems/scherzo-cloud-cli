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
reads, local Workflow V1 definition validation, runner diagnostics, and a
development-only outbound runner transport. `runner serve` connects to an explicitly
configured runner gateway, receives and
transport-acknowledges assignment offers, and emits structured service events without
claiming that execution occurred.

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
  .scherzo/workflows/check.yaml
```

`--source-root` is required and defines the complete directory boundary for the
selected workflow YAML and all static prompts, message files, attachments, and result
schemas. The selected workflow path is interpreted within that explicit root. The CLI
does not infer the boundary from the current directory, an enclosing repository, or
the YAML file's directory.

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

## Version inspection

Use `scherzo-cloud --version` or `scherzo-cloud version` for conventional one-line
output. Use `scherzo-cloud version --json` for the schema-version-1 structured contract:

```json
{
  "schemaVersion": 1,
  "command": "scherzo-cloud",
  "version": "0.6.0",
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
tokens in `~/.scherzo-cloud/credentials.json`; this store is separate from all future
runner credentials.

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
the runner. Today the default set contains only `environment.command.git`. It executes
the `git` resolved from the runner process's `PATH`, requires a parseable version at
least `0.0.1`, and reports a pass or failure for that check. A successful result does
not mean the runner is ready to serve assignments: runner configuration, machine
identity, connectivity, and execution requirements are not implemented or checked yet.

```sh
# Run the default checks.
scherzo-cloud runner doctor

# Run a named check. Repeat --check to select more than one registered check.
scherzo-cloud runner doctor --check environment.command.git

# List IDs without running any checks.
scherzo-cloud runner doctor --list-checks

# Emit the schema-version-1 JSON report.
scherzo-cloud runner doctor --json
```

Checks are registered statically by components compiled into this executable. The
command does not load plugins, read human credentials, contact Scherzo Cloud, or change
runner configuration. It executes `git --version` with a five-second deadline, bounds
captured standard output, drains standard error without reporting it, and exposes only
a normalized numeric version in its report. The JSON report has no `ready` field.

## Runner serve

`runner serve` uses a machine credential that is completely separate from human OAuth
credentials. The credential file contains exactly one private line in the form
`rnr_<ulid>.<43-character-base64url-secret>`, must be owned by the current user, and
must not grant group or other permissions. The command does not search for, read, or
reuse `~/.scherzo-cloud/credentials.json`.

```sh
scherzo-cloud runner serve \
  --gateway-url wss://runners.example.test/v1/connect \
  --credential-file ~/.scherzo-cloud/runner.credential
```

For local development only, use a loopback `ws://` URL with the explicit opt-in:

```sh
scherzo-cloud runner serve \
  --gateway-url ws://127.0.0.1:8081/v1/connect \
  --credential-file ./runner.credential \
  --allow-insecure-http
```

The runner reconnects after transient failures with jittered backoff, replies to
WebSocket Ping controls, and uses at-least-once transport acknowledgement. Terminal
failures — a gateway transport-integrity rejection (WebSocket close status 1008) or a
rejected credential or configuration at upgrade — exit nonzero instead of retrying;
restarting the process begins a fresh boot and re-reads the credential file. It
receives at most one assignment effect at a time. Receiving an offer is not assignment
acceptance or execution; repository checkout, workflow execution, and production runner
enrollment remain unimplemented.

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
    --credential-file ~/.scherzo-cloud/runner.credential
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
`0.6`. Planning takes the highest fragment impact since the latest stable tag:
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
devenv test
```

The check verifies public-source isolation, formatting, every target and feature on the
`rust-version` declared in `Cargo.toml`, checked-in Clippy policy, unit and integration
tests, and a release build.

## License

Scherzo Cloud CLI is licensed under the Apache License 2.0. See `LICENSE`.
