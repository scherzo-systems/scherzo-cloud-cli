# CLI output style

Conventions for every human-facing string the `scherzo-cloud` CLI prints: help
text, errors, warnings, progress, reports, and prompts. Machine surfaces (JSON
documents, schema files, protocol messages) are governed by their contracts,
not this guide.

## Voice

Register follows the **subject** of the message:

- **Second person when the subject is the user** — identity, account, sign-in
  state, permissions. Contractions are allowed here.
  - `! You're not signed in to Scherzo Cloud.`
- **Neutral voice when the subject is a thing** — workflows, runs, artifacts,
  checks, the runner. No contractions.
  - `state: succeeded`, `Git 2.54.0 is available.`
- **Remedies are imperative** in either register: `Sign in first.`,
  `Start a new run instead.`

Banned padding: "sorry", "unexpectedly", "Oops", and "failed to"/"could not"
chains. Use "cannot" only for invariant refusals (a thing that will never
work), plain fragments for circumstantial failures (a thing that might work
next time).

## Errors

An error has two parts: a **diagnostic line** and an optional **remedy block**.

```
error: cannot retry run: attempt 1 succeeded

A succeeded run cannot be retried. Start a new run instead:
  scherzo-cloud workflow run --run-dir <NEW_DIR> <WORKFLOW>
```

Diagnostic line rules:

- The prefix is lowercase `error:` — matching clap's usage errors, which
  appear in the same sessions and cannot be restyled.
- The message is a fragment: lowercase, no trailing period. Errors usually end
  in paths or identifiers that users copy; a period would corrupt them.
- Causes chain inline with colons, outermost first, curated to about three
  links. Each link is a bare verb phrase naming the attempted operation —
  `error: read runner credential file /path/cred.json: no such file or
  directory`. The `error:` prefix announces failure once; links never repeat
  "failed to".
- Platform leaves (OS error strings) render verbatim, capitalization and all.
- Name the thing that failed: path, endpoint, file, identifier. A diagnostic
  the user cannot act on ("invalid runner credential file" — which file?) is a
  bug.

Remedy block rules:

- Separated from the diagnostic by one blank line.
- Full imperative sentences. Commands go on their own indented line:

```
! You must sign in before managing Scherzo Cloud organizations.

Run:
  scherzo-cloud auth login
```

- Every dead end gets a remedy or an explicit statement that none exists.
- Internal codes and stage names never appear mid-sentence; a machine code may
  appear only as a labeled data field (`code: source_unavailable`).

Before and after:

| Before | After |
|---|---|
| `Error: workflow retry rejected: latest_attempt_succeeded at attempt 1: A succeeded run cannot be retried; create a new run.` | the diagnostic + remedy split shown above |
| `artifact_directory_unavailable: The artifact directory is unavailable. (artifact directory)` | `error: artifact directory unavailable: /path/no-such-artifacts` |
| `Sign-in failed during existing_credential_check: unreachable (connection).` | `error: sign in: connect to https://api.scherzo.dev: connection refused` |

## Streams and exit codes

**stdout carries the product; stderr carries commentary about producing it.**

- Reports, state dumps, JSON, and execution logs go to stdout. A
  `workflow run` step log is the product of running a workflow. The answer
  `auth status` prints is the status you asked for, even when the answer is
  "not signed in".
- Errors, warnings, and transient progress ("Connecting…") go to stderr.
- Exit codes follow the registry below; the verdict never rides only on prose.

| Code | Registry name | Meaning |
|---:|---|---|
| 0 | `Success` | The command completed successfully. |
| 1 | `GeneralFailure` | The command failed without a more specific registered class. |
| 2 | `UsageError` | The command line is invalid; this is clap's usage-error code. |
| 3 | `AuthenticationRequired` | The command requires a signed-in identity. |
| 4 | `Unavailable` | Scherzo Cloud is unreachable or the request is temporarily rate limited. |
| 130 | `Interrupted` | The command was interrupted by the user. |
| 143 | `Terminated` | The command received a termination request. |

CLI code uses `exit_code::ExitCode` rather than numeric process-exit literals.

## Reports

The report template: **verdict line → data fields → blank line → remedy —
and nothing after the remedy.** No trailing reassurances.

```
✗ Workflow definition is invalid.
workflow: empty-command-program.yaml
code: invalid_workflow_structure
location: workflow

Correct the workflow fields to match the workflow contract.
```

- Data keys are lowercase (`workflow:`, `state:`, `attempt:`), as are table
  headers and section rules (`── summary ──`). Capitalization is reserved for
  sentences: verdicts and remedies.
- Symbols: `✓`/`✗` for verdicts, `!` for attention, `·` as inline field
  separator, `──` for section rules. UTF-8 terminals are assumed.

## Terminology

One name per concept, used everywhere:

| Term | Meaning |
|---|---|
| run | the durable unit of workflow work (lives in a run directory) |
| attempt | one execution of a run |
| workflow definition | the workflow YAML (never "bundle") |
| run directory | where a run records its attempts |
| sign in / sign out | the prose verbs; the commands stay `login`/`logout` |

Banned from human prose:

- **"durable"** — architecture, not user vocabulary. Explain once in long help
  that a run directory records every attempt so runs can be inspected and
  retried.
- **Version markers** — "Workflow V1", "Artifact Set V1", "schema-version-1",
  "Schema 1". They belong to machine surfaces (`schemaVersion: 1`, JSON
  documents, schema files). `✗ Workflow V1 definition is invalid.` becomes
  `✗ Workflow definition is invalid.`
- **snake_case identifiers mid-sentence** — codes are data fields only.

## Help text

- Summaries are verb-first, sentence case, no trailing period, and state
  **purpose only**. Cardinality and contract bounds live in flags and long
  help: `List one page of organization members` becomes
  `List organization members`.
- Command verbs: **show** prints one thing; **list** prints a collection;
  **view** is interactive; **validate**/**run**/**retry** are literal.
  "Inspect" is retired — it hedges between show and view.
- Family summaries name the domain, not a verb list (the Commands list below
  already enumerates): `Validate, run, retry, and inspect local Workflow V1
  definitions` becomes `Work with local workflow definitions and runs`.
- Argument descriptions are **role-first**; preconditions come second or as
  behavior, never as a leading adjective:
  `Nonexistent durable directory for exactly one workflow run` becomes
  `Directory to create for this run (must not already exist)`.
- Shared flags keep word-identical descriptions across commands.
- Value placeholders are semantic nouns (`<WHEN>`, `<PATH>`); enumerations
  appear only in `[possible values:]`; sentinel values like `-` are explained
  in the description; static defaults appear only in `[default:]`, dynamic
  defaults in prose.
- Contract-grade precision (snapshot semantics, eligibility rules, mode
  selection) goes in after-help sections, in full sentences — the model is
  `workflow run`'s "Interactive mode:" section.
