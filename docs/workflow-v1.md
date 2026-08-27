# Workflow V1 authoring reference

Reference ID: `scherzo.workflow.v1.authoring`

Language version: `1`

Authoring guide: <https://docs.scherzo.dev/agent/workflow-authoring.md>

Full language reference: <https://docs.scherzo.dev/reference/workflow-v1.md>

Raw schema: <https://docs.scherzo.dev/schemas/workflow-v1.schema.json>

This is the concise, version-aligned authoring reference embedded in the installed
`scherzo-cloud` executable. It covers Workflow V1 authoring and definition validation;
it is not a runbook for executing workflows or changing a repository.

## Authoring loop

1. **Inspect read-only.** Find the repository root, applicable instruction files,
   source-control state, existing workflow conventions and assets, the supported CLI,
   and the exact workflow source root and YAML path.
2. **Design the definition.** Identify command and agent nodes, control and data edges,
   profiles, static support files, outputs, exports, failure policies, and finalizers.
3. **Propose one bounded change.** Name every file, configured harness/model/effort
   value, graph behavior, and the exact validation command. State that authoring and
   validation do not authorize execution, repository checks, or source-control changes.
4. **Author the approved files.** Recheck each destination before writing. Preserve
   collisions and partial assets. Write referenced prompts, attachments, and result
   schemas together with the YAML.
5. **Validate without running.** Invoke `workflow validate --json` with an explicit
   source root and workflow path. Treat only its structured `valid` outcome as success.
6. **Report and stop.** Report the path, outcome, graph, outputs, exports, and
   finalizers. Execution and source-control mutation require separate authorization.

## Mental model

- A definition is one closed YAML 1.2 document with `schemaVersion: 1`. Unknown fields,
  kinds, references, and enum values are invalid.
- The workflow source root is an explicit validation boundary containing the YAML and
  retained static files. It is not inferred from a checkout or the YAML directory.
- The execution root is separate. Commands, agents, working directories, and runtime
  outputs operate below it when a run is separately authorized.
- Ordinary steps form a DAG. After all ordinary steps are terminal, matching finalizers
  form a second DAG.
- References carry typed values and infer data dependencies. They do not interpolate
  text, read files implicitly, or grant authority.
- Definition validation resolves source bytes and checks the contract. It does not run
  nodes, inspect credentials, contact providers, or prove harness/model availability.

## Core document and graph

| Root field | Required | Meaning |
| --- | --- | --- |
| `schemaVersion` | yes | Integer `1`. |
| `description` | no | Human metadata with no execution effect. |
| `agentProfiles` | no | Workflow-local harness configurations. |
| `steps` | yes | At least one ordinary `cmd` or `agent` node. |
| `finalizers` | no | Nodes considered after the ordinary phase. |
| `exports` | no | Stable names selecting declared outputs. |

Identifiers use lower camel case, contain 1 through 64 ASCII letters or digits, and
match `^[a-z][A-Za-z0-9]*$`. Ordinary step and finalizer IDs share a namespace. V1
rejects duplicate or non-string keys, anchors, aliases, merge keys, custom tags, and
multiple YAML documents.

Both ordinary node kinds may declare `failurePolicy`, `dependsOn`, `cwd`, and `outputs`.
`failurePolicy` is `required` by default or may be `advisory`.

- `dependsOn` names unique ordinary steps and creates control-only ordering.
- An `outputs.<step>.<output>` reference creates a direct inferred data edge. Naming
  the same producer in `dependsOn` is valid and retains both data and control origins.
- The combined graph must be acyclic. Imports and exports do not create graph edges.
- A required failure stops new ordinary starts and fails the ordinary outcome. An
  advisory failure remains visible, permits control-only dependents, and does not fail
  the workflow by itself.
- A required node cannot consume an advisory node's output. Advisory nodes cannot be
  export sources.

A command uses a nonempty argument vector:

```yaml
steps:
  check:
    kind: cmd
    cwd: packages/api
    command:
      argv: ["./scripts/check.sh", "--unit"]
```

The vector is passed directly to the operating system. There is no shell parsing,
interpolation, glob expansion, redirection, pipeline construction, or implicit shell.
Put complex shell behavior in a repository-owned script. Commands alone accept named
`inputs`; each bound value is materialized under the run-time `SCHERZO_STEP_INPUTS`
directory with a `manifest.json`.

## Paths and references

Validation receives the source root through `--source-root`. The selected workflow path
must resolve inside it.

| Field | Base and rule |
| --- | --- |
| `agent.systemPrompt` | YAML directory; retained UTF-8 file inside the source root. |
| Message `{ file: ... }` | YAML directory; retained file inside the source root; text files are UTF-8. |
| JSON output `schema` | YAML directory; retained, self-contained Draft 2020-12 schema inside the source root. |
| Node `cwd` | Execution root; defaults to that root and cannot contain `..`. |
| Path output `path` | Execution root, not node `cwd`; cannot contain `..`. |

Static paths may use `..` only when normalization and symbolic-link resolution remain
inside the source root. Runtime paths may not use parent traversal. Static source bytes
are pinned during validation and are not reread from mutable workflow source at run time.

A reference object contains exactly one `ref` field:

```yaml
inputs:
  plan:
    ref: outputs.plan.result
```

| Reference | Type and availability |
| --- | --- |
| `imports.prompt` | Optional UTF-8 text; required at admission when referenced. |
| `imports.attachments` | Ordered attachment collection, possibly empty. |
| `outputs.<node>.<output>` | The declared output's exact type. |
| `finalization.context` | Engine-owned JSON; finalizer command input or agent attachment only. |

Ordinary nodes may reference imports and ordinary outputs. Finalizers may additionally
reference finalizer outputs and `finalization.context`. Exports accept only output
references. Text, JSON, files, attachment collections, and Git branches are distinct;
V1 performs no implicit conversion.

## Agent profiles

Agent nodes select one root profile. Inline harness definitions and fallback profiles
are invalid.

| Harness `kind` | Runtime profile | Required native config |
| --- | --- | --- |
| `pi` | PiJsonV1 | Nonempty `model`; `thinking`: `off`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max`. |
| `claude_code` | ClaudeCodeStreamJsonV1 | Nonempty `model`; `effort`: `low`, `medium`, `high`, `xhigh`, or `max`. |
| `codex` | CodexAppServerV1 | Nonempty `model` and nonempty native `effort`. |

Each config is closed. Do not guess a model, translate effort values between harnesses,
or add a fallback. Every agent node starts a fresh harness process, session, and
conversation.

```yaml
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: provider/model
        thinking: high

steps:
  review:
    kind: agent
    agent:
      profile: coding
      systemPrompt: ../prompts/review-system.md
      message:
        text:
          - file: ../prompts/review-request.md
          - ref: imports.prompt
        attachments:
          - ref: imports.attachments
    outputs:
      response:
        kind: text
        from: agent_response
```

`profile`, `systemPrompt`, and nonempty `message.text` are required. Text entries are
ordered and joined with two newlines. Attachments preserve order and type. Profile
validation checks syntax only; it does not install a harness or query a provider.

## Outputs and finalizers

| Semantic `kind` | Acquisition `from` | Producer | Required fields |
| --- | --- | --- | --- |
| `text` | `path` | command or agent | `path` |
| `text` | `agent_response` | agent | none |
| `json` | `path` | command or agent | `path`, `schema` |
| `json` | `agent_result` | agent | `schema` |
| `file` | `path` | command or agent | `path`, `mediaType` |
| `git_branch` | `workspace` | command or agent | none |

Every output contains both discriminators and matches exactly one row. A node may declare
any number of distinct paths, at most one native agent source, and at most one workspace
source. Commands cannot use native agent sources. Every JSON schema must be a
self-contained UTF-8 JSON Schema Draft 2020-12 document with this root dialect:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema"
}
```

That `$schema` belongs in the separate result-schema JSON file. Do not add a root
`$schema` property to workflow YAML; the closed workflow root rejects it.

`exports` gives stable names to outputs without conversion. Export sources must be
required nodes. A finalizer export also requires `succeeded` in that finalizer's trigger
set.

Finalizers replace ordinary `dependsOn` with:

- `when`: a nonempty unique set of `succeeded`, `failed`, and/or `cancelled`; omission
  selects all three ordinary outcomes.
- `after`: unique finalizer IDs that must become terminal first.

Finalizer output references infer edges within the finalizer DAG. A consumer's `when`
set must be a subset of a referenced finalizer producer's set. A required finalizer
issue turns an ordinary success into failure; advisory finalizer issues stay visible but
do not replace the ordinary outcome.

## Complete example

This definition demonstrates direct command vectors, an inferred file-data edge, an
export, and an all-outcome finalizer. Command programs and runtime output files are
checked only when a separately authorized run uses them.

<!-- workflow-reference-fixture:installed-workflow:begin -->
```yaml
# yaml-language-server: $schema=https://docs.scherzo.dev/schemas/workflow-v1.schema.json
schemaVersion: 1
description: Build and summarize a report, then always clean up.

steps:
  build:
    kind: cmd
    command:
      argv: ["./scripts/build.sh"]
    outputs:
      report:
        kind: file
        from: path
        path: artifacts/report.json
        mediaType: application/json

  summarize:
    kind: cmd
    inputs:
      report:
        ref: outputs.build.report
    command:
      argv: ["./scripts/summarize.sh"]
    outputs:
      summary:
        kind: text
        from: path
        path: artifacts/summary.txt

finalizers:
  cleanup:
    kind: cmd
    when: [succeeded, failed, cancelled]
    inputs:
      context:
        ref: finalization.context
    command:
      argv: ["./scripts/cleanup.sh"]

exports:
  summary:
    ref: outputs.summarize.summary
```
<!-- workflow-reference-fixture:installed-workflow:end -->

The `build -> summarize` edge is inferred from the output reference. Adding
`dependsOn: [build]` to `summarize` would be valid but unnecessary because the data edge
already requires `build` to succeed. `cleanup` is required by default and is considered
after every ordinary outcome.

## Safety boundaries

- Begin with read-only inspection. Do not activate an untrusted repository environment
  merely to inspect it.
- Preserve existing, partial, and unrelated files. Show collisions and obtain approval
  before replacing content.
- Authoring files, validating a definition, running a workflow, executing repository
  checks, installing tools, using the network, and mutating source control are separate
  authority boundaries.
- Never inspect or reproduce provider credentials. A valid definition proves neither
  credential readiness nor harness/model availability.
- Pass commands and paths through an argument-vector process interface. Never
  interpolate repository text or paths into shell source.
- Inputs and profiles are data, not permission to access files, processes, networks,
  tools, secrets, credentials, or repositories.

Workflow V1 has no workflow-declared retry, timeout, condition, loop, matrix, fan-out,
concurrency, secret, environment, or workspace field.

## Schema retrieval and authoritative validation

Print the installed raw structural contract, unchanged and without network access:

```sh
scherzo-cloud workflow schema
```

The schema checks document structure only. Validate a complete materialized source
bundle with the installed CLI:

```sh
scherzo-cloud workflow validate \
  --source-root <ROOT> \
  <WORKFLOW_FILE> \
  --json
```

`workflow validate --json` is authoritative for graph, type, reference, path, policy,
and static-source validation. It returns one structured result. Accept the definition
only when the process succeeds and the closed `outcome` is `valid`. Validation does not
execute commands or agents, inspect credentials, check model access, or contact Scherzo
Cloud.

For the complete online contract, use the public
[authoring guide](https://docs.scherzo.dev/agent/workflow-authoring.md),
[language reference](https://docs.scherzo.dev/reference/workflow-v1.md), and
[raw schema](https://docs.scherzo.dev/schemas/workflow-v1.schema.json).
