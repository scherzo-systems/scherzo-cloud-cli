# Scherzo workflow examples

These Workflow V1 examples provide practical recipes and terminal states for Scherzo
Cloud presentation and execution testing.

| Workflow | Demonstrates |
| --- | --- |
| `basic.yaml` | Two parallel steps followed by a join |
| `single-step.yaml` | Minimal one-step command success |
| `sequential.yaml` | Four short stages in a strict sequence |
| `parallel-fanout.yaml` | A wider fan-out followed by a join |
| `command-dataflow.yaml` | Imported command inputs, `cwd`, path-backed outputs, downstream materialization, and exports |
| `recovery.yaml` | Successful immediate retry and command-handler repair |
| `advisory-failure.yaml` | Control-only continuation and unavailable output after an advisory failure |
| `output-streams.yaml` | stdout, stderr, a quiet step, and an unterminated line |
| `hostile-output.yaml` | Deterministic long lines, Unicode, Markdown, controls, and no final newline |
| `large-response.yaml` | A deterministic, bounded 200-line output stream |
| `long-running-output.yaml` | One progress line every second for 15 minutes |
| `expected-failure.yaml` | An intentional exit 17 with downstream blocked work |
| `finalizers.yaml` | Trigger filtering, ordinary-to-finalizer dataflow, context, finalizer outputs and exports, and advisory failure |
| `finalizer-required-failure.yaml` | Successful ordinary work converted to failure by a required finalizer |
| `agent-basic.yaml` | One low-thinking Pi agent answering basic arithmetic |
| `agent-claude-basic.yaml` | One low-effort Claude Code agent answering basic arithmetic |
| `agent-codex-basic.yaml` | One low-effort Codex agent answering basic arithmetic |
| `agent-claude-structured-result.yaml` | Claude Code producing a schema-validated result |
| `agent-claude-attachment.yaml` | Static and imported prompts and attachments delivered to Claude Code |
| `agent-claude-result-pipeline.yaml` | Validated JSON passed between two Claude Code agents |
| `agent-parallel.yaml` | Three concurrent agents followed by an agent join |
| `agent-expected-failure.yaml` | Intentional startup failure from a nonexistent model |
| `agent-invalid-result.yaml` | Intentional rejection by an unsatisfiable result schema |
| `agent-slow-cancel.yaml` | A 120-second tool call and cancellation-only finalizer |

Every command node launches `sh`. Most use only shell built-ins and `sleep`;
`command-dataflow.yaml` and `recovery.yaml` also use basic local filesystem utilities.
The examples that write files confine them to the execution root, and command inputs are
read only from the engine-provided `SCHERZO_STEP_INPUTS` directory. No command example
uses the network.

`hostile-output.yaml` and `large-response.yaml` use command steps so their exact bytes
are deterministic and free of model cost. Agent examples are reserved for agent-specific
lifecycle, protocol, result, input, and cancellation behavior.

`agent-basic.yaml`, `agent-claude-basic.yaml`, and `agent-codex-basic.yaml` are the
smallest smoke tests for their respective harnesses. The other Claude examples show
schema-validated results, static and imported input delivery, and an inferred data
dependency that passes validated JSON to a second agent. Running an agent example
requires every harness selected by that workflow and its provider credentials, contacts
the model provider, and may consume billed tokens. Model-generated output remains
probabilistic.

## Run examples with imported inputs

`command-dataflow.yaml` materializes an imported prompt and attachment collection for a
command, captures text, JSON, and file outputs from paths, and supplies all three values
to a downstream command:

```sh
scherzo-cloud workflow run \
  --source-root . \
  --execution-root "$execution_root" \
  --run-dir "$run_dir" \
  --prompt-file prompts/command-dataflow-request.md \
  --attachment text/plain attachments/aurora-brief.txt \
  --plain \
  command-dataflow.yaml
```

The Claude attachment workflow includes `attachments/aurora-brief.txt` statically and
accepts imported instructions and zero or more imported updates:

```sh
scherzo-cloud workflow run \
  --source-root . \
  --execution-root "$execution_root" \
  --run-dir "$run_dir" \
  --prompt-file prompts/claude-attachment-focus.md \
  --attachment text/plain attachments/aurora-update.txt \
  agent-claude-attachment.yaml
```

Each command must use a new run directory. The execution root must meet the selected
workflow's admission requirements.

## Intentional failures and cancellation

Several examples are deliberately disruptive:

- `expected-failure.yaml` fails an ordinary command and blocks its dependents.
- `advisory-failure.yaml` still succeeds overall, but reports a failed advisory node and
  a blocked advisory consumer whose required output was unavailable.
- `finalizers.yaml` still succeeds despite its advisory notification failure.
- `finalizer-required-failure.yaml` exits unsuccessfully after its ordinary work succeeds.
- `agent-expected-failure.yaml` selects a nonexistent model.
- `agent-invalid-result.yaml` uses contradictory numeric bounds so no structured result
  can pass validation.
- `agent-slow-cancel.yaml` asks the agent to run `sleep 120`; cancel it while that tool
  call is active to observe its cancellation-only finalizer.

`recovery.yaml` also contains provisional command failures, but both nodes recover and
the workflow succeeds.

## Validate a workflow

Definition validation is offline and does not contact a model provider:

```sh
scherzo-cloud workflow validate \
  --source-root . \
  agent-codex-basic.yaml
```

Every YAML file in this directory is expected to validate. Definition-rejection cases
remain test fixtures rather than public runnable examples.

## Run and inspect a workflow

From this directory, with `scherzo-cloud` on `PATH`, the following example validates and
runs a command-only workflow, emits the terminal result as JSON, then reads the retained
run through both status and archived-view commands:

```sh
(
  set -eu
  workflow=parallel-fanout.yaml
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/scherzo-example.XXXXXX")
  trap 'rm -rf -- "$tmp_dir"' EXIT HUP INT TERM

  execution_root="$tmp_dir/execution"
  run_dir="$tmp_dir/results/${workflow%.yaml}"
  mkdir "$execution_root" "$tmp_dir/results"

  scherzo-cloud workflow validate \
    --source-root . \
    "$workflow"

  scherzo-cloud workflow run \
    --source-root . \
    --execution-root "$execution_root" \
    --run-dir "$run_dir" \
    --max-parallel 4 \
    --json \
    "$workflow"

  scherzo-cloud workflow status "$run_dir" --json
  scherzo-cloud workflow view "$run_dir" --plain
)
```

`workflow run` refuses to overwrite an existing run directory. To retry a retained run
whose latest attempt failed or was cancelled, first use `workflow status` to confirm
that it is eligible, then run:

```sh
scherzo-cloud workflow retry \
  "$run_dir" \
  --execution-root "$execution_root" \
  --json
```

Retry uses the retained workflow definition and imported values. A successful run is not
retry-eligible. See the main [`cli/README.md`](../../README.md) for the complete command,
retention, and output contracts.
